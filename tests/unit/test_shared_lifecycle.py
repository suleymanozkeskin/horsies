"""The lifecycle writes of a shared async child, on fake async connections.

Each test drives the async pre-start transition or the finalizing marker
through a fake pool, connection and cursor that record what the child sends.
The terminalization commands are replaced at ``apply_psycopg_async``; their
own statements are covered by the integration tests.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.lifecycle.commands import (
    AbandonOwnedNode,
    CancelOwnedNode,
    ExpireOwnedClaim,
)
from horsies.core.lifecycle.outcomes import (
    Applied,
    LostClaim,
    ObservedWorkflowState,
    SourceStateConflict,
)
from horsies.core.models.tasks import OutcomeCode
from horsies.core.worker import shared_lifecycle, slots, thread_runner
from horsies.core.worker.child_pool import (
    CHILD_POOL_CONNECTION_WAIT_S,
    SHARED_POOL_CONNECTION_WAIT_S,
)
from horsies.core.worker.child_runner import (
    BEGIN_SQL,
    CANCEL_REQUEST_TIMEOUT_S,
    _PRE_START_CONFIRM_ATTEMPTS,
    MARK_FINALIZING_SQL,
    RESET_PAUSED_NODE_SQL,
    SKIP_CANCELLED_NODE_SQL,
    START_WORKFLOW_NODE_SQL,
    WORKFLOW_STATUS_OF_TASK_SQL,
    set_running_params,
    set_running_sql,
)

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
# A test of a bound fails, not hangs, when the bound is gone.
OUTER_BOUND_S = 5.0


class _Row(tuple[Any, ...]):
    """A named-tuple-like row: ``status`` and index 0."""

    status: Any

    def __new__(cls, *values: Any, status: Any = None) -> _Row:
        row = super().__new__(cls, values)
        row.status = status
        return row


class _Cursor:
    """Records each statement; ``fetchone`` returns the scripted rows in order."""

    def __init__(self, rows: list[Any]) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._rows = list(rows)
        self.rowcount = 1

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.queries.append((sql, params))

    async def fetchone(self) -> Any:
        return self._rows.pop(0)


class _Conn:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_ = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, row_factory: Any = None) -> _Cursor:
        return self.cursor_

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class _Pool:
    """Hands out ``conns`` in order, one per ``connection()``."""

    def __init__(self, *conns: _Conn) -> None:
        self._conns = list(conns)

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[_Conn]:
        yield self._conns.pop(0)


class _BrokenPool:
    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[_Conn]:
        raise OSError('connection refused')
        yield _Conn(_Cursor([]))


def _pool(fake: _Pool | _BrokenPool) -> Any:
    """The fake as the pool type that the functions take."""
    return fake


def _confirm(pool: _Pool | _BrokenPool, *, is_workflow_task: bool = False) -> Any:
    return asyncio.run(
        shared_lifecycle.confirm_until_not_resumed(
            _pool(pool),
            'task-1',
            'worker-1',
            is_workflow_task,
            CLAIMED_AT,
        ),
    )


class TestSetRunning:
    def test_a_started_row_commits_and_runs_the_task(self) -> None:
        cursor = _Cursor([_Row('task-1')])
        conn = _Conn(cursor)

        assert _confirm(_Pool(conn)) is None

        assert cursor.queries == [
            (
                set_running_sql(False),
                set_running_params('task-1', 'worker-1', False, CLAIMED_AT),
            ),
        ]
        assert (conn.commits, conn.rollbacks) == (1, 0)

    def test_a_workflow_task_sets_its_node_running_in_the_same_transaction(
        self,
    ) -> None:
        cursor = _Cursor([_Row('task-1'), _Row(True)])
        conn = _Conn(cursor)

        assert _confirm(_Pool(conn), is_workflow_task=True) is None

        # The pool is in autocommit: the transaction opens before the update.
        assert [sql for sql, _ in cursor.queries] == [
            BEGIN_SQL,
            set_running_sql(True),
            START_WORKFLOW_NODE_SQL,
        ]
        assert conn.commits == 1

    def test_a_workflow_task_with_no_node_rolls_back(self) -> None:
        cursor = _Cursor([_Row('task-1'), _Row(False)])
        conn = _Conn(cursor)

        assert _confirm(_Pool(conn), is_workflow_task=True) == (
            False,
            '',
            'WORKFLOW_CHECK_FAILED',
        )
        assert cursor.queries[0] == (BEGIN_SQL, ())
        assert (conn.commits, conn.rollbacks) == (0, 1)

    def test_a_pool_error_is_ownership_unconfirmed(self) -> None:
        assert _confirm(_BrokenPool()) == (False, '', 'OWNERSHIP_UNCONFIRMED')

    def test_a_cancel_during_the_transition_propagates(self) -> None:
        """The deadline of the seat must end the run, not become a result."""

        class _HangingCursor(_Cursor):
            async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
                await asyncio.Event().wait()

        async def cancel_it() -> None:
            run = asyncio.ensure_future(
                shared_lifecycle.confirm_until_not_resumed(
                    _pool(_Pool(_Conn(_HangingCursor([])))),
                    'task-1',
                    'worker-1',
                    False,
                    CLAIMED_AT,
                ),
            )
            await asyncio.sleep(0.01)
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

        asyncio.run(cancel_it())

    def test_a_transition_that_hangs_ends_at_its_bound(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An UPDATE that waits on a row lock, or a server that does not
        answer, ends each attempt at its bound with OWNERSHIP_UNCONFIRMED:
        the run stops before user code, and the refusal backstop of the
        parent never fires for it."""

        class _HangingCursor(_Cursor):
            async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
                await asyncio.Event().wait()

        monkeypatch.setattr(shared_lifecycle, 'RUNNING_ATTEMPT_TIMEOUT_S', 0.05)
        pool = _Pool(*(_Conn(_HangingCursor([])) for _ in range(2)))

        async def run() -> tuple[Any, float]:
            loop = asyncio.get_running_loop()
            started = loop.time()
            result = await asyncio.wait_for(
                shared_lifecycle.confirm_until_not_resumed(
                    _pool(pool),
                    'task-1',
                    'worker-1',
                    False,
                    CLAIMED_AT,
                ),
                OUTER_BOUND_S,
            )
            return result, loop.time() - started

        result, elapsed_s = asyncio.run(run())

        assert result == (False, '', 'OWNERSHIP_UNCONFIRMED')
        assert elapsed_s < 1.0
        assert 'did not finish within' in caplog.text


class TestRefusalBound:
    """The refusal backstop of the parent follows from the bounds of the
    writes that a refusal makes in the child."""

    def test_the_writes_of_a_refusal_have_a_named_bound(self) -> None:
        attempt_s = (
            shared_lifecycle.RUNNING_ATTEMPT_TIMEOUT_S
            + shared_lifecycle.CANCELLED_WRITE_END_S
        )
        marker_s = (
            shared_lifecycle.FINALIZING_MARKER_TIMEOUT_S
            + shared_lifecycle.CANCELLED_WRITE_END_S
        )

        assert shared_lifecycle.REFUSAL_WRITES_BOUND_S == (
            _PRE_START_CONFIRM_ATTEMPTS * attempt_s + marker_s
        )
        assert shared_lifecycle.RUNNING_ATTEMPT_TIMEOUT_S == (
            SHARED_POOL_CONNECTION_WAIT_S
            + shared_lifecycle.RUNNING_STATEMENTS_TIMEOUT_S
        )

    def test_the_writes_of_a_thread_refusal_have_a_named_bound(self) -> None:
        write_s = (
            CHILD_POOL_CONNECTION_WAIT_S
            + thread_runner.REFUSAL_STATEMENTS_TIMEOUT_S
            + CANCEL_REQUEST_TIMEOUT_S
        )

        assert thread_runner.THREAD_REFUSAL_WRITES_BOUND_S == (
            (_PRE_START_CONFIRM_ATTEMPTS + 1) * write_s
        )

    def test_the_refusal_backstop_comes_after_the_writes_and_a_block_report(
        self,
    ) -> None:
        """A refusal whose writes end in time never meets the backstop, in
        either kind of shared child, and a blocked loop is reported first,
        with its culprit."""
        assert slots.SHARED_REFUSAL_BACKSTOP_S == (
            max(
                shared_lifecycle.REFUSAL_WRITES_BOUND_S,
                thread_runner.THREAD_REFUSAL_WRITES_BOUND_S,
            )
            + slots.SHARED_BACKSTOP_MARGIN_S
        )


class TestMissedStart:
    def test_an_expired_claim_ends_as_task_expired(self) -> None:
        cursor = _Cursor([None])
        conn = _Conn(cursor)
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(return_value=MagicMock(spec=Applied)),
        ) as apply:
            result = _confirm(_Pool(conn))

        assert result == (False, '', OutcomeCode.TASK_EXPIRED.value)
        command = apply.call_args.args[1]
        assert isinstance(command, ExpireOwnedClaim)
        assert command.fence.worker_id == 'worker-1'
        assert conn.commits == 1

    def test_a_lost_claim_rolls_back_and_skips_the_task(self) -> None:
        cursor = _Cursor([None])
        conn = _Conn(cursor)
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(return_value=MagicMock(spec=LostClaim)),
        ):
            result = _confirm(_Pool(conn))

        assert result == (False, '', 'CLAIM_LOST')
        assert (conn.commits, conn.rollbacks) == (0, 1)

    def test_a_paused_workflow_abandons_the_claim_and_resets_the_node(self) -> None:
        cursor = _Cursor([None, _Row(status='PAUSED')])
        conn = _Conn(cursor)
        outcomes = [MagicMock(spec=LostClaim), MagicMock(spec=Applied)]
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(side_effect=outcomes),
        ) as apply:
            result = _confirm(_Pool(conn), is_workflow_task=True)

        assert result == (False, '', 'WORKFLOW_STOPPED')
        abandon = apply.call_args_list[1].args[1]
        assert isinstance(abandon, AbandonOwnedNode)
        assert abandon.fence.claimed_at == CLAIMED_AT
        assert [sql for sql, _ in cursor.queries][-2:] == [
            WORKFLOW_STATUS_OF_TASK_SQL,
            RESET_PAUSED_NODE_SQL,
        ]
        assert conn.commits == 1

    def test_a_cancelled_workflow_cancels_the_task_and_skips_the_node(self) -> None:
        cursor = _Cursor([None, _Row(status='CANCELLED')])
        conn = _Conn(cursor)
        outcomes = [MagicMock(spec=LostClaim), MagicMock(spec=Applied)]
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(side_effect=outcomes),
        ) as apply:
            result = _confirm(_Pool(conn), is_workflow_task=True)

        assert result == (False, '', 'WORKFLOW_STOPPED')
        cancel = apply.call_args_list[1].args[1]
        assert isinstance(cancel, CancelOwnedNode)
        assert cancel.accepts_requeued_pending is True
        assert cursor.queries[-1][0] == SKIP_CANCELLED_NODE_SQL

    def test_a_resume_before_the_abandon_starts_the_task_on_the_next_attempt(
        self,
    ) -> None:
        resumed = SourceStateConflict(
            task_id='task-1',
            ordinality=None,
            observed=MagicMock(),
            evidence=ObservedWorkflowState(
                workflow_id='wf-1',
                workflow_status='RUNNING',
            ),
        )
        first = _Conn(_Cursor([None, _Row(status='PAUSED')]))
        second_cursor = _Cursor([_Row('task-1'), _Row(True)])
        second = _Conn(second_cursor)
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(side_effect=[MagicMock(spec=LostClaim), resumed]),
        ):
            result = _confirm(_Pool(first, second), is_workflow_task=True)

        assert result is None
        assert (first.commits, first.rollbacks) == (0, 1)
        assert second.commits == 1

    def test_a_resume_on_every_attempt_leaves_the_claim_to_its_lease(self) -> None:
        resumed = SourceStateConflict(
            task_id='task-1',
            ordinality=None,
            observed=MagicMock(),
            evidence=ObservedWorkflowState(
                workflow_id='wf-1',
                workflow_status='RUNNING',
            ),
        )
        conns = [_Conn(_Cursor([None, _Row(status='PAUSED')])) for _ in range(2)]
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(
                side_effect=[MagicMock(spec=LostClaim), resumed] * len(conns),
            ),
        ):
            result = _confirm(_Pool(*conns), is_workflow_task=True)

        assert result == (False, '', 'OWNERSHIP_UNCONFIRMED')


class TestFinalizingMarker:
    def test_the_marker_is_fenced_on_the_claim_generation(self) -> None:
        cursor = _Cursor([])
        conn = _Conn(cursor)

        asyncio.run(
            shared_lifecycle.mark_task_finalizing(
                _pool(_Pool(conn)),
                'task-1',
                'worker-1',
                CLAIMED_AT,
            ),
        )

        assert cursor.queries == [
            (
                MARK_FINALIZING_SQL,
                ('worker-1', 'task-1', 'worker-1', CLAIMED_AT, CLAIMED_AT),
            ),
        ]
        # One statement on an autocommit connection: no transaction to end.
        assert (conn.commits, conn.rollbacks) == (0, 0)

    def test_a_failed_marker_is_logged_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        asyncio.run(
            shared_lifecycle.mark_task_finalizing(
                _pool(_BrokenPool()),
                'task-1',
                'worker-1',
                CLAIMED_AT,
            ),
        )
        assert 'Failed to mark task task-1 finalizing' in caplog.text

    def test_a_marker_that_hangs_ends_at_its_bound(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        class _HangingCursor(_Cursor):
            async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
                await asyncio.Event().wait()

        monkeypatch.setattr(shared_lifecycle, 'FINALIZING_MARKER_TIMEOUT_S', 0.05)

        async def run() -> float:
            loop = asyncio.get_running_loop()
            started = loop.time()
            await shared_lifecycle.mark_task_finalizing(
                _pool(_Pool(_Conn(_HangingCursor([])))),
                'task-1',
                'worker-1',
                CLAIMED_AT,
            )
            return loop.time() - started

        assert asyncio.run(run()) < 1.0
        assert 'Failed to mark task task-1 finalizing' in caplog.text
