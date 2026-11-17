"""The lifecycle writes of a shared async child, on fake async connections.

Each test drives the async pre-start transition or the finalizing marker
through a fake pool, connection and cursor that record what the child sends.
The terminalization commands are replaced at ``apply_psycopg_async``; their
own statements are covered by the integration tests.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import logging
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
    START_ATTEMPT_SQL,
    mark_finalizing_params,
    start_attempt_params,
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


class _OrderedConn(_Conn):
    """Records each commit and rollback among the statements of its cursor,
    so a test sees where a transaction ends."""

    async def commit(self) -> None:
        await super().commit()
        self.cursor_.queries.append(('COMMIT', ()))

    async def rollback(self) -> None:
        await super().rollback()
        self.cursor_.queries.append(('ROLLBACK', ()))


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


def _apply_recording(outcomes: list[Any]) -> AsyncMock:
    """An ``apply_psycopg_async`` that records each command by its class name
    among the statements of the cursor, and returns ``outcomes`` in order."""

    async def apply(cursor: _Cursor, command: object) -> Any:
        cursor.queries.append((type(command).__name__, ()))
        return outcomes.pop(0)

    return AsyncMock(side_effect=apply)


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


_STARTED = ('STARTED', None)


def _missed(reason: str) -> tuple[str, str]:
    return ('MISSED', reason)


class TestSetRunning:
    def test_a_started_row_runs_the_task_after_one_statement(self) -> None:
        cursor = _Cursor([_STARTED])
        conn = _Conn(cursor)

        assert _confirm(_Pool(conn)) is None

        assert cursor.queries == [
            (START_ATTEMPT_SQL, start_attempt_params('task-1', 'worker-1', CLAIMED_AT)),
        ]
        assert (conn.commits, conn.rollbacks) == (0, 0)

    def test_a_workflow_task_starts_with_the_same_statement(self) -> None:
        """The function reads is_workflow_task from the locked row and
        writes the node in the same statement."""
        cursor = _Cursor([_STARTED])
        conn = _Conn(cursor)

        assert _confirm(_Pool(conn), is_workflow_task=True) is None

        assert [sql for sql, _ in cursor.queries] == [START_ATTEMPT_SQL]

    def test_a_node_that_is_not_linked_fails_the_workflow_check(self) -> None:
        cursor = _Cursor([_missed('NODE_NOT_LINKED')])
        conn = _Conn(cursor)

        assert _confirm(_Pool(conn), is_workflow_task=True) == (
            False,
            '',
            'WORKFLOW_CHECK_FAILED',
        )
        assert [sql for sql, _ in cursor.queries] == [START_ATTEMPT_SQL]

    def test_a_row_outside_the_contract_is_ownership_unconfirmed(self) -> None:
        cursor = _Cursor([_missed('SOMETHING_ELSE')])

        assert _confirm(_Pool(_Conn(cursor))) == (False, '', 'OWNERSHIP_UNCONFIRMED')

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
    def test_a_passed_deadline_expires_the_claim(self) -> None:
        cursor = _Cursor([_missed('DEADLINE_PASSED')])
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

    def test_a_refused_expiry_is_a_lost_claim(self) -> None:
        cursor = _Cursor([_missed('DEADLINE_PASSED')])
        conn = _Conn(cursor)
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(return_value=MagicMock(spec=LostClaim)),
        ):
            result = _confirm(_Pool(conn))

        assert result == (False, '', 'CLAIM_LOST')
        assert (conn.commits, conn.rollbacks) == (0, 0)

    @pytest.mark.parametrize('reason', ['NOT_HELD', 'LEASE_EXPIRED'])
    def test_a_claim_that_is_not_held_or_expired_is_lost_with_no_write(
        self, reason: str,
    ) -> None:
        cursor = _Cursor([_missed(reason)])
        conn = _Conn(cursor)
        apply = AsyncMock()
        with patch.object(shared_lifecycle, 'apply_psycopg_async', apply):
            result = _confirm(_Pool(conn))

        assert result == (False, '', 'CLAIM_LOST')
        assert [sql for sql, _ in cursor.queries] == [START_ATTEMPT_SQL]
        apply.assert_not_awaited()

    def test_a_paused_workflow_abandons_the_claim_and_resets_the_node(self) -> None:
        """The stop runs in its own transaction after the start, whose row
        lock ended with its statement."""
        cursor = _Cursor([_missed('WORKFLOW_PAUSED')])
        conn = _OrderedConn(cursor)
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            _apply_recording([MagicMock(spec=Applied)]),
        ):
            result = _confirm(_Pool(conn), is_workflow_task=True)

        assert result == (False, '', 'WORKFLOW_STOPPED')
        assert [sql for sql, _ in cursor.queries] == [
            START_ATTEMPT_SQL,
            BEGIN_SQL,
            AbandonOwnedNode.__name__,
            RESET_PAUSED_NODE_SQL,
            'COMMIT',
        ]

    def test_a_cancelled_workflow_cancels_the_task_and_skips_the_node(self) -> None:
        cursor = _Cursor([_missed('WORKFLOW_CANCELLED')])
        conn = _Conn(cursor)
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(return_value=MagicMock(spec=Applied)),
        ) as apply:
            result = _confirm(_Pool(conn), is_workflow_task=True)

        assert result == (False, '', 'WORKFLOW_STOPPED')
        cancel = apply.call_args.args[1]
        assert isinstance(cancel, CancelOwnedNode)
        assert cancel.accepts_requeued_pending is True
        assert cancel.fence.claimed_at == CLAIMED_AT
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
        first = _Conn(_Cursor([_missed('WORKFLOW_PAUSED')]))
        second_cursor = _Cursor([_STARTED])
        second = _Conn(second_cursor)
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(side_effect=[resumed]),
        ):
            result = _confirm(_Pool(first, second), is_workflow_task=True)

        assert result is None
        # The refused abandon rolls back its transaction.
        assert (first.commits, first.rollbacks) == (0, 1)
        assert [sql for sql, _ in second_cursor.queries] == [START_ATTEMPT_SQL]

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
        conns = [_Conn(_Cursor([_missed('WORKFLOW_PAUSED')])) for _ in range(2)]
        with patch.object(
            shared_lifecycle,
            'apply_psycopg_async',
            AsyncMock(side_effect=[resumed] * len(conns)),
        ):
            result = _confirm(_Pool(*conns), is_workflow_task=True)

        assert result == (False, '', 'OWNERSHIP_UNCONFIRMED')


class TestFinalizingMarker:
    def test_the_marker_is_fenced_on_the_claim_generation(self) -> None:
        cursor = _Cursor([('MARKED',)])
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
                mark_finalizing_params('task-1', 'worker-1', CLAIMED_AT),
            ),
        ]
        # One statement on an autocommit connection: no transaction to end.
        assert (conn.commits, conn.rollbacks) == (0, 0)

    def test_a_marker_that_finds_another_claim_is_logged_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG, logger='horsies.worker')

        asyncio.run(
            shared_lifecycle.mark_task_finalizing(
                _pool(_Pool(_Conn(_Cursor([('NOT_HELD',)])))),
                'task-1',
                'worker-1',
                CLAIMED_AT,
            ),
        )

        assert 'Finalizing marker for task task-1 skipped: the claim changed' in caplog.text
        assert 'Failed to mark task task-1 finalizing' not in caplog.text

    def test_a_marker_row_outside_the_contract_raises(self) -> None:
        with pytest.raises(AssertionError, match='horsies_mark_finalizing returned'):
            asyncio.run(
                shared_lifecycle.mark_task_finalizing(
                    _pool(_Pool(_Conn(_Cursor([None])))),
                    'task-1',
                    'worker-1',
                    CLAIMED_AT,
                ),
            )

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
