"""A timed-out attempt ends before its row moves.

At the deadline the worker kills the child, waits for the child's exit, and
only then writes the row. Each test keeps one timeline of the kill, the exit,
and every database step, and asserts on its order.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from psycopg import OperationalError

from horsies.core.lifecycle.commands import FailLockedTask
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedTaskState,
    TerminalizationOutcome,
)
from horsies.core.models.tasks import OutcomeCode
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.core.worker.child_protocol import RssUnreadable
from horsies.core.worker.runtime import (
    _AttemptEndUnknown,
    _RequeueOutcome,
    _RetryError,
    _SlotAnsweredFirst,
    _TimeoutSettled,
    _TimeoutWriteFailed,
)
from horsies.core.worker.slots import (
    CHILD_KILL_WAIT_S,
    Child,
    ChildExit,
    ChildLost,
    ExitUnknown,
    KilledBySignal,
    LossCause,
    SlotHandle,
    TaskFinished,
)
from horsies.core.worker.sql import (
    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
    UNCLAIM_CLAIMED_TASK_SQL,
    UPSERT_TASK_ATTEMPT_SQL,
)
from horsies.core.worker.stored_timeout import TaskTimeout
from horsies.core.worker.worker import Worker, WorkerConfig
from tests.fixtures.slots.handles import task_finished_handle, unbound_handle

TASK_ID = 'task-1'
CHILD_PID = 4242
TIMEOUT_MS = 1000
# Bounds every wait in this file, so a wrong order fails instead of hanging.
TEST_WAIT_S = 5.0
_NOW = datetime(2026, 11, 10, tzinfo=timezone.utc)
_KILLED = KilledBySignal(9)
_KILLED_BY_PARENT = ChildLost(
    cause=LossCause.KILLED_BY_PARENT,
    child_exit=_KILLED,
    partial_frame=False,
)
_RETRY_DB_ERROR: Result[Any, _RetryError] = Err(
    _RetryError(error_code='BROKER_ERROR', message='db down', task_id=TASK_ID),
)
_STATEMENT_NAMES: dict[int, str] = {
    id(SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL): 'lock row',
    id(UNCLAIM_CLAIMED_TASK_SQL): 'requeue',
    id(UPSERT_TASK_ATTEMPT_SQL): 'attempt row',
}


class _ScriptedChild:
    """A child double: it records the kill and exits when the gate opens.

    ``on_exit`` runs at the exit, before ``wait_exit`` returns. A test uses
    it to answer the handle the way the slot does after a death.
    """

    pid = CHILD_PID

    def __init__(self, timeline: list[str], child_exit: ChildExit) -> None:
        self._timeline = timeline
        self._child_exit = child_exit
        self.gate = asyncio.Event()
        self.waits_s: list[float] = []
        self.on_exit: list[SlotHandle] = []

    def kill_for_caller(self) -> None:
        self._timeline.append('kill')

    async def wait_exit(self, timeout_s: float) -> ChildExit:
        self.waits_s.append(timeout_s)
        await self.gate.wait()
        match self._child_exit:
            case ExitUnknown():
                self._timeline.append('wait ended')
            case _:
                self._timeline.append('exit seen')
                for handle in self.on_exit:
                    handle.finish(_KILLED_BY_PARENT)
        return self._child_exit


class _RecordingSession:
    """An async session double that adds each statement to the timeline."""

    def __init__(self, timeline: list[str], row: SimpleNamespace | None) -> None:
        self._timeline = timeline
        self._row = row
        self.attempt_params: list[dict[str, Any]] = []

    async def __aenter__(self) -> _RecordingSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def execute(self, statement: Any, params: dict[str, Any]) -> Any:
        self._timeline.append(_STATEMENT_NAMES.get(id(statement), 'other statement'))
        if statement is UPSERT_TASK_ATTEMPT_SQL:
            self.attempt_params.append(params)
        result = MagicMock()
        locked = statement is SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL
        result.fetchone.return_value = self._row if locked else None
        return result

    async def commit(self) -> None:
        self._timeline.append('commit')

    async def rollback(self) -> None:
        self._timeline.append('rollback')

    async def connection(self) -> Any:
        return MagicMock()


def _row(status: str) -> SimpleNamespace:
    return SimpleNamespace(
        status=status,
        retry_count=0,
        db_now=_NOW,
        started_at=_NOW,
        claimed_by_worker_id='worker-1',
        worker_hostname='host',
        worker_pid=CHILD_PID,
        worker_process_name='child-0',
        queue_name='default',
        is_workflow_task=True,
        task_name='timeout_task',
    )


def _observed() -> ObservedTaskState:
    return ObservedTaskState(
        status=TaskStatus.RUNNING,
        worker_id='worker-1',
        claimed_at=None,
    )


def _applied() -> Applied:
    return Applied(
        task_id=TASK_ID,
        ordinality=None,
        terminal_at=_NOW,
        kind=TerminalizationKind.FAIL_RUNNING,
        observed=_observed(),
    )


def _already_applied() -> AlreadyApplied:
    return AlreadyApplied(
        task_id=TASK_ID,
        ordinality=None,
        terminal_at=_NOW,
        kind=TerminalizationKind.FAIL_RUNNING,
        observed=_observed(),
    )


def _lost_claim() -> LostClaim:
    return LostClaim(
        task_id=TASK_ID,
        ordinality=None,
        observed=ObservedTaskState(
            status=TaskStatus.PENDING,
            worker_id=None,
            claimed_at=None,
        ),
    )


class _Rig:
    """A worker whose database and retry steps write to one timeline."""

    def __init__(
        self,
        *,
        row: SimpleNamespace | None,
        child_exit: ChildExit = _KILLED,
        should_retry: Result[bool, _RetryError] = Ok(False),
        schedule: Result[Any, _RetryError] = Ok('scheduled'),
    ) -> None:
        self.timeline: list[str] = []
        self.session = _RecordingSession(self.timeline, row)
        self.child = _ScriptedChild(self.timeline, child_exit)
        self.handle = unbound_handle(TASK_ID)
        self.handle.bind_child(cast(Child, self.child))
        self.fail_commands: list[FailLockedTask] = []
        self.worker = Worker(
            session_factory=MagicMock(),
            listener=MagicMock(),
            cfg=WorkerConfig(
                dsn='postgresql+psycopg://u:p@localhost/db',
                psycopg_dsn='postgresql://u:p@localhost/db',
                queues=['default'],
            ),
        )
        self.session_factory = MagicMock(side_effect=self._open_session)
        self.worker.sf = self.session_factory
        self.worker._should_retry_task = AsyncMock(  # type: ignore[method-assign]
            side_effect=self._step('retry decision', should_retry),
        )
        self.worker._schedule_retry = AsyncMock(  # type: ignore[method-assign]
            side_effect=self._step('set PENDING', schedule),
        )
        self.worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
            side_effect=self._step('phase 2', Ok(None)),
        )
        self.worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            side_effect=self._step('crash recovery', _RequeueOutcome.REQUEUED),
        )

    def _open_session(self) -> _RecordingSession:
        return self.session

    def _refuse_session(self) -> _RecordingSession:
        self.timeline.append('open session')
        raise OperationalError('db down')

    def refuse_sessions(self) -> None:
        """Make every session open fail, as a database that is down."""
        self.session_factory.side_effect = self._refuse_session

    def _step(self, name: str, answer: object) -> Any:
        async def _record(*_args: object, **_kwargs: object) -> object:
            self.timeline.append(name)
            return answer

        return _record

    def fail_row(self, answer: TerminalizationOutcome) -> Any:
        """Patch the terminalization, recording it and its command."""

        async def _apply(_connection: object, command: FailLockedTask) -> object:
            self.timeline.append('fail row')
            self.fail_commands.append(command)
            return answer

        return patch(
            'horsies.core.worker.dispatch.apply_async',
            AsyncMock(side_effect=_apply),
        )

    async def handle_timeout(self) -> object:
        return await asyncio.wait_for(
            self.worker._handle_task_timeout(self.handle, TASK_ID, TIMEOUT_MS),
            TEST_WAIT_S,
        )

    async def finalize_after_deadline(self) -> object:
        """Enter at the finalizer, with a deadline that passes at once."""
        self.worker._persist_task_terminal_state = AsyncMock(  # type: ignore[method-assign]
            side_effect=self._step('persist slot outcome', Ok(None)),
        )
        return await asyncio.wait_for(
            self.worker._finalize_after(
                self.handle,
                TASK_ID,
                timeout=TaskTimeout(ms=1),
                task_name='timeout_task',
            ),
            TEST_WAIT_S,
        )


async def _until_killed(rig: _Rig) -> None:
    deadline = asyncio.get_running_loop().time() + TEST_WAIT_S
    while 'kill' not in rig.timeline:
        assert asyncio.get_running_loop().time() < deadline, 'no kill was sent'
        await asyncio.sleep(0.01)


@pytest.mark.unit
@pytest.mark.asyncio
class TestTheKillComesFirst:
    async def test_nothing_is_written_while_the_child_runs(self) -> None:
        rig = _Rig(row=_row('RUNNING'))

        with rig.fail_row(_applied()):
            timeout = asyncio.ensure_future(rig.handle_timeout())
            await _until_killed(rig)
            await asyncio.sleep(0.05)

            assert rig.timeline == ['kill']
            rig.session_factory.assert_not_called()
            rig.child.gate.set()
            assert await timeout == _TimeoutSettled()

    async def test_the_wait_for_the_exit_has_the_kill_bound(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.gate.set()

        with rig.fail_row(_applied()):
            await rig.handle_timeout()

        assert rig.child.waits_s == [CHILD_KILL_WAIT_S]

    async def test_a_workflow_task_fails_then_phase_2_runs_after_the_exit(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.gate.set()

        with rig.fail_row(_applied()):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == [
            'kill',
            'exit seen',
            'lock row',
            'retry decision',
            'attempt row',
            'fail row',
            'commit',
            'phase 2',
        ]
        assert [command.error_code for command in rig.fail_commands] == [
            OutcomeCode.TASK_TIMEOUT.value,
        ]
        assert rig.session.attempt_params[0]['will_retry'] is False

    async def test_a_retry_is_written_only_after_the_exit(self) -> None:
        rig = _Rig(row=_row('RUNNING'), should_retry=Ok(True))
        rig.child.gate.set()

        with rig.fail_row(_applied()):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == [
            'kill',
            'exit seen',
            'lock row',
            'retry decision',
            'set PENDING',
            'attempt row',
            'commit',
        ]
        params = rig.session.attempt_params[0]
        assert (params['will_retry'], params['error_code']) == (
            True,
            OutcomeCode.TASK_TIMEOUT.value,
        )

    async def test_a_claimed_row_is_requeued_only_after_the_exit(self) -> None:
        rig = _Rig(row=_row('CLAIMED'))
        rig.child.gate.set()

        with rig.fail_row(_applied()):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == ['kill', 'exit seen', 'lock row', 'requeue', 'commit']

    async def test_a_database_error_comes_after_the_kill(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.gate.set()
        rig.refuse_sessions()

        with caplog.at_level(logging.ERROR, logger='horsies.worker'):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutWriteFailed()
        assert rig.timeline == ['kill', 'exit seen', 'open session']
        assert 'Failed to persist TASK_TIMEOUT for task task-1' in caplog.text


@pytest.mark.unit
@pytest.mark.asyncio
class TestNoExitSeen:
    async def test_no_retry_and_no_phase_2_without_the_exit(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        rig = _Rig(
            row=_row('RUNNING'),
            child_exit=ExitUnknown(),
            should_retry=Ok(True),
        )
        rig.child.gate.set()

        with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
            outcome = await rig.handle_timeout()

        assert outcome == _AttemptEndUnknown()
        assert rig.timeline == ['kill', 'wait ended']
        rig.session_factory.assert_not_called()
        critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical) == 1
        assert TASK_ID in critical[0].message
        assert f'pid={CHILD_PID}' in critical[0].message

    async def test_the_finalizer_leaves_the_row_and_does_not_wait_for_the_slot(
        self,
    ) -> None:
        """The slot never answers here: the child did not exit."""
        rig = _Rig(row=_row('RUNNING'), child_exit=ExitUnknown())
        rig.child.gate.set()

        result = await rig.finalize_after_deadline()

        assert result == Ok(None)
        assert rig.timeline == ['kill', 'wait ended']


@pytest.mark.unit
@pytest.mark.asyncio
class TestOneWriter:
    async def test_the_timeout_is_written_once_and_never_as_a_crash(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.on_exit.append(rig.handle)
        rig.child.gate.set()

        with rig.fail_row(_applied()):
            result = await rig.finalize_after_deadline()

        assert result == Ok(None)
        assert rig.handle.outcome.result() == _KILLED_BY_PARENT
        assert [command.error_code for command in rig.fail_commands] == [
            OutcomeCode.TASK_TIMEOUT.value,
        ]
        assert 'crash recovery' not in rig.timeline
        assert rig.timeline.count('phase 2') == 1

    async def test_a_failed_write_falls_back_to_crash_recovery_after_the_exit(
        self,
    ) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.on_exit.append(rig.handle)
        rig.child.gate.set()
        rig.refuse_sessions()

        await rig.finalize_after_deadline()

        assert rig.timeline == ['kill', 'exit seen', 'open session', 'crash recovery']


@pytest.mark.unit
@pytest.mark.asyncio
class TestNothingToKill:
    async def test_the_slot_outcome_stands_and_nothing_is_written(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.handle = task_finished_handle(True, '{}', None)

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            outcome = await rig.handle_timeout()

        assert outcome == _SlotAnsweredFirst()
        assert rig.timeline == []
        rig.session_factory.assert_not_called()
        assert 'no longer holds it' in caplog.text

    async def test_the_finalizer_persists_the_slot_outcome(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        finished = TaskFinished(True, '{}', None, RssUnreadable())

        async def _slot_answers_first(*_args: object, **_kwargs: object) -> object:
            rig.handle.finish(finished)
            return _SlotAnsweredFirst()

        rig.worker._handle_task_timeout = AsyncMock(  # type: ignore[method-assign]
            side_effect=_slot_answers_first,
        )
        await rig.finalize_after_deadline()

        assert rig.timeline == ['persist slot outcome']


@pytest.mark.unit
@pytest.mark.asyncio
class TestTimeoutWriteOutcomes:
    """Each other way the timeout write ends, after the exit was seen."""

    async def test_a_failed_retry_decision_is_a_failed_write(self) -> None:
        rig = _Rig(
            row=_row('RUNNING'),
            should_retry=_RETRY_DB_ERROR,
        )
        rig.child.gate.set()

        assert await rig.handle_timeout() == _TimeoutWriteFailed()
        assert 'attempt row' not in rig.timeline

    async def test_a_failed_retry_schedule_is_a_failed_write(self) -> None:
        rig = _Rig(
            row=_row('RUNNING'),
            should_retry=Ok(True),
            schedule=_RETRY_DB_ERROR,
        )
        rig.child.gate.set()

        assert await rig.handle_timeout() == _TimeoutWriteFailed()
        assert 'attempt row' not in rig.timeline

    async def test_an_expired_retry_fails_the_row(self) -> None:
        rig = _Rig(row=_row('RUNNING'), should_retry=Ok(True), schedule=Ok('expired'))
        rig.child.gate.set()

        with rig.fail_row(_applied()):
            assert await rig.handle_timeout() == _TimeoutSettled()

        assert rig.timeline[-3:] == ['fail row', 'commit', 'phase 2']
        assert rig.session.attempt_params[0]['will_retry'] is False

    async def test_a_row_failed_by_this_claim_replays_phase_2(self) -> None:
        rig = _Rig(row=None)
        rig.child.gate.set()
        rig.worker._replay_committed_terminalization = AsyncMock(  # type: ignore[method-assign]
            side_effect=rig._step('replay phase 2', None),
        )

        with patch(
            'horsies.core.worker.dispatch.classify_locked_read_miss_async',
            AsyncMock(return_value=_already_applied()),
        ):
            assert await rig.handle_timeout() == _TimeoutSettled()

        assert rig.timeline == ['kill', 'exit seen', 'lock row', 'replay phase 2']

    async def test_a_row_another_actor_holds_is_left_alone(self) -> None:
        rig = _Rig(row=None)
        rig.child.gate.set()

        with patch(
            'horsies.core.worker.dispatch.classify_locked_read_miss_async',
            AsyncMock(return_value=_lost_claim()),
        ):
            assert await rig.handle_timeout() == _TimeoutSettled()

        assert rig.timeline == ['kill', 'exit seen', 'lock row', 'rollback']

    async def test_a_status_outside_the_locked_read_is_a_failed_write(self) -> None:
        rig = _Rig(row=_row('PENDING'))
        rig.child.gate.set()

        assert await rig.handle_timeout() == _TimeoutWriteFailed()
        assert rig.timeline == ['kill', 'exit seen', 'lock row']
