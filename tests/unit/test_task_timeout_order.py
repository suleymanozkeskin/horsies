"""A timed-out attempt ends before its row moves.

At the deadline the worker kills the child, waits for the child's exit, and
only then writes the row. Each test keeps one timeline of the kill, the exit,
and every database step, and asserts on its order.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from psycopg import OperationalError

from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    CapacityWoken,
    FailedAttemptOutcome,
    FailureApplied,
    LostClaim,
    ObservedTaskState,
    PhaseTwoOwed,
    RetryNotRequested,
    RetryPastDeadline,
    RetryScheduled,
)
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import OutcomeCode
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.core.worker.child_protocol import NoTaskLoop, RssUnreadable
from horsies.core.worker.failed_attempt import (
    FinishFailedAttempt,
    NoRetry,
    RetryAfter,
)
from horsies.core.worker.runtime import (
    _FINALIZE_STAGE_TIMEOUT,
    _AttemptEndUnknown,
    _FinalizeError,
    _RequeueOutcome,
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
    KillWaitOutcome,
    LossCause,
    SlotHandle,
    SlotOutcome,
    TaskFinished,
)
from horsies.core.worker.sql import (
    READ_IN_FLIGHT_ATTEMPT_SQL,
    UNCLAIM_CLAIMED_TASK_SQL,
)
from horsies.core.worker.stored_timeout import TaskTimeout
from horsies.core.worker.worker import Worker, WorkerConfig
from tests.fixtures.slots.handles import (
    finished_handle,
    task_finished_handle,
    unbound_handle,
)

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
    pid=CHILD_PID,
)
_LOST_EXIT_UNKNOWN = ChildLost(
    cause=LossCause.CHANNEL_LOST,
    child_exit=ExitUnknown(),
    partial_frame=False,
    pid=CHILD_PID,
)
_STATEMENT_NAMES: dict[int, str] = {
    id(READ_IN_FLIGHT_ATTEMPT_SQL): 'read row',
    id(UNCLAIM_CLAIMED_TASK_SQL): 'requeue',
}
_TIMEOUT_RETRY_OPTIONS = json.dumps(
    {
        'retry_policy': {
            'auto_retry_for': [OutcomeCode.TASK_TIMEOUT.value],
            'intervals': [30],
            'backoff_strategy': 'fixed',
            'jitter': False,
        },
    }
)


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
        self.late_exit = asyncio.Event()
        self.waits_s: list[float] = []
        self.on_exit: list[SlotHandle] = []

    def kill_for_caller(self) -> None:
        self._timeline.append('kill')

    async def wait_exit_seen(self) -> KilledBySignal:
        """The exit after the bounded wait: it comes when ``late_exit`` opens."""
        await self.late_exit.wait()
        self._timeline.append('late exit seen')
        return KilledBySignal(9)

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


class _RecordingConnection:
    """An autocommit connection double that adds each statement to the timeline."""

    def __init__(self, timeline: list[str], row: SimpleNamespace | None) -> None:
        self._timeline = timeline
        self._row = row

    async def execute(self, statement: Any, params: dict[str, Any]) -> Any:
        self._timeline.append(_STATEMENT_NAMES.get(id(statement), 'other statement'))
        result = MagicMock()
        read = statement is READ_IN_FLIGHT_ATTEMPT_SQL
        result.fetchone.return_value = self._row if read else None
        result.rowcount = 1
        return result


class _RecordingSession:
    """An async session double whose connection records each statement."""

    def __init__(self, timeline: list[str], row: SimpleNamespace | None) -> None:
        self._timeline = timeline
        self.connection_double = _RecordingConnection(timeline, row)
        self.execution_options: list[dict[str, Any]] = []

    async def __aenter__(self) -> _RecordingSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def connection(
        self,
        execution_options: dict[str, Any] | None = None,
    ) -> _RecordingConnection:
        self.execution_options.append(execution_options or {})
        return self.connection_double


def _row(status: str, *, task_options: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        status=status,
        retry_count=0,
        max_retries=3 if task_options is not None else 0,
        task_options=task_options,
    )


def _observed() -> ObservedTaskState:
    return ObservedTaskState(
        status=TaskStatus.RUNNING,
        worker_id='worker-1',
        claimed_at=None,
    )


def _applied(*, workflow: bool = True) -> FailureApplied:
    return FailureApplied(
        task_id=TASK_ID,
        terminal_at=_NOW,
        observed=_observed(),
        retry=RetryNotRequested(),
        after=(
            PhaseTwoOwed(task_name='timeout_task', queue_name='default')
            if workflow
            else CapacityWoken()
        ),
    )


def _past_deadline() -> FailureApplied:
    return FailureApplied(
        task_id=TASK_ID,
        terminal_at=_NOW,
        observed=_observed(),
        retry=RetryPastDeadline(next_retry_at=_NOW, good_until=_NOW),
        after=PhaseTwoOwed(task_name='timeout_task', queue_name='default'),
    )


def _retry_scheduled() -> RetryScheduled:
    return RetryScheduled(
        task_id=TASK_ID,
        retry_count=1,
        next_retry_at=_NOW,
        queue_name='default',
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
        recovery: RecoveryConfig | None = RecoveryConfig(),
    ) -> None:
        self.timeline: list[str] = []
        self.session = _RecordingSession(self.timeline, row)
        self.child = _ScriptedChild(self.timeline, child_exit)
        self.handle = unbound_handle(TASK_ID)
        self.handle.bind_child(cast(Child, self.child))
        self.finish_commands: list[FinishFailedAttempt] = []
        self.worker = Worker(
            session_factory=MagicMock(),
            listener=MagicMock(),
            cfg=WorkerConfig(
                dsn='postgresql+psycopg://u:p@localhost/db',
                psycopg_dsn='postgresql://u:p@localhost/db',
                queues=['default'],
                recovery_config=recovery,
            ),
        )
        self.session_factory = MagicMock(side_effect=self._open_session)
        self.worker.sf = self.session_factory
        self.worker._wake_when_retry_due = MagicMock(  # type: ignore[method-assign]
            side_effect=self._wake_when_due,
        )
        self.worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
            side_effect=self._step('phase 2', Ok(None)),
        )
        self.worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            side_effect=self._step('crash recovery', _RequeueOutcome.REQUEUED),
        )

    def _wake_when_due(self, *_args: object) -> None:
        self.timeline.append('wake when due')

    def _open_session(self) -> _RecordingSession:
        return self.session

    def _refuse_session(self) -> _RecordingSession:
        self.timeline.append('open session')
        raise OperationalError('db down')

    def refuse_sessions(self) -> None:
        """Make every session open fail, as a database that is down."""
        self.session_factory.side_effect = self._refuse_session

    def slot_answers_before_the_kill(self, outcome: SlotOutcome) -> None:
        """Make the slot finish the handle after the timer fired, before the kill."""
        kill_and_wait = self.handle.kill_and_wait

        async def _answer_then_kill(wait_s: float) -> KillWaitOutcome:
            self.handle.finish(outcome)
            return await kill_and_wait(wait_s)

        self.handle.kill_and_wait = _answer_then_kill  # type: ignore[method-assign]

    def _step(self, name: str, answer: object) -> Any:
        async def _record(*_args: object, **_kwargs: object) -> object:
            self.timeline.append(name)
            return answer

        return _record

    def finish(self, answer: FailedAttemptOutcome | Exception) -> Any:
        """Patch the failed-attempt finish, recording it and its command."""

        async def _finish(
            _connection: object,
            command: FinishFailedAttempt,
        ) -> FailedAttemptOutcome:
            self.timeline.append('finish attempt')
            self.finish_commands.append(command)
            match answer:
                case Exception():
                    raise answer
                case _:
                    return answer

        return patch(
            'horsies.core.worker.dispatch.finish_failed_attempt',
            AsyncMock(side_effect=_finish),
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

        with rig.finish(_applied()):
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

        with rig.finish(_applied()):
            await rig.handle_timeout()

        assert rig.child.waits_s == [CHILD_KILL_WAIT_S]

    async def test_a_workflow_task_fails_then_phase_2_runs_after_the_exit(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.gate.set()

        with rig.finish(_applied()):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == [
            'kill',
            'exit seen',
            'read row',
            'finish attempt',
            'phase 2',
        ]
        assert [command.error_code for command in rig.finish_commands] == [
            OutcomeCode.TASK_TIMEOUT.value,
        ]
        assert [command.retry for command in rig.finish_commands] == [NoRetry()]
        assert rig.session.execution_options == [{'isolation_level': 'AUTOCOMMIT'}]

    async def test_a_plain_task_failure_runs_no_phase_2(self) -> None:
        """The function sent the capacity wake of a plain task."""
        rig = _Rig(row=_row('RUNNING'))
        rig.child.gate.set()

        with rig.finish(_applied(workflow=False)):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == ['kill', 'exit seen', 'read row', 'finish attempt']

    async def test_a_retry_is_written_only_after_the_exit(self) -> None:
        rig = _Rig(row=_row('RUNNING', task_options=_TIMEOUT_RETRY_OPTIONS))
        rig.child.gate.set()

        with rig.finish(_retry_scheduled()):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == [
            'kill',
            'exit seen',
            'read row',
            'finish attempt',
            'wake when due',
        ]
        match rig.finish_commands[0].retry:
            case RetryAfter(retry_count=retry_count, delay_s=delay_s):
                assert (retry_count, delay_s) == (0, 30.0)
            case NoRetry() as other:
                raise AssertionError(other)
        assert rig.finish_commands[0].error_code == OutcomeCode.TASK_TIMEOUT.value

    async def test_a_claimed_row_is_requeued_only_after_the_exit(self) -> None:
        rig = _Rig(row=_row('CLAIMED'))
        rig.child.gate.set()

        with rig.finish(_applied()):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == ['kill', 'exit seen', 'read row', 'requeue']

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
            row=_row('RUNNING', task_options=_TIMEOUT_RETRY_OPTIONS),
            child_exit=ExitUnknown(),
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

    async def test_with_stale_recovery_off_the_timeout_waits_for_the_late_exit(
        self,
    ) -> None:
        """Nothing else would ever move the row: the worker writes
        TASK_TIMEOUT once it sees the exit, and not before."""
        rig = _Rig(row=_row('RUNNING'), child_exit=ExitUnknown(), recovery=None)
        rig.child.gate.set()

        with rig.finish(_applied()):
            waiting = asyncio.ensure_future(rig.handle_timeout())
            await asyncio.sleep(0.05)
            assert not waiting.done()
            assert rig.timeline == ['kill', 'wait ended']
            rig.child.late_exit.set()
            outcome = await asyncio.wait_for(waiting, TEST_WAIT_S)

        assert outcome == _TimeoutSettled()
        assert rig.timeline[:4] == ['kill', 'wait ended', 'late exit seen', 'read row']
        assert [command.error_code for command in rig.finish_commands] == [
            OutcomeCode.TASK_TIMEOUT.value,
        ]

    async def test_with_stale_recovery_off_a_stop_ends_the_wait(self) -> None:
        rig = _Rig(row=_row('RUNNING'), child_exit=ExitUnknown(), recovery=None)
        rig.child.gate.set()
        waiting = asyncio.ensure_future(rig.handle_timeout())
        await asyncio.sleep(0.05)

        rig.worker._stop.set()
        outcome = await asyncio.wait_for(waiting, TEST_WAIT_S)

        assert outcome == _AttemptEndUnknown()
        rig.session_factory.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
class TestOneWriter:
    async def test_the_timeout_is_written_once_and_never_as_a_crash(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.on_exit.append(rig.handle)
        rig.child.gate.set()

        with rig.finish(_applied()):
            result = await rig.finalize_after_deadline()

        assert result == Ok(None)
        assert rig.handle.outcome.result() == _KILLED_BY_PARENT
        assert [command.error_code for command in rig.finish_commands] == [
            OutcomeCode.TASK_TIMEOUT.value,
        ]
        assert 'crash recovery' not in rig.timeline
        assert rig.timeline.count('phase 2') == 1

    async def test_a_failed_write_after_the_exit_is_retried_as_the_timeout_write(
        self,
    ) -> None:
        """No crash recovery at once: the finalizer returns a retryable
        timeout-stage error, whose retries run the TASK_TIMEOUT write again
        and hand the row to crash recovery only after the last one."""
        rig = _Rig(row=_row('RUNNING'))
        rig.child.on_exit.append(rig.handle)
        rig.child.gate.set()
        rig.refuse_sessions()

        result = await rig.finalize_after_deadline()

        assert rig.timeline == ['kill', 'exit seen', 'open session']
        _assert_timeout_write_error(result)


def _assert_timeout_write_error(result: object) -> None:
    """The finalizer gave the retryable error of a failed TASK_TIMEOUT write."""
    match result:
        case Err(err_value=_FinalizeError() as error):
            assert error.stage == _FINALIZE_STAGE_TIMEOUT
            assert error.retryable is True
        case other:
            raise AssertionError(other)


@pytest.mark.unit
@pytest.mark.asyncio
class TestSlotAnsweredAfterTheDeadline:
    """The deadline decides: a later answer of the slot is dropped."""

    async def test_a_result_after_the_deadline_is_still_a_timeout(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.handle = task_finished_handle(True, '{}', None)

        with rig.finish(_applied()):
            outcome = await rig.handle_timeout()

        assert outcome == _TimeoutSettled()
        assert rig.timeline == [
            'read row',
            'finish attempt',
            'phase 2',
        ]
        assert [command.error_code for command in rig.finish_commands] == [
            OutcomeCode.TASK_TIMEOUT.value,
        ]

    async def test_the_finalizer_drops_a_result_that_came_after_the_timer(
        self,
    ) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.slot_answers_before_the_kill(
            TaskFinished(True, '{}', None, RssUnreadable(), NoTaskLoop()),
        )

        with rig.finish(_applied()):
            result = await rig.finalize_after_deadline()

        assert result == Ok(None)
        assert 'kill' not in rig.timeline
        assert 'persist slot outcome' not in rig.timeline
        assert [command.error_code for command in rig.finish_commands] == [
            OutcomeCode.TASK_TIMEOUT.value,
        ]

    async def test_a_failed_write_after_a_late_answer_is_retried_as_the_timeout_write(
        self,
    ) -> None:
        """The late answer of the slot is not read: the deadline decides."""
        rig = _Rig(row=_row('RUNNING'))
        rig.slot_answers_before_the_kill(_KILLED_BY_PARENT)
        rig.refuse_sessions()

        result = await rig.finalize_after_deadline()

        assert rig.timeline == ['open session']
        _assert_timeout_write_error(result)

    async def test_a_late_loss_with_no_exit_seen_writes_nothing(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.handle = finished_handle(_LOST_EXIT_UNKNOWN)

        with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
            outcome = await rig.handle_timeout()

        assert outcome == _AttemptEndUnknown()
        assert rig.timeline == []
        rig.session_factory.assert_not_called()
        critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical) == 1
        assert TASK_ID in critical[0].message
        assert f'pid={CHILD_PID}' in critical[0].message


@pytest.mark.unit
@pytest.mark.asyncio
class TestTimeoutWriteOutcomes:
    """Each other way the timeout write ends, after the exit was seen."""

    async def test_a_failed_finish_is_a_failed_write(self) -> None:
        rig = _Rig(row=_row('RUNNING'))
        rig.child.gate.set()

        with rig.finish(OperationalError('db down')):
            assert await rig.handle_timeout() == _TimeoutWriteFailed()

        assert rig.timeline == ['kill', 'exit seen', 'read row', 'finish attempt']

    async def test_a_retry_past_its_deadline_fails_the_row(self) -> None:
        rig = _Rig(row=_row('RUNNING', task_options=_TIMEOUT_RETRY_OPTIONS))
        rig.child.gate.set()

        with rig.finish(_past_deadline()):
            assert await rig.handle_timeout() == _TimeoutSettled()

        assert rig.timeline[-2:] == ['finish attempt', 'phase 2']

    async def test_a_row_failed_by_this_claim_replays_phase_2(self) -> None:
        rig = _Rig(row=None)
        rig.child.gate.set()
        rig.worker._replay_committed_terminalization = AsyncMock(  # type: ignore[method-assign]
            side_effect=rig._step('replay phase 2', None),
        )

        with rig.finish(_already_applied()):
            assert await rig.handle_timeout() == _TimeoutSettled()

        assert rig.timeline == [
            'kill',
            'exit seen',
            'read row',
            'finish attempt',
            'replay phase 2',
        ]
        assert [command.retry for command in rig.finish_commands] == [NoRetry()]

    async def test_a_row_another_actor_holds_is_left_alone(self) -> None:
        rig = _Rig(row=None)
        rig.child.gate.set()

        with rig.finish(_lost_claim()):
            assert await rig.handle_timeout() == _TimeoutSettled()

        assert rig.timeline == ['kill', 'exit seen', 'read row', 'finish attempt']

    async def test_a_status_outside_the_locked_read_is_a_failed_write(self) -> None:
        rig = _Rig(row=_row('PENDING'))
        rig.child.gate.set()

        assert await rig.handle_timeout() == _TimeoutWriteFailed()
        assert rig.timeline == ['kill', 'exit seen', 'read row']
