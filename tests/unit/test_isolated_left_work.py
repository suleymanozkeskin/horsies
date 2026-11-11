"""The worker side of an isolated task whose body returned with work left on
its task loop: the deadline no longer applies, and the outcome is what the
body returned, written only after the child reported the end of that work
or after the exit of the child."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.models.recovery import RecoveryConfig
from horsies.core.types.result import Ok
from horsies.core.worker.child_protocol import (
    LoopHoldsTasks,
    RssUnreadable,
    SeatFinished,
)
from horsies.core.worker.slots import (
    Assigned,
    Child,
    ChildExit,
    ChildLost,
    ExitedWith,
    ExitUnknown,
    KilledBySignal,
    LossCause,
    LostAfterReturn,
    ReturnSeen,
    SlotHandle,
    TaskFinished,
)
from horsies.core.worker.stored_timeout import TaskTimeout
from horsies.core.worker.worker import Worker, WorkerConfig
from tests.fixtures.slots.handles import finished_handle, unbound_handle

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]

TASK_ID = 'task-1'
CHILD_PID = 4242
# Bounds every wait in this file, so a wrong path fails instead of hanging.
TEST_WAIT_S = 5.0
RETURNED = SeatFinished(ok=True, result_json='{"ok":7}', failed_reason=None)
LEFT = LoopHoldsTasks(count=1, names=('flush',))
SEEN = ReturnSeen(returned=RETURNED, left=LEFT)


def _lost(
    child_exit: ChildExit, cause: LossCause = LossCause.KILLED_BY_PARENT
) -> ChildLost:
    return ChildLost(
        cause=cause, child_exit=child_exit, partial_frame=False, pid=CHILD_PID
    )


class _ChildDouble:
    """Records a kill. Its exit is seen when ``exited`` is set."""

    pid = CHILD_PID

    def __init__(self) -> None:
        self.kills = 0
        self.exited = asyncio.Event()

    def kill_for_caller(self) -> None:
        self.kills += 1

    async def wait_exit(self, timeout_s: float) -> ChildExit:
        try:
            await asyncio.wait_for(self.exited.wait(), timeout_s)
        except TimeoutError:
            return ExitUnknown()
        return KilledBySignal(9)

    async def wait_exit_seen(self) -> KilledBySignal:
        await self.exited.wait()
        return KilledBySignal(9)


def _bound_handle(child: _ChildDouble) -> SlotHandle:
    handle = unbound_handle(TASK_ID)
    handle.bind_child(cast(Child, child))
    return handle


def _worker(recovery: RecoveryConfig | None = RecoveryConfig()) -> Worker:
    """A worker whose writers are mocks: each test checks which one ran."""
    worker = Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            recovery_config=recovery,
        ),
    )
    worker._finalize_task_result = AsyncMock(return_value=Ok(None))
    worker._recover_worker_future_failure = AsyncMock()
    worker._record_task_timeout = AsyncMock()
    return worker


def _assert_wrote_returned(worker: Worker) -> None:
    write = cast(AsyncMock, worker._finalize_task_result)
    write.assert_awaited_once()
    assert write.await_args is not None
    assert write.await_args.args == (
        TASK_ID,
        RETURNED.ok,
        RETURNED.result_json,
        RETURNED.failed_reason,
    )
    cast(AsyncMock, worker._recover_worker_future_failure).assert_not_awaited()
    cast(AsyncMock, worker._record_task_timeout).assert_not_awaited()


class TestHandle:
    async def test_a_loss_before_the_return_is_the_loss(self) -> None:
        lost = _lost(KilledBySignal(9))
        assert unbound_handle(TASK_ID).loss_outcome(lost) == lost

    async def test_a_loss_after_the_return_keeps_what_the_body_returned(self) -> None:
        handle = unbound_handle(TASK_ID)
        handle.note_return(SEEN)
        lost = _lost(ExitedWith(3), LossCause.SLOT_DEFECT)
        assert handle.loss_outcome(lost) == LostAfterReturn(
            returned=RETURNED, lost=lost
        )

    async def test_a_second_return_is_a_defect(self) -> None:
        handle = unbound_handle(TASK_ID)
        handle.note_return(SEEN)
        with pytest.raises(AssertionError, match='noted twice'):
            handle.note_return(SEEN)

    async def test_a_loss_after_the_return_marks_the_task_assigned(self) -> None:
        handle = finished_handle(LostAfterReturn(RETURNED, _lost(KilledBySignal(9))))
        assert handle.assigned.result() == Assigned()

    async def test_the_deadline_kill_after_the_return_kills_nothing(self) -> None:
        child = _ChildDouble()
        handle = _bound_handle(child)
        handle.note_return(SEEN)

        assert await handle.kill_and_wait(TEST_WAIT_S) == SEEN
        assert child.kills == 0

    async def test_the_deadline_kill_before_the_return_kills(self) -> None:
        child = _ChildDouble()
        child.exited.set()
        handle = _bound_handle(child)

        _ = await handle.kill_and_wait(TEST_WAIT_S)
        assert child.kills == 1


class TestDeadline:
    async def test_the_timeout_handler_writes_nothing_after_the_return(self) -> None:
        worker = _worker()
        child = _ChildDouble()
        handle = _bound_handle(child)
        handle.note_return(SEEN)

        outcome = await asyncio.wait_for(
            worker._handle_task_timeout(handle, TASK_ID, 1_000),
            TEST_WAIT_S,
        )

        assert outcome == SEEN
        assert child.kills == 0
        cast(AsyncMock, worker._record_task_timeout).assert_not_awaited()

    async def test_a_deadline_past_the_return_waits_for_the_outcome_of_the_slot(
        self,
    ) -> None:
        worker = _worker()
        child = _ChildDouble()
        handle = _bound_handle(child)
        handle.note_return(SEEN)
        finished = TaskFinished(
            RETURNED.ok,
            RETURNED.result_json,
            RETURNED.failed_reason,
            RssUnreadable(),
            LEFT,
        )

        waiter = asyncio.ensure_future(
            worker._await_slot_outcome(handle, TASK_ID, TaskTimeout(ms=1), None),
        )
        await asyncio.sleep(0.05)
        assert not waiter.done()
        handle.finish(finished)

        assert await asyncio.wait_for(waiter, TEST_WAIT_S) == finished
        assert child.kills == 0
        cast(AsyncMock, worker._record_task_timeout).assert_not_awaited()

    async def test_an_answered_loss_after_the_return_is_never_timed_out(self) -> None:
        """``kill_and_wait`` answers ``ReturnSeen`` first, so this is a defect."""
        worker = _worker()
        handle = finished_handle(LostAfterReturn(RETURNED, _lost(KilledBySignal(9))))

        with pytest.raises(AssertionError, match='after its body returned'):
            await worker._time_out_answered_task(
                handle,
                TASK_ID,
                1_000,
                None,
                LostAfterReturn(RETURNED, _lost(KilledBySignal(9))),
            )
        cast(AsyncMock, worker._record_task_timeout).assert_not_awaited()


class TestFinalize:
    @pytest.mark.parametrize(
        'lost',
        [
            _lost(KilledBySignal(9)),
            _lost(ExitedWith(3), LossCause.CHILD_DIED),
            _lost(ExitedWith(0), LossCause.PROTOCOL_VIOLATION),
        ],
        ids=['killed', 'died', 'violation'],
    )
    async def test_a_seen_exit_writes_what_the_body_returned(
        self, lost: ChildLost
    ) -> None:
        worker = _worker()

        result = await worker._finalize_after(
            finished_handle(LostAfterReturn(RETURNED, lost)),
            TASK_ID,
            timeout=TaskTimeout(ms=1_000),
            task_name='left_work_task',
        )

        assert result == Ok(None)
        _assert_wrote_returned(worker)

    async def test_no_exit_seen_writes_nothing_with_stale_recovery_on(self) -> None:
        worker = _worker(RecoveryConfig(auto_fail_stale_running=True))

        result = await worker._finalize_after(
            finished_handle(LostAfterReturn(RETURNED, _lost(ExitUnknown()))),
            TASK_ID,
            task_name='left_work_task',
        )

        assert result == Ok(None)
        cast(AsyncMock, worker._finalize_task_result).assert_not_awaited()
        cast(AsyncMock, worker._recover_worker_future_failure).assert_not_awaited()

    async def test_no_exit_seen_writes_after_a_late_exit_with_stale_recovery_off(
        self,
    ) -> None:
        worker = _worker(RecoveryConfig(auto_fail_stale_running=False))
        child = _ChildDouble()
        handle = _bound_handle(child)
        handle.finish(LostAfterReturn(RETURNED, _lost(ExitUnknown())))

        finalize = asyncio.ensure_future(
            worker._finalize_after(handle, TASK_ID, task_name='left_work_task'),
        )
        await asyncio.sleep(0.05)
        cast(AsyncMock, worker._finalize_task_result).assert_not_awaited()
        child.exited.set()

        assert await asyncio.wait_for(finalize, TEST_WAIT_S) == Ok(None)
        _assert_wrote_returned(worker)

    async def test_a_worker_stop_before_the_late_exit_writes_nothing(self) -> None:
        worker = _worker(RecoveryConfig(auto_fail_stale_running=False))
        handle = _bound_handle(_ChildDouble())
        handle.finish(LostAfterReturn(RETURNED, _lost(ExitUnknown())))
        worker._stop.set()

        result = await asyncio.wait_for(
            worker._finalize_return_after_loss(
                handle,
                TASK_ID,
                RETURNED,
                _lost(ExitUnknown()),
                queue_name='default',
                is_workflow_task=False,
                task_name='left_work_task',
                claimed_at=None,
            ),
            TEST_WAIT_S,
        )

        assert result == Ok(None)
        cast(AsyncMock, worker._finalize_task_result).assert_not_awaited()
