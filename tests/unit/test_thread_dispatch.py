"""The worker side of shared thread tasks: routing, the finalize path, the
backstop timer, the capacity for the claim, and the memory guard.

The slot pool is replaced where the test is about the worker's decisions; the
pool itself is covered with real children in ``test_shared_thread_slot_pool``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

from horsies.core.models.tasks import MIN_TASK_TIMEOUT_MS, Execution
from horsies.core.models.worker_profile import (
    MemoryLimit,
    NoSharedThreadChildren,
    SharedRecycle,
    SharedThreadChildren,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import finalize
from horsies.core.worker.child_protocol import (
    IsolatedChild,
    RssMb,
    SeatRefusal,
    SharedAsyncChild,
    SharedThreadChild,
    ThreadSeatTask,
    ThreadSeatWork,
)
from horsies.core.worker.claim_plan import ClaimPlan, isolated_children_plan
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.dispatch_route import (
    RunIsolated,
    RunSharedThread,
    dispatch_route,
)
from horsies.core.worker.runtime import (
    MemoryBaselineExceedsThresholdError,
    _FINALIZE_STAGE_TIMEOUT,
    _FinalizeError,
    _RequeueOutcome,
    _TimeoutSettled,
    _TimeoutWriteFailed,
)
from horsies.core.worker.shared_finalize import timeout_retry_write
from horsies.core.worker.slots import (
    Assigned,
    ChildBaseline,
    ChildKilledPastDeadlines,
    ChildLost,
    KilledBySignal,
    LossCause,
    SeatDeadlineEnded,
    SharedHandle,
    SlotPool,
)
from horsies.core.worker.stored_timeout import (
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutValueInvalid,
)
from horsies.core.worker.shared_stop import RetryPlacement
from horsies.core.worker.thread_slots import RunCulprit, ThreadRunRequest
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
THREADS = SharedThreadChildren(count=2, seats=3)
THREAD_OPTIONS = json.dumps(
    {'timeout_ms': 2000, 'execution': Execution.SHARED_THREAD.value},
)
THREAD_PLAN = ClaimPlan(
    budget_classes=frozenset({Execution.ISOLATED, Execution.SHARED_THREAD}),
)
ONLY_THREAD_PLAN = ClaimPlan(budget_classes=frozenset({Execution.SHARED_THREAD}))
ASYNC_PLAN = ClaimPlan(
    budget_classes=frozenset({Execution.ISOLATED, Execution.SHARED_ASYNC}),
)
ABSENT_TIMEOUT = TimeoutValueInvalid(
    reason='stored timeout_ms is absent for execution SHARED_THREAD',
)


def _worker(
    shared_thread: SharedThreadChildren | NoSharedThreadChildren = THREADS,
    processes: int = 1,
) -> Worker:
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
        processes=processes,
        shared_thread=shared_thread,
    )
    return Worker(session_factory=MagicMock(), listener=MagicMock(), cfg=cfg)


def _request(work: ThreadSeatWork | None = None) -> ThreadRunRequest:
    return ThreadRunRequest(
        task_id='task-1',
        task_name='hash',
        args_json=None,
        kwargs_json=None,
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=CLAIMED_AT,
        work=work or ThreadSeatTask(timeout_ms=MIN_TASK_TIMEOUT_MS),
    )


def _handle(loop: asyncio.AbstractEventLoop) -> SharedHandle:
    return SharedHandle(_request().frame_for(1), loop)


def _close_coroutine(coro: Coroutine[Any, Any, Any], **_kwargs: object) -> None:
    """Stands in for the spawn of a finalizer: the coroutine never runs."""
    coro.close()


class TestRequest:
    def test_a_timeout_out_of_bounds_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='out of bounds'):
            _request(ThreadSeatTask(timeout_ms=MIN_TASK_TIMEOUT_MS - 1))

    def test_the_frame_carries_the_work_and_the_dispatch(self) -> None:
        frame = _request().frame_for(7)
        assert frame.dispatch == 7
        assert frame.work == ThreadSeatTask(timeout_ms=MIN_TASK_TIMEOUT_MS)


class TestRoute:
    @pytest.mark.parametrize(
        'plan',
        [THREAD_PLAN, ONLY_THREAD_PLAN],
        ids=['isolated-and-thread-children', 'thread-children-only'],
    )
    def test_a_row_of_the_thread_budget_runs_on_a_thread_seat(
        self,
        plan: ClaimPlan,
    ) -> None:
        route = dispatch_route(
            Execution.SHARED_THREAD,
            plan,
            THREAD_OPTIONS,
            TaskTimeout(ms=2000),
        )
        assert route == RunSharedThread(work=ThreadSeatTask(timeout_ms=2000))

    @pytest.mark.parametrize(
        ('timeout', 'refusal'),
        [
            (NoTimeout(), ABSENT_TIMEOUT),
            (OptionsUnreadable(reason='x'), OptionsUnreadable(reason='x')),
            (TimeoutValueInvalid(reason='y'), TimeoutValueInvalid(reason='y')),
        ],
        ids=['no-timeout', 'unreadable-options', 'timeout-out-of-bounds'],
    )
    def test_a_thread_row_with_no_usable_timeout_is_refused_on_a_seat(
        self,
        timeout: StoredTimeout,
        refusal: Any,
    ) -> None:
        route = dispatch_route(Execution.SHARED_THREAD, THREAD_PLAN, None, timeout)
        assert route == RunSharedThread(work=SeatRefusal(refusal=refusal))

    @pytest.mark.parametrize(
        'plan',
        [isolated_children_plan(), ASYNC_PLAN],
        ids=['isolated-children-only', 'isolated-and-async-children'],
    )
    def test_a_worker_with_no_thread_children_runs_it_isolated(
        self,
        plan: ClaimPlan,
    ) -> None:
        route = dispatch_route(
            Execution.SHARED_THREAD,
            plan,
            THREAD_OPTIONS,
            TaskTimeout(ms=2000),
        )
        assert route == RunIsolated(timeout=TaskTimeout(ms=2000))

    def test_an_isolated_child_refuses_a_thread_row_with_no_timeout(self) -> None:
        """The outcome of the row does not depend on the kind of child."""
        route = dispatch_route(
            Execution.SHARED_THREAD,
            isolated_children_plan(),
            None,
            NoTimeout(),
        )
        assert route == RunIsolated(timeout=ABSENT_TIMEOUT)

    def test_an_isolated_row_is_not_served_by_thread_children_only(self) -> None:
        with pytest.raises(AssertionError, match='does not serve'):
            dispatch_route(Execution.ISOLATED, ONLY_THREAD_PLAN, None, NoTimeout())


@pytest.mark.asyncio
class TestDispatch:
    async def test_a_thread_row_is_submitted_to_the_thread_children(self) -> None:
        worker = _worker()
        worker._slots = MagicMock()
        worker._spawn_background = MagicMock()
        worker._finalize_shared_after = MagicMock()

        await worker._dispatch_one(
            'task-9',
            'hash',
            None,
            '{}',
            'cpu',
            False,
            timeout=TaskTimeout(ms=2000),
            claimed_at=CLAIMED_AT,
            execution=Execution.SHARED_THREAD,
            task_options=THREAD_OPTIONS,
        )

        worker._slots.submit.assert_not_called()
        request = worker._slots.submit_shared.call_args.args[0]
        assert request == ThreadRunRequest(
            task_id='task-9',
            task_name='hash',
            args_json=None,
            kwargs_json='{}',
            worker_id=worker.worker_instance_id,
            is_workflow_task=False,
            claimed_at=CLAIMED_AT,
            work=ThreadSeatTask(timeout_ms=2000),
        )
        worker._finalize_shared_after.assert_called_once_with(
            worker._slots.submit_shared.return_value,
            'task-9',
            'cpu',
            False,
            ThreadSeatTask(timeout_ms=2000),
            task_name='hash',
            claimed_at=CLAIMED_AT,
        )
        assert worker._spawn_background.call_args.kwargs['finalizer'] is True

    async def test_a_refused_thread_row_goes_to_a_thread_seat(self) -> None:
        worker = _worker(processes=0)
        worker._slots = MagicMock()
        worker._spawn_background = MagicMock(side_effect=_close_coroutine)

        await worker._dispatch_one(
            'task-9',
            'hash',
            None,
            '{}',
            timeout=NoTimeout(),
            claimed_at=CLAIMED_AT,
            execution=Execution.SHARED_THREAD,
            task_options=None,
        )

        worker._slots.submit.assert_not_called()
        request = worker._slots.submit_shared.call_args.args[0]
        assert request.work == SeatRefusal(refusal=ABSENT_TIMEOUT)

    async def test_with_no_thread_children_the_row_runs_isolated(self) -> None:
        worker = _worker(NoSharedThreadChildren())
        worker._slots = MagicMock()
        worker._spawn_background = MagicMock(side_effect=_close_coroutine)

        await worker._dispatch_one(
            'task-9',
            'hash',
            None,
            '{}',
            timeout=TaskTimeout(ms=2000),
            claimed_at=CLAIMED_AT,
            execution=Execution.SHARED_THREAD,
            task_options=THREAD_OPTIONS,
        )

        worker._slots.submit_shared.assert_not_called()
        assert worker._slots.submit.call_args.args[0].timeout == TaskTimeout(ms=2000)


@pytest.mark.asyncio
class TestFinalizeThread:
    def _worker_with_paths(self) -> Worker:
        worker = _worker()
        worker._finalize_task_result = AsyncMock(return_value=Ok(None))
        worker._record_task_timeout = AsyncMock(return_value=_TimeoutSettled())
        worker._finalize_child_loss = AsyncMock(return_value=Ok(None))
        worker._recover_worker_future_failure = AsyncMock(
            return_value=_RequeueOutcome.REQUEUED,
        )
        return worker

    async def _finalize(
        self,
        worker: Worker,
        outcome: Any,
        work: ThreadSeatWork = ThreadSeatTask(timeout_ms=1500),
    ) -> Any:
        handle = _handle(asyncio.get_running_loop())
        handle.finish(outcome)
        return await worker._finalize_shared_after(
            handle,
            'task-1',
            'default',
            False,
            work,
            task_name='hash',
            claimed_at=CLAIMED_AT,
        )

    @pytest.mark.parametrize(
        ('outcome', 'retry'),
        [
            (
                SeatDeadlineEnded(retry=RetryPlacement.SAME_CLASS),
                RetryPlacement.SAME_CLASS,
            ),
            (
                ChildKilledPastDeadlines(
                    pid=7,
                    child_exit=KilledBySignal(9),
                    culprit=RunCulprit(dispatch=1, task_id='task-1'),
                ),
                RetryPlacement.ISOLATED,
            ),
            (
                ChildKilledPastDeadlines(
                    pid=7,
                    child_exit=KilledBySignal(9),
                    culprit=RunCulprit(dispatch=2, task_id='task-2'),
                ),
                RetryPlacement.SAME_CLASS,
            ),
        ],
        ids=['ended-after-deadline', 'killed-as-culprit', 'killed-as-neighbour'],
    )
    async def test_a_task_past_its_deadline_writes_the_timeout(
        self,
        outcome: Any,
        retry: RetryPlacement,
    ) -> None:
        """The culprit of the kill, the run whose deadline closed the child,
        retries in an isolated child."""
        worker = self._worker_with_paths()

        result = await self._finalize(worker, outcome)

        assert result == Ok(None)
        worker._record_task_timeout.assert_awaited_once_with(  # type: ignore[attr-defined]
            'task-1',
            1500,
            CLAIMED_AT,
            retry=retry,
        )
        worker._finalize_child_loss.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_a_failed_timeout_write_after_the_kill_is_retried_as_that_write(
        self,
    ) -> None:
        """The error keeps the TASK_TIMEOUT write and the placement of the
        culprit; crash recovery does not run at once."""
        worker = self._worker_with_paths()
        worker._record_task_timeout.return_value = _TimeoutWriteFailed()  # type: ignore[attr-defined]
        killed = ChildKilledPastDeadlines(
            pid=7,
            child_exit=KilledBySignal(9),
            culprit=RunCulprit(dispatch=1, task_id='task-1'),
        )

        result: Result[None, _FinalizeError] = await self._finalize(worker, killed)

        match result:
            case Err(err_value=error):
                assert error.stage == _FINALIZE_STAGE_TIMEOUT
                assert timeout_retry_write(error.data) == (
                    1500,
                    RetryPlacement.ISOLATED,
                    CLAIMED_AT,
                )
            case Ok():
                raise AssertionError(result)
        worker._recover_worker_future_failure.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_a_kill_with_no_exit_seen_writes_nothing_here(self) -> None:
        """The pool gives the loss of the child; crash recovery leaves the row
        to stale-task recovery."""
        worker = self._worker_with_paths()
        lost = ChildLost(LossCause.KILLED_BY_PARENT, KilledBySignal(9), False, 7)

        await self._finalize(worker, lost)

        worker._record_task_timeout.assert_not_awaited()  # type: ignore[attr-defined]
        worker._finalize_child_loss.assert_awaited_once_with(  # type: ignore[attr-defined]
            ANY,
            'task-1',
            lost,
            CLAIMED_AT,
        )

    async def test_a_refusal_that_ends_at_a_deadline_is_a_defect(self) -> None:
        worker = self._worker_with_paths()
        refusal = SeatRefusal(refusal=TimeoutValueInvalid(reason='y'))

        with pytest.raises(AssertionError, match='deadline it does not have'):
            await self._finalize(
                worker,
                SeatDeadlineEnded(retry=RetryPlacement.SAME_CLASS),
                refusal,
            )


@pytest.mark.asyncio
class TestBackstopTimer:
    async def test_the_backstop_of_a_thread_task_has_no_grace(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The parent waits timeout + margin; the margin is cut here so the
        test waits 0.1 s. A grace in the sum would add seconds."""
        monkeypatch.setattr(finalize, 'SHARED_BACKSTOP_MARGIN_S', -0.9)
        worker = _worker()
        worker._handle_shared_backstop = AsyncMock(return_value=_TimeoutSettled())
        handle = _handle(asyncio.get_running_loop())
        handle.assigned.set_result(Assigned())
        loop = asyncio.get_running_loop()
        started = loop.time()

        work = ThreadSeatTask(timeout_ms=1000)

        outcome = await worker._await_shared_outcome(handle, 'task-1', work)

        assert outcome == _TimeoutSettled()
        assert 0.05 <= loop.time() - started < 0.5
        worker._handle_shared_backstop.assert_awaited_once_with(handle, 'task-1', work)


class TestCapacityAndConfig:
    def test_the_thread_budget_counts_the_thread_seats_in_service(self) -> None:
        worker = _worker()
        pool = MagicMock(spec=SlotPool)
        pool.slots_in_service.return_value = 1
        pool.shared_thread_capacity.return_value = 5
        worker._slots = pool

        assert worker._capacity_in_service(Execution.SHARED_THREAD) == 5
        assert [
            (entry.budget, entry.in_service) for entry in worker._claim_capacity()
        ] == [(Execution.ISOLATED, 1), (Execution.SHARED_THREAD, 5)]
        pool.shared_async_capacity.assert_not_called()

    def test_the_pool_config_carries_the_thread_children(self) -> None:
        assert _worker()._slot_pool_config().shared_thread == THREADS

    def test_the_seats_and_the_pending_limit_count_the_thread_seats(self) -> None:
        worker = _worker()
        assert worker.cfg.seat_count() == 1 + 2 * 3
        assert worker.cfg.child_count() == 3
        assert worker._pending_task_limit() == 2 * (1 + 2 * 3)

    def test_a_worker_with_only_thread_children_is_valid(self) -> None:
        assert _worker(processes=0).cfg.child_count() == 2


class TestThreadMemoryBaseline:
    def _worker(self) -> Worker:
        return _worker(
            SharedThreadChildren(
                count=1,
                seats=4,
                recycle=SharedRecycle(above=MemoryLimit(mb=100)),
            ),
        )

    def test_a_thread_baseline_at_its_limit_is_refused(self) -> None:
        with pytest.raises(MemoryBaselineExceedsThresholdError, match='shared thread'):
            self._worker()._check_memory_baseline(
                (ChildBaseline(1, RssMb(100.0), SharedThreadChild(seats=4)),),
            )

    @pytest.mark.parametrize(
        'kind',
        [IsolatedChild(), SharedAsyncChild(seats=4)],
        ids=['isolated', 'shared-async'],
    )
    def test_the_thread_limit_applies_to_thread_children_only(
        self,
        kind: IsolatedChild | SharedAsyncChild,
    ) -> None:
        self._worker()._check_memory_baseline((ChildBaseline(1, RssMb(500.0), kind),))
