"""The worker side of shared async tasks: routing, the finalize path, the
backstop of a run, the capacity for the claim, and the memory guard.

The slot pool is replaced where the test is about the worker's decisions; the
pool itself is covered with real children in ``test_shared_slot_pool``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.models.tasks import (
    DEFAULT_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
    Execution,
)
from horsies.core.models.worker_profile import (
    MemoryLimit,
    NoSharedAsyncChildren,
    SharedAsyncChildren,
    SharedRecycle,
)
from horsies.core.worker.claim_plan import ClaimPlan, isolated_children_plan
from horsies.core.types.result import Err, Ok
from horsies.core.worker import finalize
from horsies.core.worker.child_protocol import (
    IsolatedChild,
    RssMb,
    SeatRefusal,
    SeatTask,
    SharedAsyncChild,
)
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.dispatch_route import RunIsolated, RunShared, dispatch_route
from horsies.core.worker.runtime import (
    MemoryBaselineExceedsThresholdError,
    _AttemptEndUnknown,
    _RequeueOutcome,
    _TimeoutSettled,
    _TimeoutWriteFailed,
)
from horsies.core.worker.slots import (
    Assigned,
    ChildBaseline,
    ChildEnded,
    ChildEndUnknown,
    ChildLost,
    DeadlineAcknowledged,
    KilledBySignal,
    LossCause,
    NotDispatched,
    NotRunCause,
    SeatDeadlineEnded,
    SeatTaskFinished,
    SharedAnswered,
    SharedHandle,
    SharedRunRequest,
    SlotPool,
)
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutValueInvalid,
)
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
SHARED = SharedAsyncChildren(count=2, seats=5)


def _options(**fields: Any) -> str:
    return json.dumps(fields)


SHARED_OPTIONS = _options(
    timeout_ms=2000,
    execution=Execution.SHARED_ASYNC.value,
    cancel_grace_ms=700,
)


def _worker(
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren = SHARED,
    processes: int = 1,
) -> Worker:
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
        processes=processes,
        shared_async=shared_async,
    )
    return Worker(session_factory=MagicMock(), listener=MagicMock(), cfg=cfg)


def _request(timeout_ms: int = MIN_TASK_TIMEOUT_MS) -> SharedRunRequest:
    return SharedRunRequest(
        task_id='task-1',
        task_name='fetch',
        args_json=None,
        kwargs_json=None,
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=CLAIMED_AT,
        work=SeatTask(timeout_ms=timeout_ms, cancel_grace_ms=DEFAULT_CANCEL_GRACE_MS),
    )


def _close_coroutine(coro: Coroutine[Any, Any, Any], **_kwargs: object) -> None:
    """Stands in for the spawn of a finalizer: the coroutine never runs."""
    coro.close()


def _handle(loop: asyncio.AbstractEventLoop) -> SharedHandle:
    return SharedHandle(_request().frame_for(1), loop)


SPLIT_PLAN = ClaimPlan(
    budget_classes=frozenset({Execution.ISOLATED, Execution.SHARED_ASYNC}),
)


ONLY_SHARED_PLAN = ClaimPlan(budget_classes=frozenset({Execution.SHARED_ASYNC}))

REFUSED_SHARED_ROWS = [
    pytest.param(
        SHARED_OPTIONS,
        NoTimeout(),
        TimeoutValueInvalid(
            reason='stored timeout_ms is absent for execution SHARED_ASYNC',
        ),
        id='no-timeout',
    ),
    pytest.param(
        '{not json',
        OptionsUnreadable(reason='x'),
        OptionsUnreadable(reason='x'),
        id='unreadable-options',
    ),
    pytest.param(
        SHARED_OPTIONS,
        TimeoutValueInvalid(reason='y'),
        TimeoutValueInvalid(reason='y'),
        id='timeout-out-of-bounds',
    ),
    pytest.param(
        _options(timeout_ms=2000, execution='SHARED_ASYNC', cancel_grace_ms=1),
        TaskTimeout(ms=2000),
        CancelGraceInvalid(reason='stored cancel_grace_ms is below 100 ms'),
        id='grace-out-of-bounds',
    ),
    pytest.param(
        _options(timeout_ms=2000, execution='SHARED_ASYNC', cancel_grace_ms='5'),
        TaskTimeout(ms=2000),
        CancelGraceInvalid(reason='stored cancel_grace_ms is a JSON str, not an int'),
        id='grace-not-an-int',
    ),
    pytest.param(
        _options(timeout_ms=2000, execution='SHARED_ASYNC'),
        TaskTimeout(ms=2000),
        CancelGraceInvalid(
            reason='stored cancel_grace_ms is absent for execution SHARED_ASYNC',
        ),
        id='grace-absent',
    ),
    pytest.param(
        None,
        TaskTimeout(ms=2000),
        CancelGraceInvalid(
            reason='stored cancel_grace_ms is absent for execution SHARED_ASYNC',
        ),
        id='no-options',
    ),
]


class TestRoute:
    def test_a_row_of_the_shared_async_budget_runs_on_a_seat(self) -> None:
        route = dispatch_route(
            Execution.SHARED_ASYNC,
            SPLIT_PLAN,
            SHARED_OPTIONS,
            TaskTimeout(ms=2000),
        )
        assert route == RunShared(
            work=SeatTask(timeout_ms=2000, cancel_grace_ms=700),
        )

    def test_a_worker_with_no_shared_children_runs_it_isolated(self) -> None:
        route = dispatch_route(
            Execution.SHARED_ASYNC,
            isolated_children_plan(),
            SHARED_OPTIONS,
            TaskTimeout(ms=2000),
        )
        assert route == RunIsolated(timeout=TaskTimeout(ms=2000))

    @pytest.mark.parametrize(
        'timeout',
        [NoTimeout(), TaskTimeout(ms=2000), TimeoutValueInvalid(reason='y')],
        ids=['no-timeout', 'timeout', 'refused-timeout'],
    )
    def test_an_isolated_row_keeps_its_stored_timeout(
        self,
        timeout: StoredTimeout,
    ) -> None:
        route = dispatch_route(Execution.ISOLATED, SPLIT_PLAN, None, timeout)
        assert route == RunIsolated(timeout=timeout)

    @pytest.mark.parametrize(
        'plan',
        [SPLIT_PLAN, ONLY_SHARED_PLAN],
        ids=['isolated-and-shared-children', 'shared-children-only'],
    )
    @pytest.mark.parametrize(('options', 'timeout', 'refusal'), REFUSED_SHARED_ROWS)
    def test_a_shared_row_with_no_usable_timeout_or_grace_is_refused_on_a_seat(
        self,
        plan: ClaimPlan,
        options: str | None,
        timeout: StoredTimeout,
        refusal: Any,
    ) -> None:
        route = dispatch_route(Execution.SHARED_ASYNC, plan, options, timeout)
        assert route == RunShared(work=SeatRefusal(refusal=refusal))

    @pytest.mark.parametrize(('options', 'timeout', 'refusal'), REFUSED_SHARED_ROWS)
    def test_a_worker_with_no_shared_children_refuses_it_in_an_isolated_child(
        self,
        options: str | None,
        timeout: StoredTimeout,
        refusal: Any,
    ) -> None:
        route = dispatch_route(
            Execution.SHARED_ASYNC,
            isolated_children_plan(),
            options,
            timeout,
        )
        assert route == RunIsolated(timeout=refusal)

    def test_a_class_that_the_worker_does_not_serve_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='does not serve'):
            dispatch_route(Execution.ISOLATED, ONLY_SHARED_PLAN, None, NoTimeout())


@pytest.mark.asyncio
class TestDispatch:
    async def test_a_shared_row_is_submitted_to_the_shared_children(self) -> None:
        worker = _worker()
        worker._slots = MagicMock()
        worker._spawn_background = MagicMock()
        worker._finalize_shared_after = MagicMock()

        await worker._dispatch_one(
            'task-9',
            'fetch',
            None,
            '{}',
            'io',
            False,
            timeout=TaskTimeout(ms=2000),
            claimed_at=CLAIMED_AT,
            execution=Execution.SHARED_ASYNC,
            task_options=SHARED_OPTIONS,
        )

        worker._slots.submit.assert_not_called()
        request = worker._slots.submit_shared.call_args.args[0]
        assert request == SharedRunRequest(
            task_id='task-9',
            task_name='fetch',
            args_json=None,
            kwargs_json='{}',
            worker_id=worker.worker_instance_id,
            is_workflow_task=False,
            claimed_at=CLAIMED_AT,
            work=SeatTask(timeout_ms=2000, cancel_grace_ms=700),
        )
        worker._finalize_shared_after.assert_called_once_with(
            worker._slots.submit_shared.return_value,
            'task-9',
            'io',
            False,
            SeatTask(timeout_ms=2000, cancel_grace_ms=700),
            task_name='fetch',
            claimed_at=CLAIMED_AT,
        )
        assert worker._spawn_background.call_args.kwargs['finalizer'] is True

    async def test_with_no_shared_children_the_row_runs_isolated(self) -> None:
        worker = _worker(NoSharedAsyncChildren())
        worker._slots = MagicMock()
        worker._spawn_background = MagicMock(
            side_effect=_close_coroutine,
        )

        await worker._dispatch_one(
            'task-9',
            'fetch',
            None,
            '{}',
            timeout=TaskTimeout(ms=2000),
            claimed_at=CLAIMED_AT,
            execution=Execution.SHARED_ASYNC,
            task_options=SHARED_OPTIONS,
        )

        worker._slots.submit_shared.assert_not_called()
        assert worker._slots.submit.call_args.args[0].task_id == 'task-9'

    @pytest.mark.parametrize(
        'processes',
        [1, 0],
        ids=['isolated-and-shared-children', 'shared-children-only'],
    )
    async def test_a_refused_shared_row_goes_to_a_seat_on_either_shape(
        self,
        processes: int,
    ) -> None:
        """Never to an isolated child, and never back to the queue."""
        worker = _worker(processes=processes)
        worker._slots = MagicMock()
        worker._spawn_background = MagicMock(side_effect=_close_coroutine)
        worker._requeue_claimed_task = AsyncMock()
        refusal = CancelGraceInvalid(
            reason='stored cancel_grace_ms is absent for execution SHARED_ASYNC',
        )

        await worker._dispatch_one(
            'task-9',
            'fetch',
            None,
            '{}',
            timeout=TaskTimeout(ms=2000),
            claimed_at=CLAIMED_AT,
            execution=Execution.SHARED_ASYNC,
            task_options=_options(timeout_ms=2000, execution='SHARED_ASYNC'),
        )

        worker._slots.submit.assert_not_called()
        worker._requeue_claimed_task.assert_not_awaited()
        request = worker._slots.submit_shared.call_args.args[0]
        assert request.work == SeatRefusal(refusal=refusal)

    async def test_a_worker_with_no_shared_children_sends_the_refusal_isolated(
        self,
    ) -> None:
        worker = _worker(NoSharedAsyncChildren())
        worker._slots = MagicMock()
        worker._spawn_background = MagicMock(side_effect=_close_coroutine)

        await worker._dispatch_one(
            'task-9',
            'fetch',
            None,
            '{}',
            timeout=NoTimeout(),
            claimed_at=CLAIMED_AT,
            execution=Execution.SHARED_ASYNC,
            task_options=SHARED_OPTIONS,
        )

        worker._slots.submit_shared.assert_not_called()
        assert worker._slots.submit.call_args.args[0].timeout == TimeoutValueInvalid(
            reason='stored timeout_ms is absent for execution SHARED_ASYNC',
        )


@pytest.mark.asyncio
class TestFinalizeShared:
    def _worker_with_paths(self) -> Worker:
        worker = _worker()
        worker._finalize_task_result = AsyncMock(return_value=Ok(None))
        worker._record_task_timeout = AsyncMock(return_value=_TimeoutSettled())
        worker._finalize_child_loss = AsyncMock(return_value=Ok(None))
        worker._requeue_claimed_task = AsyncMock(
            return_value=_RequeueOutcome.REQUEUED,
        )
        worker._recover_worker_future_failure = AsyncMock(
            return_value=_RequeueOutcome.REQUEUED,
        )
        return worker

    async def _finalize(
        self,
        worker: Worker,
        outcome: Any,
        work: SeatTask | SeatRefusal = SeatTask(timeout_ms=1500, cancel_grace_ms=500),
    ) -> Any:
        handle = _handle(asyncio.get_running_loop())
        handle.finish(outcome)
        return await worker._finalize_shared_after(
            handle,
            'task-1',
            'default',
            False,
            work,
            task_name='fetch',
            claimed_at=CLAIMED_AT,
        )

    async def test_a_result_takes_the_path_of_an_isolated_result(self) -> None:
        worker = self._worker_with_paths()

        await self._finalize(worker, SeatTaskFinished(True, '{"x":1}', None))

        worker._finalize_task_result.assert_awaited_once_with(  # type: ignore[attr-defined]
            'task-1',
            True,
            '{"x":1}',
            None,
            queue_name='default',
            is_workflow_task=False,
            task_name='fetch',
            claimed_at=CLAIMED_AT,
        )

    async def test_a_reported_deadline_writes_the_timeout(self) -> None:
        worker = self._worker_with_paths()

        result = await self._finalize(worker, SeatDeadlineEnded())

        assert result == Ok(None)
        worker._record_task_timeout.assert_awaited_once_with(  # type: ignore[attr-defined]
            'task-1',
            1500,
            CLAIMED_AT,
        )
        worker._recover_worker_future_failure.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_a_failed_timeout_write_takes_crash_recovery(self) -> None:
        worker = self._worker_with_paths()
        worker._record_task_timeout.return_value = _TimeoutWriteFailed()  # type: ignore[attr-defined]

        result = await self._finalize(worker, SeatDeadlineEnded())

        assert isinstance(result, Err)
        worker._recover_worker_future_failure.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_the_result_of_a_refusal_takes_the_path_of_an_isolated_result(
        self,
    ) -> None:
        worker = self._worker_with_paths()
        refusal = SeatRefusal(refusal=TimeoutValueInvalid(reason='y'))

        await self._finalize(
            worker,
            SeatTaskFinished(True, '{"err":1}', 'StoredTimeoutRefused: y'),
            refusal,
        )

        worker._finalize_task_result.assert_awaited_once_with(  # type: ignore[attr-defined]
            'task-1',
            True,
            '{"err":1}',
            'StoredTimeoutRefused: y',
            queue_name='default',
            is_workflow_task=False,
            task_name='fetch',
            claimed_at=CLAIMED_AT,
        )

    async def test_a_refusal_that_ends_at_a_deadline_is_a_defect(self) -> None:
        """The pool refuses such a report, so it never reaches here."""
        worker = self._worker_with_paths()
        refusal = SeatRefusal(refusal=TimeoutValueInvalid(reason='y'))

        with pytest.raises(AssertionError, match='deadline it does not have'):
            await self._finalize(worker, SeatDeadlineEnded(), refusal)

        worker._record_task_timeout.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_a_lost_child_takes_crash_recovery(self) -> None:
        worker = self._worker_with_paths()
        lost = ChildLost(LossCause.CHILD_DIED, KilledBySignal(9), False, 42)

        await self._finalize(worker, lost)

        worker._finalize_child_loss.assert_awaited_once_with(  # type: ignore[attr-defined]
            'task-1',
            lost,
            CLAIMED_AT,
        )

    async def test_a_task_that_reached_no_child_is_requeued(self) -> None:
        worker = self._worker_with_paths()

        await self._finalize(worker, NotDispatched(NotRunCause.POOL_STOPPING))

        worker._requeue_claimed_task.assert_awaited_once()  # type: ignore[attr-defined]
        worker._record_task_timeout.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
class TestBackstopTimer:
    async def test_the_backstop_runs_after_the_deadline_and_the_grace(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The parent waits timeout + grace + margin; the margin is cut here so
        the test waits 0.1 s."""
        monkeypatch.setattr(finalize, 'SHARED_BACKSTOP_MARGIN_S', -1.4)
        worker = _worker()
        worker._handle_shared_backstop = AsyncMock(
            return_value=_TimeoutSettled(),
        )
        handle = _handle(asyncio.get_running_loop())
        handle.assigned.set_result(Assigned())
        loop = asyncio.get_running_loop()
        started = loop.time()

        outcome = await worker._await_shared_outcome(
            handle,
            'task-1',
            SeatTask(timeout_ms=1000, cancel_grace_ms=500),
            CLAIMED_AT,
        )

        assert outcome == _TimeoutSettled()
        assert 0.05 <= loop.time() - started < 1.0
        worker._handle_shared_backstop.assert_awaited_once_with(
            handle,
            'task-1',
            1000,
            CLAIMED_AT,
        )

    async def test_an_outcome_before_the_backstop_removes_the_timer(self) -> None:
        worker = _worker()
        worker._handle_shared_backstop = AsyncMock()
        handle = _handle(asyncio.get_running_loop())
        finished = SeatTaskFinished(True, '1', None)
        handle.finish(finished)

        outcome = await worker._await_shared_outcome(
            handle,
            'task-1',
            SeatTask(timeout_ms=1000, cancel_grace_ms=500),
            CLAIMED_AT,
        )

        assert outcome == finished
        worker._handle_shared_backstop.assert_not_awaited()

    async def test_a_refusal_has_no_backstop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No task code runs, so the parent waits for the outcome, as for an
        isolated refusal; a backstop at the cut margin would fire at once."""
        monkeypatch.setattr(finalize, 'SHARED_BACKSTOP_MARGIN_S', -100.0)
        worker = _worker()
        worker._handle_shared_backstop = AsyncMock()
        handle = _handle(asyncio.get_running_loop())
        handle.assigned.set_result(Assigned())
        finished = SeatTaskFinished(True, '1', None)
        asyncio.get_running_loop().call_later(0.1, handle.finish, finished)

        outcome = await worker._await_shared_outcome(
            handle,
            'task-1',
            SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
            CLAIMED_AT,
        )

        assert outcome == finished
        worker._handle_shared_backstop.assert_not_awaited()


@pytest.mark.asyncio
class TestBackstopDecision:
    async def _backstop(self, worker: Worker, kill: Any, handle: SharedHandle) -> Any:
        handle.backstop_kill = AsyncMock(return_value=kill)
        return await worker._handle_shared_backstop(handle, 'task-1', 1000, CLAIMED_AT)

    async def test_a_frozen_child_killed_and_seen_exiting_writes_the_timeout(
        self,
    ) -> None:
        worker = _worker()
        worker._record_task_timeout = AsyncMock(return_value=_TimeoutSettled())

        outcome = await self._backstop(
            worker,
            ChildEnded(pid=7, child_exit=KilledBySignal(9)),
            _handle(asyncio.get_running_loop()),
        )

        assert outcome == _TimeoutSettled()
        worker._record_task_timeout.assert_awaited_once_with('task-1', 1000, CLAIMED_AT)

    async def test_no_exit_seen_writes_nothing(self) -> None:
        worker = _worker()
        worker._record_task_timeout = AsyncMock()

        outcome = await self._backstop(
            worker,
            ChildEndUnknown(pid=7),
            _handle(asyncio.get_running_loop()),
        )

        assert outcome == _AttemptEndUnknown()
        worker._record_task_timeout.assert_not_awaited()

    async def test_a_live_child_is_not_killed_and_the_outcome_is_awaited(self) -> None:
        worker = _worker()
        worker._record_task_timeout = AsyncMock()
        handle = _handle(asyncio.get_running_loop())
        asyncio.get_running_loop().call_later(
            0.02,
            handle.finish,
            SeatDeadlineEnded(),
        )

        outcome = await self._backstop(worker, DeadlineAcknowledged(pid=7), handle)

        assert outcome == SeatDeadlineEnded()
        worker._record_task_timeout.assert_not_awaited()

    async def test_a_run_that_already_ended_gives_its_outcome(self) -> None:
        worker = _worker()
        finished = SeatTaskFinished(True, '1', None)

        outcome = await self._backstop(
            worker,
            SharedAnswered(finished),
            _handle(asyncio.get_running_loop()),
        )

        assert outcome == finished

    async def test_a_failed_write_after_the_kill_gives_the_loss_of_the_child(
        self,
    ) -> None:
        worker = _worker()
        worker._record_task_timeout = AsyncMock(return_value=_TimeoutWriteFailed())
        handle = _handle(asyncio.get_running_loop())
        lost = ChildLost(LossCause.KILLED_BY_PARENT, KilledBySignal(9), False, 7)
        handle.finish(lost)

        outcome = await self._backstop(
            worker,
            ChildEnded(pid=7, child_exit=KilledBySignal(9)),
            handle,
        )

        assert outcome == lost


class TestCapacityAndConfig:
    def test_each_budget_counts_its_own_children(self) -> None:
        worker = _worker()
        pool = MagicMock(spec=SlotPool)
        pool.slots_in_service.return_value = 1
        pool.shared_async_capacity.return_value = 7
        worker._slots = pool

        assert worker._capacity_in_service(Execution.ISOLATED) == 1
        assert worker._capacity_in_service(Execution.SHARED_ASYNC) == 7
        assert [
            (entry.budget, entry.in_service) for entry in worker._claim_capacity()
        ] == [(Execution.ISOLATED, 1), (Execution.SHARED_ASYNC, 7)]

    def test_the_pool_config_carries_the_shared_children(self) -> None:
        worker = _worker()
        assert worker._slot_pool_config().shared_async == SHARED
        assert worker._slot_pool_config().slots == 1

    def test_the_pending_limit_counts_the_seats(self) -> None:
        assert _worker()._pending_task_limit() == 2 * (1 + 2 * 5)

    def test_a_worker_with_only_shared_children_is_valid(self) -> None:
        worker = _worker(processes=0)
        assert worker.cfg.seat_count() == 10

    def test_a_worker_with_no_child_is_refused(self) -> None:
        with pytest.raises(ValueError, match='at least one child'):
            _worker(NoSharedAsyncChildren(), processes=0)


class TestSharedMemoryBaseline:
    def _worker(self, shared_mb: int | None) -> Worker:
        recycle = (
            SharedRecycle(above=MemoryLimit(mb=shared_mb))
            if shared_mb is not None
            else SharedRecycle()
        )
        return _worker(SharedAsyncChildren(count=1, seats=4, recycle=recycle))

    def test_a_shared_baseline_at_its_limit_is_refused(self) -> None:
        with pytest.raises(MemoryBaselineExceedsThresholdError, match='shared async'):
            self._worker(100)._check_memory_baseline(
                (ChildBaseline(1, RssMb(100.0), SharedAsyncChild(seats=4)),),
            )

    def test_the_shared_limit_does_not_apply_to_isolated_children(self) -> None:
        self._worker(100)._check_memory_baseline(
            (ChildBaseline(1, RssMb(500.0), IsolatedChild()),),
        )

    def test_the_isolated_limit_does_not_apply_to_shared_children(self) -> None:
        worker = self._worker(None)
        worker.cfg.max_memory_per_child_mb = 100
        worker._check_memory_baseline(
            (ChildBaseline(1, RssMb(500.0), SharedAsyncChild(seats=4)),),
        )
