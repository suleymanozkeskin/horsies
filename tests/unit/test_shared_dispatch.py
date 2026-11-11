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
from typing import Any, cast
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import (
    DEFAULT_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
    Execution,
    OperationalErrorCode,
    OutcomeCode,
)
from horsies.core.models.worker_profile import (
    ResolvedPool,
    MemoryLimit,
    NoSharedAsyncChildren,
    SharedAsyncChildren,
    SharedRecycle,
)
from horsies.core.worker.claim_plan import ClaimPlan, isolated_children_plan
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import dispatch as dispatch_module
from horsies.core.worker import finalize
from horsies.core.worker.child_protocol import (
    IsolatedChild,
    RssMb,
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SharedAsyncChild,
    ThreadSeatTask,
)
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.dispatch_route import RunIsolated, RunShared, dispatch_route
from horsies.core.worker.runtime import (
    MemoryBaselineExceedsThresholdError,
    _FINALIZE_STAGE_FUTURE,
    _FINALIZE_STAGE_TIMEOUT,
    _FINALIZE_TIMEOUT_MAX_RETRIES,
    _AttemptEndUnknown,
    _FinalizeError,
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
    SeatKilled,
    SeatTaskFinished,
    SharedAnswered,
    SharedHandle,
    SharedRunRequest,
    SlotPool,
    StuckAcknowledged,
)
from horsies.core.worker.shared_finalize import (
    future_retry_data,
    future_retry_write,
    timeout_retry_write,
)
from horsies.core.worker.shared_stop import (
    BesideBlockedLoop,
    BlockedTheLoop,
    RetryPlacement,
    ReturnedBeforeKill,
    TimedOutAtKill,
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

_POOL = ResolvedPool(min_size=0, max_size=2, check=True)
_ASYNC_KIND = SharedAsyncChild(seats=4, pool=_POOL, executor_threads=4, task_send_pool=2)

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

    @pytest.mark.parametrize('retry', list(RetryPlacement))
    async def test_a_reported_deadline_writes_the_timeout(
        self,
        retry: RetryPlacement,
    ) -> None:
        """The retry of a run that the child reported stuck runs isolated."""
        worker = self._worker_with_paths()

        result = await self._finalize(worker, SeatDeadlineEnded(retry=retry))

        assert result == Ok(None)
        worker._record_task_timeout.assert_awaited_once_with(  # type: ignore[attr-defined]
            'task-1',
            1500,
            CLAIMED_AT,
            retry=retry,
        )
        worker._recover_worker_future_failure.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_a_failed_timeout_write_is_retried_as_the_timeout_write(
        self,
    ) -> None:
        """Crash recovery does not write WORKER_CRASHED at once: the error
        keeps the TASK_TIMEOUT write, and its retries run it again."""
        worker = self._worker_with_paths()
        worker._record_task_timeout.return_value = _TimeoutWriteFailed()  # type: ignore[attr-defined]

        result: Result[None, _FinalizeError] = await self._finalize(
            worker,
            SeatDeadlineEnded(retry=RetryPlacement.ISOLATED),
        )

        match result:
            case Err(err_value=error):
                assert error.stage == _FINALIZE_STAGE_TIMEOUT
                assert error.retryable is True
                assert timeout_retry_write(error.data) == (
                    1500,
                    RetryPlacement.ISOLATED,
                    CLAIMED_AT,
                )
            case Ok():
                raise AssertionError(result)
        worker._recover_worker_future_failure.assert_not_awaited()  # type: ignore[attr-defined]

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
            await self._finalize(
                worker,
                SeatDeadlineEnded(retry=RetryPlacement.SAME_CLASS),
                refusal,
            )

        worker._record_task_timeout.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_a_lost_child_takes_crash_recovery(self) -> None:
        worker = self._worker_with_paths()
        lost = ChildLost(LossCause.CHILD_DIED, KilledBySignal(9), False, 42)

        await self._finalize(worker, lost)

        worker._finalize_child_loss.assert_awaited_once_with(  # type: ignore[attr-defined]
            ANY,
            'task-1',
            lost,
            CLAIMED_AT,
        )

    async def test_a_task_that_reached_no_child_is_requeued(self) -> None:
        worker = self._worker_with_paths()

        await self._finalize(worker, NotDispatched(NotRunCause.POOL_STOPPING))

        worker._requeue_claimed_task.assert_awaited_once()  # type: ignore[attr-defined]
        worker._record_task_timeout.assert_not_awaited()  # type: ignore[attr-defined]


class _KillWrites:
    """A worker whose writers are mocks, kept typed for the asserts."""

    def __init__(self) -> None:
        self.worker = _worker()
        self.timeout = AsyncMock(return_value=_TimeoutSettled())
        self.recovery = AsyncMock(return_value=_RequeueOutcome.REQUEUED)
        self.result = AsyncMock(return_value=Ok(None))
        self.worker._record_task_timeout = self.timeout
        self.worker._recover_worker_future_failure = self.recovery
        self.worker._finalize_task_result = self.result

    async def finalize(
        self,
        verdict: TimedOutAtKill
        | BlockedTheLoop
        | BesideBlockedLoop
        | ReturnedBeforeKill,
        work: SeatTask | SeatRefusal = SeatTask(timeout_ms=1500, cancel_grace_ms=500),
    ) -> Result[None, _FinalizeError]:
        handle = _handle(asyncio.get_running_loop())
        handle.finish(SeatKilled(verdict=verdict, pid=7, child_exit=KilledBySignal(9)))
        return await self.worker._finalize_shared_after(
            handle,
            'task-1',
            'default',
            False,
            work,
            task_name='fetch',
            claimed_at=CLAIMED_AT,
        )


@pytest.mark.asyncio
class TestFinalizeKilledRun:
    """The verdicts of a run whose child the worker killed and saw exit."""

    @pytest.mark.parametrize('retry', list(RetryPlacement))
    async def test_a_run_past_its_deadline_writes_the_timeout(
        self,
        retry: RetryPlacement,
    ) -> None:
        writes = _KillWrites()

        result = await writes.finalize(TimedOutAtKill(retry=retry))

        assert result == Ok(None)
        writes.timeout.assert_awaited_once_with(
            'task-1',
            1500,
            CLAIMED_AT,
            retry=retry,
        )
        writes.recovery.assert_not_awaited()

    async def test_the_run_that_blocked_the_loop_retries_isolated(self) -> None:
        writes = _KillWrites()

        result = await writes.finalize(
            BlockedTheLoop(blocked_ms=5100, where='app.py:7 in fetch'),
        )

        assert result == Ok(None)
        writes.recovery.assert_awaited_once()
        assert writes.recovery.await_args is not None
        assert writes.recovery.await_args.kwargs == {
            'claimed_at': CLAIMED_AT,
            'failure_code': OperationalErrorCode.TASK_BLOCKED_LOOP,
            'retry': RetryPlacement.ISOLATED,
        }
        assert 'app.py:7 in fetch' in writes.recovery.await_args.args[1]
        writes.timeout.assert_not_awaited()

    async def test_a_neighbour_of_a_blocked_loop_gets_its_own_code(self) -> None:
        writes = _KillWrites()

        await writes.finalize(
            BesideBlockedLoop(holder_task_id='task-9', blocked_ms=5100),
        )

        assert writes.recovery.await_args is not None
        assert writes.recovery.await_args.kwargs == {
            'claimed_at': CLAIMED_AT,
            'failure_code': OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP,
            'retry': RetryPlacement.SAME_CLASS,
        }
        assert 'Task task-9 held the event loop' in writes.recovery.await_args.args[1]

    async def test_a_run_that_returned_before_the_kill_writes_its_result(self) -> None:
        """The work that the run left was stuck; the run itself returned in
        time: no timeout, no crash recovery, and no isolated retry."""
        writes = _KillWrites()
        returned = SeatFinished(ok=True, result_json='{"x":1}', failed_reason=None)

        result = await writes.finalize(ReturnedBeforeKill(returned=returned))

        assert result == Ok(None)
        writes.result.assert_awaited_once_with(
            'task-1',
            True,
            '{"x":1}',
            None,
            queue_name='default',
            is_workflow_task=False,
            task_name='fetch',
            claimed_at=CLAIMED_AT,
        )
        writes.timeout.assert_not_awaited()
        writes.recovery.assert_not_awaited()

    async def test_a_database_error_of_the_write_is_retryable(self) -> None:
        """The error keeps the verdict, so its retry writes the same code in
        the same class, not the crash code."""
        writes = _KillWrites()
        writes.recovery.return_value = _RequeueOutcome.DB_ERROR

        result = await writes.finalize(
            BlockedTheLoop(blocked_ms=5100, where='app.py:7 in fetch'),
        )

        match result:
            case Err(err_value=error):
                assert error.retryable is True
                assert future_retry_write(error.data) == (
                    OperationalErrorCode.TASK_BLOCKED_LOOP,
                    RetryPlacement.ISOLATED,
                )
            case Ok():
                raise AssertionError(result)

    async def test_the_retry_of_a_failed_kill_write_writes_the_same_verdict(
        self,
    ) -> None:
        writes = _KillWrites()
        error = writes.worker._make_finalize_error(
            task_id='task-1',
            stage=_FINALIZE_STAGE_FUTURE,
            message='kill write failed',
            retryable=True,
            data=future_retry_data(
                OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP,
                RetryPlacement.SAME_CLASS,
                CLAIMED_AT,
            ),
        )

        await writes.worker._retry_finalize_future(error, 0.0)

        assert writes.recovery.await_args is not None
        assert writes.recovery.await_args.kwargs == {
            'claimed_at': CLAIMED_AT,
            'failure_code': OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP,
            'retry': RetryPlacement.SAME_CLASS,
        }

    async def test_a_timeout_code_is_kept_for_the_retry_of_crash_recovery(
        self,
    ) -> None:
        data = future_retry_data(
            OutcomeCode.TASK_TIMEOUT,
            RetryPlacement.ISOLATED,
            CLAIMED_AT,
        )

        assert future_retry_write(data) == (
            OutcomeCode.TASK_TIMEOUT,
            RetryPlacement.ISOLATED,
        )

    async def test_a_crash_error_retries_as_a_crash(self) -> None:
        assert future_retry_write({'claimed_at': None}) == (
            OperationalErrorCode.WORKER_CRASHED,
            RetryPlacement.SAME_CLASS,
        )
        assert future_retry_write(None) == (
            OperationalErrorCode.WORKER_CRASHED,
            RetryPlacement.SAME_CLASS,
        )

    async def test_a_refusal_past_a_deadline_is_a_defect(self) -> None:
        """A refusal has no deadline, so the slot never gives this verdict."""
        writes = _KillWrites()

        with pytest.raises(AssertionError, match='deadline it does not have'):
            await writes.finalize(
                TimedOutAtKill(retry=RetryPlacement.SAME_CLASS),
                SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
            )


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
        killed = SeatKilled(
            verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS),
            pid=7,
            child_exit=KilledBySignal(9),
        )
        worker._handle_shared_backstop = AsyncMock(return_value=killed)
        handle = _handle(asyncio.get_running_loop())
        handle.assigned.set_result(Assigned())
        loop = asyncio.get_running_loop()
        started = loop.time()
        work = SeatTask(timeout_ms=1000, cancel_grace_ms=500)

        outcome = await worker._await_shared_outcome(handle, 'task-1', work)

        assert outcome == killed
        assert 0.05 <= loop.time() - started < 1.0
        worker._handle_shared_backstop.assert_awaited_once_with(handle, 'task-1', work)

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
        )

        assert outcome == finished
        worker._handle_shared_backstop.assert_not_awaited()

    async def test_a_refusal_has_its_own_backstop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A refusal has no deadline. A child that freezes while it holds only
        refusals is still ended: at the refusal backstop, cut here to 0.05 s."""
        monkeypatch.setattr(finalize, 'SHARED_REFUSAL_BACKSTOP_S', 0.05)
        worker = _worker()
        lost = ChildLost(LossCause.KILLED_BY_PARENT, KilledBySignal(9), False, 7)
        worker._handle_shared_backstop = AsyncMock(return_value=lost)
        handle = _handle(asyncio.get_running_loop())
        handle.assigned.set_result(Assigned())
        refusal = SeatRefusal(refusal=TimeoutValueInvalid(reason='y'))

        outcome = await asyncio.wait_for(
            worker._await_shared_outcome(handle, 'task-1', refusal),
            timeout=5.0,
        )

        assert outcome == lost
        worker._handle_shared_backstop.assert_awaited_once_with(
            handle,
            'task-1',
            refusal,
        )

    async def test_a_refusal_that_ends_before_its_backstop_is_not_killed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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
        )

        assert outcome == finished
        worker._handle_shared_backstop.assert_not_awaited()


# A wait that the code under test must end by itself fails the test past this
# time, instead of hanging it.
OUTCOME_BOUND_S = 5.0


@pytest.mark.asyncio
class TestBackstopDecision:
    WORK = SeatTask(timeout_ms=1000, cancel_grace_ms=500)

    async def _backstop(self, worker: Worker, kill: Any, handle: SharedHandle) -> Any:
        handle.backstop_kill = AsyncMock(return_value=kill)
        return await worker._handle_shared_backstop(handle, 'task-1', self.WORK)

    async def test_a_frozen_child_killed_and_seen_exiting_gives_the_slot_verdict(
        self,
    ) -> None:
        """The slot answers each run of the killed child; this run is past its
        deadline, so its verdict is the timeout. Nothing is written here."""
        worker = _worker()
        worker._record_task_timeout = AsyncMock()
        handle = _handle(asyncio.get_running_loop())
        killed = SeatKilled(
            verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS),
            pid=7,
            child_exit=KilledBySignal(9),
        )
        handle.finish(killed)

        outcome = await self._backstop(
            worker,
            ChildEnded(pid=7, child_exit=KilledBySignal(9)),
            handle,
        )

        assert outcome == killed
        worker._record_task_timeout.assert_not_awaited()

    async def test_no_exit_seen_writes_nothing(self) -> None:
        """With stale RUNNING recovery on, the row is left to it."""
        worker = _worker()
        worker.cfg.recovery_config = RecoveryConfig()
        worker._record_task_timeout = AsyncMock()

        outcome = await self._backstop(
            worker,
            ChildEndUnknown(pid=7),
            _handle(asyncio.get_running_loop()),
        )

        assert outcome == _AttemptEndUnknown()
        worker._record_task_timeout.assert_not_awaited()

    async def test_with_stale_recovery_off_a_late_exit_gives_the_kill_verdict(
        self,
    ) -> None:
        """The run was past its deadline at the backstop: once the exit is
        seen, it times out in its class, as at a seen exit."""
        worker = _worker()
        handle = _handle(asyncio.get_running_loop())
        handle.wait_child_exit = AsyncMock(return_value=KilledBySignal(9))

        outcome = await asyncio.wait_for(
            self._backstop(worker, ChildEndUnknown(pid=7), handle),
            OUTCOME_BOUND_S,
        )

        assert outcome == SeatKilled(
            verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS),
            pid=7,
            child_exit=KilledBySignal(9),
        )

    async def test_a_live_child_is_not_killed_and_the_outcome_is_awaited(self) -> None:
        worker = _worker()
        worker._record_task_timeout = AsyncMock()
        handle = _handle(asyncio.get_running_loop())
        ended = SeatDeadlineEnded(retry=RetryPlacement.ISOLATED)
        asyncio.get_running_loop().call_later(0.02, handle.finish, ended)

        outcome = await self._backstop(worker, DeadlineAcknowledged(pid=7), handle)

        assert outcome == ended
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

    async def test_a_child_silent_after_the_acknowledged_cancel_gets_the_stuck_backstop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The child reported the second cancel, then froze: no stuck report
        and no end come. The stuck backstop kills it, and the slot verdict
        is the outcome."""
        monkeypatch.setattr(dispatch_module, 'SHARED_STUCK_BACKSTOP_S', 0.01)
        worker = _worker()
        handle = _handle(asyncio.get_running_loop())
        killed = SeatKilled(
            verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS),
            pid=7,
            child_exit=KilledBySignal(9),
        )

        async def kill_the_frozen_child(_wait_s: float) -> ChildEnded:
            handle.finish(killed)
            return ChildEnded(pid=7, child_exit=KilledBySignal(9))

        stuck_backstop = AsyncMock(side_effect=kill_the_frozen_child)
        handle.stuck_backstop_kill = stuck_backstop

        outcome = await asyncio.wait_for(
            self._backstop(worker, DeadlineAcknowledged(pid=7), handle),
            OUTCOME_BOUND_S,
        )

        assert outcome == killed
        stuck_backstop.assert_awaited_once()

    async def test_a_stuck_report_leaves_the_run_on_its_seat(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(dispatch_module, 'SHARED_STUCK_BACKSTOP_S', 0.01)
        worker = _worker()
        handle = _handle(asyncio.get_running_loop())
        ended = SeatDeadlineEnded(retry=RetryPlacement.ISOLATED)
        handle.stuck_backstop_kill = AsyncMock(return_value=StuckAcknowledged(pid=7))
        asyncio.get_running_loop().call_later(0.05, handle.finish, ended)

        outcome = await asyncio.wait_for(
            self._backstop(worker, DeadlineAcknowledged(pid=7), handle),
            OUTCOME_BOUND_S,
        )

        assert outcome == ended

    async def test_a_thread_run_has_no_stuck_backstop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A thread cannot be cancelled: after its deadline report the run
        waits for its end or for the kill of its child."""
        monkeypatch.setattr(dispatch_module, 'SHARED_STUCK_BACKSTOP_S', 0.01)
        worker = _worker()
        handle = _handle(asyncio.get_running_loop())
        handle.backstop_kill = AsyncMock(return_value=DeadlineAcknowledged(pid=7))
        stuck_backstop = AsyncMock()
        handle.stuck_backstop_kill = stuck_backstop
        ended = SeatDeadlineEnded(retry=RetryPlacement.SAME_CLASS)
        asyncio.get_running_loop().call_later(0.05, handle.finish, ended)

        outcome = await asyncio.wait_for(
            worker._handle_shared_backstop(handle, 'task-1', ThreadSeatTask(1000)),
            OUTCOME_BOUND_S,
        )

        assert outcome == ended
        stuck_backstop.assert_not_awaited()


class _TimeoutWriteRetries:
    """A worker whose timeout write and crash recovery are recorded."""

    def __init__(self) -> None:
        self.worker = _worker()
        self.timeout = AsyncMock(return_value=_TimeoutSettled())
        self.recovery = AsyncMock(return_value=_RequeueOutcome.REQUEUED)
        self.handled = AsyncMock()
        self.worker._record_task_timeout = self.timeout
        self.worker._recover_worker_future_failure = self.recovery

    def error(self) -> Any:
        return self.worker._timeout_write_error(
            'task-1',
            1500,
            CLAIMED_AT,
            RetryPlacement.ISOLATED,
        )


@pytest.mark.asyncio
class TestTimeoutWriteRetry:
    """A TASK_TIMEOUT write that failed after the attempt ended runs again,
    bounded; crash recovery writes TASK_TIMEOUT only after the last retry."""

    async def test_the_retry_runs_the_same_timeout_write(self) -> None:
        retries = _TimeoutWriteRetries()

        await retries.worker._retry_finalize_timeout(retries.error(), 0.0)

        retries.timeout.assert_awaited_once_with(
            'task-1',
            1500,
            CLAIMED_AT,
            retry=RetryPlacement.ISOLATED,
        )
        retries.recovery.assert_not_awaited()

    async def test_a_failed_retry_goes_back_to_the_bounded_handler(self) -> None:
        retries = _TimeoutWriteRetries()
        retries.timeout.return_value = _TimeoutWriteFailed()
        error = retries.error()
        retries.worker._handle_finalize_error = retries.handled

        await retries.worker._retry_finalize_timeout(error, 0.0)

        retries.handled.assert_awaited_once_with(error)
        retries.recovery.assert_not_awaited()

    async def test_the_handler_schedules_the_timeout_write_until_its_bound(
        self,
    ) -> None:
        retries = _TimeoutWriteRetries()
        spawned: list[str] = []

        def spawn(coroutine: Any, *, name: str, **_kwargs: Any) -> None:
            coroutine.close()
            spawned.append(name)

        retries.worker._spawn_background = MagicMock(side_effect=spawn)
        error = retries.error()

        for _ in range(_FINALIZE_TIMEOUT_MAX_RETRIES + 1):
            await retries.worker._handle_finalize_error(error)

        assert spawned == [
            *(['finalize-retry-timeout-task-1'] * _FINALIZE_TIMEOUT_MAX_RETRIES),
            'finalize-timeout-recovery-task-1',
        ]

    async def test_crash_recovery_after_the_last_retry_decides_on_the_timeout(
        self,
    ) -> None:
        retries = _TimeoutWriteRetries()

        await retries.worker._recover_timed_out_row(retries.error())

        assert retries.recovery.await_args is not None
        assert retries.recovery.await_args.kwargs == {
            'claimed_at': CLAIMED_AT,
            'failure_code': OutcomeCode.TASK_TIMEOUT,
            'retry': RetryPlacement.ISOLATED,
        }

    async def test_a_database_error_of_crash_recovery_keeps_the_timeout_code(
        self,
    ) -> None:
        retries = _TimeoutWriteRetries()
        retries.recovery.return_value = _RequeueOutcome.DB_ERROR
        retries.worker._handle_finalize_error = retries.handled

        await retries.worker._recover_timed_out_row(retries.error())

        assert retries.handled.await_args is not None
        handed = retries.handled.await_args.args[0]
        assert handed.stage == _FINALIZE_STAGE_FUTURE
        assert future_retry_write(handed.data) == (
            OutcomeCode.TASK_TIMEOUT,
            RetryPlacement.ISOLATED,
        )


def test_the_data_of_a_timeout_write_is_a_closed_shape() -> None:
    with pytest.raises(AssertionError, match='no TASK_TIMEOUT write'):
        timeout_retry_write({'claimed_at': None})


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
                (ChildBaseline(1, RssMb(100.0), _ASYNC_KIND),),
            )

    def test_the_shared_limit_does_not_apply_to_isolated_children(self) -> None:
        self._worker(100)._check_memory_baseline(
            (ChildBaseline(1, RssMb(500.0), IsolatedChild(pool=_POOL, task_send_pool=2)),),
        )

    def test_the_isolated_limit_does_not_apply_to_shared_children(self) -> None:
        worker = self._worker(None)
        worker.cfg.max_memory_per_child_mb = 100
        worker._check_memory_baseline(
            (ChildBaseline(1, RssMb(500.0), _ASYNC_KIND),),
        )


class _NewKindBaselines:
    """The pool side of the guard of a kind that was down at the first
    start: each baseline once, then no more."""

    def __init__(self, *baselines: ChildBaseline) -> None:
        self._baselines: asyncio.Queue[ChildBaseline] = asyncio.Queue()
        for baseline in baselines:
            self._baselines.put_nowait(baseline)

    async def wait_new_kind_baseline(self) -> ChildBaseline:
        return await self._baselines.get()


class TestNewKindBaselineGuard:
    """Every shared async slot was down at the first start, so its kind had
    no baseline then. Its first ready child gets the guard."""

    def _worker(self) -> Worker:
        recycle = SharedRecycle(above=MemoryLimit(mb=100))
        return _worker(SharedAsyncChildren(count=1, seats=4, recycle=recycle))

    @pytest.mark.asyncio
    async def test_a_late_baseline_at_its_limit_stops_the_worker(self) -> None:
        worker = self._worker()
        pool = _NewKindBaselines(
            ChildBaseline(1, RssMb(100.0), _ASYNC_KIND),
        )

        await asyncio.wait_for(
            worker._new_kind_baseline_loop(cast(SlotPool, pool)),
            5.0,
        )

        assert worker._stop.is_set()
        match worker._fatal_background_error:
            case MemoryBaselineExceedsThresholdError() as error:
                assert 'shared async' in str(error)
            case other:
                raise AssertionError(other)

    @pytest.mark.asyncio
    async def test_a_late_baseline_under_its_limit_keeps_the_worker(self) -> None:
        worker = self._worker()
        pool = _NewKindBaselines(
            ChildBaseline(1, RssMb(50.0), _ASYNC_KIND),
        )
        guard = asyncio.ensure_future(
            worker._new_kind_baseline_loop(cast(SlotPool, pool)),
        )

        await asyncio.sleep(0.05)

        assert not guard.done()
        assert not worker._stop.is_set()
        assert worker._fatal_background_error is None
        guard.cancel()
        await asyncio.gather(guard, return_exceptions=True)
