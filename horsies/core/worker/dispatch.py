"""Dispatch: hand claimed tasks to child slots; contain failures.

DispatchMixin submits claimed rows to the slot pool and contains child
failures: requeue of a task that never reached a child, recovery of a task
whose child was lost, and the timeout handlers, which end the attempt before
they write the ownership-guarded row. A ``SHARED_ASYNC`` row goes to a shared
async child when the claim plan of the worker claimed it into the
``SHARED_ASYNC`` budget, and a ``SHARED_THREAD`` row to a shared thread child
when it claimed it into the ``SHARED_THREAD`` budget (``dispatch_route``).
Worker-internal
mixin: the ``TYPE_CHECKING`` block declares the slice of ``Worker`` it relies
on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, assert_never

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.lifecycle.commands import FailLockedTask
from horsies.core.lifecycle.fences import PriorLockedRead
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import (
    apply_async,
    classify_locked_read_miss_async,
)
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    Execution,
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.worker.child_protocol import (
    RunFrame,
    SeatRefusal,
    SeatTask,
    SeatWork,
    SharedWork,
    ThreadSeatTask,
    ThreadSeatWork,
)
from horsies.core.worker.dispatch_route import (
    RunIsolated,
    RunShared,
    RunSharedThread,
    dispatch_route,
)
from horsies.core.worker.runtime import (
    CrashRecoveryCode,
    _AttemptEndUnknown,
    _FinalizeError,
    _RequeueOutcome,
    _RetryError,
    _TimeoutOutcome,
    _TimeoutSettled,
    _TimeoutWriteFailed,
)
from horsies.core.worker.slots import (
    CHILD_KILL_WAIT_S,
    SHARED_STUCK_BACKSTOP_S,
    ChildEnded,
    ChildEndUnknown,
    ChildLost,
    DeadlineAcknowledged,
    ExitedWith,
    ExitUnknown,
    KilledBySignal,
    LossCause,
    LostAfterReturn,
    NotDispatched,
    NothingToKill,
    ReturnSeen,
    SharedAnswered,
    SharedHandle,
    SharedOutcome,
    SeatKilled,
    SharedRunRequest,
    SlotAnswered,
    StuckAcknowledged,
    SlotHandle,
    SlotOutcome,
    SlotPool,
    TaskFinished,
    describe_exit,
)
from horsies.core.worker.runner_heartbeat import (
    StaleRunningRecoveryOff,
    StaleRunningRecoveryOn,
    stale_running_recovery,
)
from horsies.core.worker.shared_stop import RetryPlacement, TimedOutAtKill
from horsies.core.worker.stored_timeout import NoTimeout, StoredTimeout
from horsies.core.worker.thread_slots import ThreadRunRequest
from horsies.core.worker.sql import (
    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
    UNCLAIM_CLAIMED_TASK_SQL,
    UPSERT_TASK_ATTEMPT_SQL,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from typing import Literal

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.worker.config import WorkerConfig
    from horsies.core.worker.finalize import _PersistedTaskOutcome

logger = get_logger('worker')


def _outcome_at_late_backstop_exit(
    work: SharedWork,
    pid: int,
    child_exit: ExitedWith | KilledBySignal,
) -> SeatKilled | ChildLost:
    """The outcome of a run whose child the first backstop killed, when the
    worker saw its exit only after the bounded wait. The run was past its
    deadline, as at a seen exit (``TimedOutAtKill`` in its class); a refusal
    has no deadline and takes the crash path. Pure."""
    match work:
        case SeatTask() | ThreadSeatTask():
            return SeatKilled(
                verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS),
                pid=pid,
                child_exit=child_exit,
            )
        case SeatRefusal():
            return ChildLost(LossCause.KILLED_BY_PARENT, child_exit, False, pid)
        case _ as unreachable:
            assert_never(unreachable)


class DispatchMixin:
    """Slot submission, timeout containment, dispatch-failure recovery."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        worker_instance_id: str
        _slots: SlotPool | None
        _stop: asyncio.Event

        # Cross-concern methods provided by sibling mixins / Worker.
        async def _finalize_after(
            self,
            handle: SlotHandle,
            task_id: str,
            queue_name: str = 'default',
            is_workflow_task: bool = True,
            timeout: StoredTimeout = NoTimeout(),
            *,
            task_name: str,
            claimed_at: Optional[datetime] = None,
        ) -> Result[None, _FinalizeError]: ...
        async def _finalize_shared_after(
            self,
            handle: SharedHandle,
            task_id: str,
            queue_name: str,
            is_workflow_task: bool,
            work: SharedWork,
            *,
            task_name: str,
            claimed_at: Optional[datetime],
        ) -> Result[None, _FinalizeError]: ...
        async def _finalize_workflow_phase(
            self,
            task_id: str,
            tr: TaskResult[Any, TaskError],
            *,
            queue_name: str = 'default',
            is_workflow_task: bool = True,
            task_name: str,
        ) -> Result[None, _FinalizeError]: ...
        async def _load_persisted_task_result(
            self,
            task_id: str,
            *,
            session: AsyncSession | None = None,
        ) -> Result[_PersistedTaskOutcome, _FinalizeError]: ...
        async def _handle_finalize_error(self, err: Any) -> None: ...
        async def _should_retry_task(
            self, task_id: str, error: TaskError, session: AsyncSession
        ) -> Result[bool, _RetryError]: ...
        async def _schedule_retry(
            self,
            task_id: str,
            session: AsyncSession,
            queue_name: str,
            *,
            retry: RetryPlacement = RetryPlacement.SAME_CLASS,
        ) -> Result[
            Literal['scheduled', 'reaper_reclaimed', 'expired'], _RetryError
        ]: ...
        def _spawn_background(
            self,
            coro: Coroutine[Any, Any, Any],
            *,
            name: str,
            finalizer: bool = False,
            service: bool = False,
        ) -> asyncio.Task[Any]: ...

    async def _requeue_claimed_task(
        self,
        task_id: str,
        reason: str,
        claimed_at: Optional[datetime] = None,
    ) -> _RequeueOutcome:
        try:
            async with self.sf() as s:
                res = await s.execute(
                    UNCLAIM_CLAIMED_TASK_SQL,
                    {
                        'id': task_id,
                        'wid': self.worker_instance_id,
                        'claimed_at': claimed_at,
                    },
                )
                await s.commit()
                rowcount = getattr(res, 'rowcount', 0) or 0
                requeued = rowcount > 0
        except Exception as exc:
            logger.error(
                'DB error while requeueing task %s (%s): %s',
                task_id,
                reason,
                exc,
            )
            return _RequeueOutcome.DB_ERROR

        if requeued:
            logger.warning(
                'Requeued worker-owned in-flight task %s: %s',
                task_id,
                reason,
            )
            return _RequeueOutcome.REQUEUED
        else:
            logger.warning(
                'Failed to requeue task %s (not owned or not requeueable): %s',
                task_id,
                reason,
            )
            return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED

    async def _recover_worker_future_failure(
        self,
        task_id: str,
        reason: str,
        claimed_at: Optional[datetime] = None,
        *,
        failure_code: CrashRecoveryCode = OperationalErrorCode.WORKER_CRASHED,
        retry: RetryPlacement = RetryPlacement.SAME_CLASS,
    ) -> _RequeueOutcome:
        """Recover a task whose child ended without a task result.

        CLAIMED means user code never started and can be unclaimed. RUNNING
        means user code may have executed, so recovery must respect retry policy.

        Call it only when no child can still run the attempt: after the
        child's exit was seen, or for a task that never reached a child. It
        writes ``failure_code`` (WORKER_CRASHED for a crash) or a retry, and
        the attempt must end before its row moves. ``retry`` is where that
        retry runs (``_schedule_retry``).

        ``claimed_at`` fences every statement to the dispatch's claim
        generation; None disables the fence.
        """
        try:
            task_error = TaskError(
                error_code=failure_code,
                message=reason,
                data={
                    'task_id': task_id,
                    'worker_id': self.worker_instance_id,
                },
            )
            task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
            failure_command = FailLockedTask(
                task_id=task_id,
                fence=PriorLockedRead(worker_id=self.worker_instance_id),
                result_json=serialize_error_payload(task_result),
                error_code=failure_code.value,
                # The task summary reason stays unchanged. The attempt row
                # holds the reason of this failure.
                failed_reason=None,
            )
            async with self.sf() as s:
                ctx_result = await s.execute(
                    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
                    {
                        'id': task_id,
                        'wid': self.worker_instance_id,
                        'claimed_at': claimed_at,
                    },
                )
                ctx_row = ctx_result.fetchone()
                if ctx_row is None:
                    miss_outcome = await classify_locked_read_miss_async(
                        await s.connection(),
                        failure_command,
                        claimed_at=claimed_at,
                    )
                    match miss_outcome:
                        case AlreadyApplied():
                            await self._replay_committed_terminalization(
                                task_id,
                                s,
                            )
                            return _RequeueOutcome.TERMINAL_REPLAYED
                        case LostClaim() | SourceStateConflict() | TaskAbsent():
                            await s.rollback()
                            return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
                        case Applied():
                            await s.rollback()
                            raise RuntimeError(
                                'locked-read miss classifier returned APPLIED'
                            )
                        case _ as unreachable:
                            assert_never(unreachable)

                status_value = (
                    ctx_row.status.value
                    if hasattr(ctx_row.status, 'value')
                    else str(ctx_row.status)
                )
                if status_value == 'CLAIMED':
                    res = await s.execute(
                        UNCLAIM_CLAIMED_TASK_SQL,
                        {
                            'id': task_id,
                            'wid': self.worker_instance_id,
                            'claimed_at': claimed_at,
                        },
                    )
                    await s.commit()
                    return (
                        _RequeueOutcome.REQUEUED
                        if (getattr(res, 'rowcount', 0) or 0) > 0
                        else _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
                    )

                attempt_num = (ctx_row.retry_count or 0) + 1
                db_now = ctx_row.db_now or datetime.now(timezone.utc)
                attempt_started_at = ctx_row.started_at or db_now
                attempt_worker = {
                    'worker_id': ctx_row.claimed_by_worker_id,
                    'worker_hostname': ctx_row.worker_hostname,
                    'worker_pid': ctx_row.worker_pid,
                    'worker_process_name': ctx_row.worker_process_name,
                }

                should_retry_r = await self._should_retry_task(
                    task_id,
                    task_error,
                    s,
                )
                if is_err(should_retry_r):
                    logger.error(
                        'Retry decision failed while recovering task %s (%s): %s',
                        task_id,
                        reason,
                        should_retry_r.err_value.message,
                    )
                    return _RequeueOutcome.DB_ERROR
                if should_retry_r.ok_value:
                    retry_r = await self._schedule_retry(
                        task_id,
                        s,
                        queue_name=ctx_row.queue_name or 'default',
                        retry=retry,
                    )
                    if is_err(retry_r):
                        logger.error(
                            'Retry scheduling failed while recovering task %s (%s): %s',
                            task_id,
                            reason,
                            retry_r.err_value.message,
                        )
                        return _RequeueOutcome.DB_ERROR
                    match retry_r.ok_value:
                        case 'scheduled':
                            await s.execute(
                                UPSERT_TASK_ATTEMPT_SQL,
                                {
                                    'task_id': task_id,
                                    'attempt': attempt_num,
                                    'outcome': 'FAILED',
                                    'will_retry': True,
                                    'started_at': attempt_started_at,
                                    'finished_at': db_now,
                                    'error_code': failure_code.value,
                                    'error_message': reason,
                                    'failed_reason': reason,
                                    **attempt_worker,
                                },
                            )
                            await s.commit()
                            return _RequeueOutcome.REQUEUED
                        case 'expired' | 'reaper_reclaimed':
                            pass

                await s.execute(
                    UPSERT_TASK_ATTEMPT_SQL,
                    {
                        'task_id': task_id,
                        'attempt': attempt_num,
                        'outcome': 'FAILED',
                        'will_retry': False,
                        'started_at': attempt_started_at,
                        'finished_at': db_now,
                        'error_code': failure_code.value,
                        'error_message': reason,
                        'failed_reason': reason,
                        **attempt_worker,
                    },
                )
                terminalization = await apply_async(
                    await s.connection(),
                    failure_command,
                )
                match terminalization:
                    case Applied():
                        await s.commit()
                    case AlreadyApplied():
                        await self._replay_committed_terminalization(task_id, s)
                        return _RequeueOutcome.TERMINAL_REPLAYED
                    case LostClaim() | SourceStateConflict() | TaskAbsent():
                        await s.rollback()
                        return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
                    case _ as unreachable:
                        assert_never(unreachable)
                phase2_r = await self._finalize_workflow_phase(
                    task_id,
                    task_result,
                    queue_name=ctx_row.queue_name or 'default',
                    is_workflow_task=bool(ctx_row.is_workflow_task),
                    task_name=str(ctx_row.task_name or ''),
                )
                if is_err(phase2_r):
                    await self._handle_finalize_error(phase2_r.err_value)
                return _RequeueOutcome.REQUEUED
        except Exception as exc:
            logger.error(
                'DB error while recovering future failure for task %s (%s): %s',
                task_id,
                reason,
                exc,
            )
            return _RequeueOutcome.DB_ERROR

    async def _replay_committed_terminalization(
        self,
        task_id: str,
        session: AsyncSession,
    ) -> None:
        """Replay phase 2 from the terminal row while its lock is still held."""
        load_r = await self._load_persisted_task_result(
            task_id,
            session=session,
        )
        await session.rollback()
        if is_err(load_r):
            await self._handle_finalize_error(load_r.err_value)
            return
        persisted = load_r.ok_value
        phase2_r = await self._finalize_workflow_phase(
            task_id,
            persisted.result,
            queue_name=persisted.queue_name,
            is_workflow_task=persisted.is_workflow_task,
            task_name=persisted.task_name,
        )
        if is_err(phase2_r):
            await self._handle_finalize_error(phase2_r.err_value)

    async def _dispatch_one(
        self,
        task_id: str,
        task_name: str,
        args_json: Optional[str],
        kwargs_json: Optional[str],
        queue_name: str = 'default',
        is_workflow_task: bool = True,
        timeout: StoredTimeout = NoTimeout(),
        claimed_at: Optional[datetime] = None,
        *,
        execution: Execution,
        task_options: str | None,
    ) -> None:
        """Submit to the slot pool and spawn the finalizer.

        Never raises for a task: the pool answers every submit, and the
        finalizer handles that answer. ``execution`` is the class column of
        the row, and ``task_options`` its stored options; with the claim plan
        and ``timeout`` they decide the kind of child that runs the row
        (``dispatch_route``).

        ``claimed_at`` is the claim generation this dispatch was born from
        (the claim statement's RETURNING); it fences the child's ownership
        confirm and every parent-side finalize/recovery statement so a
        stale dispatch cannot touch a re-claimed row (C10).

        ``timeout`` is the stored timeout as the claim read it. A refusal is
        sent to the child, which sets the row RUNNING and fails the task with
        WORKER_SERIALIZATION_ERROR before it resolves the task. A shared row
        without a usable stored timeout or grace is refused the same way, by
        the child that its budget names.
        """
        slots = self._slots
        assert slots is not None, 'dispatch before the slot pool started'

        match dispatch_route(execution, self.cfg.claim_plan(), task_options, timeout):
            case RunShared(work=work):
                self._dispatch_shared(
                    slots,
                    work,
                    task_id,
                    task_name,
                    args_json,
                    kwargs_json,
                    queue_name,
                    is_workflow_task,
                    claimed_at,
                )
                return
            case RunSharedThread(work=thread_work):
                self._dispatch_shared_thread(
                    slots,
                    ThreadRunRequest(
                        task_id=task_id,
                        task_name=task_name,
                        args_json=args_json,
                        kwargs_json=kwargs_json,
                        worker_id=self.worker_instance_id,
                        is_workflow_task=is_workflow_task,
                        claimed_at=claimed_at,
                        work=thread_work,
                    ),
                    queue_name,
                )
                return
            case RunIsolated(timeout=isolated_timeout):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        handle = slots.submit(
            RunFrame(
                task_id=task_id,
                task_name=task_name,
                args_json=args_json,
                kwargs_json=kwargs_json,
                worker_id=self.worker_instance_id,
                is_workflow_task=is_workflow_task,
                timeout=isolated_timeout,
                claimed_at=claimed_at,
            ),
        )

        # When done, record the outcome
        self._spawn_background(
            self._finalize_after(
                handle,
                task_id,
                queue_name,
                is_workflow_task,
                timeout=isolated_timeout,
                task_name=task_name,
                claimed_at=claimed_at,
            ),
            name=f'finalize-{task_id}',
            finalizer=True,
        )

    def _dispatch_shared(
        self,
        slots: SlotPool,
        work: SeatWork,
        task_id: str,
        task_name: str,
        args_json: Optional[str],
        kwargs_json: Optional[str],
        queue_name: str,
        is_workflow_task: bool,
        claimed_at: Optional[datetime],
    ) -> None:
        """Submit a ``SHARED_ASYNC`` row to the shared async children, and
        spawn its finalizer. ``work`` is the task with its deadline, or the
        refusal that fails it. Never raises for a task."""
        handle = slots.submit_shared(
            SharedRunRequest(
                task_id=task_id,
                task_name=task_name,
                args_json=args_json,
                kwargs_json=kwargs_json,
                worker_id=self.worker_instance_id,
                is_workflow_task=is_workflow_task,
                claimed_at=claimed_at,
                work=work,
            ),
        )
        self._spawn_background(
            self._finalize_shared_after(
                handle,
                task_id,
                queue_name,
                is_workflow_task,
                work,
                task_name=task_name,
                claimed_at=claimed_at,
            ),
            name=f'finalize-{task_id}',
            finalizer=True,
        )

    def _dispatch_shared_thread(
        self,
        slots: SlotPool,
        request: ThreadRunRequest,
        queue_name: str,
    ) -> None:
        """Submit a ``SHARED_THREAD`` row to the shared thread children, and
        spawn its finalizer. ``request.work`` is the task with its deadline,
        or the refusal that fails it. Never raises for a task."""
        handle = slots.submit_shared(request)
        work: ThreadSeatWork = request.work
        self._spawn_background(
            self._finalize_shared_after(
                handle,
                request.task_id,
                queue_name,
                request.is_workflow_task,
                work,
                task_name=request.task_name,
                claimed_at=request.claimed_at,
            ),
            name=f'finalize-{request.task_id}',
            finalizer=True,
        )

    # ----- finalize (write back to DB + notify) -----

    async def _handle_shared_backstop(
        self,
        handle: SharedHandle,
        task_id: str,
        work: SharedWork,
    ) -> SharedOutcome | _AttemptEndUnknown:
        """Act on a shared run with no end at its backstop time.

        The child reports at once the step of the stop of a run that is
        due: the deadline of a thread task (a thread cannot be cancelled),
        the second cancel of an async task. The watchdog of an async child
        reports a blocked loop. A child that has reported neither that step
        nor the end of the run by the backstop is frozen; a refusal reports
        neither, so its backstop always ends the child. ``backstop_kill``
        kills the child, bound to this run, and waits for its exit. The slot
        then answers each run of the child by its verdict (``shared_stop``):
        this run, past its deadline, ``TASK_TIMEOUT``; a refusal, the crash
        path. A child that reported the step is alive: nothing is killed
        here, and ``_await_after_acknowledged`` waits for the outcome.

        Returns the outcome to finalize, or ``_AttemptEndUnknown`` (no exit
        seen: nothing is written, the row is left to stale-task recovery).
        """
        match work:
            case SeatTask(timeout_ms=timeout_ms) | ThreadSeatTask(timeout_ms=timeout_ms):
                limit = f'timeout_ms={timeout_ms}'
            case SeatRefusal():
                limit = 'the refusal backstop'
            case _ as unreachable:
                assert_never(unreachable)
        match await handle.backstop_kill(CHILD_KILL_WAIT_S):
            case ChildEnded(pid=pid, child_exit=child_exit):
                logger.warning(
                    'Killed shared child pid=%s (%s): task %s passed %s and the '
                    'child reported neither the due step of its stop nor its '
                    'end; every task of that child ends with it',
                    pid,
                    describe_exit(child_exit),
                    task_id,
                    limit,
                )
                return await asyncio.shield(handle.outcome)
            case ChildEndUnknown(pid=pid):
                match await self._wait_for_unseen_exit(handle, task_id, pid):
                    case ExitedWith() | KilledBySignal() as child_exit:
                        return _outcome_at_late_backstop_exit(work, pid, child_exit)
                    case _AttemptEndUnknown() as unknown:
                        return unknown
                    case _ as unreachable:
                        assert_never(unreachable)
            case DeadlineAcknowledged(pid=pid):
                logger.warning(
                    'Task %s has not ended in shared child pid=%s, which '
                    'reported the stop of its deadline; it keeps its seat until '
                    'it ends, its child is killed, or the worker stops',
                    task_id,
                    pid,
                )
                return await self._await_after_acknowledged(handle, task_id, work)
            case SharedAnswered(outcome=outcome):
                return outcome
            case NothingToKill():
                raise AssertionError(
                    f'backstop of task {task_id} ran before a child took it',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _await_after_acknowledged(
        self,
        handle: SharedHandle,
        task_id: str,
        work: SharedWork,
    ) -> SharedOutcome | _AttemptEndUnknown:
        """Wait for the outcome of a run whose child reported the due step of
        its stop at the backstop.

        A run of a shared async task has one more step: within
        ``STUCK_REPORT_DELAY_S`` of that cancel the child reports it stuck,
        or it ends, and a blocked loop is reported within the block limit. A
        child that has done neither ``SHARED_STUCK_BACKSTOP_S`` later froze
        after its report: ``stuck_backstop_kill`` kills it, bound to this
        run, and the slot answers each run of the child by its verdict. A
        stuck run waits for the kill of its child when every run left in it
        is stuck. A thread task and a refusal have no such step: the outcome
        comes when the run ends or the slot kills the child.

        Returns the outcome to finalize, or ``_AttemptEndUnknown`` (no exit
        seen: nothing is written).
        """
        match work:
            case SeatTask():
                pass
            case ThreadSeatTask() | SeatRefusal():
                return await asyncio.shield(handle.outcome)
            case _ as unreachable:
                assert_never(unreachable)
        try:
            return await asyncio.wait_for(
                asyncio.shield(handle.outcome),
                timeout=SHARED_STUCK_BACKSTOP_S,
            )
        except asyncio.TimeoutError:
            pass
        match await handle.stuck_backstop_kill(CHILD_KILL_WAIT_S):
            case ChildEnded(pid=pid, child_exit=child_exit):
                logger.warning(
                    'Killed shared child pid=%s (%s): the child reported the '
                    'last cancel of task %s, then neither the task stuck nor its '
                    'end within %ss; every task of that child ends with it',
                    pid,
                    describe_exit(child_exit),
                    task_id,
                    SHARED_STUCK_BACKSTOP_S,
                )
                return await asyncio.shield(handle.outcome)
            case ChildEndUnknown(pid=pid):
                match await self._wait_for_unseen_exit(handle, task_id, pid):
                    case ExitedWith() | KilledBySignal() as child_exit:
                        return ChildLost(
                            LossCause.KILLED_BY_PARENT,
                            child_exit,
                            False,
                            pid,
                        )
                    case _AttemptEndUnknown() as unknown:
                        return unknown
                    case _ as unreachable:
                        assert_never(unreachable)
            case StuckAcknowledged():
                return await asyncio.shield(handle.outcome)
            case SharedAnswered(outcome=outcome):
                return outcome
            case NothingToKill():
                raise AssertionError(
                    f'stuck backstop of task {task_id} ran before a child took it',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _handle_task_timeout(
        self,
        handle: SlotHandle,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime] = None,
    ) -> _TimeoutOutcome | ReturnSeen:
        """End a timed-out attempt, then record the timeout.

        Once the deadline fires, the outcome of this dispatch is TASK_TIMEOUT,
        unless the slot already read that the body of the task returned with
        work left on its task loop (``ReturnSeen``). The attempt ends before
        its row moves. The child that holds this task gets SIGKILL, with its
        session, and the parent waits up to ``CHILD_KILL_WAIT_S`` for that
        child's exit. Only after the exit are TASK_TIMEOUT, the retry, the
        attempt row or the requeue written, and only then does phase 2 run.
        So a retry never runs next to the attempt that it replaces, and phase
        2 cannot start a successor while that attempt runs. The kill is bound
        to this task's slot, so it cannot reach the next task. A slot that
        answered after the timer fired has nothing to kill; see
        ``_time_out_answered_task``.

        Returns:
        - ``ReturnSeen``: the body returned before the kill, so its outcome
          stands. Nothing is killed or written; the caller waits for the
          outcome of the slot, which ends the work that the task left.
        - ``_TimeoutSettled`` or ``_TimeoutWriteFailed``: the attempt ended,
          and ``_record_task_timeout`` ran.
        - ``_AttemptEndUnknown``: no exit was seen, so the attempt may still
          have effects in flight. Nothing is written and phase 2 does not
          run (``_wait_for_unseen_exit``). With stale RUNNING recovery on,
          the row keeps its state for stale-task recovery: a RUNNING row
          fails after ``running_stale_threshold_ms`` with no runner heartbeat
          (the kill ended its heartbeats), and a CLAIMED row is claimed again
          when its lease ends. With it off, the worker first waits for the
          exit, and records the timeout once it sees it; only a stop of the
          worker before that exit gives this outcome. Logged at critical
          level with the task id and pid.

        Does not raise for an expected database failure. A handle that no
        child has taken is a caller defect: the deadline starts only when a
        child takes the task.
        """
        match await handle.kill_and_wait(CHILD_KILL_WAIT_S):
            case ChildEnded(pid=pid, child_exit=child_exit):
                logger.warning(
                    'Killed the child pid=%s of timed-out task %s '
                    '(timeout_ms=%s, %s); the slot restarts and other tasks '
                    'are not affected',
                    pid,
                    task_id,
                    timeout_ms,
                    describe_exit(child_exit),
                )
                return await self._record_task_timeout(
                    task_id,
                    timeout_ms,
                    claimed_at,
                    retry=RetryPlacement.SAME_CLASS,
                )
            case ChildEndUnknown(pid=pid):
                match await self._wait_for_unseen_exit(handle, task_id, pid):
                    case ExitedWith() | KilledBySignal():
                        return await self._record_task_timeout(
                            task_id,
                            timeout_ms,
                            claimed_at,
                            retry=RetryPlacement.SAME_CLASS,
                        )
                    case _AttemptEndUnknown() as unknown:
                        return unknown
                    case _ as unreachable:
                        assert_never(unreachable)
            case SlotAnswered(outcome=outcome):
                return await self._time_out_answered_task(
                    handle,
                    task_id,
                    timeout_ms,
                    claimed_at,
                    outcome,
                )
            case ReturnSeen() as seen:
                logger.debug(
                    'Task %s reached timeout_ms=%s after its body returned; the '
                    'deadline no longer applies, and the outcome is what it '
                    'returned',
                    task_id,
                    timeout_ms,
                )
                return seen
            case NothingToKill():
                raise AssertionError(
                    f'deadline of task {task_id} fired before a child took it',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _time_out_answered_task(
        self,
        handle: SlotHandle,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime],
        outcome: SlotOutcome,
    ) -> _TimeoutOutcome:
        """Record the timeout of a task whose slot answered after the deadline fired.

        The deadline decides the outcome: a result that reached the slot after
        the timer fired is dropped, and the row gets TASK_TIMEOUT through
        ``_record_task_timeout``, the one writer. The slot's answer shows that
        the attempt ended. The exception is a loss whose exit the slot did not
        see: it waits for that exit as ``_handle_task_timeout`` does
        (``_wait_for_unseen_exit``), and writes nothing when it gives
        ``_AttemptEndUnknown``.
        """
        match outcome:
            case ChildLost(child_exit=ExitUnknown(), pid=pid):
                match await self._wait_for_unseen_exit(handle, task_id, pid):
                    case ExitedWith() | KilledBySignal():
                        return await self._record_task_timeout(
                            task_id,
                            timeout_ms,
                            claimed_at,
                            retry=RetryPlacement.SAME_CLASS,
                        )
                    case _AttemptEndUnknown() as unknown:
                        return unknown
                    case _ as unreachable:
                        assert_never(unreachable)
            case TaskFinished() | ChildLost():
                logger.warning(
                    'Task %s reached timeout_ms=%s as its slot answered; the '
                    'answer is dropped and TASK_TIMEOUT is recorded',
                    task_id,
                    timeout_ms,
                )
                return await self._record_task_timeout(
                    task_id,
                    timeout_ms,
                    claimed_at,
                    retry=RetryPlacement.SAME_CLASS,
                )
            case NotDispatched():
                raise AssertionError(
                    f'deadline of task {task_id} fired for a task that never '
                    'reached a child',
                )
            case LostAfterReturn():
                # ``kill_and_wait`` answers ReturnSeen before it reads the
                # outcome of a handle whose return the slot read.
                raise AssertionError(
                    f'deadline of task {task_id} was applied after its body '
                    'returned',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _wait_for_unseen_exit(
        self,
        handle: SlotHandle | SharedHandle,
        task_id: str,
        pid: int,
    ) -> ExitedWith | KilledBySignal | _AttemptEndUnknown:
        """Decide what follows a kill or a loss of the child ``pid`` of a
        task whose exit the slot did not see in its bounded wait.

        With stale RUNNING recovery on, ``_AttemptEndUnknown`` at once: the
        row keeps its state for stale-task recovery. With it off, nothing
        else would ever move the row, so the worker waits for the exit of
        that child, with no bound but the life of the worker, and returns it:
        the caller writes the row as at a seen exit. A stop of the worker
        before that exit ends the wait with ``_AttemptEndUnknown``. Each end
        is logged. Writes nothing.
        """
        match stale_running_recovery(self.cfg.recovery_config):
            case StaleRunningRecoveryOn():
                logger.critical(
                    'Child pid=%s of task %s did not exit %ss after SIGKILL; '
                    'nothing is written, and the row is left to stale-task '
                    'recovery',
                    pid,
                    task_id,
                    CHILD_KILL_WAIT_S,
                )
                return _AttemptEndUnknown()
            case StaleRunningRecoveryOff():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        logger.critical(
            'Child pid=%s of task %s did not exit %ss after SIGKILL; stale '
            'RUNNING recovery is off, so the worker writes the row once it '
            'sees the exit',
            pid,
            task_id,
            CHILD_KILL_WAIT_S,
        )
        match await self._exit_or_stop(handle):
            case ExitedWith() | KilledBySignal() as child_exit:
                logger.warning(
                    'Child pid=%s of task %s exited (%s); writing its row',
                    pid,
                    task_id,
                    describe_exit(child_exit),
                )
                return child_exit
            case _AttemptEndUnknown() as unknown:
                logger.critical(
                    'The worker stops before child pid=%s of task %s exited; '
                    'nothing is written',
                    pid,
                    task_id,
                )
                return unknown
            case _ as unreachable:
                assert_never(unreachable)

    async def _exit_or_stop(
        self,
        handle: SlotHandle | SharedHandle,
    ) -> ExitedWith | KilledBySignal | _AttemptEndUnknown:
        """Wait for the exit of the child of ``handle``, or for the stop of
        the worker, whichever comes first. A handle that no child took is a
        caller defect: ``AssertionError``."""
        exit_wait = asyncio.ensure_future(handle.wait_child_exit())
        stop_wait = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait(
                {exit_wait, stop_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for waiter in (exit_wait, stop_wait):
                waiter.cancel()
            await asyncio.gather(exit_wait, stop_wait, return_exceptions=True)
        if not exit_wait.done() or exit_wait.cancelled():
            return _AttemptEndUnknown()
        match exit_wait.result():
            case ExitedWith() | KilledBySignal() as child_exit:
                return child_exit
            case NothingToKill():
                raise AssertionError(
                    'an exit was awaited for a task that no child took',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _record_task_timeout(
        self,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime],
        *,
        retry: RetryPlacement,
    ) -> _TimeoutSettled | _TimeoutWriteFailed:
        """Record the timeout of an attempt whose child has exited.

        Ownership-guarded like every finalize path, and fenced to the claim
        generation ``claimed_at``; None disables the fence. ``retry`` is
        where the retry runs, when the policy retries the timeout.

        ``_TimeoutSettled`` covers each way the row is settled:
        - a RUNNING row fails with TASK_TIMEOUT and its attempt row, then
          phase 2 runs; a phase-2 error goes to the finalize retry handler;
        - a RUNNING row whose policy retries TASK_TIMEOUT gets its retry and
          its attempt row;
        - a CLAIMED row (the child never confirmed RUNNING) is requeued;
        - a row that this claim already failed has its phase 2 replayed;
        - a row that another actor holds (reaper reclaim, cancel) is left
          alone.

        ``_TimeoutWriteFailed``: a database error, or a failed retry decision
        or retry schedule, logged here. An error before the commit leaves the
        row unchanged; an error after it leaves the row written. The caller
        returns a timeout-stage finalize error: its retries run this write
        again, each reading the row under its lock, and crash recovery writes
        TASK_TIMEOUT after the last one failed.
        """
        try:
            async with self.sf() as s:
                return await self._settle_timed_out_row(
                    s,
                    task_id,
                    timeout_ms,
                    claimed_at,
                    retry,
                )
        except Exception as exc:
            logger.error(
                'Failed to persist TASK_TIMEOUT for task %s: %s; the write '
                'is retried',
                task_id,
                exc,
            )
            return _TimeoutWriteFailed()

    async def _settle_timed_out_row(
        self,
        s: AsyncSession,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime],
        retry: RetryPlacement,
    ) -> _TimeoutSettled | _TimeoutWriteFailed:
        """Lock this claim's row and settle it for a timeout.

        The contract is the one of ``_record_task_timeout``. Database errors
        raise to that caller.
        """
        task_error = TaskError(
            error_code=OutcomeCode.TASK_TIMEOUT,
            message=f'Task exceeded timeout_ms={timeout_ms}',
            data={'task_id': task_id, 'timeout_ms': timeout_ms},
        )
        task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
        failure_command = FailLockedTask(
            task_id=task_id,
            fence=PriorLockedRead(worker_id=self.worker_instance_id),
            result_json=serialize_error_payload(task_result),
            error_code=OutcomeCode.TASK_TIMEOUT.value,
            # The task summary reason stays unchanged. The attempt row holds
            # the reason of this failure.
            failed_reason=None,
        )
        ctx_result = await s.execute(
            SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
            {
                'id': task_id,
                'wid': self.worker_instance_id,
                'claimed_at': claimed_at,
            },
        )
        ctx_row = ctx_result.fetchone()
        if ctx_row is None:
            await self._settle_timeout_miss(s, task_id, failure_command, claimed_at)
            return _TimeoutSettled()

        status_value = (
            ctx_row.status.value
            if hasattr(ctx_row.status, 'value')
            else str(ctx_row.status)
        )
        match status_value:
            case 'CLAIMED':
                await s.execute(
                    UNCLAIM_CLAIMED_TASK_SQL,
                    {
                        'id': task_id,
                        'wid': self.worker_instance_id,
                        'claimed_at': claimed_at,
                    },
                )
                await s.commit()
                logger.warning(
                    'Task %s hit timeout_ms=%s before user code started; requeued',
                    task_id,
                    timeout_ms,
                )
                return _TimeoutSettled()
            case 'RUNNING':
                return await self._fail_timed_out_attempt(
                    s,
                    ctx_row,
                    task_result,
                    failure_command,
                    retry,
                )
            case _:
                raise RuntimeError(
                    f'in-flight read of task {task_id} returned status '
                    f'{status_value!r}',
                )

    async def _settle_timeout_miss(
        self,
        s: AsyncSession,
        task_id: str,
        failure_command: FailLockedTask,
        claimed_at: Optional[datetime],
    ) -> None:
        """Settle a timeout whose locked read found no in-flight row of this claim.

        A row that this claim already failed has its phase 2 replayed. A row
        that another actor holds is left alone. Database errors raise.
        """
        miss_outcome = await classify_locked_read_miss_async(
            await s.connection(),
            failure_command,
            claimed_at=claimed_at,
        )
        match miss_outcome:
            case AlreadyApplied():
                await self._replay_committed_terminalization(task_id, s)
            case LostClaim() | SourceStateConflict() | TaskAbsent():
                await s.rollback()
            case Applied():
                await s.rollback()
                raise RuntimeError('locked-read miss classifier returned APPLIED')
            case _ as unreachable:
                assert_never(unreachable)

    async def _fail_timed_out_attempt(
        self,
        s: AsyncSession,
        ctx_row: Any,
        task_result: TaskResult[None, TaskError],
        failure_command: FailLockedTask,
        retry: RetryPlacement,
    ) -> _TimeoutSettled | _TimeoutWriteFailed:
        """Retry or fail a RUNNING row whose child has exited, then run phase 2.

        ``ctx_row`` is this claim's row, locked in ``s``. The attempt row is
        written in the same transaction as the retry or the fail. Phase 2
        runs only after the fail commits. Database errors raise.
        """
        task_id = failure_command.task_id
        task_error = task_result.unwrap_err()
        queue_name = ctx_row.queue_name or 'default'
        attempt_num = (ctx_row.retry_count or 0) + 1
        db_now = ctx_row.db_now or datetime.now(timezone.utc)
        attempt_started_at = ctx_row.started_at or db_now

        retry_r = await self._schedule_timeout_retry(
            s,
            task_id,
            task_error,
            queue_name,
            retry,
        )
        if is_err(retry_r):
            return _TimeoutWriteFailed()
        retry_scheduled = retry_r.ok_value

        await s.execute(
            UPSERT_TASK_ATTEMPT_SQL,
            {
                'task_id': task_id,
                'attempt': attempt_num,
                'outcome': 'FAILED',
                'will_retry': retry_scheduled,
                'started_at': attempt_started_at,
                'finished_at': db_now,
                'error_code': OutcomeCode.TASK_TIMEOUT.value,
                'error_message': task_error.message,
                'failed_reason': task_error.message,
                'worker_id': ctx_row.claimed_by_worker_id,
                'worker_hostname': ctx_row.worker_hostname,
                'worker_pid': ctx_row.worker_pid,
                'worker_process_name': ctx_row.worker_process_name,
            },
        )
        if retry_scheduled:
            await s.commit()
            return _TimeoutSettled()

        terminalization = await apply_async(await s.connection(), failure_command)
        match terminalization:
            case Applied():
                await s.commit()
            case AlreadyApplied():
                await self._replay_committed_terminalization(task_id, s)
                return _TimeoutSettled()
            case LostClaim() | SourceStateConflict() | TaskAbsent():
                await s.rollback()
                return _TimeoutSettled()
            case _ as unreachable:
                assert_never(unreachable)
        phase2_r = await self._finalize_workflow_phase(
            task_id,
            task_result,
            queue_name=queue_name,
            is_workflow_task=bool(ctx_row.is_workflow_task),
            task_name=str(ctx_row.task_name or ''),
        )
        if is_err(phase2_r):
            await self._handle_finalize_error(phase2_r.err_value)
        return _TimeoutSettled()

    async def _schedule_timeout_retry(
        self,
        s: AsyncSession,
        task_id: str,
        task_error: TaskError,
        queue_name: str,
        retry: RetryPlacement,
    ) -> Result[bool, _RetryError]:
        """Set a timed-out RUNNING row PENDING when its retry policy asks for it.

        Ok(True): the retry is written in ``s``, not committed. Ok(False): no
        retry; the policy declines it, it would land after ``good_until``, or
        the row is no longer RUNNING. Err: the decision or the schedule
        failed, logged here; ``s`` is then not usable for more statements.
        """
        should_retry_r = await self._should_retry_task(task_id, task_error, s)
        if is_err(should_retry_r):
            logger.error(
                'Retry decision failed for timed-out task %s: %s; crash '
                'recovery will classify the row',
                task_id,
                should_retry_r.err_value.message,
            )
            return Err(should_retry_r.err_value)
        if not should_retry_r.ok_value:
            return Ok(False)
        retry_r = await self._schedule_retry(
            task_id,
            s,
            queue_name=queue_name,
            retry=retry,
        )
        if is_err(retry_r):
            logger.error(
                'Retry scheduling failed for timed-out task %s: %s; crash '
                'recovery will classify the row',
                task_id,
                retry_r.err_value.message,
            )
            return Err(retry_r.err_value)
        match retry_r.ok_value:
            case 'scheduled':
                return Ok(True)
            case 'expired' | 'reaper_reclaimed':
                return Ok(False)
            case _ as unreachable:
                assert_never(unreachable)
