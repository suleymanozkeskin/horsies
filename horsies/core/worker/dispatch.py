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
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, assert_never

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.lifecycle.commands import FailLockedTask
from horsies.core.lifecycle.fences import PriorLockedRead
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    CapacityWoken,
    FailedAttemptOutcome,
    FailureApplied,
    LostClaim,
    PhaseTwoOwed,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    Execution,
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Result, is_err
from horsies.core.utils.db import autocommit_connection
from horsies.core.worker.child_protocol import (
    RunFrame,
    SeatRefusal,
    SeatTask,
    SeatWork,
    SharedWork,
    ThreadSeatTask,
    ThreadSeatWork,
)
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    AttemptNotHeld,
    ClaimedAttempt,
    NoRetry,
    RetryRequest,
    RunningAttempt,
    finish_failed_attempt,
    finish_for,
    read_in_flight_attempt,
    retry_request_for,
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
from horsies.core.worker.sql import UNCLAIM_CLAIMED_TASK_SQL

if TYPE_CHECKING:
    from collections.abc import Coroutine

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


def _error_code_text(error: TaskError) -> str:
    """The error code of ``error`` as the row stores it. Pure."""
    code = error.error_code
    match code:
        case Enum():
            return str(code.value)
        case str():
            return code
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class _AttemptUnclaimed:
    """The row was CLAIMED: no task code ran, and it is back in the queue."""


@dataclass(frozen=True, slots=True)
class _UnclaimMissed:
    """The row was CLAIMED when read, and the requeue matched no row."""


@dataclass(frozen=True, slots=True)
class _AttemptFinished:
    """``horsies_finish_failed_attempt`` answered, with the request it got."""

    outcome: FailedAttemptOutcome
    request: RetryRequest


type _EndedAttemptWrite = _AttemptUnclaimed | _UnclaimMissed | _AttemptFinished


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
        def _wake_when_retry_due(
            self,
            scheduled: RetryScheduled,
            request: RetryRequest,
        ) -> None: ...
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
        retry runs.

        ``claimed_at`` fences every statement to the dispatch's claim
        generation; None disables the fence. ``DB_ERROR``: a database error,
        logged here; the row is unchanged, or the write committed and its
        reply was lost.
        """
        task_error = TaskError(
            error_code=failure_code,
            message=reason,
            data={
                'task_id': task_id,
                'worker_id': self.worker_instance_id,
            },
        )
        try:
            written = await self._write_ended_attempt(
                task_id,
                task_error,
                claimed_at,
                retry,
            )
        except Exception as exc:
            logger.error(
                'DB error while recovering future failure for task %s (%s): %s',
                task_id,
                reason,
                exc,
            )
            return _RequeueOutcome.DB_ERROR
        match written:
            case _AttemptUnclaimed():
                return _RequeueOutcome.REQUEUED
            case _UnclaimMissed():
                return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
            case _AttemptFinished():
                return await self._settle_finished_attempt(
                    written,
                    TaskResult(err=task_error),
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _write_ended_attempt(
        self,
        task_id: str,
        task_error: TaskError,
        claimed_at: Optional[datetime],
        retry: RetryPlacement,
    ) -> _EndedAttemptWrite:
        """Write the failure of an attempt whose child has ended.

        One unlocked read of the row of this claim generation, in autocommit.
        A CLAIMED row goes back to the queue (``UNCLAIM_CLAIMED_TASK_SQL``).
        Any other row goes to ``horsies_finish_failed_attempt``: a RUNNING
        row with the retry that its policy asks for (placed by ``retry``),
        and a row that this claim does not hold with ``NoRetry``, which the
        function classifies without a write. The attempt row records the
        error message as its message and its reason; the task's summary
        reason is not written. Database errors raise.
        """
        failure = FailLockedTask(
            task_id=task_id,
            fence=PriorLockedRead(worker_id=self.worker_instance_id),
            result_json=serialize_error_payload(TaskResult(err=task_error)),
            error_code=_error_code_text(task_error),
            failed_reason=None,
        )
        async with self.sf() as s:
            connection = await autocommit_connection(s)
            attempt = await read_in_flight_attempt(
                connection,
                task_id=task_id,
                worker_id=self.worker_instance_id,
                claimed_at=claimed_at,
            )
            match attempt:
                case ClaimedAttempt():
                    unclaimed = await connection.execute(
                        UNCLAIM_CLAIMED_TASK_SQL,
                        {
                            'id': task_id,
                            'wid': self.worker_instance_id,
                            'claimed_at': claimed_at,
                        },
                    )
                    if (unclaimed.rowcount or 0) > 0:
                        return _AttemptUnclaimed()
                    return _UnclaimMissed()
                case RunningAttempt():
                    request = retry_request_for(attempt, failure.error_code, retry)
                case AttemptNotHeld():
                    request = NoRetry()
                case _ as unreachable:
                    assert_never(unreachable)
            outcome = await finish_failed_attempt(
                connection,
                finish_for(
                    failure,
                    claimed_at=claimed_at,
                    retry=request,
                    attempt=AttemptFailure(
                        kind=AttemptFailureKind.FAILED,
                        error_message=task_error.message,
                        failed_reason=task_error.message,
                    ),
                ),
            )
        return _AttemptFinished(outcome=outcome, request=request)

    async def _settle_finished_attempt(
        self,
        written: _AttemptFinished,
        task_result: TaskResult[None, TaskError],
    ) -> _RequeueOutcome:
        """Run what follows a failed-attempt finish, after its session closed.

        A workflow task's failure runs phase 2; a plain task's failure got
        its capacity wake from the function. A retry gets its delayed wake. A
        committed failure of this claim (a replay) has its phase 2 replayed.
        A phase-2 error goes to the finalize retry handler.
        """
        match written.outcome:
            case FailureApplied(task_id=task_id, after=after):
                match after:
                    case PhaseTwoOwed(task_name=task_name, queue_name=queue_name):
                        phase2_r = await self._finalize_workflow_phase(
                            task_id,
                            task_result,
                            queue_name=queue_name,
                            is_workflow_task=True,
                            task_name=task_name,
                        )
                        if is_err(phase2_r):
                            await self._handle_finalize_error(phase2_r.err_value)
                    case CapacityWoken():
                        pass
                    case _ as unreachable:
                        assert_never(unreachable)
                return _RequeueOutcome.REQUEUED
            case RetryScheduled() as scheduled:
                self._wake_when_retry_due(scheduled, written.request)
                return _RequeueOutcome.REQUEUED
            case AlreadyApplied(task_id=task_id):
                await self._replay_committed_terminalization(task_id)
                return _RequeueOutcome.TERMINAL_REPLAYED
            case LostClaim() | SourceStateConflict() | TaskAbsent():
                return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
            case _ as unreachable:
                assert_never(unreachable)

    async def _replay_committed_terminalization(self, task_id: str) -> None:
        """Replay phase 2 from the terminal row this claim committed.

        A plain task owes nothing: the failed-attempt finish sent its
        capacity wake in the statement that committed. Total: a failed load
        or phase 2 goes to the finalize retry handler.
        """
        load_r = await self._load_persisted_task_result(task_id)
        if is_err(load_r):
            await self._handle_finalize_error(load_r.err_value)
            return
        persisted = load_r.ok_value
        if not persisted.is_workflow_task:
            return
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
        - a RUNNING row fails with TASK_TIMEOUT and its attempt row, in one
          statement; a workflow task then runs phase 2, and a phase-2 error
          goes to the finalize retry handler;
        - a RUNNING row whose policy retries TASK_TIMEOUT gets its retry and
          its attempt row, in one statement;
        - a CLAIMED row (the child never confirmed RUNNING) is requeued;
        - a row that this claim already failed has its phase 2 replayed;
        - a row that another actor holds (reaper reclaim, cancel) is left
          alone.

        ``_TimeoutWriteFailed``: a database error, logged here. The row is
        unchanged, or the write committed and its reply was lost. The caller
        returns a timeout-stage finalize error: its retries run this write
        again, and the function classifies a committed failure as a replay;
        crash recovery writes TASK_TIMEOUT after the last retry failed.
        """
        try:
            return await self._settle_timed_out_row(
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
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime],
        retry: RetryPlacement,
    ) -> _TimeoutSettled:
        """Settle this claim's row for a timeout.

        The contract is the one of ``_record_task_timeout``. Database errors
        raise to that caller.
        """
        task_error = TaskError(
            error_code=OutcomeCode.TASK_TIMEOUT,
            message=f'Task exceeded timeout_ms={timeout_ms}',
            data={'task_id': task_id, 'timeout_ms': timeout_ms},
        )
        written = await self._write_ended_attempt(
            task_id,
            task_error,
            claimed_at,
            retry,
        )
        match written:
            case _AttemptUnclaimed():
                logger.warning(
                    'Task %s hit timeout_ms=%s before user code started; requeued',
                    task_id,
                    timeout_ms,
                )
            case _UnclaimMissed():
                logger.warning(
                    'Task %s hit timeout_ms=%s before user code started; the '
                    'requeue found no row of this claim',
                    task_id,
                    timeout_ms,
                )
            case _AttemptFinished():
                _ = await self._settle_finished_attempt(
                    written,
                    TaskResult(err=task_error),
                )
            case _ as unreachable:
                assert_never(unreachable)
        return _TimeoutSettled()
