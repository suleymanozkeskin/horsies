"""Dispatch: hand claimed tasks to child slots; contain failures.

DispatchMixin submits claimed rows to the slot pool and contains child
failures: requeue of a task that never reached a child, recovery of a task
whose child was lost, and the timeout handler, which ends the attempt before it
writes the ownership-guarded row. Worker-internal mixin: the ``TYPE_CHECKING``
block declares the slice of ``Worker`` it relies on.
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
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.worker.child_protocol import RunFrame
from horsies.core.worker.runtime import (
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
    ChildEnded,
    ChildEndUnknown,
    ChildLost,
    ExitUnknown,
    NotDispatched,
    NothingToKill,
    SlotAnswered,
    SlotHandle,
    SlotOutcome,
    SlotPool,
    TaskFinished,
    describe_exit,
)
from horsies.core.worker.stored_timeout import NoTimeout, StoredTimeout
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


class DispatchMixin:
    """Slot submission, timeout containment, dispatch-failure recovery."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        worker_instance_id: str
        _slots: SlotPool | None

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
            self, task_id: str, session: AsyncSession, queue_name: str
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
    ) -> _RequeueOutcome:
        """Recover a task whose child ended without a task result.

        CLAIMED means user code never started and can be unclaimed. RUNNING
        means user code may have executed, so recovery must respect retry policy.

        Call it only when no child can still run the attempt: after the
        child's exit was seen, or for a task that never reached a child. It
        writes WORKER_CRASHED or a retry, and the attempt must end before its
        row moves.

        ``claimed_at`` fences every statement to the dispatch's claim
        generation; None disables the fence.
        """
        try:
            task_error = TaskError(
                error_code=OperationalErrorCode.WORKER_CRASHED,
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
                error_code=OperationalErrorCode.WORKER_CRASHED.value,
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
                                    'error_code': OperationalErrorCode.WORKER_CRASHED.value,
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
                        'error_code': OperationalErrorCode.WORKER_CRASHED.value,
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
    ) -> None:
        """Submit to the slot pool and spawn the finalizer.

        Never raises for a task: the pool answers every submit, and the
        finalizer handles that answer.

        ``claimed_at`` is the claim generation this dispatch was born from
        (the claim statement's RETURNING); it fences the child's ownership
        confirm and every parent-side finalize/recovery statement so a
        stale dispatch cannot touch a re-claimed row (C10).

        ``timeout`` is the stored timeout as the claim read it. A refusal is
        sent to the child, which sets the row RUNNING and fails the task with
        WORKER_SERIALIZATION_ERROR before it resolves the task.
        """
        slots = self._slots
        assert slots is not None, 'dispatch before the slot pool started'

        # Get heartbeat interval from recovery config (milliseconds)
        runner_heartbeat_interval_ms = 30_000  # default: 30 seconds
        if self.cfg.recovery_config:
            runner_heartbeat_interval_ms = (
                self.cfg.recovery_config.runner_heartbeat_interval_ms
            )

        handle = slots.submit(
            RunFrame(
                task_id=task_id,
                task_name=task_name,
                args_json=args_json,
                kwargs_json=kwargs_json,
                worker_id=self.worker_instance_id,
                runner_heartbeat_interval_ms=runner_heartbeat_interval_ms,
                is_workflow_task=is_workflow_task,
                timeout=timeout,
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
                timeout=timeout,
                task_name=task_name,
                claimed_at=claimed_at,
            ),
            name=f'finalize-{task_id}',
            finalizer=True,
        )

    # ----- finalize (write back to DB + notify) -----

    async def _handle_task_timeout(
        self,
        handle: SlotHandle,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime] = None,
    ) -> _TimeoutOutcome:
        """End a timed-out attempt, then record the timeout.

        Once the deadline fires, the outcome of this dispatch is TASK_TIMEOUT.
        The attempt ends before its row moves. The child that holds this task
        gets SIGKILL, with its session, and the parent waits up to
        ``CHILD_KILL_WAIT_S`` for that child's exit. Only after the exit are
        TASK_TIMEOUT, the retry, the attempt row or the requeue written, and
        only then does phase 2 run. So a retry never runs next to the attempt
        that it replaces, and phase 2 cannot start a successor while that
        attempt runs. The kill is bound to this task's slot, so it cannot
        reach the next task. A slot that answered after the timer fired has
        nothing to kill; see ``_time_out_answered_task``.

        Returns:
        - ``_TimeoutSettled`` or ``_TimeoutWriteFailed``: the attempt ended,
          and ``_record_task_timeout`` ran.
        - ``_AttemptEndUnknown``: no exit was seen, so the attempt may still
          have effects in flight. Nothing is written and phase 2 does not
          run. The row keeps its state for stale-task recovery: a RUNNING row
          fails after ``running_stale_threshold_ms`` with no runner heartbeat,
          and a CLAIMED row is claimed again when its lease ends. Logged at
          critical level with the task id and pid.

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
                )
            case ChildEndUnknown(pid=pid):
                logger.critical(
                    'Child pid=%s of timed-out task %s did not exit %ss after '
                    'SIGKILL; nothing is written, and the row is left to '
                    'stale-task recovery',
                    pid,
                    task_id,
                    CHILD_KILL_WAIT_S,
                )
                return _AttemptEndUnknown()
            case SlotAnswered(outcome=outcome):
                return await self._time_out_answered_task(
                    task_id,
                    timeout_ms,
                    claimed_at,
                    outcome,
                )
            case NothingToKill():
                raise AssertionError(
                    f'deadline of task {task_id} fired before a child took it',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _time_out_answered_task(
        self,
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
        see: that returns ``_AttemptEndUnknown`` and writes nothing, as in
        ``_handle_task_timeout``.
        """
        match outcome:
            case ChildLost(child_exit=ExitUnknown(), pid=pid):
                logger.critical(
                    'Timed-out task %s lost its child pid=%s with no exit seen; '
                    'nothing is written, and the row is left to stale-task '
                    'recovery',
                    task_id,
                    pid,
                )
                return _AttemptEndUnknown()
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
                )
            case NotDispatched():
                raise AssertionError(
                    f'deadline of task {task_id} fired for a task that never '
                    'reached a child',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _record_task_timeout(
        self,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime],
    ) -> _TimeoutSettled | _TimeoutWriteFailed:
        """Record the timeout of an attempt whose child has exited.

        Ownership-guarded like every finalize path, and fenced to the claim
        generation ``claimed_at``; None disables the fence.

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
        row unchanged; an error after it leaves the row written. The caller's
        crash recovery reads the row under its lock and acts on its state.
        """
        try:
            async with self.sf() as s:
                return await self._settle_timed_out_row(
                    s,
                    task_id,
                    timeout_ms,
                    claimed_at,
                )
        except Exception as exc:
            logger.error(
                'Failed to persist TASK_TIMEOUT for task %s: %s; crash '
                'recovery will classify the row',
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

        retry_r = await self._schedule_timeout_retry(s, task_id, task_error, queue_name)
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
        retry_r = await self._schedule_retry(task_id, s, queue_name=queue_name)
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
