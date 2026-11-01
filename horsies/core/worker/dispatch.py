"""Dispatch: hand claimed tasks to child slots; contain failures.

DispatchMixin submits claimed rows to the slot pool and contains child
failures: requeue of a task that never reached a child, recovery of a task
whose child was lost, and the ownership-guarded timeout handler. Worker-internal
mixin: the ``TYPE_CHECKING`` block declares the slice of ``Worker`` it relies on.
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
from horsies.core.types.result import Result, is_err
from horsies.core.worker.child_protocol import RunFrame
from horsies.core.worker.runtime import _FinalizeError, _RequeueOutcome, _RetryError
from horsies.core.worker.slots import KillSent, NothingToKill, SlotHandle, SlotPool
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
            timeout_ms: Optional[int] = None,
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

        ``claimed_at`` fences every statement to the dispatch's claim
        generation (C10); None disables the fence.
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
                # T05 leaves the task summary reason unchanged. The attempt row
                # owns this failure's reason.
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
        timeout_ms: Optional[int] = None,
        claimed_at: Optional[datetime] = None,
    ) -> None:
        """Submit to the slot pool and spawn the finalizer.

        Never raises for a task: the pool answers every submit, and the
        finalizer handles that answer.

        ``claimed_at`` is the claim generation this dispatch was born from
        (the claim statement's RETURNING); it fences the child's ownership
        confirm and every parent-side finalize/recovery statement so a
        stale dispatch cannot touch a re-claimed row (C10).
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
                timeout_ms=timeout_ms,
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
                timeout_ms=timeout_ms,
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
    ) -> None:
        """Persist TASK_TIMEOUT for an over-deadline task and kill its child.

        Ownership-guarded like every finalize path: a row already resolved by
        another actor (reaper reclaim, cancel) is left alone, and a CLAIMED row
        (child never confirmed RUNNING) is requeued. The child is killed on
        every path, also after a database error: a child past its deadline may
        be hung, and only its exit frees the slot and ends the wait for it.

        ``claimed_at`` fences the row lookup to this dispatch's claim
        generation (C10); a re-claimed row is left alone entirely.

        A hung child keeps executing user code whatever the row says, and a
        zombie attempt racing a re-claimed retry would be a double execution.
        The kill is bound to this task's slot, so it cannot reach the next
        task, and it goes to the child's session, so task subprocesses go too.
        """
        try:
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
                # T05 leaves the task summary reason unchanged. The attempt row
                # owns this failure's reason.
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
                            return
                        case LostClaim() | SourceStateConflict() | TaskAbsent():
                            await s.rollback()
                            return
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
                        'Task %s hit timeout_ms=%s before user code started; '
                        'requeued',
                        task_id,
                        timeout_ms,
                    )
                    return

                attempt_num = (ctx_row.retry_count or 0) + 1
                db_now = ctx_row.db_now or datetime.now(timezone.utc)
                attempt_started_at = ctx_row.started_at or db_now
                attempt_worker = {
                    'worker_id': ctx_row.claimed_by_worker_id,
                    'worker_hostname': ctx_row.worker_hostname,
                    'worker_pid': ctx_row.worker_pid,
                    'worker_process_name': ctx_row.worker_process_name,
                }

                retry_scheduled = False
                should_retry_r = await self._should_retry_task(
                    task_id,
                    task_error,
                    s,
                )
                if is_err(should_retry_r):
                    logger.error(
                        'Retry decision failed for timed-out task %s: %s; '
                        'killing the child anyway (crash recovery will '
                        'classify the row)',
                        task_id,
                        should_retry_r.err_value.message,
                    )
                    return
                if should_retry_r.ok_value:
                    retry_r = await self._schedule_retry(
                        task_id,
                        s,
                        queue_name=ctx_row.queue_name or 'default',
                    )
                    if is_err(retry_r):
                        logger.error(
                            'Retry scheduling failed for timed-out task %s: %s; '
                            'killing the child anyway (crash recovery will '
                            'classify the row)',
                            task_id,
                            retry_r.err_value.message,
                        )
                        return
                    retry_scheduled = retry_r.ok_value == 'scheduled'

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
                        **attempt_worker,
                    },
                )
                if retry_scheduled:
                    await s.commit()
                    return

                terminalization = await apply_async(
                    await s.connection(),
                    failure_command,
                )
                match terminalization:
                    case Applied():
                        await s.commit()
                    case AlreadyApplied():
                        await self._replay_committed_terminalization(task_id, s)
                        return
                    case LostClaim() | SourceStateConflict() | TaskAbsent():
                        await s.rollback()
                        return
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
        except Exception as exc:
            logger.error(
                'Failed to persist TASK_TIMEOUT for task %s: %s; killing the '
                'child anyway (crash recovery will classify the row)',
                task_id,
                exc,
            )
        finally:
            self._kill_timed_out_child(handle, task_id, timeout_ms)

    def _kill_timed_out_child(
        self,
        handle: SlotHandle,
        task_id: str,
        timeout_ms: int,
    ) -> None:
        """Kill the child that holds this task, if it still holds it."""
        match handle.kill():
            case KillSent():
                logger.warning(
                    'Killed the child of timed-out task %s (timeout_ms=%s); the '
                    'slot restarts and other tasks are not affected',
                    task_id,
                    timeout_ms,
                )
            case NothingToKill():
                logger.warning(
                    'Skipping kill for timed-out task %s: its child no longer '
                    'holds it',
                    task_id,
                )
            case _ as unreachable:
                assert_never(unreachable)
