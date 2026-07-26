"""Dispatch: hand claimed tasks to the process pool; contain failures.

DispatchMixin submits claimed rows to the executor, arms the per-task
timeout, and contains child failures: requeue on dispatch failure,
BrokenProcessPool recovery, the ownership-guarded timeout handler, and
the live-children-only SIGKILL. Worker-internal mixin: the
``TYPE_CHECKING`` block declares the slice of ``Worker`` it relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import signal
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, cast

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.logging import get_logger
from concurrent.futures.process import BrokenProcessPool

from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Result, is_err
from horsies.core.worker.child_runner import _run_task_entry
from horsies.core.worker.runtime import _FinalizeError, _RequeueOutcome, _RetryError
from horsies.core.worker.sql import (
    MARK_TASK_FAILED_SQL,
    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
    UNCLAIM_CLAIMED_TASK_SQL,
    UPSERT_TASK_ATTEMPT_SQL,
)
from horsies.core.utils.url import to_psycopg_url

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from concurrent.futures import ProcessPoolExecutor
    from typing import Literal

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.worker.config import WorkerConfig

logger = get_logger('worker')


class DispatchMixin:
    """Executor submission, timeout containment, dispatch-failure recovery."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        worker_instance_id: str
        _executor: Optional[ProcessPoolExecutor]

        # Cross-concern methods provided by sibling mixins / Worker.
        async def _finalize_after(
            self,
            fut: asyncio.Future[tuple[bool, str, Optional[str]]],
            task_id: str,
            queue_name: str = 'default',
            is_workflow_task: bool = True,
            timeout_ms: Optional[int] = None,
            *,
            executor: ProcessPoolExecutor,
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
        async def _handle_finalize_error(self, err: Any) -> None: ...
        async def _restart_executor(
            self,
            reason: str,
            failed_executor: ProcessPoolExecutor,
        ) -> None: ...
        async def _ensure_executor(self, reason: str) -> None: ...
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

    async def _handle_broken_pool(
        self,
        task_id: str,
        exc: BaseException,
        failed_executor: ProcessPoolExecutor,
        claimed_at: Optional[datetime] = None,
    ) -> None:
        """Recover the task whose pool broke, then replace the executor.

        Raises:
            ExecutorRestartFailedError: from ``_restart_executor``
                (process-fatal). On the dispatch path it propagates to the
                main loop. On the ``_finalize_after`` path it escapes the
                background finalizer task (a raise inside an except handler
                bypasses the sibling arms) into ``_on_done``, which captures
                it and stops the worker; ``run_forever`` re-raises it for a
                non-zero exit — no executorless zombie window.
        """
        outcome = await self._recover_worker_future_failure(
            task_id,
            f'Broken process pool: {exc}',
            claimed_at=claimed_at,
        )
        if outcome is _RequeueOutcome.DB_ERROR:
            logger.critical(
                'Requeue DB_ERROR: task %s may remain orphaned in-flight '
                '(worker=%s, reason=broken pool: %s)',
                task_id,
                self.worker_instance_id,
                exc,
            )
        await self._restart_executor(
            f'Broken process pool: {exc}',
            failed_executor=failed_executor,
        )

    async def _recover_worker_future_failure(
        self,
        task_id: str,
        reason: str,
        claimed_at: Optional[datetime] = None,
    ) -> _RequeueOutcome:
        """Recover a task whose child future failed without a task result.

        CLAIMED means user code never started and can be unclaimed. RUNNING
        means user code may have executed, so recovery must respect retry policy.

        ``claimed_at`` fences every statement to the dispatch's claim
        generation (C10); None disables the fence.
        """
        try:
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
                    await s.rollback()
                    return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED

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

                task_error = TaskError(
                    error_code=OperationalErrorCode.WORKER_CRASHED,
                    message=reason,
                    data={
                        'task_id': task_id,
                        'worker_id': self.worker_instance_id,
                    },
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
                    task_id, task_error, s,
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
                        task_id, s, queue_name=ctx_row.queue_name or 'default',
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

                task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
                result_json = serialize_error_payload(task_result)
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
                mark_failed_res = await s.execute(
                    MARK_TASK_FAILED_SQL,
                    {
                        'result_json': result_json,
                        'id': task_id,
                        'wid': self.worker_instance_id,
                        'error_code': OperationalErrorCode.WORKER_CRASHED.value,
                    },
                )
                if mark_failed_res.fetchone() is None:
                    await s.rollback()
                    return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
                await s.commit()
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
        """Submit to process pool; attach completion handler.

        Total for per-task failures: dispatch errors requeue or recover the
        task and return.

        ``claimed_at`` is the claim generation this dispatch was born from
        (the claim statement's RETURNING); it fences the child's ownership
        confirm and every parent-side finalize/recovery statement so a
        stale dispatch cannot touch a re-claimed row (C10).

        Raises:
            ExecutorRestartFailedError: from ``_ensure_executor``
                (process-fatal; propagates through the claim loop to the
                main loop, which lets the worker crash for a supervisor
                restart).
        """
        if self._executor is None:
            await self._ensure_executor('Executor missing before dispatch')
            if self._executor is None:
                outcome = await self._requeue_claimed_task(
                    task_id,
                    'Executor unavailable after restart attempt',
                    claimed_at=claimed_at,
                )
                if outcome is _RequeueOutcome.DB_ERROR:
                    logger.critical(
                        'Requeue DB_ERROR: task %s may remain orphaned CLAIMED '
                        '(worker=%s, reason=executor unavailable)',
                        task_id,
                        self.worker_instance_id,
                    )
                return
        executor = self._executor
        if executor is None:
            return
        loop = asyncio.get_running_loop()

        # Get heartbeat interval from recovery config (milliseconds)
        runner_heartbeat_interval_ms = 30_000  # default: 30 seconds
        if self.cfg.recovery_config:
            runner_heartbeat_interval_ms = (
                self.cfg.recovery_config.runner_heartbeat_interval_ms
            )

        # Pass task_id and database_url to task process for self-heartbeat
        database_url = to_psycopg_url(self.cfg.dsn)
        try:
            fut = loop.run_in_executor(
                executor,
                _run_task_entry,
                task_name,
                args_json,
                kwargs_json,
                task_id,
                database_url,
                self.worker_instance_id,
                runner_heartbeat_interval_ms,
                is_workflow_task,
                timeout_ms,
                claimed_at,
            )
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(task_id, exc, executor, claimed_at=claimed_at)
            return
        except Exception as exc:
            outcome = await self._recover_worker_future_failure(
                task_id,
                f'Failed to dispatch task to executor: {exc}',
                claimed_at=claimed_at,
            )
            if outcome is _RequeueOutcome.DB_ERROR:
                logger.critical(
                    'Requeue DB_ERROR: task %s may remain orphaned CLAIMED '
                    '(worker=%s, reason=dispatch failed: %s)',
                    task_id,
                    self.worker_instance_id,
                    exc,
                )
            return

        # When done, record the outcome
        self._spawn_background(
            self._finalize_after(
                fut,
                task_id,
                queue_name,
                is_workflow_task,
                timeout_ms=timeout_ms,
                executor=executor,
                task_name=task_name,
                claimed_at=claimed_at,
            ),
            name=f'finalize-{task_id}',
            finalizer=True,
        )

    # ----- finalize (write back to DB + notify) -----

    async def _handle_task_timeout(
        self,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime] = None,
    ) -> None:
        """Persist TASK_TIMEOUT for an over-deadline task and SIGKILL its child.

        Ownership-guarded like every finalize path: a row already resolved by
        another actor (reaper reclaim, cancel) is left alone. A CLAIMED row
        (child never confirmed RUNNING) is requeued instead of killed — the
        child's ownership confirm will come back CLAIM_LOST.

        ``claimed_at`` fences the row lookup to this dispatch's claim
        generation (C10); a re-claimed row is left alone entirely.

        The kill happens on both the retry-scheduled and terminal-failure
        branches: the hung child keeps executing user code either way, and a
        zombie attempt racing a re-claimed retry would be a double execution.
        """
        kill_pid: int | None = None
        try:
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
                    return

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

                kill_pid = ctx_row.worker_pid
                task_error = TaskError(
                    error_code=OutcomeCode.TASK_TIMEOUT,
                    message=f'Task exceeded timeout_ms={timeout_ms}',
                    data={'task_id': task_id, 'timeout_ms': timeout_ms},
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

                retry_scheduled = False
                should_retry_r = await self._should_retry_task(
                    task_id, task_error, s,
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
                        task_id, s, queue_name=ctx_row.queue_name or 'default',
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

                task_result: TaskResult[None, TaskError] = TaskResult(
                    err=task_error,
                )
                mark_result = await s.execute(
                    MARK_TASK_FAILED_SQL,
                    {
                        'result_json': serialize_error_payload(task_result),
                        'id': task_id,
                        'wid': self.worker_instance_id,
                        'error_code': OutcomeCode.TASK_TIMEOUT.value,
                    },
                )
                if mark_result.fetchone() is None:
                    await s.rollback()
                    return
                await s.commit()
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
            if kill_pid:
                self._kill_owned_child(kill_pid, task_id, timeout_ms)

    def _kill_owned_child(
        self, kill_pid: int, task_id: str, timeout_ms: int
    ) -> None:
        """SIGKILL a timed-out child only if it is a live child of ours.

        The pid comes from the task row and can be stale: a concurrent
        timeout or broken pool may already have restarted the executor and
        reaped the child, after which the OS is free to reuse the pid for
        an unrelated process. Membership in the live executor's process map
        confines the kill to processes this worker owns; a missing pid
        means the pool teardown already terminated the child.
        """
        from multiprocessing.process import BaseProcess

        live_children = cast(
            'dict[int, BaseProcess]',
            getattr(self._executor, '_processes', None) or {},
        )
        if kill_pid not in live_children:
            logger.warning(
                'Skipping kill for timed-out task %s: pid=%s is not a live '
                'child of the current executor (already reaped or pool '
                'restarted)',
                task_id,
                kill_pid,
            )
            return
        try:
            os.kill(kill_pid, signal.SIGKILL)
            logger.warning(
                'Killed child pid=%s for timed-out task %s '
                '(timeout_ms=%s); the process pool restarts and '
                'sibling tasks recover via crash recovery',
                kill_pid,
                task_id,
                timeout_ms,
            )
        except ProcessLookupError:
            pass
