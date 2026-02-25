# app/core/worker.py
# pyright: reportPrivateUsage=false
from __future__ import annotations
import asyncio
import uuid
import os
import random
import socket
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timezone, timedelta
from importlib import import_module
from typing import Any, Optional
import hashlib
import sys
from psycopg.types.json import Jsonb
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.app import Horsies
from horsies.core.brokers.listener import PostgresListener
from horsies.core.codec.serde import (
    loads_json,
    task_result_from_json,
)
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.logging import get_logger
from horsies.core.worker.current import set_current_app
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.types.result import Ok, Err, Result, is_err
from horsies.core.defaults import DEFAULT_CLAIM_LEASE_MS
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.url import to_psycopg_url

# --- Imports from sibling modules (extracted for maintainability) ---
from horsies.core.worker.config import WorkerConfig  # noqa: F401
from horsies.core.worker.child_pool import _initialize_worker_pool  # noqa: F401
from horsies.core.codec.serde import serialize_error_payload
from horsies.core.worker.child_runner import (  # noqa: F401
    _locate_app,
    _child_initializer,
    _run_task_entry,
    import_by_path,
    _dedupe_paths,
    _build_sys_path_roots,
    _derive_sys_path_roots_from_file,
    _debug_imports_enabled,
    _debug_imports_log,
    _is_retryable_db_error,
    _heartbeat_worker,
)
from horsies.core.worker.sql import (  # noqa: F401
    CLAIM_SQL,
    CLAIM_ADVISORY_LOCK_SQL,
    COUNT_GLOBAL_IN_FLIGHT_SQL,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
    COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_RUNNING_FOR_WORKER_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_RUNNING_IN_QUEUE_SQL,
    GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
    UNCLAIM_PAUSED_TASKS_SQL,
    UNCLAIM_CLAIMED_TASK_SQL,
    RESET_PAUSED_WORKFLOW_TASKS_SQL,
    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
    SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
    MARK_TASK_FAILED_WORKER_SQL,
    MARK_TASK_FAILED_SQL,
    MARK_TASK_COMPLETED_SQL,
    GET_TASK_QUEUE_NAME_SQL,
    NOTIFY_TASK_NEW_SQL,
    NOTIFY_TASK_QUEUE_SQL,
    CHECK_WORKFLOW_TASK_EXISTS_SQL,
    GET_TASK_RETRY_INFO_SQL,
    GET_TASK_RETRY_CONFIG_SQL,
    SCHEDULE_TASK_RETRY_SQL,
    NOTIFY_DELAYED_SQL,
    INSERT_CLAIMER_HEARTBEAT_SQL,
    RENEW_CLAIM_LEASE_SQL,
    INSERT_WORKER_STATE_SQL,
    DELETE_EXPIRED_HEARTBEATS_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_EXPIRED_TASKS_SQL,
    _RETENTION_CLEANUP_INTERVAL_S,
    _FINALIZER_DRAIN_TIMEOUT_S,
)

logger = get_logger("worker")

_FINALIZE_STAGE_PHASE1 = 'phase1_persist'
_FINALIZE_STAGE_PHASE2 = 'phase2_workflow'
_FINALIZE_STAGE_FUTURE = 'future'
_FINALIZE_PHASE1_MAX_RETRIES = 3
_FINALIZE_PHASE2_MAX_RETRIES = 5
_FINALIZE_RETRY_BASE_DELAY_S = 0.5
_FINALIZE_RETRY_MAX_DELAY_S = 15.0

GET_TASK_STATUS_RESULT_SQL = text("""
    SELECT status, result
    FROM horsies_tasks
    WHERE id = :id
""")


@dataclass
class _RetryBackoff:
    initial_ms: int
    max_ms: int
    max_attempts: int
    attempts: int = 0

    def reset(self) -> None:
        self.attempts = 0

    def can_retry(self) -> bool:
        match self.max_attempts:
            case 0:
                return True
            case _:
                return self.attempts < self.max_attempts

    def next_delay_seconds(self) -> float:
        self.attempts += 1
        exponent = max(0, self.attempts - 1)
        base_ms = min(self.max_ms, int(self.initial_ms * (2**exponent)))
        jitter_range = base_ms * 0.25
        delay_ms = base_ms + random.uniform(-jitter_range, jitter_range)
        return max(0.1, delay_ms / 1000.0)


@dataclass(frozen=True)
class _FinalizeError:
    error_code: LibraryErrorCode | str
    message: str
    stage: str
    task_id: str
    retryable: bool = False
    data: dict[str, Any] | None = None


class _RequeueOutcome(str, Enum):
    REQUEUED = 'REQUEUED'
    NOT_OWNER_OR_NOT_CLAIMED = 'NOT_OWNER_OR_NOT_CLAIMED'
    DB_ERROR = 'DB_ERROR'


def _collect_psutil_metrics() -> tuple[float, float, float]:
    """Collect process metrics. Blocking — must run in a thread."""
    import psutil

    process = psutil.Process()
    memory_info = process.memory_info()
    return (
        memory_info.rss / 1024 / 1024,
        process.memory_percent(),
        process.cpu_percent(interval=0.1),
    )


class Worker:
    """
    Async master that:
      - Subscribes to queue channels
      - Claims tasks (priority + sent_at) with SKIP LOCKED
      - Executes in a process pool
      - On completion, writes result/failed, COMMITs, and NOTIFY task_done
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        listener: PostgresListener,
        cfg: WorkerConfig,
    ):
        self.sf = session_factory
        self.listener = listener
        self.cfg = cfg
        self.worker_instance_id = str(uuid.uuid4())
        self._started_at = datetime.now(timezone.utc)
        self._app: Horsies | None = None
        self._resilience = self.cfg.resilience_config or WorkerResilienceConfig()
        # Delay creation of the process pool until after preloading modules so that
        # any import/validation errors surface in the main process at startup.
        self._executor: Optional[ProcessPoolExecutor] = None
        self._stop = asyncio.Event()
        self._service_tasks: set[asyncio.Task[Any]] = set()
        self._finalizer_tasks: set[asyncio.Task[Any]] = set()
        self._finalize_retry_attempts: dict[tuple[str, str], int] = {}

    def request_stop(self) -> None:
        """Request worker to stop gracefully."""
        self._stop.set()

    def _spawn_background(
        self,
        coro: Any,
        *,
        name: str,
        finalizer: bool = False,
    ) -> asyncio.Task[Any]:
        """Create a tracked background task with automatic cleanup."""
        task_group = self._finalizer_tasks if finalizer else self._service_tasks
        task = asyncio.create_task(coro, name=name)
        task_group.add(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            task_group.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.error(f'Background task {t.get_name()!r} failed: {exc}')
                return
            if finalizer:
                result = t.result()
                if isinstance(result, Err):
                    self._spawn_background(
                        self._handle_finalize_error(result.err_value),
                        name=f'finalize-error-{t.get_name()}',
                    )

        task.add_done_callback(_on_done)
        return task

    def _create_executor(self) -> ProcessPoolExecutor:
        child_database_url = to_psycopg_url(self.cfg.dsn)
        return ProcessPoolExecutor(
            max_workers=self.cfg.processes,
            initializer=_child_initializer,
            initargs=(
                self.cfg.app_locator,
                self.cfg.imports,
                self.cfg.sys_path_roots,
                self.cfg.loglevel,
                child_database_url,
            ),
        )

    async def _restart_executor(self, reason: str) -> None:
        if self._stop.is_set():
            return
        if self._executor is None:
            self._executor = self._create_executor()
            logger.warning(f'Executor created after restart request: {reason}')
            return

        loop = asyncio.get_running_loop()
        executor = self._executor
        self._executor = None
        logger.error(f'Restarting worker executor: {reason}')
        try:
            await loop.run_in_executor(
                None, lambda: executor.shutdown(wait=True, cancel_futures=True)
            )
        except Exception as e:
            logger.error(f'Error shutting down broken executor: {e}')
        self._executor = self._create_executor()

    def _make_retry_backoff(self) -> _RetryBackoff:
        return _RetryBackoff(
            initial_ms=self._resilience.db_retry_initial_ms,
            max_ms=self._resilience.db_retry_max_ms,
            max_attempts=self._resilience.db_retry_max_attempts,
        )

    async def _sleep_with_stop(self, delay_seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay_seconds)
        except asyncio.TimeoutError:
            return

    async def _cleanup_after_failed_start(self) -> None:
        try:
            await self.listener.close()
        except Exception as e:
            logger.error(f'Error closing listener after failed start: {e}')

        if self._executor:
            loop = asyncio.get_running_loop()
            executor = self._executor
            self._executor = None
            try:
                await loop.run_in_executor(
                    None, lambda: executor.shutdown(wait=True, cancel_futures=True)
                )
            except Exception as e:
                logger.error(f'Error shutting down executor after failed start: {e}')

    async def _handle_retryable_start_error(
        self,
        exc: BaseException,
        backoff: _RetryBackoff,
    ) -> None:
        if not backoff.can_retry():
            logger.error(
                f'Worker start failed after {backoff.attempts} attempts: {exc}'
            )
            raise

        await self._cleanup_after_failed_start()
        delay = backoff.next_delay_seconds()
        logger.error(
            f'Worker start failed: {exc}. Retrying in {delay:.1f}s '
            f'(attempt {backoff.attempts}/{backoff.max_attempts or "inf"})'
        )
        await self._sleep_with_stop(delay)

    async def _start_with_resilience_config(self) -> None:
        backoff = self._make_retry_backoff()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self.start(), timeout=30.0)
                return
            except asyncio.TimeoutError as exc:
                await self._handle_retryable_start_error(exc, backoff)
                continue
            except Exception as exc:
                if is_retryable_connection_error(exc):
                    await self._handle_retryable_start_error(exc, backoff)
                    continue
                raise

    # ----- lifecycle -----

    async def start(self) -> None:
        logger.debug('Starting worker')
        # Preload the app and task modules in the main process to fail fast
        self._preload_modules_main()

        try:
            start_r = await self.listener.start()
            if is_err(start_r):
                err = start_r.err_value
                raise err.exception or RuntimeError(err.message)
            # Surface concurrency configuration clearly for operators
            max_claimed_effective = (
                self.cfg.max_claim_per_worker
                if self.cfg.max_claim_per_worker > 0
                else self.cfg.processes
            )
            logger.info(
                'Concurrency config: processes=%s, cluster_wide_cap=%s, max_claim_per_worker=%s, max_claim_batch=%s',
                self.cfg.processes,
                (
                    self.cfg.cluster_wide_cap
                    if self.cfg.cluster_wide_cap is not None
                    else 'unlimited'
                ),
                max_claimed_effective,
                self.cfg.max_claim_batch,
            )

            # Subscribe to each queue channel (and a global) in one batch.
            all_channels = [f'task_queue_{q}' for q in self.cfg.queues] + ['task_new']
            listen_r = await self.listener.listen_many(all_channels)
            if is_err(listen_r):
                err = listen_r.err_value
                raise err.exception or RuntimeError(err.message)
            all_queues = listen_r.ok_value
            self._queues = all_queues[:-1]
            self._global = all_queues[-1]
            logger.info(f'Subscribed to queues: {self.cfg.queues} + global')

            # Create the process pool only after listener startup/subscription succeeds.
            self._executor = self._create_executor()

            # Start claimer heartbeat loop (CLAIMED coverage)
            self._spawn_background(
                self._claimer_heartbeat_loop(), name='claimer-heartbeat',
            )
            # Start worker state heartbeat loop for monitoring
            self._spawn_background(
                self._worker_state_heartbeat_loop(), name='worker-state-heartbeat',
            )
            logger.info('Worker state heartbeat loop started for monitoring')
            # Start reaper loop for automatic stale task handling
            if self.cfg.recovery_config:
                self._spawn_background(self._reaper_loop(), name='reaper')
                logger.info('Reaper loop started for automatic stale task recovery')
        except Exception:
            await self._cleanup_after_failed_start()
            raise

    async def stop(
        self,
        *,
        force: bool = False,
        finalizer_timeout_s: float = _FINALIZER_DRAIN_TIMEOUT_S,
    ) -> None:
        self._stop.set()
        # Service loops are safe to cancel.
        if self._service_tasks:
            service_tasks = tuple(self._service_tasks)
            for task in service_tasks:
                task.cancel()
            await asyncio.gather(*service_tasks, return_exceptions=True)
            self._service_tasks.clear()

        # Finalizers persist task outcomes and should be drained gracefully.
        if self._finalizer_tasks:
            finalizer_tasks = tuple(self._finalizer_tasks)
            if force:
                for task in finalizer_tasks:
                    task.cancel()
                await asyncio.gather(*finalizer_tasks, return_exceptions=True)
            else:
                done, pending = await asyncio.wait(
                    finalizer_tasks, timeout=max(0.0, finalizer_timeout_s)
                )
                if pending:
                    logger.warning(
                        'Worker stop timed out with %s finalize task(s) still running; cancelling pending finalizers',
                        len(pending),
                    )
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                if done:
                    await asyncio.gather(*done, return_exceptions=True)
            self._finalizer_tasks.clear()
        # Close the Postgres listener early to avoid UNLISTEN races on dispatcher connection
        try:
            await self.listener.close()
            logger.info('Postgres listener closed')
        except Exception as e:
            logger.error(f'Error closing Postgres listener: {e}')
        # Shutdown executor
        if self._executor:
            # Offload blocking shutdown to a thread to avoid freezing the event loop
            loop = asyncio.get_running_loop()
            executor = self._executor
            self._executor = None
            try:
                await loop.run_in_executor(
                    None, lambda: executor.shutdown(wait=True, cancel_futures=True)
                )  # TODO:inspect this behaviour more in depth!
            except Exception as e:
                logger.error(f'Error shutting down executor: {e}')
            logger.info('Worker executor shutdown')
        # Release per-process registries/caches on shutdown.
        try:
            from horsies.core.workflows.registry import clear_workflow_registry
            from horsies.core.codec.serde import clear_serde_caches

            clear_workflow_registry()
            clear_serde_caches()
        except Exception as e:
            logger.error(f'Error clearing workflow/serde registries: {e}')
        logger.info('Worker stopped')

    def _preload_modules_main(self) -> None:
        """Import the app and all task modules in the main process.

        This ensures Pydantic validations and module-level side effects run once
        and any configuration errors surface during startup rather than inside
        the child process initializer.
        """
        try:
            sys_path_roots_resolved = _build_sys_path_roots(
                self.cfg.app_locator, self.cfg.imports, self.cfg.sys_path_roots
            )
            _debug_imports_log(
                f'[preload] app_locator={self.cfg.app_locator!r} sys_path_roots={sys_path_roots_resolved}'
            )
            for root in sys_path_roots_resolved:
                if root not in sys.path:
                    sys.path.insert(0, root)

            # Load app object (variable or factory)
            app = _locate_app(self.cfg.app_locator)
            # Optionally set as current for consistency in main process
            set_current_app(app)
            self._app = app

            # Suppress accidental sends while importing modules for discovery
            try:
                app.suppress_sends(True)
            except Exception:
                pass

            # Import declared modules that contain task definitions
            combined_imports = list(self.cfg.imports)
            try:
                combined_imports.extend(app.get_discovered_task_modules())
            except Exception:
                pass
            combined_imports = _dedupe_paths(combined_imports)
            _debug_imports_log(f'[preload] import_modules={combined_imports}')
            for m in combined_imports:
                if m.endswith('.py') or os.path.sep in m:
                    import_by_path(os.path.abspath(m))
                else:
                    import_module(m)

            # Re-enable sends after import completes
            try:
                app.suppress_sends(False)
            except Exception:
                pass
            _debug_imports_log(f'[preload] registered_tasks={app.list_tasks()}')
        except Exception as e:
            # Surface the error clearly and re-raise to stop startup
            logger.error(f'Failed during preload of task modules: {e}')
            raise

    # ----- main loop -----

    async def run_forever(self) -> None:
        """Main orchestrator loop."""
        await self._start_with_resilience_config()
        if self._stop.is_set():
            return
        logger.info('Worker started')
        try:
            backoff = self._make_retry_backoff()
            while not self._stop.is_set():
                try:
                    # Single budgeted claim pass, then wait for new NOTIFY
                    await self._claim_and_dispatch_all()

                    # Wait for a NOTIFY from any queue (coalesce bursts).
                    await self._wait_for_any_notify(
                        poll_interval_ms=self._resilience.notify_poll_interval_ms
                    )
                    await self._claim_and_dispatch_all()
                    backoff.reset()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if is_retryable_connection_error(exc):
                        if not backoff.can_retry():
                            logger.error(
                                f'Worker loop failed after {backoff.attempts} attempts: {exc}'
                            )
                            raise
                        delay = backoff.next_delay_seconds()
                        logger.error(
                            f'Worker loop error: {exc}. Retrying in {delay:.1f}s '
                            f'(attempt {backoff.attempts}/{backoff.max_attempts or "inf"})'
                        )
                        await self._sleep_with_stop(delay)
                        continue
                    raise
        finally:
            await self.stop()

    async def _wait_for_any_notify(self, poll_interval_ms: int) -> None:
        """Wait on any subscribed queue channel; coalesce a burst."""
        import contextlib

        queue_tasks = [
            asyncio.create_task(q.get()) for q in (self._queues + [self._global])
        ]
        # Add only the stop event as an additional wait condition (no periodic polling)
        stop_task = asyncio.create_task(self._stop.wait())
        all_tasks = queue_tasks + [stop_task]
        timeout_seconds = max(0.0, poll_interval_ms / 1000.0)
        done, pending = await asyncio.wait(
            all_tasks,
            return_when=asyncio.FIRST_COMPLETED,
            timeout=timeout_seconds,
        )

        # Check if stop was signaled
        if self._stop.is_set():
            # Cancel all pending tasks and await them to avoid warnings
            for p in pending:
                p.cancel()
            for p in pending:
                with contextlib.suppress(asyncio.CancelledError):
                    await p
            return

        # Timeout: fall back to polling
        if not done:
            for p in pending:
                p.cancel()
            for p in pending:
                with contextlib.suppress(asyncio.CancelledError):
                    await p
            return

        # cancel the rest to avoid background tasks piling up and await them
        for p in pending:
            p.cancel()
        for p in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await p

        # drain a burst
        drained = 0
        for q in self._queues + [self._global]:
            while drained < self.cfg.coalesce_notifies and not q.empty():
                try:
                    q.get_nowait()
                    drained += 1
                except asyncio.QueueEmpty:
                    break

    # ----- claim & dispatch -----

    async def _claim_and_dispatch_all(self) -> bool:
        """
        Claim tasks subject to:
          - max_claim_per_worker guard (prevents over-claiming)
          - queue priorities (CUSTOM mode)
          - per-queue max_concurrency (CUSTOM mode)
          - worker global concurrency (processes)
        Returns True if anything was claimed.
        """
        # Guard: Check if we've already claimed too many tasks
        # Default depends on mode:
        # - Hard cap (prefetch_buffer=0): default to processes
        # - Soft cap (prefetch_buffer>0): default to processes + prefetch_buffer
        if self.cfg.max_claim_per_worker > 0:
            # User explicitly set a limit - use it
            max_claimed = self.cfg.max_claim_per_worker
        elif self.cfg.prefetch_buffer > 0:
            # Soft cap mode: allow claiming up to processes + prefetch_buffer
            max_claimed = self.cfg.processes + self.cfg.prefetch_buffer
        else:
            # Hard cap mode: limit to processes
            max_claimed = self.cfg.processes
        claimed_count = await self._count_claimed_for_worker()
        if claimed_count >= max_claimed:
            return False

        # Cluster-wide, lock-guarded claim to avoid races. One short transaction.
        # CLAIM_SQL RETURNING provides dispatch payload directly (no separate load query).
        claimed_rows: list[dict[str, Any]] = []

        # Queue order: if custom priorities provided, sort by priority; otherwise keep given order
        if self.cfg.queue_priorities:
            ordered_queues = sorted(
                [q for q in self.cfg.queues if q in self.cfg.queue_priorities],
                key=lambda q: self.cfg.queue_priorities.get(q, 100),
            )
        else:
            ordered_queues = list(self.cfg.queues)

        # Open one transaction, take a global advisory xact lock
        async with self.sf() as s:
            # Take a cluster-wide transaction-scoped advisory lock to serialize claiming
            await s.execute(
                CLAIM_ADVISORY_LOCK_SQL,
                {'key': self._advisory_key_global()},
            )

            # Compute local budget and optional global remaining
            # Hard cap mode (prefetch_buffer=0): count RUNNING + CLAIMED for strict enforcement
            # Soft cap mode (prefetch_buffer>0): count only RUNNING, allow prefetch with lease
            hard_cap_mode = self.cfg.prefetch_buffer == 0

            if hard_cap_mode:
                # Hard cap: count both RUNNING and CLAIMED for this worker
                local_in_flight = await self._count_in_flight_for_worker()
                max_local_capacity = self.cfg.processes
            else:
                # Soft cap: count only RUNNING to allow prefetch beyond processes
                local_in_flight = await self._count_only_running_for_worker()
                max_local_capacity = self.cfg.processes + self.cfg.prefetch_buffer
            local_available = max(0, int(max_local_capacity) - int(local_in_flight))
            budget_remaining = local_available

            global_remaining: Optional[int] = None
            if self.cfg.cluster_wide_cap is not None:
                # Hard cap mode: count RUNNING + CLAIMED globally
                # (Note: prefetch_buffer must be 0 when cluster_wide_cap is set, enforced by config validation)
                res = await s.execute(COUNT_GLOBAL_IN_FLIGHT_SQL)
                row = res.fetchone()
                if row:
                    in_flight_global = int(row[0])
                else:
                    in_flight_global = 0
                global_remaining = max(
                    0, int(self.cfg.cluster_wide_cap) - in_flight_global
                )

            # Total claim budget for this pass: local budget capped by global remaining (if any)
            total_remaining = (
                budget_remaining
                if global_remaining is None
                else min(budget_remaining, global_remaining)
            )
            if total_remaining <= 0:
                # Nothing to claim globally or locally
                await s.commit()
                return False

            for qname in ordered_queues:
                if total_remaining <= 0:
                    break

                # Compute queue remaining in cluster (only if custom-configured)
                q_remaining: Optional[int] = None
                if (
                    self.cfg.queue_priorities
                    and qname in self.cfg.queue_max_concurrency
                ):
                    # Hard cap mode: count RUNNING + CLAIMED for this queue
                    # Soft cap mode: count only RUNNING
                    if hard_cap_mode:
                        resq = await s.execute(
                            COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
                            {'q': qname},
                        )
                    else:
                        resq = await s.execute(
                            COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
                            {'q': qname},
                        )
                    row = resq.fetchone()
                    if row:
                        in_flight_q = int(row[0])
                    else:
                        in_flight_q = 0
                    max_q = int(self.cfg.queue_max_concurrency.get(qname, 0))
                    q_remaining = max(0, max_q - in_flight_q)

                # Determine how many we may claim from this queue
                # Hierarchy: max_claim_batch (fairness) -> q_remaining (queue cap) -> total_remaining (worker budget)

                if self.cfg.queue_priorities:
                    # Strict priority mode: try to fill remaining budget from this queue
                    # Ignore max_claim_batch (which forces round-robin fairness)
                    per_queue_cap = total_remaining
                else:
                    # Default mode: use max_claim_batch to ensure fairness across queues
                    per_queue_cap = self.cfg.max_claim_batch

                if q_remaining is not None:
                    per_queue_cap = min(per_queue_cap, q_remaining)
                to_claim = min(total_remaining, per_queue_cap)
                if to_claim <= 0:
                    continue

                batch_rows = await self._claim_batch_locked(s, qname, to_claim)
                if not batch_rows:
                    continue
                claimed_rows.extend(batch_rows)
                total_remaining -= len(batch_rows)

            await s.commit()

        if not claimed_rows:
            return False

        # Post-claim guard: filter out tasks for non-runnable workflow states.
        claimed_rows = await self._filter_nonrunnable_workflow_tasks(claimed_rows)

        for row in claimed_rows:
            await self._dispatch_one(
                row['id'], row['task_name'], row['args'], row['kwargs'],
            )
        return len(claimed_rows) > 0

    async def _filter_nonrunnable_workflow_tasks(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Filter out tasks belonging to non-runnable workflows (PAUSED/CANCELLED).

        Post-claim guard:
        - PAUSED workflow: unclaim task (back to PENDING), reset workflow_task to READY
        - CANCELLED workflow: hard-cancel task + mark workflow_task SKIPPED

        Returns the filtered list of rows that should be dispatched.
        """
        if not rows:
            return rows

        task_ids = [row['id'] for row in rows]
        paused_task_ids: set[str] = set()
        cancelled_task_ids: set[str] = set()

        # Find tasks belonging to non-runnable workflows.
        async with self.sf() as s:
            res = await s.execute(
                GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
                {'ids': task_ids},
            )
            for row in res.fetchall():
                task_id = row[0]
                wf_status = row[1]
                if wf_status == 'PAUSED':
                    paused_task_ids.add(task_id)
                elif wf_status == 'CANCELLED':
                    cancelled_task_ids.add(task_id)

            if paused_task_ids:
                # Unclaim paused-workflow tasks so they can be picked up on resume.
                await s.execute(
                    UNCLAIM_PAUSED_TASKS_SQL,
                    {'ids': list(paused_task_ids)},
                )
                # Keep workflow_task metadata consistent with unclaimed tasks.
                await s.execute(
                    RESET_PAUSED_WORKFLOW_TASKS_SQL,
                    {'ids': list(paused_task_ids)},
                )

            if cancelled_task_ids:
                # Cancel claimed/pending task rows so they are no longer claimable.
                await s.execute(
                    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
                    {'ids': list(cancelled_task_ids)},
                )
                # Ensure workflow_task rows no longer sit in enqueueable states.
                await s.execute(
                    SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
                    {'ids': list(cancelled_task_ids)},
                )

            if paused_task_ids or cancelled_task_ids:
                await s.commit()

        blocked_task_ids = paused_task_ids | cancelled_task_ids
        return [row for row in rows if row['id'] not in blocked_task_ids]

    def _advisory_key_global(self) -> int:
        """Compute a stable 64-bit advisory lock key for this cluster."""
        basis = (self.cfg.psycopg_dsn or self.cfg.dsn or 'horsies').encode(
            'utf-8', errors='ignore'
        )
        h = hashlib.sha256(b'horsies-global:' + basis).digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    # Stale detection is handled via heartbeat policy for RUNNING tasks.

    def _compute_claim_expires_at(self) -> datetime:
        """Compute claim lease expiration. Always bounded (never None).

        Uses explicit claim_lease_ms when configured (soft-cap or user override),
        otherwise falls back to DEFAULT_CLAIM_LEASE_MS for crash-recovery safety.
        """
        lease_ms = (
            self.cfg.claim_lease_ms
            if self.cfg.claim_lease_ms is not None
            else DEFAULT_CLAIM_LEASE_MS
        )
        return datetime.now(timezone.utc) + timedelta(milliseconds=lease_ms)

    async def _claim_batch_locked(
        self, s: AsyncSession, queue: str, limit: int,
    ) -> list[dict[str, Any]]:
        """Claim up to *limit* tasks and return dispatch-ready row dicts.

        CLAIM_SQL RETURNING provides id/task_name/args/kwargs atomically,
        eliminating the previous claim-commit → separate-load gap.
        """
        res = await s.execute(
            CLAIM_SQL,
            {
                'queue': queue,
                'lim': limit,
                'worker_id': self.worker_instance_id,
                'claim_expires_at': self._compute_claim_expires_at(),
            },
        )
        cols = res.keys()
        return [dict(zip(cols, row)) for row in res.fetchall()]

    async def _count_claimed_for_worker(self) -> int:
        """Count only CLAIMED tasks for this worker (not yet RUNNING)."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_CLAIMED_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _count_only_running_for_worker(self) -> int:
        """Count only RUNNING tasks for this worker (excludes CLAIMED)."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _count_in_flight_for_worker(self) -> int:
        """Count RUNNING + CLAIMED tasks for this worker (hard cap mode)."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_IN_FLIGHT_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _count_running_in_queue(self, queue_name: str) -> int:
        """Count RUNNING tasks in a given queue across the cluster."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_IN_QUEUE_SQL,
                {'q': queue_name},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _requeue_claimed_task(self, task_id: str, reason: str) -> _RequeueOutcome:
        try:
            async with self.sf() as s:
                res = await s.execute(
                    UNCLAIM_CLAIMED_TASK_SQL,
                    {'id': task_id, 'wid': self.worker_instance_id},
                )
                await s.commit()
                rowcount = getattr(res, 'rowcount', 0) or 0
                requeued = rowcount > 0
        except Exception as exc:
            logger.error(
                'DB error while requeueing task %s (%s): %s',
                task_id, reason, exc,
            )
            return _RequeueOutcome.DB_ERROR

        if requeued:
            logger.warning('Requeued CLAIMED task %s: %s', task_id, reason)
            return _RequeueOutcome.REQUEUED
        else:
            logger.warning(
                'Failed to requeue task %s (not CLAIMED or owner mismatch): %s',
                task_id, reason,
            )
            return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED

    async def _handle_broken_pool(self, task_id: str, exc: BaseException) -> None:
        outcome = await self._requeue_claimed_task(
            task_id, f'Broken process pool: {exc}',
        )
        if outcome is _RequeueOutcome.DB_ERROR:
            logger.critical(
                'Requeue DB_ERROR: task %s may remain orphaned CLAIMED '
                '(worker=%s, reason=broken pool: %s)',
                task_id, self.worker_instance_id, exc,
            )
        await self._restart_executor(f'Broken process pool: {exc}')

    async def _dispatch_one(
        self,
        task_id: str,
        task_name: str,
        args_json: Optional[str],
        kwargs_json: Optional[str],
    ) -> None:
        """Submit to process pool; attach completion handler."""
        if self._executor is None:
            await self._restart_executor('Executor missing before dispatch')
            if self._executor is None:
                outcome = await self._requeue_claimed_task(
                    task_id, 'Executor unavailable after restart attempt',
                )
                if outcome is _RequeueOutcome.DB_ERROR:
                    logger.critical(
                        'Requeue DB_ERROR: task %s may remain orphaned CLAIMED '
                        '(worker=%s, reason=executor unavailable)',
                        task_id, self.worker_instance_id,
                    )
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
                self._executor,
                _run_task_entry,
                task_name,
                args_json,
                kwargs_json,
                task_id,
                database_url,
                self.worker_instance_id,
                runner_heartbeat_interval_ms,
            )
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(task_id, exc)
            return
        except Exception as exc:
            outcome = await self._requeue_claimed_task(
                task_id, f'Failed to dispatch task to executor: {exc}',
            )
            if outcome is _RequeueOutcome.DB_ERROR:
                logger.critical(
                    'Requeue DB_ERROR: task %s may remain orphaned CLAIMED '
                    '(worker=%s, reason=dispatch failed: %s)',
                    task_id, self.worker_instance_id, exc,
                )
            return

        # When done, record the outcome
        self._spawn_background(
            self._finalize_after(fut, task_id),
            name=f'finalize-{task_id}',
            finalizer=True,
        )

    # ----- finalize (write back to DB + notify) -----

    async def _finalize_after(
        self, fut: 'asyncio.Future[tuple[bool, str, Optional[str]]]', task_id: str
    ) -> Result[None, _FinalizeError]:
        try:
            ok, result_json_str, failed_reason = await fut
        except asyncio.CancelledError:
            raise
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(task_id, exc)
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_FUTURE,
                    message=f'Broken process pool during finalize: {exc}',
                    retryable=False,
                    data={'exception_type': type(exc).__name__},
                )
            )
        except Exception as exc:
            requeue_outcome = await self._requeue_claimed_task(
                task_id, f'Worker future failed before result: {exc}'
            )
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_FUTURE,
                    message=f'Worker future failed before result: {exc}',
                    retryable=is_retryable_connection_error(exc)
                        and requeue_outcome != _RequeueOutcome.REQUEUED,
                    data={
                        'exception_type': type(exc).__name__,
                        'requeue_outcome': requeue_outcome.value,
                    },
                )
            )
        now = datetime.now(timezone.utc)

        phase1_r = await self._persist_task_terminal_state(
            task_id=task_id,
            now=now,
            ok=ok,
            result_json_str=result_json_str,
            failed_reason=failed_reason,
        )
        if is_err(phase1_r):
            return phase1_r
        tr = phase1_r.ok_value
        if tr is None:
            self._clear_finalize_retry_attempts(task_id, _FINALIZE_STAGE_PHASE1)
            return Ok(None)

        # Phase 2: workflow progression + capacity wakeups in a separate transaction.
        # If this fails, the terminal task result remains durable and workflow recovery can resume.
        phase2_r = await self._finalize_workflow_phase(task_id, tr)
        if is_err(phase2_r):
            return phase2_r
        self._clear_finalize_retry_attempts(task_id, _FINALIZE_STAGE_PHASE1)
        self._clear_finalize_retry_attempts(task_id, _FINALIZE_STAGE_PHASE2)
        return Ok(None)

    def _make_finalize_error(
        self,
        *,
        task_id: str,
        stage: str,
        message: str,
        retryable: bool,
        data: dict[str, Any] | None = None,
        error_code: LibraryErrorCode | str = LibraryErrorCode.BROKER_ERROR,
    ) -> _FinalizeError:
        return _FinalizeError(
            error_code=error_code,
            message=message,
            stage=stage,
            task_id=task_id,
            retryable=retryable,
            data=data,
        )

    def _clear_finalize_retry_attempts(self, task_id: str, stage: str) -> None:
        self._finalize_retry_attempts.pop((task_id, stage), None)

    async def _persist_task_terminal_state(
        self,
        *,
        task_id: str,
        now: datetime,
        ok: bool,
        result_json_str: str,
        failed_reason: str | None,
    ) -> Result[TaskResult[Any, TaskError] | None, _FinalizeError]:
        """Phase 1 of finalization: persist task terminal state/result durably."""
        try:
            # Note: Heartbeat thread in task process automatically dies when process completes.
            async with self.sf() as s:
                if not ok:
                    # CLAIM_LOST: Another worker reclaimed this task - do nothing
                    # The task is not failed; it belongs to another worker now
                    match failed_reason:
                        case (
                            'CLAIM_LOST'
                            | 'OWNERSHIP_UNCONFIRMED'
                            | 'WORKFLOW_CHECK_FAILED'
                            | 'WORKFLOW_STOPPED'
                        ):
                            logger.debug(
                                f'Task {task_id} aborted with reason={failed_reason}, skipping finalization'
                            )
                            return Ok(None)
                        case _:
                            pass

                    # worker-level failure (rare): mark FAILED with reason
                    res = await s.execute(
                        MARK_TASK_FAILED_WORKER_SQL,
                        {
                            'now': now,
                            'reason': failed_reason or 'Worker failure',
                            'id': task_id,
                        },
                    )
                    if res.fetchone() is None:
                        logger.warning(
                            f'Task {task_id} finalize aborted: status is no longer RUNNING '
                            f'(reaper likely reclaimed). Skipping to prevent double-execution.'
                        )
                        return Ok(None)
                    # Trigger automatically sends NOTIFY on UPDATE
                    await s.commit()
                    return Ok(None)

                # Parse the TaskResult we produced
                _loads_r = loads_json(result_json_str)
                if is_err(_loads_r):
                    logger.error(f'Task {task_id} result JSON is corrupt: {_loads_r.err_value}')
                    _err_tr: TaskResult[None, TaskError] = TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.WORKER_SERIALIZATION_ERROR,
                            message=f'Result JSON corrupt: {_loads_r.err_value}',
                            data={'task_id': task_id},
                        ),
                    )
                    await s.execute(
                        MARK_TASK_FAILED_SQL,
                        {'now': now, 'result_json': serialize_error_payload(_err_tr), 'id': task_id},
                    )
                    await s.commit()
                    return Ok(None)
                _tr_r = task_result_from_json(_loads_r.ok_value)
                if is_err(_tr_r):
                    logger.error(f'Task {task_id} result deser failed: {_tr_r.err_value}')
                    _err_tr = TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.WORKER_SERIALIZATION_ERROR,
                            message=f'Result deser failed: {_tr_r.err_value}',
                            data={'task_id': task_id},
                        ),
                    )
                    await s.execute(
                        MARK_TASK_FAILED_SQL,
                        {'now': now, 'result_json': serialize_error_payload(_err_tr), 'id': task_id},
                    )
                    await s.commit()
                    return Ok(None)
                tr = _tr_r.ok_value
                if tr.is_err():
                    # Check if this task should be retried
                    task_error = tr.unwrap_err()
                    match task_error.error_code if task_error else None:
                        case 'WORKFLOW_STOPPED':
                            logger.debug(
                                f'Task {task_id} skipped due to workflow stop, skipping finalization'
                            )
                            return Ok(None)
                        case _:
                            pass
                    should_retry = await self._should_retry_task(task_id, task_error, s)
                    if should_retry:
                        retry_ok = await self._schedule_retry(task_id, s)
                        if not retry_ok:
                            logger.warning(
                                f'Task {task_id} retry aborted during finalize: '
                                f'task no longer RUNNING (reaper reclaimed).'
                            )
                        await s.commit()
                        return Ok(None)

                    # Mark as failed if no retry
                    fail_res = await s.execute(
                        MARK_TASK_FAILED_SQL,
                        {'now': now, 'result_json': result_json_str, 'id': task_id},
                    )
                    if fail_res.fetchone() is None:
                        logger.warning(
                            f'Task {task_id} finalize-fail aborted: status is no longer RUNNING '
                            f'(reaper likely reclaimed). Skipping to prevent double-execution.'
                        )
                        return Ok(None)
                else:
                    comp_res = await s.execute(
                        MARK_TASK_COMPLETED_SQL,
                        {'now': now, 'result_json': result_json_str, 'id': task_id},
                    )
                    if comp_res.fetchone() is None:
                        logger.warning(
                            f'Task {task_id} finalize-complete aborted: status is no longer RUNNING '
                            f'(reaper likely reclaimed). Skipping to prevent double-execution.'
                        )
                        return Ok(None)

                await s.commit()
                return Ok(tr)
        except Exception as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE1,
                    message='Failed to persist terminal task state',
                    retryable=is_retryable_connection_error(exc),
                    data={
                        'exception_type': type(exc).__name__,
                        'exception': str(exc)[:500],
                        'outcome': {
                            'ok': ok,
                            'result_json_str': result_json_str,
                            'failed_reason': failed_reason,
                        },
                    },
                )
            )

    async def _finalize_workflow_phase(
        self,
        task_id: str,
        tr: 'TaskResult[Any, TaskError]',
    ) -> Result[None, _FinalizeError]:
        """Phase 2 of finalization: workflow advancement and worker wake notifications."""
        try:
            async with self.sf() as s:
                # Handle workflow task completion (if this task is part of a workflow)
                await self._handle_workflow_task_if_needed(s, task_id, tr)

                # Proactively wake workers to re-check capacity/backlog.
                try:
                    # Notify workers globally and on the specific queue to wake claims
                    # Fetch queue name for this task
                    resq = await s.execute(
                        GET_TASK_QUEUE_NAME_SQL,
                        {'id': task_id},
                    )
                    rowq = resq.fetchone()
                    qname = str(rowq[0]) if rowq and rowq[0] else 'default'
                    payload = f'capacity:{task_id}'
                    await s.execute(NOTIFY_TASK_NEW_SQL, {'c1': 'task_new', 'p': payload})
                    await s.execute(
                        NOTIFY_TASK_QUEUE_SQL,
                        {'c2': f'task_queue_{qname}', 'p': payload},
                    )
                except Exception:
                    # Non-fatal if NOTIFY fails; workflow state is already persisted.
                    pass

                # Trigger automatically sends NOTIFY on UPDATE; commit to flush NOTIFYs
                await s.commit()

            return Ok(None)
        except Exception as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message='Workflow finalize phase failed after task terminal state persisted',
                    retryable=is_retryable_connection_error(exc),
                    data={
                        'task_id': task_id,
                        'phase': 'finalize_phase_2',
                        'exception_type': type(exc).__name__,
                        'exception': str(exc)[:500],
                    },
                )
            )

    async def _load_persisted_task_result(
        self, task_id: str
    ) -> Result[TaskResult[Any, TaskError], _FinalizeError]:
        """Load a terminal task's persisted TaskResult for phase-2 replay retries."""
        try:
            async with self.sf() as s:
                res = await s.execute(GET_TASK_STATUS_RESULT_SQL, {'id': task_id})
                row = res.fetchone()
                if row is None:
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE2,
                            message='Cannot replay finalize phase-2: task row not found',
                            retryable=False,
                        )
                    )
                status = str(row[0]) if row[0] is not None else ''
                raw_result = row[1]
                if status not in ('COMPLETED', 'FAILED') or raw_result is None:
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE2,
                            message='Cannot replay finalize phase-2: terminal task result unavailable',
                            retryable=False,
                            data={'status': status},
                        )
                    )
                loads_r = loads_json(raw_result)
                if is_err(loads_r):
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE2,
                            message=f'Cannot replay finalize phase-2: stored result JSON corrupt: {loads_r.err_value}',
                            retryable=False,
                        )
                    )
                tr_r = task_result_from_json(loads_r.ok_value)
                if is_err(tr_r):
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE2,
                            message=f'Cannot replay finalize phase-2: stored result deserialize failed: {tr_r.err_value}',
                            retryable=False,
                        )
                    )
                return Ok(tr_r.ok_value)
        except Exception as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message='Cannot replay finalize phase-2: loading persisted task result failed',
                    retryable=is_retryable_connection_error(exc),
                    data={
                        'exception_type': type(exc).__name__,
                        'exception': str(exc)[:500],
                    },
                )
            )

    async def _handle_finalize_error(self, err: Any) -> None:
        """Handle finalize Result errors with bounded retries for phase1/phase2."""
        if not isinstance(err, _FinalizeError):
            logger.error(f'Unexpected finalize error payload type: {type(err).__name__}')
            return

        stage = err.stage
        task_id = err.task_id
        if stage == _FINALIZE_STAGE_PHASE1:
            max_attempts = _FINALIZE_PHASE1_MAX_RETRIES
        elif stage == _FINALIZE_STAGE_PHASE2:
            max_attempts = _FINALIZE_PHASE2_MAX_RETRIES
        else:
            logger.error(
                f'Finalize error ({task_id}) stage={stage}: {err.message}; data={err.data}'
            )
            return

        key = (task_id, stage)
        attempts = self._finalize_retry_attempts.get(key, 0)

        if not err.retryable:
            logger.error(
                f'Finalize error non-retryable ({task_id}) stage={stage}: {err.message}; data={err.data}'
            )
            self._clear_finalize_retry_attempts(task_id, stage)
            return

        if attempts >= max_attempts:
            logger.critical(
                f'Finalize retries exhausted for task {task_id} stage={stage} after {attempts} attempts: '
                f'{err.message}; data={err.data}'
            )
            self._clear_finalize_retry_attempts(task_id, stage)
            return

        attempt_no = attempts + 1
        self._finalize_retry_attempts[key] = attempt_no
        delay = min(
            _FINALIZE_RETRY_MAX_DELAY_S,
            _FINALIZE_RETRY_BASE_DELAY_S * (2 ** (attempt_no - 1)),
        )
        logger.warning(
            f'Finalize retry scheduled for task {task_id} stage={stage} '
            f'({attempt_no}/{max_attempts}) in {delay:.1f}s: {err.message}'
        )

        if stage == _FINALIZE_STAGE_PHASE1:
            self._spawn_background(
                self._retry_finalize_phase1(err, delay),
                name=f'finalize-retry-phase1-{task_id}',
            )
        else:
            self._spawn_background(
                self._retry_finalize_phase2(err, delay),
                name=f'finalize-retry-phase2-{task_id}',
            )

    async def _retry_finalize_phase1(self, err: _FinalizeError, delay_s: float) -> None:
        """Retry phase-1 terminal persistence from captured child outcome payload."""
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return

        outcome = (err.data or {}).get('outcome')
        if not isinstance(outcome, dict):
            logger.error(
                f'Finalize phase-1 retry missing outcome payload for task {err.task_id}'
            )
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
            return

        ok = bool(outcome.get('ok', False))
        result_json_str = outcome.get('result_json_str')
        if not isinstance(result_json_str, str):
            logger.error(
                f'Finalize phase-1 retry missing result_json_str for task {err.task_id}'
            )
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
            return
        failed_reason_raw = outcome.get('failed_reason')
        failed_reason = (
            str(failed_reason_raw) if failed_reason_raw is not None else None
        )

        phase1_r = await self._persist_task_terminal_state(
            task_id=err.task_id,
            now=datetime.now(timezone.utc),
            ok=ok,
            result_json_str=result_json_str,
            failed_reason=failed_reason,
        )
        if is_err(phase1_r):
            await self._handle_finalize_error(phase1_r.err_value)
            return

        tr = phase1_r.ok_value
        if tr is None:
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
            return

        phase2_r = await self._finalize_workflow_phase(err.task_id, tr)
        if is_err(phase2_r):
            await self._handle_finalize_error(phase2_r.err_value)
            return

        self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
        self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE2)

    async def _retry_finalize_phase2(self, err: _FinalizeError, delay_s: float) -> None:
        """Retry phase-2 workflow advancement from persisted terminal task result."""
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return

        load_r = await self._load_persisted_task_result(err.task_id)
        if is_err(load_r):
            await self._handle_finalize_error(load_r.err_value)
            return

        phase2_r = await self._finalize_workflow_phase(err.task_id, load_r.ok_value)
        if is_err(phase2_r):
            await self._handle_finalize_error(phase2_r.err_value)
            return

        self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE2)

    async def _handle_workflow_task_if_needed(
        self,
        session: 'AsyncSession',
        task_id: str,
        result: 'TaskResult[Any, TaskError]',
    ) -> None:
        """
        Check if task is part of a workflow and handle accordingly.

        This method is called after a task completes (success or failure).
        It updates the workflow_task record and triggers dependency resolution.
        """
        from horsies.core.workflows.engine import on_workflow_task_complete

        # Quick check: is this task linked to a workflow?
        check = await session.execute(
            CHECK_WORKFLOW_TASK_EXISTS_SQL,
            {'tid': task_id},
        )

        if check.fetchone() is None:
            return  # Not a workflow task

        broker = self._app.get_broker() if self._app is not None else None
        # Handle workflow task completion
        await on_workflow_task_complete(session, task_id, result, broker)

    async def _should_retry_task(
        self, task_id: str, error: TaskError, session: AsyncSession,
    ) -> bool:
        """Check if a task should be retried based on its configuration and current retry count."""
        result = await session.execute(
            GET_TASK_RETRY_INFO_SQL,
            {'id': task_id},
        )
        row = result.fetchone()

        if not row:
            return False

        retry_count = row.retry_count or 0
        max_retries = row.max_retries or 0

        if retry_count >= max_retries or max_retries == 0:
            return False

        # Parse task options to check auto_retry_for (nested in retry_policy)
        try:
            if not row.task_options:
                return False
            opts_r = loads_json(row.task_options)
            if is_err(opts_r):
                return False
            task_options_data = opts_r.ok_value
            if not isinstance(task_options_data, dict):
                return False
            retry_policy_raw = task_options_data.get('retry_policy', {})
            if not isinstance(retry_policy_raw, dict):
                return False
            auto_retry_for = retry_policy_raw.get('auto_retry_for')
        except Exception:
            return False

        if not isinstance(auto_retry_for, list) or not auto_retry_for:
            return False

        # Match error code against auto_retry_for (enum value or string)
        code = (
            error.error_code.value
            if isinstance(error.error_code, LibraryErrorCode)
            else error.error_code
        )
        if code and code in auto_retry_for:
            return True

        return False

    async def _schedule_retry(self, task_id: str, session: AsyncSession) -> bool:
        """Schedule a task for retry by updating its status and next retry time.

        Returns True if the retry was scheduled, False if the task was already
        reclaimed by the reaper (status no longer RUNNING).
        """
        # Get current retry configuration
        result = await session.execute(
            GET_TASK_RETRY_CONFIG_SQL,
            {'id': task_id},
        )
        row = result.fetchone()

        if not row:
            return False

        retry_count = (row.retry_count or 0) + 1

        # Parse retry policy from task options
        try:
            if not row.task_options:
                retry_policy_data = {}
            else:
                opts_r = loads_json(row.task_options)
                task_options_data = opts_r.ok_value if not is_err(opts_r) else {}
                if not isinstance(task_options_data, dict):
                    retry_policy_data = {}
                else:
                    retry_policy_data = task_options_data.get('retry_policy', {})
                    if not isinstance(retry_policy_data, dict):
                        retry_policy_data = {}
        except Exception:
            retry_policy_data = {}

        # Calculate retry delay
        delay_seconds = self._calculate_retry_delay(retry_count, retry_policy_data)
        next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        # Update task for retry — guarded by AND status = 'RUNNING'
        res = await session.execute(
            SCHEDULE_TASK_RETRY_SQL,
            {'id': task_id, 'retry_count': retry_count, 'next_retry_at': next_retry_at},
        )
        if res.fetchone() is None:
            logger.warning(
                f'Task {task_id} retry aborted: status is no longer RUNNING '
                f'(reaper likely reclaimed). Skipping to prevent double-execution.'
            )
            return False

        # Schedule a delayed notification using asyncio for the task's actual queue
        queue_name = await self._get_task_queue_name(task_id)
        self._spawn_background(
            self._schedule_delayed_notification(
                delay_seconds, f'task_queue_{queue_name}', f'retry:{task_id}',
            ),
            name=f'delayed-notify-{task_id}',
        )

        logger.info(
            f'Scheduled task {task_id} for retry #{retry_count} at {next_retry_at}'
        )
        return True

    def _calculate_retry_delay(
        self, retry_attempt: int, retry_policy_data: dict[str, Any]
    ) -> float:
        """Calculate the delay in seconds for a retry attempt."""
        # Default retry policy values (these match RetryPolicy defaults)
        intervals = retry_policy_data.get(
            'intervals', [60, 300, 900]
        )  # 1min, 5min, 15min
        backoff_strategy = retry_policy_data.get('backoff_strategy', 'fixed')
        jitter = retry_policy_data.get('jitter', True)

        # Calculate base delay based on strategy
        if backoff_strategy == 'fixed':
            # Fixed strategy: use intervals directly, clamped to last interval
            # if retry_attempt exceeds length (possible after DB deserialization, old data)
            clamped_index = min(retry_attempt - 1, len(intervals) - 1)
            base_delay = intervals[clamped_index]

        elif backoff_strategy == 'exponential':
            # Exponential strategy: use intervals[0] as base and apply exponential multiplier
            base_interval = intervals[0] if intervals else 60
            base_delay = base_interval * (2 ** (retry_attempt - 1))

        else:
            # Fallback (shouldn't happen due to Literal type validation)
            base_delay = intervals[0] if intervals else 60

        # Apply jitter (±25% randomization)
        if jitter:
            jitter_range = base_delay * 0.25
            base_delay += random.uniform(-jitter_range, jitter_range)

        return float(max(1.0, base_delay))

    async def _schedule_delayed_notification(
        self, delay_seconds: float, channel: str, payload: str
    ) -> None:
        """Schedule a delayed notification to wake up the worker for retry."""
        try:
            await asyncio.sleep(delay_seconds)

            # Send notification to trigger retry processing
            async with self.sf() as session:
                await session.execute(
                    NOTIFY_DELAYED_SQL,
                    {'channel': channel, 'payload': payload},
                )
                await session.commit()

            logger.debug(f'Sent delayed notification for retry: {payload}')
        except asyncio.CancelledError:
            logger.debug(f'Delayed notification cancelled for: {payload}')
        except Exception as e:
            logger.error(f'Error sending delayed notification for {payload}: {e}')

    async def _get_task_queue_name(self, task_id: str) -> str:
        """Fetch the queue_name for a given task id."""
        async with self.sf() as session:
            res = await session.execute(
                GET_TASK_QUEUE_NAME_SQL,
                {'id': task_id},
            )
            row = res.fetchone()
            return str(row[0]) if row and row[0] else 'default'

    async def _claimer_heartbeat_loop(self) -> None:
        """Emit claimer heartbeats for tasks we've claimed but not yet started."""
        # Get heartbeat interval from recovery config (milliseconds)
        claimer_heartbeat_interval_ms = 30_000  # default: 30 seconds
        if self.cfg.recovery_config:
            claimer_heartbeat_interval_ms = (
                self.cfg.recovery_config.claimer_heartbeat_interval_ms
            )

        try:
            while not self._stop.is_set():
                try:
                    async with self.sf() as s:
                        await s.execute(
                            INSERT_CLAIMER_HEARTBEAT_SQL,
                            {
                                'wid': self.worker_instance_id,
                                'host': socket.gethostname(),
                                'pid': os.getpid(),
                            },
                        )
                        # Extend claim lease for CLAIMED tasks owned by this worker,
                        # but only if claimed_at is recent enough. Tasks claimed longer
                        # ago than max_claim_renew_age_ms are left to expire, preventing
                        # indefinite renewal of orphaned claims.
                        await s.execute(
                            RENEW_CLAIM_LEASE_SQL,
                            {
                                'wid': self.worker_instance_id,
                                'new_expires_at': self._compute_claim_expires_at(),
                                'max_claim_age_ms': self.cfg.max_claim_renew_age_ms,
                            },
                        )
                        await s.commit()
                except Exception as e:
                    logger.error(f'Claimer heartbeat error: {e}')
                # Convert to seconds only for asyncio.sleep
                await asyncio.sleep(claimer_heartbeat_interval_ms / 1000.0)
        except asyncio.CancelledError:
            return

    async def _update_worker_state(self) -> None:
        """Update worker state snapshot in database for monitoring."""
        try:
            rss_mb, mem_pct, cpu_pct = await asyncio.to_thread(
                _collect_psutil_metrics,
            )

            # Get current task counts
            running = await self._count_only_running_for_worker()
            claimed = await self._count_claimed_for_worker()

            # Serialize recovery config
            recovery_dict = None
            if self.cfg.recovery_config:
                recovery_dict = {
                    'auto_requeue_stale_claimed': self.cfg.recovery_config.auto_requeue_stale_claimed,
                    'claimed_stale_threshold_ms': self.cfg.recovery_config.claimed_stale_threshold_ms,
                    'auto_fail_stale_running': self.cfg.recovery_config.auto_fail_stale_running,
                    'running_stale_threshold_ms': self.cfg.recovery_config.running_stale_threshold_ms,
                    'check_interval_ms': self.cfg.recovery_config.check_interval_ms,
                    'runner_heartbeat_interval_ms': self.cfg.recovery_config.runner_heartbeat_interval_ms,
                    'claimer_heartbeat_interval_ms': self.cfg.recovery_config.claimer_heartbeat_interval_ms,
                    'heartbeat_retention_hours': self.cfg.recovery_config.heartbeat_retention_hours,
                    'worker_state_retention_hours': self.cfg.recovery_config.worker_state_retention_hours,
                    'terminal_record_retention_hours': self.cfg.recovery_config.terminal_record_retention_hours,
                }

            async with self.sf() as s:
                await s.execute(
                    INSERT_WORKER_STATE_SQL,
                    {
                        'wid': self.worker_instance_id,
                        'host': socket.gethostname(),
                        'pid': os.getpid(),
                        'procs': self.cfg.processes,
                        'mcb': self.cfg.max_claim_batch,
                        'mcpw': self.cfg.max_claim_per_worker
                        if self.cfg.max_claim_per_worker > 0
                        else self.cfg.processes,
                        'cwc': self.cfg.cluster_wide_cap,
                        'queues': self.cfg.queues,
                        'qp': Jsonb(self.cfg.queue_priorities)
                        if self.cfg.queue_priorities
                        else None,
                        'qmc': Jsonb(self.cfg.queue_max_concurrency)
                        if self.cfg.queue_max_concurrency
                        else None,
                        'recovery': Jsonb(recovery_dict) if recovery_dict else None,
                        'running': running,
                        'claimed': claimed,
                        'mem_mb': rss_mb,
                        'mem_pct': mem_pct,
                        'cpu_pct': cpu_pct,
                        'started': self._started_at,
                    },
                )
                await s.commit()
        except Exception as e:
            logger.error(f'Failed to update worker state: {e}')

    async def _worker_state_heartbeat_loop(self) -> None:
        """Periodically update worker state for monitoring (every 5 seconds)."""
        worker_state_interval_ms = 5_000  # 5 seconds

        try:
            while not self._stop.is_set():
                try:
                    await self._update_worker_state()
                except Exception as e:
                    logger.error(f'Worker state heartbeat error: {e}')

                # Wait for interval or stop signal
                await asyncio.sleep(worker_state_interval_ms / 1000.0)
        except asyncio.CancelledError:
            return

    async def _reaper_loop(self) -> None:
        """Automatic stale task handling loop.

        Periodically checks for and recovers stale tasks based on RecoveryConfig:
        - Requeues tasks stuck in CLAIMED (safe - user code never ran)
        - Marks stale RUNNING tasks as FAILED (not safe to requeue)
        - Prunes old telemetry and terminal records based on retention settings

        Non-retryable errors (schema drift, code bugs) are tracked per-operation.
        After _REAPER_MAX_PERMANENT_FAILURES consecutive permanent failures, the
        failing operation is disabled for the process lifetime to avoid spamming
        logs every check_interval_ms forever.
        """
        if not self.cfg.recovery_config:
            return

        recovery_cfg = self.cfg.recovery_config
        check_interval_ms = recovery_cfg.check_interval_ms
        temp_broker = None
        owns_temp_broker = False
        next_retention_cleanup_at = time.monotonic()

        # Per-operation consecutive permanent failure counters.
        # Transient failures and successes reset the counter.
        _REAPER_MAX_PERMANENT_FAILURES = 3
        requeue_permanent_failures = 0
        requeue_disabled = False
        mark_failed_permanent_failures = 0
        mark_failed_disabled = False

        logger.info(
            f'Reaper configuration: auto_requeue_claimed={recovery_cfg.auto_requeue_stale_claimed}, '
            f'auto_fail_running={recovery_cfg.auto_fail_stale_running}, '
            f'check_interval={check_interval_ms}ms ({check_interval_ms/1000:.1f}s)',
        )

        try:
            # Prefer the app-owned broker to avoid creating redundant engines/listeners.
            if self._app is not None:
                try:
                    temp_broker = self._app.get_broker()
                except Exception as app_broker_err:
                    logger.warning(
                        'Reaper could not reuse app broker; falling back to temporary broker: %s',
                        app_broker_err,
                    )
                    temp_broker = None

            if temp_broker is None:
                from horsies.core.brokers.postgres import PostgresBroker
                from horsies.core.models.broker import PostgresConfig

                temp_broker_config = PostgresConfig(database_url=self.cfg.dsn)
                temp_broker = PostgresBroker(temp_broker_config)
                owns_temp_broker = True
                if self._app is not None:
                    temp_broker.app = self._app

            while not self._stop.is_set():
                try:
                    # Auto-requeue stale CLAIMED tasks
                    if recovery_cfg.auto_requeue_stale_claimed and not requeue_disabled:
                        match await temp_broker.requeue_stale_claimed(
                            stale_threshold_ms=recovery_cfg.claimed_stale_threshold_ms,
                        ):
                            case Ok(requeued):
                                requeue_permanent_failures = 0
                                if requeued > 0:
                                    logger.info(
                                        f'Reaper requeued {requeued} stale CLAIMED task(s)',
                                    )
                            case Err(err) if err.retryable:
                                requeue_permanent_failures = 0
                                logger.warning(
                                    f'Reaper requeue_stale_claimed transient failure '
                                    f'(will retry next cycle): {err.message}',
                                )
                            case Err(err):
                                requeue_permanent_failures += 1
                                if requeue_permanent_failures >= _REAPER_MAX_PERMANENT_FAILURES:
                                    requeue_disabled = True
                                    logger.critical(
                                        f'Reaper requeue_stale_claimed disabled after '
                                        f'{requeue_permanent_failures} consecutive permanent '
                                        f'failures. Last error: {err.message}. '
                                        f'Requires deploy or manual intervention.',
                                    )
                                else:
                                    logger.error(
                                        f'Reaper requeue_stale_claimed permanent failure '
                                        f'({requeue_permanent_failures}/{_REAPER_MAX_PERMANENT_FAILURES} '
                                        f'before disable): {err.message}',
                                    )

                    # Auto-fail stale RUNNING tasks
                    if recovery_cfg.auto_fail_stale_running and not mark_failed_disabled:
                        match await temp_broker.mark_stale_tasks_as_failed(
                            stale_threshold_ms=recovery_cfg.running_stale_threshold_ms,
                        ):
                            case Ok(failed):
                                mark_failed_permanent_failures = 0
                                if failed > 0:
                                    logger.warning(
                                        f'Reaper marked {failed} stale RUNNING task(s) as FAILED',
                                    )
                            case Err(err) if err.retryable:
                                mark_failed_permanent_failures = 0
                                logger.warning(
                                    f'Reaper mark_stale_tasks_as_failed transient failure '
                                    f'(will retry next cycle): {err.message}',
                                )
                            case Err(err):
                                mark_failed_permanent_failures += 1
                                if mark_failed_permanent_failures >= _REAPER_MAX_PERMANENT_FAILURES:
                                    mark_failed_disabled = True
                                    logger.critical(
                                        f'Reaper mark_stale_tasks_as_failed disabled after '
                                        f'{mark_failed_permanent_failures} consecutive permanent '
                                        f'failures. Last error: {err.message}. '
                                        f'Requires deploy or manual intervention.',
                                    )
                                else:
                                    logger.error(
                                        f'Reaper mark_stale_tasks_as_failed permanent failure '
                                        f'({mark_failed_permanent_failures}/{_REAPER_MAX_PERMANENT_FAILURES} '
                                        f'before disable): {err.message}',
                                    )

                    # Recover stuck workflows
                    try:
                        from horsies.core.workflows.recovery import (
                            recover_stuck_workflows,
                        )

                        async with temp_broker.session_factory() as s:
                            recovered = await recover_stuck_workflows(s, temp_broker)
                            if recovered > 0:
                                logger.info(
                                    f'Reaper recovered {recovered} stuck workflow task(s)'
                                )
                            await s.commit()
                    except Exception as wf_err:
                        logger.error(f'Workflow recovery error: {wf_err}')

                    now_monotonic = time.monotonic()
                    if now_monotonic >= next_retention_cleanup_at:
                        try:
                            deleted_heartbeats = 0
                            deleted_worker_states = 0
                            deleted_workflow_tasks = 0
                            deleted_workflows = 0
                            deleted_tasks = 0

                            async with temp_broker.session_factory() as s:
                                if recovery_cfg.heartbeat_retention_hours is not None:
                                    hb_result = await s.execute(
                                        DELETE_EXPIRED_HEARTBEATS_SQL,
                                        {
                                            'retention_hours': recovery_cfg.heartbeat_retention_hours,
                                        },
                                    )
                                    deleted_heartbeats = int(hb_result.rowcount or 0)

                                if recovery_cfg.worker_state_retention_hours is not None:
                                    ws_result = await s.execute(
                                        DELETE_EXPIRED_WORKER_STATES_SQL,
                                        {
                                            'retention_hours': recovery_cfg.worker_state_retention_hours,
                                        },
                                    )
                                    deleted_worker_states = int(ws_result.rowcount or 0)

                                if recovery_cfg.terminal_record_retention_hours is not None:
                                    params = {
                                        'retention_hours': recovery_cfg.terminal_record_retention_hours
                                    }
                                    wt_result = await s.execute(
                                        DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
                                        params,
                                    )
                                    wf_result = await s.execute(
                                        DELETE_EXPIRED_WORKFLOWS_SQL,
                                        params,
                                    )
                                    task_result = await s.execute(
                                        DELETE_EXPIRED_TASKS_SQL,
                                        params,
                                    )
                                    deleted_workflow_tasks = int(wt_result.rowcount or 0)
                                    deleted_workflows = int(wf_result.rowcount or 0)
                                    deleted_tasks = int(task_result.rowcount or 0)

                                await s.commit()

                            if (
                                deleted_heartbeats > 0
                                or deleted_worker_states > 0
                                or deleted_workflow_tasks > 0
                                or deleted_workflows > 0
                                or deleted_tasks > 0
                            ):
                                logger.info(
                                    'Reaper retention cleanup: heartbeats=%s worker_states=%s workflow_tasks=%s workflows=%s tasks=%s',
                                    deleted_heartbeats,
                                    deleted_worker_states,
                                    deleted_workflow_tasks,
                                    deleted_workflows,
                                    deleted_tasks,
                                )
                        except Exception as cleanup_err:
                            logger.error(
                                f'Retention cleanup error: {cleanup_err}'
                            )
                        finally:
                            next_retention_cleanup_at = (
                                now_monotonic + _RETENTION_CLEANUP_INTERVAL_S
                            )

                except Exception as e:
                    logger.error(f'Reaper loop error: {e}')

                # Wait for check interval or stop signal (convert ms to seconds for asyncio)
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=check_interval_ms / 1000.0
                    )
                    break  # Stop signal received
                except asyncio.TimeoutError:
                    continue  # Continue to next iteration

        except asyncio.CancelledError:
            logger.info('Reaper loop cancelled')
            return
        finally:
            if owns_temp_broker and temp_broker is not None:
                close_result = await temp_broker.close_async()
                if is_err(close_result):
                    logger.error(
                        f'Error closing reaper broker: {close_result.err_value.message}'
                    )


"""
horsies examples/instance.py worker --loglevel=info --processes=8 
"""
