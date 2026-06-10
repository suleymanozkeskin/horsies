# app/core/worker.py
# pyright: reportPrivateUsage=false
from __future__ import annotations
import asyncio
import contextlib
import uuid
import os
import signal
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from enum import Enum
from datetime import datetime, timezone, timedelta
from importlib import import_module
from collections.abc import Coroutine
from typing import Any, Optional, TYPE_CHECKING, cast
import hashlib
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.app import Horsies
from horsies.core.brokers.listener import PostgresListener
from horsies.core.codec.json_io import (
    loads_json,
    SerializationError,
)
from horsies.core.models.health import WORKER_PING_CHANNEL
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    validate_task_result_envelope,
)
from pydantic import ValidationError
from horsies.core.models.tasks import (
    TaskResult,
    TaskError,
    OperationalErrorCode,
    OutcomeCode,
    BuiltInTaskCode,
)
from horsies.core.logging import get_logger
from horsies.core.worker.current import set_current_app
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.types.result import Ok, Err, Result, is_err
from horsies.core.defaults import DEFAULT_CLAIM_LEASE_MS
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.url import to_psycopg_url

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker

# --- Imports from sibling modules (extracted for maintainability) ---
from horsies.core.worker.config import WorkerConfig as WorkerConfig  # noqa: F401
from horsies.core.worker.child_pool import _initialize_worker_pool as _initialize_worker_pool  # noqa: F401
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.worker.child_runner import (  # noqa: F401
    CHILD_HOOK_FAILURE_EXIT_CODE as CHILD_HOOK_FAILURE_EXIT_CODE,
    _locate_app as _locate_app,
    _child_initializer as _child_initializer,
    _run_task_entry as _run_task_entry,
    import_by_path as import_by_path,
    _dedupe_paths as _dedupe_paths,
    _build_sys_path_roots as _build_sys_path_roots,
    _derive_sys_path_roots_from_file as _derive_sys_path_roots_from_file,
    _debug_imports_enabled as _debug_imports_enabled,
    _debug_imports_log as _debug_imports_log,
    _is_retryable_db_error as _is_retryable_db_error,
    _heartbeat_worker as _heartbeat_worker,
)
from horsies.core.worker.sql import (  # noqa: F401
    CLAIM_SQL as CLAIM_SQL,
    CLAIM_ADVISORY_LOCK_SQL as CLAIM_ADVISORY_LOCK_SQL,
    REAPER_GATE_TRY_LOCK_SQL as REAPER_GATE_TRY_LOCK_SQL,
    COUNT_GLOBAL_IN_FLIGHT_SQL as COUNT_GLOBAL_IN_FLIGHT_SQL,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL as COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
    COUNT_QUEUE_IN_FLIGHT_SOFT_SQL as COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL as COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_RUNNING_FOR_WORKER_SQL as COUNT_RUNNING_FOR_WORKER_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL as COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_RUNNING_IN_QUEUE_SQL as COUNT_RUNNING_IN_QUEUE_SQL,
    GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL as GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
    UNCLAIM_PAUSED_TASKS_SQL as UNCLAIM_PAUSED_TASKS_SQL,
    UNCLAIM_CLAIMED_TASK_SQL as UNCLAIM_CLAIMED_TASK_SQL,
    RESET_PAUSED_WORKFLOW_TASKS_SQL as RESET_PAUSED_WORKFLOW_TASKS_SQL,
    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL as CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
    SKIP_CANCELLED_WORKFLOW_TASKS_SQL as SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
    MARK_TASK_FAILED_WORKER_SQL as MARK_TASK_FAILED_WORKER_SQL,
    MARK_TASK_FAILED_SQL as MARK_TASK_FAILED_SQL,
    MARK_TASK_COMPLETED_SQL as MARK_TASK_COMPLETED_SQL,
    SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL as SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
    UPSERT_TASK_ATTEMPT_SQL as UPSERT_TASK_ATTEMPT_SQL,
    NOTIFY_TASK_QUEUE_SQL as NOTIFY_TASK_QUEUE_SQL,
    CHECK_WORKFLOW_TASK_EXISTS_SQL as CHECK_WORKFLOW_TASK_EXISTS_SQL,
    GET_TASK_RETRY_INFO_SQL as GET_TASK_RETRY_INFO_SQL,
    GET_TASK_RETRY_CONFIG_SQL as GET_TASK_RETRY_CONFIG_SQL,
    GET_TASK_RETRY_POSTCHECK_SQL as GET_TASK_RETRY_POSTCHECK_SQL,
    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL as SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
    SCHEDULE_TASK_RETRY_SQL as SCHEDULE_TASK_RETRY_SQL,
    NOTIFY_DELAYED_SQL as NOTIFY_DELAYED_SQL,
    INSERT_CLAIMER_HEARTBEAT_SQL as INSERT_CLAIMER_HEARTBEAT_SQL,
    RENEW_CLAIM_LEASE_SQL as RENEW_CLAIM_LEASE_SQL,
    INSERT_WORKER_STATE_SQL as INSERT_WORKER_STATE_SQL,
    DELETE_EXPIRED_HEARTBEATS_SQL as DELETE_EXPIRED_HEARTBEATS_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL as DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOW_TASKS_SQL as DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL as DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_EXPIRED_TASKS_SQL as DELETE_EXPIRED_TASKS_SQL,
    WORKFLOW_TERMINAL_VALUES as WORKFLOW_TERMINAL_VALUES,
    TASK_TERMINAL_VALUES as TASK_TERMINAL_VALUES,
    _RETENTION_CLEANUP_INTERVAL_S as _RETENTION_CLEANUP_INTERVAL_S,
    _FINALIZER_DRAIN_TIMEOUT_S as _FINALIZER_DRAIN_TIMEOUT_S,
)

logger = get_logger('worker')

# Shared runtime types/helpers (extracted; re-exported for compatibility).
from horsies.core.worker.health import HealthMixin as HealthMixin  # noqa: E402
from horsies.core.worker.reaper import ReaperMixin as ReaperMixin  # noqa: E402
from horsies.core.worker.retrying import RetryMixin as RetryMixin  # noqa: E402
from horsies.core.worker.runtime import (  # noqa: F401,E402
    _FINALIZE_STAGE_PHASE1 as _FINALIZE_STAGE_PHASE1,
    _FINALIZE_STAGE_PHASE2 as _FINALIZE_STAGE_PHASE2,
    _FINALIZE_STAGE_FUTURE as _FINALIZE_STAGE_FUTURE,
    _FINALIZE_FUTURE_MAX_RETRIES as _FINALIZE_FUTURE_MAX_RETRIES,
    _FINALIZE_PHASE1_MAX_RETRIES as _FINALIZE_PHASE1_MAX_RETRIES,
    _FINALIZE_PHASE2_MAX_RETRIES as _FINALIZE_PHASE2_MAX_RETRIES,
    _FINALIZE_RETRY_BASE_DELAY_S as _FINALIZE_RETRY_BASE_DELAY_S,
    _FINALIZE_RETRY_MAX_DELAY_S as _FINALIZE_RETRY_MAX_DELAY_S,
    _REAPER_MAX_PERMANENT_FAILURES as _REAPER_MAX_PERMANENT_FAILURES,
    ChildHookFailedError as ChildHookFailedError,
    _RetryBackoff as _RetryBackoff,
    _FinalizeError as _FinalizeError,
    _RequeueOutcome as _RequeueOutcome,
    _ReaperPassState as _ReaperPassState,
    _collect_psutil_metrics as _collect_psutil_metrics,
    _parse_timeout_ms as _parse_timeout_ms,
    _warm_child_process as _warm_child_process,
)

GET_TASK_STATUS_RESULT_SQL = text("""
    SELECT status, task_name, result
    FROM horsies_tasks
    WHERE id = :id
""")




class Worker(HealthMixin, ReaperMixin, RetryMixin):
    """
    Async master that:
      - Subscribes to queue channels
      - Claims tasks (priority + enqueued_at) with SKIP LOCKED
      - Executes in a process pool
      - On completion, writes result/failed, COMMITs, and NOTIFY task_done
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        listener: PostgresListener,
        cfg: WorkerConfig,
        broker: PostgresBroker | None = None,
    ):
        self.sf = session_factory
        self.listener = listener
        self.cfg = cfg
        self.broker = broker
        self.worker_instance_id = str(uuid.uuid4())
        self._started_at = datetime.now(timezone.utc)
        self._app: Horsies | None = None
        self._resilience = self.cfg.resilience_config or WorkerResilienceConfig()
        # Delay creation of the process pool until after preloading modules so that
        # any import/validation errors surface in the main process at startup.
        self._executor: Optional[ProcessPoolExecutor] = None
        self._executor_restart_lock = asyncio.Lock()
        self._parent_db_sockets_open = False
        self._stop = asyncio.Event()
        self._ping_queue: asyncio.Queue[Any] | None = None
        self._service_tasks: set[asyncio.Task[Any]] = set()
        self._finalizer_tasks: set[asyncio.Task[Any]] = set()
        self._finalize_retry_attempts: dict[tuple[str, str], int] = {}

    def request_stop(self) -> None:
        """Request worker to stop gracefully."""
        self._stop.set()

    def _spawn_background(
        self,
        coro: Coroutine[Any, Any, Any],
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

    def _create_executor(
        self,
        *,
        avoid_parent_fd_inheritance: bool = False,
    ) -> ProcessPoolExecutor:
        child_database_url = to_psycopg_url(self.cfg.dsn)
        kwargs: dict[str, Any] = {}
        if avoid_parent_fd_inheritance:
            kwargs['mp_context'] = multiprocessing.get_context('spawn')
        return ProcessPoolExecutor(
            max_workers=self.cfg.processes,
            initializer=_child_initializer,
            initargs=(
                self.cfg.app_locator,
                self.cfg.imports,
                self.cfg.sys_path_roots,
                self.cfg.loglevel,
                child_database_url,
                self.cfg.pgbouncer_transaction_mode,
                self.cfg.child_pool_min_size,
                self.cfg.child_pool_max_size,
            ),
            **kwargs,
        )

    async def _warm_executor(self) -> None:
        """Start child processes while the parent has not opened listener sockets."""
        if self._executor is None:
            return
        loop = asyncio.get_running_loop()
        pids: set[int] = set()
        for _ in range(3):
            futures = [
                loop.run_in_executor(self._executor, _warm_child_process)
                for _ in range(self.cfg.processes)
            ]
            pids.update(await asyncio.gather(*futures))
            if len(pids) >= self.cfg.processes:
                break
        if len(pids) < self.cfg.processes:
            raise RuntimeError(
                'worker child warmup started '
                f'{len(pids)}/{self.cfg.processes} process(es)'
            )
        logger.info(
            'Worker child processes ready: %s/%s',
            len(pids),
            self.cfg.processes,
        )

    async def _create_warmed_executor(
        self,
        *,
        avoid_parent_fd_inheritance: bool = False,
    ) -> None:
        executor = self._create_executor(
            avoid_parent_fd_inheritance=avoid_parent_fd_inheritance,
        )
        self._executor = executor
        try:
            await self._warm_executor()
        except Exception as warm_exc:
            self._executor = None
            # Snapshot child processes before shutdown clears the mapping;
            # joined Process objects keep their exitcode afterwards.
            from multiprocessing.process import BaseProcess

            child_processes = list(
                cast(
                    'dict[int, BaseProcess]',
                    getattr(executor, '_processes', None) or {},
                ).values()
            )
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(
                    None, lambda: executor.shutdown(wait=True, cancel_futures=True)
                )
            except Exception as shutdown_exc:
                logger.error(
                    f'Error shutting down failed executor warmup: {shutdown_exc}'
                )
            if any(
                p.exitcode == CHILD_HOOK_FAILURE_EXIT_CODE
                for p in child_processes
            ):
                raise ChildHookFailedError(
                    'on_child_process_start hook failed in a worker child '
                    '(see the child log line above for the hook name); '
                    'fix the hook — the worker will not restart-loop on it'
                ) from warm_exc
            raise

    async def _restart_executor(
        self,
        reason: str,
        failed_executor: Optional[ProcessPoolExecutor] = None,
    ) -> None:
        if self._stop.is_set():
            return
        async with self._executor_restart_lock:
            if self._stop.is_set():
                return
            if failed_executor is not None and failed_executor is not self._executor:
                logger.warning(
                    f'Executor restart skipped; executor already replaced: {reason}'
                )
                return
            if self._executor is None:
                try:
                    await self._create_warmed_executor(
                        avoid_parent_fd_inheritance=self._parent_db_sockets_open,
                    )
                except ChildHookFailedError as exc:
                    self._stop_for_child_hook_failure(exc)
                    return
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
            try:
                await self._create_warmed_executor(
                    avoid_parent_fd_inheritance=self._parent_db_sockets_open,
                )
            except ChildHookFailedError as exc:
                self._stop_for_child_hook_failure(exc)

    def _stop_for_child_hook_failure(self, exc: ChildHookFailedError) -> None:
        """Stop the worker on a hook failure instead of restart-looping.

        The hook re-runs in every replacement child, so retrying the
        executor restart would fail the same way forever. run_forever
        observes the stop flag and shuts down gracefully.
        """
        logger.critical('Stopping worker: %s', exc)
        self._stop.set()

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
        # Cancel any background loops spawned before the failure so a retry
        # starts clean instead of leaking orphans and spawning duplicates.
        # _stop is left unset: the resilience loop must be free to retry.
        if self._service_tasks:
            service_tasks = tuple(self._service_tasks)
            for task in service_tasks:
                task.cancel()
            await asyncio.gather(*service_tasks, return_exceptions=True)
            self._service_tasks.clear()

        try:
            await self.listener.close()
            self._parent_db_sockets_open = False
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
            # Fork and initialize children before the parent opens
            # listener/coordinator DB sockets. Psycopg connections are not
            # process-safe and must not be inherited by child processes.
            await self._create_warmed_executor()

            start_r = await self.listener.start()
            if is_err(start_r):
                err = start_r.err_value
                logger.error(
                    'Postgres LISTEN failed. If database_url points to PgBouncer '
                    'transaction pooling, set PostgresConfig.session_database_url '
                    'to a direct/session-capable Postgres URL. Original error: %s',
                    err.message,
                )
                raise err.exception or RuntimeError(err.message)
            self._parent_db_sockets_open = True
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

            # Subscribe to each queue channel in one batch. The global
            # task_new channel is deliberately NOT subscribed: queue channels
            # cover every insert this worker can act on, while task_new would
            # wake every worker for every insert cluster-wide — including
            # queues it cannot claim from (thundering herd).
            all_channels = [f'task_queue_{q}' for q in self.cfg.queues]
            listen_r = await self.listener.listen_many(all_channels)
            if is_err(listen_r):
                err = listen_r.err_value
                logger.error(
                    'Postgres LISTEN failed. If database_url points to PgBouncer '
                    'transaction pooling, set PostgresConfig.session_database_url '
                    'to a direct/session-capable Postgres URL. Original error: %s',
                    err.message,
                )
                raise err.exception or RuntimeError(err.message)
            self._queues = listen_r.ok_value
            logger.info(f'Subscribed to queues: {self.cfg.queues}')

            # Subscribe to the shared ping channel on a dedicated queue (kept
            # separate from task-dispatch queues so pings are never drained by
            # the claim loop). Done BEFORE spawning any background loop so a
            # subscription failure cannot orphan already-running loops.
            ping_listen_r = await self.listener.listen(WORKER_PING_CHANNEL)
            if is_err(ping_listen_r):
                err = ping_listen_r.err_value
                logger.error(
                    'Failed to subscribe to worker ping channel %r: %s',
                    WORKER_PING_CHANNEL,
                    err.message,
                )
                raise err.exception or RuntimeError(err.message)
            self._ping_queue = ping_listen_r.ok_value
            logger.info('Subscribed to worker ping channel')

            # Start claimer heartbeat loop (CLAIMED coverage)
            self._spawn_background(
                self._claimer_heartbeat_loop(),
                name='claimer-heartbeat',
            )
            # Start worker state heartbeat loop for monitoring
            self._spawn_background(
                self._worker_state_heartbeat_loop(),
                name='worker-state-heartbeat',
            )
            logger.info('Worker state heartbeat loop started for monitoring')
            # Serve liveness pings from a background loop.
            self._spawn_background(self._ping_responder_loop(), name='ping-responder')
            logger.info('Ping responder loop started for liveness probes')
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

        if force and self._finalizer_tasks:
            finalizer_tasks = tuple(self._finalizer_tasks)
            for task in finalizer_tasks:
                task.cancel()
            await asyncio.gather(*finalizer_tasks, return_exceptions=True)
            self._finalizer_tasks.clear()

        # Bound for the executor wait below: a task that ignores shutdown
        # this long is hung and gets SIGKILLed (crash recovery + ownership
        # guards classify the rows); without a bound, one hung task made
        # the worker unkillable except by external SIGKILL.
        executor_shutdown_grace_s = 300.0

        # Shutdown the executor BEFORE draining finalizers: finalizers block
        # on child futures, and the executor wait is what bounds child
        # completion. Draining first burned the timeout on still-running
        # children, then the executor wait completed the work anyway and the
        # cancelled finalizers discarded its results (row left RUNNING until
        # a reaper recorded the finished task as WORKER_CRASHED).
        if self._executor:
            # Offload blocking shutdown to a thread to avoid freezing the event loop
            loop = asyncio.get_running_loop()
            executor = self._executor
            self._executor = None
            shutdown_future = loop.run_in_executor(
                None, lambda: executor.shutdown(wait=True, cancel_futures=True)
            )
            try:
                await asyncio.wait_for(
                    asyncio.shield(shutdown_future),
                    timeout=executor_shutdown_grace_s,
                )
            except asyncio.TimeoutError:
                # _processes is a private-but-stable CPython mapping
                # (pid -> Process); there is no public way to enumerate a
                # pool's children.
                from multiprocessing.process import BaseProcess

                processes = cast(
                    'dict[int, BaseProcess]',
                    getattr(executor, '_processes', None) or {},
                )
                pids = [
                    proc.pid
                    for proc in processes.values()
                    if proc.pid is not None
                ]
                logger.error(
                    'Executor shutdown exceeded %.0fs (hung task?); killing '
                    '%d child process(es): %s',
                    executor_shutdown_grace_s,
                    len(pids),
                    pids,
                )
                for pid in pids:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                with contextlib.suppress(Exception):
                    await shutdown_future
            except Exception as e:
                logger.error(f'Error shutting down executor: {e}')
            logger.info('Worker executor shutdown')

        # Finalizers persist task outcomes; with all child futures resolved
        # they only have DB writes left, so the drain timeout bounds DB work
        # rather than task runtime.
        if self._finalizer_tasks:
            finalizer_tasks = tuple(self._finalizer_tasks)
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

        # Close the Postgres listener after finalizers so completion NOTIFYs
        # and result waits drain through normally.
        try:
            await self.listener.close()
            self._parent_db_sockets_open = False
            logger.info('Postgres listener closed')
        except Exception as e:
            logger.error(f'Error closing Postgres listener: {e}')
        # Release per-process registries on shutdown.
        try:
            from horsies.core.workflows.registry import clear_workflow_registry

            clear_workflow_registry()
        except Exception as e:
            logger.error(f'Error clearing workflow registry: {e}')
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
            app.set_role('worker')
            # Optionally set as current for consistency in main process
            set_current_app(app)
            self._app = app

            # Suppress accidental sends while importing modules for discovery
            app.suppress_sends(True)
            try:
                # Import declared modules that contain task definitions
                combined_imports = list(self.cfg.imports)
                combined_imports.extend(app.get_discovered_task_modules())
                combined_imports = _dedupe_paths(combined_imports)
                _debug_imports_log(f'[preload] import_modules={combined_imports}')
                for m in combined_imports:
                    if m.endswith('.py') or os.path.sep in m:
                        import_by_path(os.path.abspath(m))
                    else:
                        import_module(m)
            finally:
                app.suppress_sends(False)
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
            asyncio.create_task(q.get()) for q in self._queues
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
        for q in self._queues:
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

        # Open one transaction; serialize claim passes only when a
        # multi-worker read-then-act invariant exists (cluster/queue caps).
        # Without caps, CLAIM_SQL's FOR UPDATE SKIP LOCKED already makes
        # concurrent claiming safe, and the lock would only cap cluster
        # claim throughput at 1/claim-pass-latency.
        async with self.sf() as s:
            if self._claim_pass_needs_serialization():
                await s.execute(
                    CLAIM_ADVISORY_LOCK_SQL,
                    {'key': self._advisory_key_global()},
                )

            # Compute local budget and optional global remaining
            # Hard cap mode (prefetch_buffer=0): count RUNNING + CLAIMED for strict enforcement
            # Soft cap mode (prefetch_buffer>0): count only RUNNING, allow prefetch with lease
            hard_cap_mode = self.cfg.prefetch_buffer == 0
            claimed_count = await self._count_claimed_for_worker(s)
            if claimed_count >= max_claimed:
                await s.commit()
                return False
            remaining_claim_allowance = max(0, int(max_claimed) - int(claimed_count))

            if hard_cap_mode:
                # Hard cap: count both RUNNING and CLAIMED for this worker
                local_in_flight = await self._count_in_flight_for_worker(s)
                max_local_capacity = self.cfg.processes
            else:
                # Soft cap: queue/global caps count only RUNNING, but local
                # prefetch budget must include already CLAIMED rows so a worker
                # cannot hoard beyond processes + prefetch_buffer.
                local_in_flight = await self._count_only_running_for_worker(s)
                max_local_capacity = self.cfg.processes + self.cfg.prefetch_buffer
            local_available = max(
                0,
                int(max_local_capacity)
                - int(local_in_flight)
                - (0 if hard_cap_mode else int(claimed_count)),
            )
            budget_remaining = local_available

            global_remaining: Optional[int] = None
            if self.cfg.cluster_wide_cap is not None:
                # Hard cap mode: count RUNNING + CLAIMED globally
                # (Note: prefetch_buffer must be 0 when cluster_wide_cap is set, enforced by config validation)
                res = await s.execute(COUNT_GLOBAL_IN_FLIGHT_SQL)
                row = res.fetchone()
                if row:
                    in_flight_global = int(row.cnt)
                else:
                    in_flight_global = 0
                global_remaining = max(
                    0, int(self.cfg.cluster_wide_cap) - in_flight_global
                )

            # Total claim budget for this pass: local budget capped by global remaining (if any)
            total_remaining = min(budget_remaining, remaining_claim_allowance)
            if global_remaining is not None:
                total_remaining = min(total_remaining, global_remaining)
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
                        in_flight_q = int(row.cnt)
                    else:
                        in_flight_q = 0
                    max_q = int(self.cfg.queue_max_concurrency.get(qname, 0))
                    q_remaining = max(0, max_q - in_flight_q)

                # Determine how many we may claim from this queue.
                # A positive max_claim_batch is an explicit fairness cap. The
                # default 0 means fill the remaining worker/queue budget.

                if self.cfg.max_claim_batch > 0:
                    per_queue_cap = self.cfg.max_claim_batch
                elif self.cfg.queue_priorities:
                    # Strict priority mode: try to fill remaining budget from this queue
                    per_queue_cap = total_remaining
                else:
                    per_queue_cap = total_remaining

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
                row['id'],
                row['task_name'],
                row['args'],
                row['kwargs'],
                row.get('queue_name') or 'default',
                bool(row.get('is_workflow_task', False)),
                timeout_ms=_parse_timeout_ms(row.get('task_options'), row['id']),
            )
        return len(claimed_rows) > 0

    async def _filter_nonrunnable_workflow_tasks(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Filter out tasks belonging to non-runnable workflows (PAUSED/CANCELLED).

        Post-claim guard:
        - PAUSED workflow: cancel claimed task row, reset workflow_task to READY
        - CANCELLED workflow: hard-cancel task + mark workflow_task SKIPPED

        Returns the filtered list of rows that should be dispatched.
        """
        if not rows:
            return rows

        workflow_rows = [
            row for row in rows if bool(row.get('is_workflow_task', True))
        ]
        if not workflow_rows:
            return rows

        task_ids = [row['id'] for row in workflow_rows]
        paused_task_ids: set[str] = set()
        cancelled_task_ids: set[str] = set()

        # Find tasks belonging to non-runnable workflows.
        async with self.sf() as s:
            res = await s.execute(
                GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
                {'ids': task_ids},
            )
            for row in res.fetchall():
                task_id = row.id
                wf_status = row.status
                if wf_status == 'PAUSED':
                    paused_task_ids.add(task_id)
                elif wf_status == 'CANCELLED':
                    cancelled_task_ids.add(task_id)

            if paused_task_ids:
                # Unclaim paused-workflow tasks so they can be picked up on resume.
                paused_res = await s.execute(
                    UNCLAIM_PAUSED_TASKS_SQL,
                    {'ids': list(paused_task_ids), 'wid': self.worker_instance_id},
                )
                unclaimed_paused_task_ids = [
                    str(task_id) for task_id in paused_res.scalars().all()
                ]
                # Keep workflow_task metadata consistent with unclaimed tasks.
                if unclaimed_paused_task_ids:
                    await s.execute(
                        RESET_PAUSED_WORKFLOW_TASKS_SQL,
                        {'ids': unclaimed_paused_task_ids},
                    )

            if cancelled_task_ids:
                # Cancel this worker's claimed task rows so they are no longer claimable.
                cancelled_res = await s.execute(
                    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
                    {'ids': list(cancelled_task_ids), 'wid': self.worker_instance_id},
                )
                cancelled_owned_task_ids = [
                    str(task_id) for task_id in cancelled_res.scalars().all()
                ]
                # Ensure workflow_task rows no longer sit in enqueueable states.
                if cancelled_owned_task_ids:
                    await s.execute(
                        SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
                        {'ids': cancelled_owned_task_ids},
                    )

            if paused_task_ids or cancelled_task_ids:
                await s.commit()

        blocked_task_ids = paused_task_ids | cancelled_task_ids
        return [row for row in rows if row['id'] not in blocked_task_ids]

    def _claim_pass_needs_serialization(self) -> bool:
        """Whether the claim pass must hold the cluster advisory lock.

        Serialization is required only for read-then-act cap accounting:
        a cluster_wide_cap, or an active per-queue max_concurrency (CUSTOM
        mode with a configured queue this worker claims from). Workers in a
        capped cluster must share the same cap config — a mixed fleet
        already breaks cap semantics regardless of locking.
        """
        if self.cfg.cluster_wide_cap is not None:
            return True
        if not self.cfg.queue_priorities:
            return False
        return any(
            queue_name in self.cfg.queue_max_concurrency
            for queue_name in self.cfg.queues
        )

    def _advisory_key_global(self) -> int:
        """Compute a stable 64-bit advisory lock key for claim serialization.

        PostgreSQL advisory locks are scoped to the current database, so a
        fixed key serializes all Horsies claim passes per database. A
        DSN-derived key (pre-0.1.7) silently split the lock when workers
        reached the same database through different DSN spellings (host vs
        IP, PgBouncer vs direct), letting cap accounting race.
        """
        h = hashlib.sha256(b'horsies:claim:v1').digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)



    def _claim_lease_ms(self) -> int:
        """Return the bounded claim lease duration in milliseconds.

        Uses explicit claim_lease_ms when configured (soft-cap or user override),
        otherwise falls back to DEFAULT_CLAIM_LEASE_MS for crash-recovery safety.
        The database computes the actual expiry timestamp.
        """
        return int(
            self.cfg.claim_lease_ms
            if self.cfg.claim_lease_ms is not None
            else DEFAULT_CLAIM_LEASE_MS
        )

    def _compute_claim_expires_at(self) -> datetime:
        """Compatibility helper: local view of the configured claim lease.

        SQL writes use DB time; this remains for diagnostics/tests that need to
        inspect the effective lease duration without touching the database.
        """
        return datetime.now(timezone.utc) + timedelta(milliseconds=self._claim_lease_ms())

    async def _claim_batch_locked(
        self,
        s: AsyncSession,
        queue: str,
        limit: int,
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
                # Lease expiry is computed server-side (now() + lease);
                # no local-clock timestamp is passed.
                'claim_lease_ms': self._claim_lease_ms(),
            },
        )
        cols = res.keys()
        return [dict(zip(cols, row)) for row in res.fetchall()]

    async def _count_claimed_for_worker(self, session: AsyncSession | None = None) -> int:
        """Count only CLAIMED tasks for this worker (not yet RUNNING)."""
        if session is not None:
            res = await session.execute(
                COUNT_CLAIMED_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0
        async with self.sf() as s:
            res = await s.execute(
                COUNT_CLAIMED_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0

    async def _count_only_running_for_worker(
        self, session: AsyncSession | None = None
    ) -> int:
        """Count only RUNNING tasks for this worker (excludes CLAIMED)."""
        if session is not None:
            res = await session.execute(
                COUNT_RUNNING_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0

    async def _count_in_flight_for_worker(
        self, session: AsyncSession | None = None
    ) -> int:
        """Count RUNNING + CLAIMED tasks for this worker (hard cap mode)."""
        if session is not None:
            res = await session.execute(
                COUNT_IN_FLIGHT_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0
        async with self.sf() as s:
            res = await s.execute(
                COUNT_IN_FLIGHT_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0

    async def _count_running_in_queue(self, queue_name: str) -> int:
        """Count RUNNING tasks in a given queue across the cluster."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_IN_QUEUE_SQL,
                {'q': queue_name},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0

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
        failed_executor: Optional[ProcessPoolExecutor] = None,
    ) -> None:
        outcome = await self._recover_worker_future_failure(
            task_id,
            f'Broken process pool: {exc}',
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
    ) -> _RequeueOutcome:
        """Recover a task whose child future failed without a task result.

        CLAIMED means user code never started and can be unclaimed. RUNNING
        means user code may have executed, so recovery must respect retry policy.
        """
        try:
            async with self.sf() as s:
                ctx_result = await s.execute(
                    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
                    {'id': task_id, 'wid': self.worker_instance_id},
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
                        {'id': task_id, 'wid': self.worker_instance_id},
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

                should_retry = await self._should_retry_task(task_id, task_error, s)
                if should_retry:
                    retry_outcome = await self._schedule_retry(
                        task_id, s, queue_name=ctx_row.queue_name or 'default',
                    )
                    match retry_outcome:
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
    ) -> None:
        """Submit to process pool; attach completion handler."""
        if self._executor is None:
            await self._restart_executor('Executor missing before dispatch')
            if self._executor is None:
                outcome = await self._requeue_claimed_task(
                    task_id,
                    'Executor unavailable after restart attempt',
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
            )
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(task_id, exc, executor)
            return
        except Exception as exc:
            outcome = await self._recover_worker_future_failure(
                task_id,
                f'Failed to dispatch task to executor: {exc}',
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
                executor,
                timeout_ms=timeout_ms,
            ),
            name=f'finalize-{task_id}',
            finalizer=True,
        )

    # ----- finalize (write back to DB + notify) -----

    async def _handle_task_timeout(self, task_id: str, timeout_ms: int) -> None:
        """Persist TASK_TIMEOUT for an over-deadline task and SIGKILL its child.

        Ownership-guarded like every finalize path: a row already resolved by
        another actor (reaper reclaim, cancel) is left alone. A CLAIMED row
        (child never confirmed RUNNING) is requeued instead of killed — the
        child's ownership confirm will come back CLAIM_LOST.

        The kill happens on both the retry-scheduled and terminal-failure
        branches: the hung child keeps executing user code either way, and a
        zombie attempt racing a re-claimed retry would be a double execution.
        """
        kill_pid: int | None = None
        try:
            async with self.sf() as s:
                ctx_result = await s.execute(
                    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
                    {'id': task_id, 'wid': self.worker_instance_id},
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
                        {'id': task_id, 'wid': self.worker_instance_id},
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
                if await self._should_retry_task(task_id, task_error, s):
                    retry_outcome = await self._schedule_retry(
                        task_id, s, queue_name=ctx_row.queue_name or 'default',
                    )
                    retry_scheduled = retry_outcome == 'scheduled'

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

    async def _finalize_after(
        self,
        fut: 'asyncio.Future[tuple[bool, str, Optional[str]]]',
        task_id: str,
        queue_name: str = 'default',
        is_workflow_task: bool = True,
        executor: Optional[ProcessPoolExecutor] = None,
        timeout_ms: Optional[int] = None,
    ) -> Result[None, _FinalizeError]:
        try:
            if timeout_ms is not None:
                try:
                    # shield: wait_for must not cancel the executor future —
                    # the child keeps running regardless, and the original
                    # future is awaited again below after the kill.
                    ok, result_json_str, failed_reason = await asyncio.wait_for(
                        asyncio.shield(fut), timeout=timeout_ms / 1000.0,
                    )
                except asyncio.TimeoutError:
                    await self._handle_task_timeout(task_id, timeout_ms)
                    # The SIGKILL breaks the shared process pool; awaiting
                    # the original future routes into the BrokenProcessPool
                    # branch below, which restarts the executor (sibling
                    # tasks recover through their own finalizers).
                    ok, result_json_str, failed_reason = await fut
            else:
                ok, result_json_str, failed_reason = await fut
        except asyncio.CancelledError:
            raise
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(task_id, exc, executor)
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
            requeue_outcome = await self._recover_worker_future_failure(
                task_id, f'Worker future failed before result: {exc}'
            )
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_FUTURE,
                    message=f'Worker future failed before result: {exc}',
                    # Retryability is keyed on the recovery DB failure, not
                    # the child-future exception: a non-connection future
                    # error whose requeue hit a transient DB blip must still
                    # retry the recovery instead of waiting on the reaper.
                    retryable=requeue_outcome is _RequeueOutcome.DB_ERROR,
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
            return Err(
                self._with_finalize_context(
                    phase1_r.err_value,
                    queue_name=queue_name,
                    is_workflow_task=is_workflow_task,
                )
            )
        tr = phase1_r.ok_value
        if tr is None:
            self._clear_finalize_retry_attempts(task_id, _FINALIZE_STAGE_PHASE1)
            return Ok(None)

        # Phase 2: workflow progression + capacity wakeups in a separate transaction.
        # Plain tasks only need capacity notifications.
        phase2_r = await self._finalize_workflow_phase(
            task_id,
            tr,
            queue_name=queue_name,
            is_workflow_task=is_workflow_task,
        )
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
        error_code: BuiltInTaskCode | str = OperationalErrorCode.BROKER_ERROR,
    ) -> _FinalizeError:
        return _FinalizeError(
            error_code=error_code,
            message=message,
            stage=stage,
            task_id=task_id,
            retryable=retryable,
            data=data,
        )

    def _with_finalize_context(
        self,
        err: _FinalizeError,
        *,
        queue_name: str,
        is_workflow_task: bool,
    ) -> _FinalizeError:
        data = dict(err.data or {})
        data.setdefault('queue_name', queue_name or 'default')
        data.setdefault('is_workflow_task', is_workflow_task)
        return _FinalizeError(
            error_code=err.error_code,
            message=err.message,
            stage=err.stage,
            task_id=err.task_id,
            retryable=err.retryable,
            data=data,
        )

    def _finalize_context_from_error(self, err: _FinalizeError) -> tuple[str, bool]:
        data = err.data or {}
        is_workflow_task_raw = data.get('is_workflow_task')
        return (
            str(data.get('queue_name') or 'default'),
            is_workflow_task_raw if isinstance(is_workflow_task_raw, bool) else True,
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
        """Phase 1 of finalization: persist task terminal state/result durably.

        Uses SELECT FOR UPDATE to lock the RUNNING row, extract attempt context,
        upsert an immutable attempt row, then transition the task state — all
        within a single transaction.
        """
        try:
            # Note: Heartbeat thread in task process automatically dies when process completes.
            async with self.sf() as s:
                # Pre-exec aborts: no attempt row, no state change
                if not ok:
                    match failed_reason:
                        case (
                            'CLAIM_LOST'
                            | 'OWNERSHIP_UNCONFIRMED'
                            | 'WORKFLOW_CHECK_FAILED'
                            | 'WORKFLOW_STOPPED'
                            | 'TASK_EXPIRED'
                        ):
                            logger.debug(
                                f'Task {task_id} aborted with reason={failed_reason}, skipping finalization'
                            )
                            return Ok(None)
                        case _:
                            pass

                # Lock the RUNNING row and extract context for attempt history
                ctx_result = await s.execute(
                    SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
                    {'id': task_id, 'wid': self.worker_instance_id},
                )
                ctx_row = ctx_result.fetchone()
                if ctx_row is None:
                    logger.warning(
                        f'Task {task_id} finalize aborted: status is no longer RUNNING '
                        f'or task is no longer owned by this worker (reaper reclaim '
                        f'or re-claim by another worker). Skipping to prevent '
                        f'clobbering the current attempt.'
                    )
                    return Ok(None)

                attempt_num = (ctx_row.retry_count or 0) + 1
                db_now = ctx_row.db_now or now
                attempt_started_at = ctx_row.started_at or db_now
                attempt_worker = {
                    'worker_id': ctx_row.claimed_by_worker_id,
                    'worker_hostname': ctx_row.worker_hostname,
                    'worker_pid': ctx_row.worker_pid,
                    'worker_process_name': ctx_row.worker_process_name,
                }

                if not ok:
                    # Worker-level failure (rare): write WORKER_FAILURE attempt, mark FAILED
                    _err_tr: TaskResult[None, TaskError] = TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.BROKER_ERROR,
                            message=failed_reason or 'Worker failure',
                            data={'task_id': task_id},
                        )
                    )
                    result_payload = serialize_error_payload(_err_tr)
                    await s.execute(
                        UPSERT_TASK_ATTEMPT_SQL,
                        {
                            'task_id': task_id,
                            'attempt': attempt_num,
                            'outcome': 'WORKER_FAILURE',
                            'will_retry': False,
                            'started_at': attempt_started_at,
                            'finished_at': db_now,
                            'error_code': OperationalErrorCode.BROKER_ERROR.value,
                            'error_message': failed_reason or 'Worker failure',
                            'failed_reason': failed_reason or 'Worker failure',
                            **attempt_worker,
                        },
                    )
                    fail_res = await s.execute(
                        MARK_TASK_FAILED_WORKER_SQL,
                        {
                            'reason': failed_reason or 'Worker failure',
                            'result_json': result_payload,
                            'error_code': OperationalErrorCode.BROKER_ERROR.value,
                            'id': task_id,
                            'wid': self.worker_instance_id,
                        },
                    )
                    if fail_res.fetchone() is None:
                        logger.warning(
                            f'Task {task_id} worker-fail finalize aborted: status is no longer RUNNING'
                        )
                        return Ok(None)
                    await s.commit()
                    return Ok(_err_tr)

                # --- ok=True: parse the TaskResult ---
                _loads_r = loads_json(result_json_str)
                if is_err(_loads_r):
                    logger.error(
                        f'Task {task_id} result JSON is corrupt: {_loads_r.err_value}'
                    )
                    _err_tr: TaskResult[None, TaskError] = TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                            message=f'Result JSON corrupt: {_loads_r.err_value}',
                            data={'task_id': task_id},
                        ),
                    )
                    await s.execute(
                        UPSERT_TASK_ATTEMPT_SQL,
                        {
                            'task_id': task_id,
                            'attempt': attempt_num,
                            'outcome': 'FAILED',
                            'will_retry': False,
                            'started_at': attempt_started_at,
                            'finished_at': db_now,
                            'error_code': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
                            'error_message': f'Result JSON corrupt: {_loads_r.err_value}',
                            'failed_reason': None,
                            **attempt_worker,
                        },
                    )
                    await s.execute(
                        MARK_TASK_FAILED_SQL,
                        {
                            'result_json': serialize_error_payload(_err_tr),
                            'id': task_id,
                            'wid': self.worker_instance_id,
                            'error_code': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
                        },
                    )
                    await s.commit()
                    return Ok(_err_tr)

                # Strict-serde phase 6: decode the persisted envelope.
                # Err-fast-path mirrors ``app.get_result_async``: validate
                # envelope shape, then read the err slot first because
                # TaskError is fixed-schema and decodes without OkT.
                # ``task_ok_type`` is only required when the ok slot is
                # populated. Without this branch a row whose worker
                # wrote ``WORKER_RESOLUTION_ERROR`` (unknown task name)
                # gets re-wrapped here as ``WORKER_SERIALIZATION_ERROR``
                # because the local registry has no entry for that task.
                task_name_for_decode = ctx_row.task_name
                _decode_err: Exception | None = None
                tr: TaskResult[Any, TaskError] | None = None
                try:
                    envelope = validate_task_result_envelope(_loads_r.ok_value)
                except StrictJsonError as exc:
                    _decode_err = exc
                else:
                    err_slot = envelope.get('err')
                    if err_slot is not None:
                        try:
                            err_value = decode_task_error(err_slot)
                            tr = TaskResult(err=err_value)
                        except (StrictJsonError, ValidationError) as exc:
                            _decode_err = exc
                    else:
                        source_task = (
                            self._app.tasks.get(task_name_for_decode)
                            if self._app is not None
                            else None
                        )
                        source_ok_type = (
                            getattr(source_task, 'task_ok_type', None)
                            if source_task is not None
                            else None
                        )
                        if source_ok_type is None:
                            _decode_err = SerializationError(
                                f'Task {task_name_for_decode!r} not registered or '
                                f'missing task_ok_type during finalize'
                            )
                        else:
                            try:
                                tr = decode_task_result(
                                    _loads_r.ok_value, source_ok_type,
                                )
                            except (StrictJsonError, ValidationError) as exc:
                                _decode_err = exc
                if _decode_err is not None:
                    logger.error(
                        f'Task {task_id} result decode failed: {_decode_err}'
                    )
                    _err_tr = TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                            message=f'Result decode failed: {_decode_err}',
                            data={'task_id': task_id},
                        ),
                    )
                    await s.execute(
                        UPSERT_TASK_ATTEMPT_SQL,
                        {
                            'task_id': task_id,
                            'attempt': attempt_num,
                            'outcome': 'FAILED',
                            'will_retry': False,
                            'started_at': attempt_started_at,
                            'finished_at': db_now,
                            'error_code': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
                            'error_message': f'Result decode failed: {_decode_err}',
                            'failed_reason': None,
                            **attempt_worker,
                        },
                    )
                    await s.execute(
                        MARK_TASK_FAILED_SQL,
                        {
                            'result_json': serialize_error_payload(_err_tr),
                            'id': task_id,
                            'wid': self.worker_instance_id,
                            'error_code': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
                        },
                    )
                    await s.commit()
                    return Ok(_err_tr)

                # `tr` is set by either the err-fast-path or the ok-slot
                # typed decode above; the _decode_err early-return covers
                # all failure paths, so tr is non-None here.
                assert tr is not None
                if tr.is_err():
                    task_error = tr.unwrap_err()
                    _raw_code = task_error.error_code if task_error else None
                    error_code_str: str | None = (
                        _raw_code.value if isinstance(_raw_code, Enum) else _raw_code
                    )
                    match _raw_code:
                        case 'WORKFLOW_STOPPED':
                            logger.debug(
                                f'Task {task_id} skipped due to workflow stop, skipping finalization'
                            )
                            return Ok(None)
                        case _:
                            pass

                    should_retry = await self._should_retry_task(task_id, task_error, s)
                    if should_retry:
                        retry_outcome = await self._schedule_retry(
                            task_id, s, queue_name=ctx_row.queue_name or 'default',
                        )
                        match retry_outcome:
                            case 'scheduled':
                                # Retry scheduled: write FAILED attempt with will_retry=True
                                await s.execute(
                                    UPSERT_TASK_ATTEMPT_SQL,
                                    {
                                        'task_id': task_id,
                                        'attempt': attempt_num,
                                        'outcome': 'FAILED',
                                        'will_retry': True,
                                        'started_at': attempt_started_at,
                                        'finished_at': db_now,
                                        'error_code': error_code_str,
                                        'error_message': task_error.message
                                        if task_error
                                        else None,
                                        'failed_reason': None,
                                        **attempt_worker,
                                    },
                                )
                                await s.commit()
                                return Ok(None)
                            case 'reaper_reclaimed':
                                logger.warning(
                                    f'Task {task_id} retry aborted during finalize: '
                                    f'task no longer RUNNING (reaper reclaimed).'
                                )
                                await s.commit()
                                return Ok(None)
                            case 'expired':
                                logger.info(
                                    f'Task {task_id} retry skipped: good_until exceeded, '
                                    f'falling through to mark as failed.'
                                )
                                # Fall through to mark task FAILED with original error.

                    # Terminal failure: write FAILED attempt with will_retry=False
                    await s.execute(
                        UPSERT_TASK_ATTEMPT_SQL,
                        {
                            'task_id': task_id,
                            'attempt': attempt_num,
                            'outcome': 'FAILED',
                            'will_retry': False,
                            'started_at': attempt_started_at,
                            'finished_at': db_now,
                            'error_code': error_code_str,
                            'error_message': task_error.message if task_error else None,
                            'failed_reason': None,
                            **attempt_worker,
                        },
                    )
                    fail_res = await s.execute(
                        MARK_TASK_FAILED_SQL,
                        {
                            'result_json': result_json_str,
                            'id': task_id,
                            'wid': self.worker_instance_id,
                            'error_code': error_code_str,
                        },
                    )
                    if fail_res.fetchone() is None:
                        logger.warning(
                            f'Task {task_id} finalize-fail aborted: status/ownership '
                            f'changed (reaper reclaim or re-claim by another worker). '
                            f'Skipping to prevent clobbering the current attempt.'
                        )
                        return Ok(None)
                else:
                    # Success: write COMPLETED attempt
                    await s.execute(
                        UPSERT_TASK_ATTEMPT_SQL,
                        {
                            'task_id': task_id,
                            'attempt': attempt_num,
                            'outcome': 'COMPLETED',
                            'will_retry': False,
                            'started_at': attempt_started_at,
                            'finished_at': db_now,
                            'error_code': None,
                            'error_message': None,
                            'failed_reason': None,
                            **attempt_worker,
                        },
                    )
                    comp_res = await s.execute(
                        MARK_TASK_COMPLETED_SQL,
                        {
                            'result_json': result_json_str,
                            'id': task_id,
                            'wid': self.worker_instance_id,
                        },
                    )
                    if comp_res.fetchone() is None:
                        logger.warning(
                            f'Task {task_id} finalize-complete aborted: status/ownership '
                            f'changed (reaper reclaim or re-claim by another worker). '
                            f'Skipping to prevent clobbering the current attempt.'
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
        *,
        queue_name: str = 'default',
        is_workflow_task: bool = True,
    ) -> Result[None, _FinalizeError]:
        """Phase 2 of finalization: workflow advancement and worker wake notifications."""
        try:
            async with self.sf() as s:
                # Handle workflow task completion (if this task is part of a workflow)
                await self._handle_workflow_task_if_needed(
                    s,
                    task_id,
                    tr,
                    is_workflow_task=is_workflow_task,
                )

                # Proactively wake workers of this queue to re-check
                # capacity/backlog (workers no longer listen on task_new).
                try:
                    payload = f'capacity:{task_id}'
                    await s.execute(
                        NOTIFY_TASK_QUEUE_SQL,
                        {'c2': f'task_queue_{queue_name or "default"}', 'p': payload},
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
                        'queue_name': queue_name or 'default',
                        'is_workflow_task': is_workflow_task,
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
                status = str(row.status) if row.status is not None else ''
                raw_result = row.result
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
                # Strict-serde phase 6: err-fast-path then typed ok decode.
                # TaskError has a fixed schema and decodes without OkT,
                # so we must read the err slot before requiring a local
                # registry entry — otherwise a persisted err result for
                # an unknown task name would be unrecoverable during
                # replay (mirrors ``app.get_result_async`` ordering).
                source_task_name = row.task_name
                try:
                    envelope = validate_task_result_envelope(loads_r.ok_value)
                except StrictJsonError as exc:
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE2,
                            message=(
                                f'Cannot replay finalize phase-2: stored '
                                f'result envelope invalid: {exc}'
                            ),
                            retryable=False,
                        )
                    )
                err_slot = envelope.get('err')
                if err_slot is not None:
                    try:
                        err_value = decode_task_error(err_slot)
                    except (StrictJsonError, ValidationError) as exc:
                        return Err(
                            self._make_finalize_error(
                                task_id=task_id,
                                stage=_FINALIZE_STAGE_PHASE2,
                                message=(
                                    f'Cannot replay finalize phase-2: '
                                    f'stored err decode failed: {exc}'
                                ),
                                retryable=False,
                            )
                        )
                    return Ok(TaskResult(err=err_value))
                source_task = (
                    self._app.tasks.get(source_task_name)
                    if self._app is not None and source_task_name is not None
                    else None
                )
                source_ok_type = (
                    getattr(source_task, 'task_ok_type', None)
                    if source_task is not None
                    else None
                )
                if source_ok_type is None:
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE2,
                            message=(
                                f'Cannot replay finalize phase-2: task '
                                f'{source_task_name!r} not registered or '
                                f'missing task_ok_type'
                            ),
                            retryable=False,
                        )
                    )
                try:
                    decoded_tr = decode_task_result(
                        loads_r.ok_value, source_ok_type,
                    )
                except (StrictJsonError, ValidationError) as exc:
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE2,
                            message=(
                                f'Cannot replay finalize phase-2: '
                                f'stored result decode failed: {exc}'
                            ),
                            retryable=False,
                        )
                    )
                return Ok(decoded_tr)
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
        """Handle finalize Result errors with bounded retries."""
        if not isinstance(err, _FinalizeError):
            logger.error(
                f'Unexpected finalize error payload type: {type(err).__name__}'
            )
            return

        stage = err.stage
        task_id = err.task_id
        if stage == _FINALIZE_STAGE_FUTURE:
            max_attempts = _FINALIZE_FUTURE_MAX_RETRIES
        elif stage == _FINALIZE_STAGE_PHASE1:
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

        if stage == _FINALIZE_STAGE_FUTURE:
            self._spawn_background(
                self._retry_finalize_future(err, delay),
                name=f'finalize-retry-future-{task_id}',
                finalizer=True,
            )
        elif stage == _FINALIZE_STAGE_PHASE1:
            self._spawn_background(
                self._retry_finalize_phase1(err, delay),
                name=f'finalize-retry-phase1-{task_id}',
                finalizer=True,
            )
        else:
            self._spawn_background(
                self._retry_finalize_phase2(err, delay),
                name=f'finalize-retry-phase2-{task_id}',
                finalizer=True,
            )

    async def _retry_finalize_future(self, err: _FinalizeError, delay_s: float) -> None:
        """Retry requeue after a child future failed before producing an outcome."""
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return

        outcome = await self._recover_worker_future_failure(
            err.task_id,
            f'Retry after future-stage finalize error: {err.message}',
        )
        if outcome is _RequeueOutcome.REQUEUED:
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_FUTURE)
            return
        if outcome is _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED:
            logger.warning(
                f'Future-stage finalize retry found no owned in-flight row for task {err.task_id}'
            )
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_FUTURE)
            return

        data = dict(err.data or {})
        data['requeue_outcome'] = outcome.value
        await self._handle_finalize_error(
            self._make_finalize_error(
                task_id=err.task_id,
                stage=_FINALIZE_STAGE_FUTURE,
                message=err.message,
                retryable=True,
                data=data,
            )
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
            queue_name, is_workflow_task = self._finalize_context_from_error(err)
            await self._handle_finalize_error(
                self._with_finalize_context(
                    phase1_r.err_value,
                    queue_name=queue_name,
                    is_workflow_task=is_workflow_task,
                )
            )
            return

        tr = phase1_r.ok_value
        if tr is None:
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
            return

        queue_name, is_workflow_task = self._finalize_context_from_error(err)
        phase2_r = await self._finalize_workflow_phase(
            err.task_id,
            tr,
            queue_name=queue_name,
            is_workflow_task=is_workflow_task,
        )
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

        queue_name, is_workflow_task = self._finalize_context_from_error(err)
        phase2_r = await self._finalize_workflow_phase(
            err.task_id,
            load_r.ok_value,
            queue_name=queue_name,
            is_workflow_task=is_workflow_task,
        )
        if is_err(phase2_r):
            await self._handle_finalize_error(phase2_r.err_value)
            return

        self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE2)

    async def _handle_workflow_task_if_needed(
        self,
        session: 'AsyncSession',
        task_id: str,
        result: 'TaskResult[Any, TaskError]',
        *,
        is_workflow_task: bool = True,
    ) -> None:
        """
        Check if task is part of a workflow and handle accordingly.

        This method is called after a task completes (success or failure).
        It updates the workflow_task record and triggers dependency resolution.
        """
        from horsies.core.workflows.engine import on_workflow_task_complete

        if not is_workflow_task:
            return

        # Quick check: is this task linked to a workflow?
        check = await session.execute(
            CHECK_WORKFLOW_TASK_EXISTS_SQL,
            {'tid': task_id},
        )

        if check.fetchone() is None:
            return  # Not a workflow task

        # Handle workflow task completion
        await on_workflow_task_complete(session, task_id, result, self.broker)













"""
horsies examples/instance.py worker --loglevel=info --processes=8 
"""
