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
from datetime import datetime, timezone
from importlib import import_module
from collections.abc import Coroutine
from typing import Any, Optional, TYPE_CHECKING, cast
import sys
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.app import Horsies
from horsies.core.brokers.listener import PostgresListener
from horsies.core.models.health import WORKER_PING_CHANNEL
from horsies.core.logging import get_logger
from horsies.core.worker.current import set_current_app
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.types.result import Err, is_err
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.url import to_psycopg_url

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker

# --- Imports from sibling modules (extracted for maintainability) ---
from horsies.core.worker.config import WorkerConfig as WorkerConfig  # noqa: F401
from horsies.core.worker.child_pool import _initialize_worker_pool as _initialize_worker_pool  # noqa: F401
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
from horsies.core.worker.claiming import ClaimMixin as ClaimMixin  # noqa: E402
from horsies.core.worker.dispatch import DispatchMixin as DispatchMixin  # noqa: E402
from horsies.core.worker.finalize import (  # noqa: E402
    GET_TASK_STATUS_RESULT_SQL as GET_TASK_STATUS_RESULT_SQL,
    FinalizeMixin as FinalizeMixin,
)
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





class Worker(ClaimMixin, DispatchMixin, FinalizeMixin, HealthMixin, ReaperMixin, RetryMixin):
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













































"""
horsies examples/instance.py worker --loglevel=info --processes=8 
"""
