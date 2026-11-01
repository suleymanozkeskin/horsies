# app/core/worker.py
# pyright: reportPrivateUsage=false
from __future__ import annotations
import asyncio
import uuid
import os
from datetime import datetime, timezone
from importlib import import_module
from collections.abc import Coroutine
from typing import Any, TYPE_CHECKING, assert_never
import sys
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.app import Horsies
from horsies.core.brokers.listener import PostgresListener
from horsies.core.models.health import WORKER_PING_CHANNEL
from horsies.core.logging import get_logger
from horsies.core.worker.current import set_current_app
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.url import to_psycopg_url

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker

# --- Imports from sibling modules (extracted for maintainability) ---
from horsies.core.worker.config import WorkerConfig as WorkerConfig  # noqa: F401
from horsies.core.worker.child_pool import (
    _initialize_worker_pool as _initialize_worker_pool,
)  # noqa: F401
from horsies.core.worker.child_protocol import InitFrame, RssMb
from horsies.core.worker.slots import (
    BootAttemptsExhausted,
    ChildBaseline,
    ChildHookFailed,
    SlotPool,
    SlotPoolConfig,
    SpawnFailed,
)
from horsies.core.worker.child_runner import (  # noqa: F401
    CHILD_HOOK_FAILURE_EXIT_CODE as CHILD_HOOK_FAILURE_EXIT_CODE,
    _locate_app as _locate_app,
    _child_initializer as _child_initializer,
    _confirm_ownership_and_set_running as _confirm_ownership_and_set_running,
    _run_task_entry as _run_task_entry,
    import_by_path as import_by_path,
    _dedupe_paths as _dedupe_paths,
    _build_sys_path_roots as _build_sys_path_roots,
    _derive_sys_path_roots_from_file as _derive_sys_path_roots_from_file,
    _debug_imports_enabled as _debug_imports_enabled,
    _debug_imports_log as _debug_imports_log,
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
    GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL as GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
    UNCLAIM_CLAIMED_TASK_SQL as UNCLAIM_CLAIMED_TASK_SQL,
    RESET_PAUSED_WORKFLOW_TASKS_SQL as RESET_PAUSED_WORKFLOW_TASKS_SQL,
    SKIP_CANCELLED_WORKFLOW_TASKS_SQL as SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
    SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL as SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
    UPSERT_TASK_ATTEMPT_SQL as UPSERT_TASK_ATTEMPT_SQL,
    NOTIFY_TASK_QUEUE_SQL as NOTIFY_TASK_QUEUE_SQL,
    GET_TASK_RETRY_INFO_SQL as GET_TASK_RETRY_INFO_SQL,
    GET_TASK_RETRY_CONFIG_SQL as GET_TASK_RETRY_CONFIG_SQL,
    GET_TASK_RETRY_POSTCHECK_SQL as GET_TASK_RETRY_POSTCHECK_SQL,
    SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL as SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL,
    SCHEDULE_TASK_RETRY_SQL as SCHEDULE_TASK_RETRY_SQL,
    NOTIFY_DELAYED_SQL as NOTIFY_DELAYED_SQL,
    INSERT_CLAIMER_HEARTBEAT_SQL as INSERT_CLAIMER_HEARTBEAT_SQL,
    RENEW_CLAIM_LEASE_SQL as RENEW_CLAIM_LEASE_SQL,
    INSERT_WORKER_STATE_SQL as INSERT_WORKER_STATE_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL as DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL as DELETE_EXPIRED_WORKFLOWS_SQL,
    WORKFLOW_TERMINAL_VALUES as WORKFLOW_TERMINAL_VALUES,
    TASK_TERMINAL_VALUES as TASK_TERMINAL_VALUES,
    _FINALIZER_DRAIN_TIMEOUT_S as _FINALIZER_DRAIN_TIMEOUT_S,
)

logger = get_logger('worker')


def _frame_text(text: str) -> str:
    """Return ``text`` as a frame can carry it. Bytes that are not UTF-8 are escaped."""
    return text.encode('utf-8', 'backslashreplace').decode('utf-8')


def _frame_sys_path() -> tuple[str, ...]:
    """Return ``sys.path`` for a child. ``''`` means the current directory.

    ``sys.path`` may hold any object, and Python skips what it cannot use. A
    ``Path`` is converted. Anything else that a frame cannot carry is left out
    with a warning, as it could not be restored.
    """
    entries: list[str] = []
    for entry in sys.path:
        try:
            text = os.fsdecode(entry) or os.getcwd()
            text.encode('utf-8')
        except (TypeError, UnicodeError):
            logger.warning('Leaving sys.path entry %r out of worker children', entry)
            continue
        entries.append(text)
    return tuple(entries)

# How long running tasks get to finish at shutdown.
_DRAIN_TIMEOUT_S = 300.0
_HOOK_FAILURE_MESSAGE = (
    'on_child_process_start hook failed in a worker child (see the child log '
    'line above for the hook name); fix the hook, the worker will not '
    'restart-loop on it'
)

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
    _FINALIZE_STAGE_PARENT as _FINALIZE_STAGE_PARENT,
    _FINALIZE_FUTURE_MAX_RETRIES as _FINALIZE_FUTURE_MAX_RETRIES,
    _FINALIZE_PHASE1_MAX_RETRIES as _FINALIZE_PHASE1_MAX_RETRIES,
    _FINALIZE_PHASE2_MAX_RETRIES as _FINALIZE_PHASE2_MAX_RETRIES,
    _FINALIZE_PARENT_MAX_RETRIES as _FINALIZE_PARENT_MAX_RETRIES,
    _FINALIZE_RETRY_BASE_DELAY_S as _FINALIZE_RETRY_BASE_DELAY_S,
    _FINALIZE_RETRY_MAX_DELAY_S as _FINALIZE_RETRY_MAX_DELAY_S,
    _REAPER_MAX_PERMANENT_FAILURES as _REAPER_MAX_PERMANENT_FAILURES,
    ChildHookFailedError as ChildHookFailedError,
    ChildStartFailedError as ChildStartFailedError,
    ServiceLoopDiedError as ServiceLoopDiedError,
    MemoryBaselineExceedsThresholdError as MemoryBaselineExceedsThresholdError,
    _RetryBackoff as _RetryBackoff,
    _FinalizeError as _FinalizeError,
    _RetryError as _RetryError,
    _RequeueOutcome as _RequeueOutcome,
    _ReaperPassState as _ReaperPassState,
    _collect_psutil_metrics as _collect_psutil_metrics,
    _parse_timeout_ms as _parse_timeout_ms,
)


class Worker(
    ClaimMixin, DispatchMixin, FinalizeMixin, HealthMixin, ReaperMixin, RetryMixin
):
    """
    Async master that:
      - Subscribes to queue channels
      - Claims tasks (priority + enqueued_at) with SKIP LOCKED
      - Executes in worker-owned child processes
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
        # Last coverage-ensure outcome, published with worker-state
        # snapshots so operators see covered_through and typed failures.
        self._partition_coverage_health: dict[str, Any] | None = None
        # Last pruning-pass outcome: detach/drop counts and the typed
        # refusals that skipped a leaf.
        self._partition_pruning_health: dict[str, Any] | None = None
        self._phase2_recovery_health: dict[str, Any] | None = None
        self._workflow_recovery_health: dict[str, Any] | None = None
        self._orphan_task_recovery_health: dict[str, Any] | None = None
        # Created after preloading modules, so that import and validation
        # errors surface in the main process at startup.
        self._slots: SlotPool | None = None
        self._stop = asyncio.Event()
        self._ping_queue: asyncio.Queue[Any] | None = None
        self._service_tasks: set[asyncio.Task[Any]] = set()
        self._finalizer_tasks: set[asyncio.Task[Any]] = set()
        # Set when a finalizer task ends, on every outcome. Some exits free
        # a slot without a queue NOTIFY (a requeue to PENDING, an expiry
        # before start), so the claim loop also waits on this local wake.
        # Releases coalesce: the loop clears it before each claim pass.
        self._capacity_released = asyncio.Event()
        self._finalize_retry_attempts: dict[tuple[str, str], int] = {}
        self._fatal_background_error: (
            ChildStartFailedError | ServiceLoopDiedError | None
        ) = None

    def request_stop(self) -> None:
        """Request worker to stop gracefully."""
        self._stop.set()

    def _spawn_background(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str,
        finalizer: bool = False,
        service: bool = False,
    ) -> asyncio.Task[Any]:
        """Create a tracked background task with automatic cleanup.

        ``service=True`` marks a worker-lifetime loop (claimer heartbeat,
        worker-state snapshot, ping responder, reaper): one that must run for
        as long as the worker claims. Ending for any reason other than a
        requested shutdown — an escaped exception or an unexpected return —
        is process-fatal (``ServiceLoopDiedError``): the loops contain their
        own per-iteration errors, and a worker that keeps claiming with a
        dead heartbeat/reaper loop is a zombie that only looks healthy.

        Shutdown is recognized by any of: the task was cancelled, a
        cancellation was requested (the loops absorb ``CancelledError`` and
        return — ``_cleanup_after_failed_start`` cancels them with ``_stop``
        deliberately unset so the resilience retry can respawn them), or
        ``_stop`` is set.
        """
        task_group = self._finalizer_tasks if finalizer else self._service_tasks
        task = asyncio.create_task(coro, name=name)
        task_group.add(task)

        def _escalate_fatal(
            err: ChildStartFailedError | ServiceLoopDiedError,
        ) -> None:
            # Capture and stop; run_forever re-raises it so the process
            # exits non-zero for a supervisor restart.
            self._fatal_background_error = err
            self._stop.set()

        def _on_done(t: asyncio.Task[Any]) -> None:
            task_group.discard(t)
            if finalizer:
                self._capacity_released.set()
            if t.cancelled():
                return
            shutdown_requested = t.cancelling() > 0 or self._stop.is_set()
            exc = t.exception()
            if exc is not None:
                match exc:
                    case ChildStartFailedError():
                        # Process-fatal: a worker that cannot start a child
                        # must not keep claiming tasks.
                        logger.critical(
                            f'Background task {t.get_name()!r} hit a '
                            f'process-fatal error; stopping worker: {exc}'
                        )
                        _escalate_fatal(exc)
                    case _ if service and not shutdown_requested:
                        logger.critical(
                            f'Service loop {t.get_name()!r} died with the '
                            f'worker still claiming; stopping worker: {exc}'
                        )
                        died = ServiceLoopDiedError(
                            f'service loop {t.get_name()!r} died: {exc}'
                        )
                        died.__cause__ = exc
                        _escalate_fatal(died)
                    case _:
                        logger.error(f'Background task {t.get_name()!r} failed: {exc}')
                return
            if service and not shutdown_requested:
                logger.critical(
                    f'Service loop {t.get_name()!r} returned with no shutdown '
                    f'requested; stopping worker'
                )
                _escalate_fatal(ServiceLoopDiedError(
                    f'service loop {t.get_name()!r} returned unexpectedly'
                ))
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

    def _slot_pool_config(self) -> SlotPoolConfig:
        """Build the pool settings for a child to restore the parent's imports."""
        return SlotPoolConfig(
            slots=self.cfg.processes,
            init=InitFrame(
                app_locator=self.cfg.app_locator,
                imports=tuple(self.cfg.imports),
                sys_path_roots=tuple(self.cfg.sys_path_roots),
                parent_sys_path=_frame_sys_path(),
                parent_argv=tuple(_frame_text(str(arg)) for arg in sys.argv),
                loglevel=self.cfg.loglevel,
                database_url=to_psycopg_url(self.cfg.dsn),
                connect_kwargs=self.cfg.child_connect_kwargs,
                child_pool_min_size=self.cfg.child_pool_min_size,
                child_pool_max_size=self.cfg.child_pool_max_size,
                child_pool_check=self.cfg.child_pool_check,
                process_name='horsies-child',
            ),
            max_tasks_per_child=self.cfg.max_tasks_per_child,
            max_memory_per_child_mb=self.cfg.max_memory_per_child_mb,
        )

    def _pending_task_limit(self) -> int:
        """The most tasks that may wait for a slot: twice the claim budget."""
        budget = max(
            self.cfg.processes + self.cfg.prefetch_buffer,
            self.cfg.max_claim_per_worker,
        )
        return 2 * budget

    async def _start_slots(self) -> None:
        """Start every child and publish the pool.

        Raises:
            ChildHookFailedError: an ``on_child_process_start`` hook failed.
                Never retried.
            ChildStartFailedError: a child could not start.
            MemoryBaselineExceedsThresholdError: the booted child baseline is
                at or above ``max_memory_per_child_mb``.
        """
        if self.cfg.app_locator.startswith('__main__:'):
            raise ChildStartFailedError(
                f'app locator {self.cfg.app_locator!r} cannot work: a child is a '
                'new program with its own __main__. Use a module path or a file '
                'path.',
            )
        pool = SlotPool(self._slot_pool_config(), self._pending_task_limit())
        self._slots = pool
        match await pool.start():
            case Ok(baselines):
                logger.info(
                    'Worker child processes ready: %s/%s',
                    len(baselines),
                    self.cfg.processes,
                )
                self._check_memory_baseline(baselines)
            case Err(ChildHookFailed()):
                raise ChildHookFailedError(_HOOK_FAILURE_MESSAGE)
            case Err(failure):
                raise ChildStartFailedError(f'worker child start failed: {failure}')

    async def _slot_failure_loop(self, pool: SlotPool) -> None:
        """Service loop: end the worker when no replacement child can start.

        Running tasks drain first, through ``stop``. Returns after a hook
        failure, because that stop is a graceful one.

        Raises:
            ChildStartFailedError: no replacement child could start.
        """
        failure = await pool.wait_failed()
        match failure:
            case ChildHookFailed():
                self._stop_for_child_hook_failure(
                    ChildHookFailedError(_HOOK_FAILURE_MESSAGE),
                )
            case SpawnFailed() | BootAttemptsExhausted():
                raise ChildStartFailedError(f'worker child start failed: {failure}')
            case _ as unreachable:
                assert_never(unreachable)

    def _check_memory_baseline(self, baselines: tuple[ChildBaseline, ...]) -> None:
        """Fail or warn when the booted child baseline crowds the threshold.

        Raises:
            MemoryBaselineExceedsThresholdError: a child's settled baseline RSS
                is at or above ``max_memory_per_child_mb`` — the app does not
                fit this per-child budget, so recycling would fire every task.
        """
        threshold = self.cfg.max_memory_per_child_mb
        if threshold is None:
            return
        samples = [
            baseline.rss.value
            for baseline in baselines
            if isinstance(baseline.rss, RssMb)
        ]
        if not samples:
            logger.warning(
                'max_memory_per_child_mb=%sMB is set but child baseline RSS '
                'was unreadable; skipping the startup baseline guard. Per-child '
                'recycle is best-effort and will not fire while the per-task RSS '
                'reader keeps failing the same way (fail-open).',
                threshold,
            )
            return
        max_baseline = max(samples)
        if max_baseline >= threshold:
            raise MemoryBaselineExceedsThresholdError(
                f'child baseline {max_baseline:.0f}MB >= '
                f'max_memory_per_child_mb {threshold}MB; the app does not fit '
                'this per-child budget — raise the budget or the threshold'
            )
        if max_baseline >= 0.8 * threshold:
            logger.warning(
                'Child baseline %.0fMB is within 80%% of '
                'max_memory_per_child_mb %sMB; little headroom for per-task '
                'working set, so children may recycle frequently',
                max_baseline,
                threshold,
            )
        else:
            logger.info(
                'Child baseline %.0fMB vs max_memory_per_child_mb %sMB',
                max_baseline,
                threshold,
            )

    def _stop_for_child_hook_failure(self, exc: ChildHookFailedError) -> None:
        """Stop the worker on a hook failure instead of restart-looping.

        The hook re-runs in every replacement child, so starting another
        child would fail the same way forever. run_forever observes the stop
        flag and shuts down gracefully.
        """
        logger.critical('Stopping worker: %s', exc)
        self._stop.set()

    def _raise_captured_fatal_error(self) -> None:
        """Re-raise a process-fatal error captured from a background task.

        Raises:
            ChildStartFailedError: ``_on_done`` captured it from the slot
                failure loop and set stop; re-raising here makes
                ``run_forever`` exit non-zero so the supervisor restarts the
                worker.
            ServiceLoopDiedError: a worker-lifetime service loop died with
                no shutdown requested; re-raising crashes the worker so the
                supervisor restarts it instead of leaving a claiming worker
                with dead heartbeat/reaper loops.
        """
        if self._fatal_background_error is not None:
            raise self._fatal_background_error

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
        """Tear down partial start state so the resilience loop can retry.

        Total: every I/O step (listener close, slot pool stop) contains and
        logs its own failure (cancellation exempt).
        """
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
        except Exception as e:
            logger.error(f'Error closing listener after failed start: {e}')

        # A start attempt can be cancelled while its children boot. They run
        # outside the attempt, so a retry must not find them still alive.
        pool, self._slots = self._slots, None
        if pool is not None:
            await pool.stop(0)

    async def _handle_retryable_start_error(
        self,
        exc: BaseException,
        backoff: _RetryBackoff,
    ) -> None:
        """Clean up, back off, and let the resilience loop retry the start.

        Raises:
            BaseException: re-raises the active start error (bare ``raise``)
                once the configured retry budget is exhausted — before any
                cleanup; final-failure state is left to process exit.
        """
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
        """Start the worker under the configured startup-retry policy.

        Each attempt is bounded at 30s. Timeouts and transient connection
        errors are backoff-retried per the resilience config. Returns
        without starting (and without raising) when stop is requested
        mid-backoff; ``run_forever`` checks ``_stop`` after this call.

        Raises:
            Exception: a non-retryable start error, or a retryable one after
                the retry budget is exhausted — fail-fast at boot; the CLI
                exits non-zero. On the exhausted-timeout arm the attempt was
                cancelled mid-start, so partial state is left to process
                exit rather than torn down.
        """
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
        """Bring up the worker: preload, start children, listen, spawn loops.

        Raises:
            Exception: any startup failure propagates.
                ``_preload_modules_main`` runs before any state exists and
                propagates bare; everything after it (child start incl.
                ChildHookFailedError, LISTEN/subscription errors) propagates
                after ``_cleanup_after_failed_start`` tears down partial
                state. ``_start_with_resilience_config`` owns the
                retry-vs-fail-fast decision.
        """
        logger.debug('Starting worker')
        # Preload the app and task modules in the main process to fail fast
        self._preload_modules_main()

        try:
            # Start children before any database work, so a bad app or hook
            # fails at once. Children are new programs: they inherit no socket.
            await self._start_slots()

            # A worker broker proves both database paths before maintenance
            # starts.
            match self.broker:
                case None:
                    raise RuntimeError(
                        'worker startup requires a PostgresBroker with a '
                        'session-capable partition maintenance database'
                    )
                case broker:
                    await broker.validate_worker_database_paths()
            match await broker.select_resources():
                case Err(loop_limit):
                    raise AssertionError(
                        f'worker broker resources: {loop_limit.describe()}'
                    )
                case Ok(resources):
                    pass

            # Partitioned writes need their partitions before the first
            # claim: heartbeat leaves for this worker's liveness rows,
            # history leaves for terminalizations, staged readers published.
            # Refusing to start when the present instant has no heartbeat
            # coverage fails at the moment an operator is attached instead
            # of on the first RUNNING transition.
            from horsies.core.history.maintenance.coverage import (
                StartupCoverageRefused,
                ensure_startup_coverage_in_database,
            )

            from horsies.core.models.retention import RetentionConfig

            retention_cfg = self.cfg.retention_config or RetentionConfig()
            startup_coverage = await ensure_startup_coverage_in_database(
                resources.partition_maintenance,
                history_horizon_days=retention_cfg.history_leaf_horizon_days,
                heartbeat_horizon_hours=(
                    retention_cfg.heartbeat_leaf_horizon_hours
                ),
                declared_classes=retention_cfg.registrable_classes(),
            )
            match startup_coverage:
                case StartupCoverageRefused(outcome=refused_outcome):
                    raise RuntimeError(
                        'worker startup refused: no heartbeat leaf covers '
                        f'the present instant after the ensure attempt — '
                        f'{refused_outcome!r}'
                    )
                case _:
                    pass


            start_r = await self.listener.start()
            if is_err(start_r):
                err = start_r.err_value
                logger.error(
                    'Postgres LISTEN failed. If database_url points to PgBouncer '
                    'transaction pooling, set PostgresConfig.session_database_url '
                    'to a direct/session-capable Postgres URL. Original error: %s',
                    err.message,
                )
                raise err.exception
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
                raise err.exception
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
                raise err.exception
            self._ping_queue = ping_listen_r.ok_value
            logger.info('Subscribed to worker ping channel')

            assert self._slots is not None
            self._spawn_background(
                self._slot_failure_loop(self._slots),
                name='slot-failure-watch',
                service=True,
            )
            # Start claimer heartbeat loop (CLAIMED coverage)
            self._spawn_background(
                self._claimer_heartbeat_loop(),
                name='claimer-heartbeat',
                service=True,
            )
            # Start worker state heartbeat loop for monitoring
            self._spawn_background(
                self._worker_state_heartbeat_loop(),
                name='worker-state-heartbeat',
                service=True,
            )
            logger.info('Worker state heartbeat loop started for monitoring')
            # Serve liveness pings from a background loop.
            self._spawn_background(
                self._ping_responder_loop(),
                name='ping-responder',
                service=True,
            )
            logger.info('Ping responder loop started for liveness probes')
            # Start reaper loop for automatic stale task handling
            if self.cfg.recovery_config:
                self._spawn_background(
                    self._reaper_loop(), name='reaper', service=True,
                )
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
        """Stop loops, drain the slot pool, drain finalizers, close.

        Best-effort total: every I/O step contains and logs its own failure,
        so a ``stop()`` inside ``run_forever``'s ``finally`` cannot mask the
        original crash (cancellation exempt).
        """
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

        # Drain the pool BEFORE the finalizers: finalizers wait on task
        # outcomes, and this is what bounds them. Queued tasks are released at
        # once and requeued by their finalizers. A task that ignores shutdown
        # this long is hung and is killed, and crash recovery classifies its
        # row; without a bound, one hung task would make the worker unkillable
        # except by external SIGKILL.
        if self._slots is not None:
            report = await self._slots.stop(_DRAIN_TIMEOUT_S)
            if report.killed_task_ids:
                logger.error(
                    'Drain exceeded %.0fs (hung task?); killed tasks: %s',
                    _DRAIN_TIMEOUT_S,
                    list(report.killed_task_ids),
                )
            logger.info('Worker child processes stopped')

        # Finalizers persist task outcomes; with every outcome resolved they
        # only have DB writes left, so the drain timeout bounds DB work rather
        # than task runtime.
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

        Raises:
            Exception: import/validation failures are logged and re-raised —
                fail-fast at boot. Runs before ``start()`` creates any state,
                so there is nothing to clean up.
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
        """Main orchestrator loop — the worker's recovery seam.

        Claim-pass and notify-wait errors land here: transient connection
        errors are backoff-retried per the resilience config; anything else
        (including ChildStartFailedError and an exhausted backoff)
        re-raises so the process exits non-zero for a supervisor restart.
        A ChildStartFailedError that escaped a BACKGROUND task is
        captured by ``_on_done`` (which sets stop) and re-raised here after
        shutdown — same non-zero exit, no worker left without children.
        ``stop()`` always runs once the loop is entered; startup failures
        exit via ``_cleanup_after_failed_start`` instead.

        Raises:
            Exception: the non-retryable or retry-exhausted loop error, or
                the captured background ChildStartFailedError.
        """
        await self._start_with_resilience_config()
        if self._stop.is_set():
            # The start may have finished before the stop: children must not outlive it.
            await self.stop()
            self._raise_captured_fatal_error()
            return
        logger.info('Worker started')
        try:
            backoff = self._make_retry_backoff()
            while not self._stop.is_set():
                try:
                    # Single budgeted claim pass, then wait for new NOTIFY.
                    # A capacity release during a pass leaves the wake set,
                    # so the next wait returns at once.
                    self._capacity_released.clear()
                    await self._claim_and_dispatch_all()

                    # Wait for a NOTIFY from any queue or a local capacity
                    # release (coalesce bursts).
                    await self._wait_for_any_notify(
                        poll_interval_ms=self._resilience.notify_poll_interval_ms
                    )
                    self._capacity_released.clear()
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
        self._raise_captured_fatal_error()

    async def _wait_for_any_notify(self, poll_interval_ms: int) -> None:
        """Wait on any subscribed queue channel or a local capacity release;
        coalesce a burst.

        Returns at once when a release is already pending. Pending waiter
        tasks are cancelled and awaited on every exit path; an unexpected
        escape lands in ``run_forever``'s loop policy.
        """
        import contextlib

        queue_tasks = [asyncio.create_task(q.get()) for q in self._queues]
        stop_task = asyncio.create_task(self._stop.wait())
        capacity_task = asyncio.create_task(self._capacity_released.wait())
        all_tasks = queue_tasks + [stop_task, capacity_task]
        timeout_seconds = max(0.0, poll_interval_ms / 1000.0)
        try:
            done, pending = await asyncio.wait(
                all_tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=timeout_seconds,
            )
        except asyncio.CancelledError:
            # asyncio.wait does not cancel its awaitables on cancellation;
            # without this the waiter tasks linger until loop teardown.
            for t in all_tasks:
                t.cancel()
            raise

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
