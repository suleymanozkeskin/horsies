# horsies/core/scheduler/service.py
from __future__ import annotations
import asyncio
import hashlib
import importlib
import os
import sys
from datetime import datetime, timezone
from typing import Optional
import inspect
from sqlalchemy import text
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.schedule import ScheduleConfig, TaskSchedule
from horsies.core.errors import ConfigurationError, RegistryError, ErrorCode
from horsies.core.scheduler.state import ScheduleStateManager
from horsies.core.scheduler.calculator import calculate_next_run, should_run_now
from horsies.core.logging import get_logger
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError, BrokerResult
from pydantic import ValidationError as _PydanticValidationError

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import encode_kwargs, underlying_task_fn
from horsies.core.codec.json_io import dumps_json
from horsies.core.utils.fingerprint import enqueue_fingerprint, schedule_slot_task_id
from horsies.core.types.result import Err, is_err
from horsies.core.worker.worker import import_by_path

logger = get_logger('scheduler')

SCHEDULE_ADVISORY_LOCK_SQL = text(
    """SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))"""
)


class Scheduler:
    """
    Main scheduler service for executing scheduled tasks.

    Responsibilities:
    1. Load schedule configuration from app
    2. Track schedule state in database
    3. Calculate next run times
    4. Enqueue tasks via broker when schedules are due
    5. Handle catch-up logic for missed runs

    Runs as a separate process/dyno from workers.
    """

    def __init__(self, app: Horsies):
        self.app = app
        self.broker: Optional[PostgresBroker] = None
        self.state_manager: Optional[ScheduleStateManager] = None
        self._stop = asyncio.Event()
        self._initialized = False

        # Validate that schedule config exists
        if not app.config.schedule:
            raise ConfigurationError(
                message='app config must have schedule configuration',
                code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=['schedule config is None or not provided in AppConfig'],
                help_text='add schedule=ScheduleConfig(...) to your AppConfig',
            )

        self.schedule_config: ScheduleConfig = app.config.schedule

        logger.info(
            f'Scheduler initialized with {len(self.schedule_config.schedules)} schedules, '
            f'check_interval={self.schedule_config.check_interval_seconds}s'
        )

    async def start(self) -> None:
        """Initialize broker, state manager, and database schema.

        Schema initialization retry is handled at the CLI orchestration level
        (see scheduler_command in cli.py), not here. This method assumes the
        schema is ready or will succeed on first attempt.

        Raises:
            RuntimeError: schema initialization failed (broker Err unwrapped).
            RegistryError | ConfigurationError: schedule validation failed.
                All propagate through run_forever to the CLI boundary —
                fail-fast at boot, exit 1; nothing has been enqueued yet.
        """
        if self._initialized:
            return

        self.broker = self.app.get_broker()
        init_result = await self.broker.ensure_schema_initialized()
        if is_err(init_result):
            err = init_result.err_value
            raise RuntimeError(
                f'Schema initialization failed: {err.message}',
            ) from err.exception
        logger.info('Broker initialized')

        # Import task modules so task registry is populated for scheduled names
        self._preload_task_modules()

        # Initialize state manager with broker's session factory
        self.state_manager = ScheduleStateManager(self.broker.session_factory)
        logger.info('State manager initialized')

        # Validate schedules eagerly (task exists, queue valid)
        self._validate_schedules()

        # Initialize state for all schedules
        await self._initialize_schedules()

        self._initialized = True
        logger.info('Scheduler started successfully')

    async def stop(self) -> None:
        """Clean shutdown of scheduler.

        Total: broker close returns a Result that is logged on Err, so a
        stop inside run_forever's finally cannot mask the original error.
        """
        self._stop.set()

        if self.broker:
            close_result = await self.broker.close_async()
            if is_err(close_result):
                logger.error(f'Broker close failed: {close_result.err_value.message}')
            else:
                logger.info('Broker closed')

        logger.info('Scheduler stopped')

    def request_stop(self) -> None:
        """Request scheduler to stop gracefully."""
        self._stop.set()

    async def run_forever(self) -> None:
        """Main scheduler loop — the scheduler's recovery seam.

        Per-tick errors (get_due_states, per-schedule escapes) are logged
        and the next tick retries; no error type changes the response, so
        no classification happens here. start() failures and anything
        escaping the tick handler propagate to the CLI boundary (exit 1).
        stop() always runs via finally once the loop is entered.

        Raises:
            Exception: startup failure or a bug escaping the tick handler.
        """
        logger.info('Starting scheduler loop')

        try:
            await self.start()

            while not self._stop.is_set():
                try:
                    await self._check_and_run_schedules()
                except Exception as e:
                    logger.error(f'Error in scheduler loop: {e}', exc_info=True)

                # Wait for check interval or stop signal
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self.schedule_config.check_interval_seconds,
                    )
                    break  # Stop signal received
                except asyncio.TimeoutError:
                    continue  # Continue to next iteration

        finally:
            await self.stop()

    async def _initialize_schedules(self) -> None:
        """
        Initialize state for all configured schedules.

        Detects configuration changes and recalculates next_run_at when pattern or timezone changes.

        Per-schedule isolation: one schedule's init/recalc failure is logged
        and the rest proceed (a failed-init schedule stays dormant until the
        next restart — see ScheduleStateManager.initialize_state).

        Raises:
            RuntimeError: state manager not initialized (programmer error;
                start() wires it before calling this).
        """
        if not self.state_manager:
            raise RuntimeError('State manager not initialized')

        now = datetime.now(timezone.utc)
        configured_names = {schedule.name for schedule in self.schedule_config.schedules}

        # State rows not in this scheduler's config are left untouched:
        # deleting them breaks rolling deploys (the still-running old-config
        # scheduler loses its rows and silently stops firing) and shared-DB
        # topologies (one app's scheduler wiping another app's schedules).
        # Orphan rows are inert — get_due_states filters to configured
        # names — so we only surface them for operators.
        try:
            existing_states = await self.state_manager.get_all_states()
            orphaned = [
                state.schedule_name
                for state in existing_states
                if state.schedule_name not in configured_names
            ]
            if orphaned:
                logger.warning(
                    f'{len(orphaned)} schedule_state row(s) not in this '
                    f"scheduler's config (other scheduler, or removed "
                    f'schedules): {orphaned}. Rows are kept; delete '
                    f'manually if the schedules are gone for good.'
                )
        except Exception as e:
            logger.warning(f'Failed to inspect schedule state rows: {e}')

        failed_schedules: list[str] = []
        for schedule in self.schedule_config.schedules:
            if not schedule.enabled:
                logger.debug(f'Skipping disabled schedule: {schedule.name}')
                continue

            try:
                await self._initialize_single_schedule(schedule, now)
            except Exception as e:
                logger.error(
                    f"Failed to initialize schedule '{schedule.name}': {e}",
                    exc_info=True,
                )
                failed_schedules.append(schedule.name)

        if failed_schedules:
            logger.error(
                f'{len(failed_schedules)} schedule(s) failed to initialize: '
                f'{failed_schedules}; remaining schedules will proceed',
            )

    async def _initialize_single_schedule(
        self, schedule: TaskSchedule, now: datetime,
    ) -> None:
        """Initialize or update state for a single schedule.

        Raises on failure so the caller can isolate per-schedule errors.
        """
        if not self.state_manager:
            raise RuntimeError('State manager not initialized')

        config_hash = self._compute_config_hash(schedule)
        state = await self.state_manager.get_state(schedule.name)

        if state is None:
            next_run = calculate_next_run(schedule.pattern, now, schedule.timezone)
            await self.state_manager.initialize_state(
                schedule.name, next_run, config_hash,
            )
            logger.info(
                f"Initialized new schedule '{schedule.name}', next_run={next_run}",
            )
        elif state.config_hash != config_hash:
            logger.warning(
                f"Schedule '{schedule.name}' configuration changed "
                f'(pattern or timezone), recalculating next_run_at',
            )
            next_run = calculate_next_run(
                schedule.pattern, now, schedule.timezone,
            )
            await self.state_manager.update_next_run(
                schedule.name, next_run, config_hash,
            )
            logger.info(
                f"Updated schedule '{schedule.name}' due to config change, "
                f'new next_run={next_run}',
            )
        else:
            logger.debug(
                f"Schedule '{schedule.name}' already initialized, "
                f'next_run={state.next_run_at}',
            )

    async def _check_and_run_schedules(self) -> None:
        """Check all schedules and execute those that are due.

        Per-schedule isolation: _check_schedule errors are logged and the
        remaining due schedules still run. The get_due_states read raises
        to run_forever's tick handler (retry next tick).

        Raises:
            RuntimeError: scheduler not initialized (programmer error).
            Exception: DB failure reading due states.
        """
        if not self.state_manager or not self.broker:
            raise RuntimeError('Scheduler not properly initialized')

        now = datetime.now(timezone.utc)

        enabled_schedules: dict[str, TaskSchedule] = {
            schedule.name: schedule
            for schedule in self.schedule_config.schedules
            if schedule.enabled
        }
        if not enabled_schedules:
            return

        # Bulk fetch due states to avoid O(N) per-tick queries
        due_states = await self.state_manager.get_due_states(
            list(enabled_schedules.keys()),
            now,
        )

        for state in due_states:
            schedule = enabled_schedules.get(state.schedule_name)
            if schedule is None:
                continue

            try:
                await self._check_schedule(schedule, now)
            except Exception as e:
                logger.error(
                    f"Error checking schedule '{schedule.name}': {e}", exc_info=True
                )

    def _schedule_advisory_key(self, schedule_name: str) -> int:
        """
        Compute a stable 64-bit advisory lock key for a schedule.

        Prevents concurrent schedulers from double-enqueuing the same schedule.
        """
        basis = f'horsies-schedule:{schedule_name}'.encode('utf-8')
        h = hashlib.sha256(basis).digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    def _compute_config_hash(self, schedule: TaskSchedule) -> str:
        """
        Compute hash of schedule configuration for change detection.

        Includes pattern and timezone to detect when schedule needs recalculation.

        Raises:
            RuntimeError: dumps_json rejected model_dump(mode='json') output
                — impossible for valid patterns, so this is a horsies bug
                surfacing loudly, not an operational error to retry.
        """
        from horsies.core.codec.json_io import dumps_json

        ser_result = dumps_json(
            {
                # mode='json' is required: time-based patterns carry a
                # datetime.time, which strict dumps_json rejects in python
                # mode. mode='json' emits it as an isoformat string.
                'pattern': schedule.pattern.model_dump(mode='json'),
                'timezone': schedule.timezone,
            },
        )
        if is_err(ser_result):
            # model_dump(mode='json') output is always JSON-safe; failure here is a bug
            raise RuntimeError(
                f"Failed to serialize config for schedule '{schedule.name}': {ser_result.err_value}",
            )
        return hashlib.sha256(ser_result.ok_value.encode('utf-8')).hexdigest()

    async def _check_schedule(
        self, schedule: TaskSchedule, check_time: datetime
    ) -> None:
        """
        Check a single schedule and run if due.

        Uses PostgreSQL advisory lock to prevent race conditions when multiple
        scheduler processes are running.

        Error handling: the execution body contains its own failures
        (rollback, slot stays due, next tick retries; enqueue errors are
        typed Results classified retryable/permanent). The pre-execution
        state reads raise to _check_and_run_schedules' per-schedule except;
        the session context unwinds, releasing the advisory lock.

        Raises:
            RuntimeError: scheduler not initialized (programmer error).
            Exception: DB failure on the advisory lock or state reads.
        """
        if not self.state_manager or not self.broker:
            raise RuntimeError('Scheduler not properly initialized')

        # Use advisory lock to prevent double-enqueue from concurrent schedulers
        lock_key = self._schedule_advisory_key(schedule.name)

        async with self.broker.session_factory() as session:
            # Acquire transaction-scoped advisory lock for this specific schedule
            await session.execute(
                SCHEDULE_ADVISORY_LOCK_SQL,
                {'key': lock_key},
            )

            # Now we have exclusive access to this schedule's execution
            # Get current state
            state = await self.state_manager.get_state(schedule.name, session=session)

            if state is None:
                # Schedule state missing (shouldn't happen after initialization)
                logger.warning(
                    f"Schedule state missing for '{schedule.name}', reinitializing"
                )
                config_hash = self._compute_config_hash(schedule)
                next_run = calculate_next_run(
                    schedule.pattern, check_time, schedule.timezone
                )
                await self.state_manager.initialize_state(
                    schedule.name,
                    next_run,
                    config_hash,
                    session=session,
                )
                await session.commit()
                return

            # Check if schedule should run now
            if not should_run_now(state.next_run_at, check_time):
                await session.commit()
                return

            # Execute schedule with catch-up logic
            try:
                # Calculate missed runs if catch_up_missed is enabled
                due_runs: list[datetime] = []
                if schedule.catch_up_missed and state.next_run_at:
                    due_runs = self._calculate_missed_runs(
                        schedule=schedule,
                        first_due_run=state.next_run_at,
                        current_time=check_time,
                        max_runs=schedule.max_catch_up_runs,
                    )

                if due_runs:
                    if len(due_runs) > 1:
                        logger.warning(
                            f"Schedule '{schedule.name}' has {len(due_runs)} due run(s), "
                            f'catching up with cap={schedule.max_catch_up_runs}',
                        )

                    last_task_id = ''
                    caught_up: list[datetime] = []
                    permanent_skip_slot: datetime | None = None
                    for due_time in due_runs:
                        result = await self._enqueue_scheduled_task(schedule, slot_time=due_time)
                        if is_err(result):
                            err = result.err_value
                            if err.retryable:
                                logger.error(
                                    f"Schedule '{schedule.name}' catch-up failed at run "
                                    f'{len(caught_up) + 1}/{len(due_runs)} (transient), '
                                    f'will retry next tick: {err.message}',
                                )
                            else:
                                # A permanent error for this slot can never
                                # succeed on retry; mark the slot consumed so
                                # the schedule does not wedge on it forever.
                                permanent_skip_slot = due_time
                                logger.error(
                                    f"Schedule '{schedule.name}' catch-up failed at run "
                                    f'{len(caught_up) + 1}/{len(due_runs)} (permanent), '
                                    f'skipping slot {due_time}: {err.message}',
                                )
                            break
                        last_task_id = result.ok_value
                        caught_up.append(due_time)
                        logger.info(
                            f"Schedule '{schedule.name}' catch-up: enqueued task {last_task_id} "
                            f'for scheduled run at {due_time}',
                        )

                    progress_slot = permanent_skip_slot or (
                        caught_up[-1] if caught_up else None
                    )
                    if progress_slot is not None:
                        # Save progress for enqueued runs (and a permanently
                        # failed slot, which is treated as consumed) —
                        # prevents double-enqueue and permanent-error wedges
                        # on the next tick.
                        next_run = calculate_next_run(
                            schedule.pattern, progress_slot, schedule.timezone,
                        )
                        if caught_up:
                            await self.state_manager.update_after_run(
                                schedule_name=schedule.name,
                                task_id=last_task_id,
                                executed_at=check_time,
                                next_run_at=next_run,
                                session=session,
                            )
                        else:
                            await self.state_manager.update_next_run(
                                schedule_name=schedule.name,
                                next_run_at=next_run,
                                session=session,
                            )
                        await session.commit()
                        if len(caught_up) < len(due_runs):
                            logger.warning(
                                f"Schedule '{schedule.name}' partially caught up: "
                                f'{len(caught_up)}/{len(due_runs)} runs enqueued',
                            )
                    else:
                        # All failed transiently — don't advance state, will
                        # retry next tick
                        await session.rollback()

                else:
                    # Normal execution (catch_up_missed=False): fire only the
                    # latest due slot. Older missed slots (scheduler downtime)
                    # are dropped, and the schedule resumes strictly in the
                    # future. slot_time stays slot-aligned for deterministic
                    # task_id. Fallback to check_time only if next_run_at is
                    # somehow None (should not happen since should_run_now
                    # already checked it).
                    first_due = (
                        state.next_run_at if state.next_run_at is not None else check_time
                    )
                    slot_time, next_run, skipped = self._advance_to_latest_due_slot(
                        schedule, first_due, check_time,
                    )
                    if skipped:
                        logger.warning(
                            f"Schedule '{schedule.name}' missed {skipped} run(s); "
                            f'skipping to latest due slot {slot_time} '
                            f'(catch_up_missed=False)',
                        )
                    if next_run <= check_time:
                        # Scan cap reached on a very deep backlog: persist
                        # progress without firing a stale slot; the next tick
                        # continues advancing from here.
                        await self.state_manager.update_next_run(
                            schedule_name=schedule.name,
                            next_run_at=next_run,
                            session=session,
                        )
                        await session.commit()
                        return

                    result = await self._enqueue_scheduled_task(schedule, slot_time=slot_time)
                    if is_err(result):
                        err = result.err_value
                        if err.retryable:
                            logger.warning(
                                f"Schedule '{schedule.name}' enqueue failed (transient), "
                                f'will retry next tick: {err.message}',
                            )
                            await session.rollback()
                            return
                        # A permanent error (e.g. PAYLOAD_MISMATCH after a
                        # deploy changed the schedule's kwargs) can never
                        # succeed on retry: advance past the doomed slot so
                        # the schedule does not retry it every tick forever.
                        logger.error(
                            f"Schedule '{schedule.name}' enqueue failed (permanent), "
                            f'skipping slot {slot_time}: {err.message}',
                        )
                        await self.state_manager.update_next_run(
                            schedule_name=schedule.name,
                            next_run_at=next_run,
                            session=session,
                        )
                        await session.commit()
                        return

                    task_id = result.ok_value
                    logger.info(
                        f"Schedule '{schedule.name}' executed: enqueued task {task_id} "
                        f"for task '{schedule.task_name}'",
                    )

                    # next_run (strictly future) was computed alongside the
                    # latest due slot above.
                    await self.state_manager.update_after_run(
                        schedule_name=schedule.name,
                        task_id=task_id,
                        executed_at=check_time,
                        next_run_at=next_run,
                        session=session,
                    )
                    await session.commit()

            except Exception as e:
                logger.error(
                    f"Failed to execute schedule '{schedule.name}': {e}", exc_info=True,
                )
                await session.rollback()

        # Advisory lock automatically released at transaction end

    # Bound the per-tick slot scan so a pathological backlog (tiny period,
    # very long downtime) cannot stall a tick; progress is persisted and the
    # next tick continues from where this one stopped.
    _MAX_SKIP_SCAN_PER_TICK = 100_000

    def _advance_to_latest_due_slot(
        self,
        schedule: TaskSchedule,
        first_due: datetime,
        check_time: datetime,
    ) -> tuple[datetime, datetime, int]:
        """Advance through due slots to the latest one (catch_up_missed=False).

        Args:
            schedule: TaskSchedule configuration
            first_due: First due scheduled run (<= check_time)
            check_time: Current time

        Returns:
            (latest_due_slot, next_run_after_slot, skipped_count).
            next_run_after_slot may still be <= check_time when the scan cap
            was reached; callers must not fire a slot in that case.
        """
        slot = first_due
        skipped = 0
        next_run = calculate_next_run(schedule.pattern, slot, schedule.timezone)
        while next_run <= check_time and skipped < self._MAX_SKIP_SCAN_PER_TICK:
            if next_run <= slot:
                logger.error(
                    "Non-monotonic next_run calculated for schedule '%s': "
                    'current=%s next=%s',
                    schedule.name,
                    slot,
                    next_run,
                )
                break
            slot = next_run
            skipped += 1
            next_run = calculate_next_run(schedule.pattern, slot, schedule.timezone)
        return slot, next_run, skipped

    def _calculate_missed_runs(
        self,
        schedule: TaskSchedule,
        first_due_run: datetime,
        current_time: datetime,
        max_runs: int,
    ) -> list[datetime]:
        """
        Calculate all due runs between first due run and current time (inclusive).

        Args:
            schedule: TaskSchedule configuration
            first_due_run: First due scheduled run
            current_time: Current time
            max_runs: Upper bound on runs returned in one pass

        Returns:
            List of due run times, sorted chronologically
        """
        due_runs: list[datetime] = []
        if first_due_run > current_time:
            return due_runs

        cursor = first_due_run
        while cursor <= current_time and len(due_runs) < max_runs:
            due_runs.append(cursor)
            next_run = calculate_next_run(schedule.pattern, cursor, schedule.timezone)
            if next_run <= cursor:
                logger.error(
                    "Non-monotonic next_run calculated for schedule '%s': current=%s next=%s",
                    schedule.name,
                    cursor,
                    next_run,
                )
                break
            cursor = next_run

        if cursor <= current_time and len(due_runs) >= max_runs:
            logger.warning(
                "Catch-up cap reached for schedule '%s' (cap=%s), backlog remains",
                schedule.name,
                max_runs,
            )

        return due_runs

    def _preload_task_modules(self) -> None:
        """Import discovered task modules so @app.task registrations run."""
        modules = self.app.get_discovered_task_modules()

        if not modules:
            logger.warning(
                'No task modules discovered; scheduler may not resolve tasks'
            )
            return

        # Save/restore rather than force-False: a caller that already
        # suppressed sends (e.g. app.check()) must keep its setting.
        prev_suppress = self.app._suppress_sends
        self.app.suppress_sends(True)
        try:
            for module in modules:
                if module.endswith('.py') or os.path.sep in module:
                    try:
                        import_by_path(os.path.abspath(module))
                    except Exception as e:
                        logger.warning(f"Failed to import task module '{module}': {e}")
                else:
                    # Ensure repo root is on sys.path for module imports
                    cwd = os.getcwd()
                    if cwd not in sys.path:
                        sys.path.insert(0, cwd)
                    try:
                        importlib.import_module(module)
                    except Exception as e:
                        logger.warning(f"Failed to import task module '{module}': {e}")
        finally:
            self.app.suppress_sends(prev_suppress)

    def _resolve_schedule_queue(self, schedule: TaskSchedule) -> str:
        """
        Determine the queue name for a schedule, preferring the schedule's explicit queue,
        otherwise falling back to the task's declared queue (stored on the task wrapper).

        Raises:
            ConfigurationError: queue not valid for this app config (from
                validate_queue_name). Startup validation wraps it per
                schedule; the enqueue path treats it as a programming
                error because _validate_schedules already proved it valid.
        """
        task = self.app.tasks.get(schedule.task_name)
        task_queue = getattr(task, 'task_queue_name', None) if task else None

        queue_name = (
            schedule.queue_name if schedule.queue_name is not None else task_queue
        )
        return self.app.validate_queue_name(queue_name)

    def _validate_schedules(self) -> None:
        """Fail fast on invalid schedules (missing tasks/queues)."""
        for sched in self.schedule_config.schedules:
            if not sched.enabled:
                continue
            if sched.task_name not in self.app.tasks:
                available = list(self.app.tasks.keys())
                raise RegistryError(
                    message=f"scheduled task '{sched.task_name}' not registered",
                    code=ErrorCode.TASK_NOT_REGISTERED,
                    notes=[
                        f"schedule '{sched.name}' references task '{sched.task_name}'",
                        f'available tasks: {available}'
                        if available
                        else 'no tasks registered',
                    ],
                    help_text='ensure the task is defined with @app.task and imported before scheduler starts',
                )
            try:
                self._resolve_schedule_queue(sched)
            except Exception as e:
                raise ConfigurationError(
                    message=f"invalid queue configuration for schedule '{sched.name}'",
                    code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                    notes=[f'underlying error: {e}'],
                    help_text='check queue_name in schedule matches app.config queue settings',
                ) from e
            if sched.args:
                # Enqueue rejects positional args unconditionally
                # (strict-serde has no typed wire representation for them);
                # without this check the schedule passes startup and then
                # fails permanently on every tick.
                raise ConfigurationError(
                    message=f"schedule '{sched.name}' carries positional args; schedules are kwargs-only",
                    code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                    notes=[f'{len(sched.args)} positional arg(s) found'],
                    help_text='move positional args to kwargs to satisfy the strict-serde wire contract',
                )
            self._validate_schedule_signature(sched)
            self._validate_schedule_serializable(sched)

    def _validate_schedule_signature(self, schedule: TaskSchedule) -> None:
        """Ensure schedule args/kwargs bind cleanly to the task signature."""
        task = self.app.tasks.get(schedule.task_name)
        original_fn = getattr(task, '_original_fn', None) if task else None
        if original_fn is None:
            return  # Cannot validate without original function signature

        sig = inspect.signature(original_fn)
        try:
            sig.bind(*schedule.args, **schedule.kwargs)
        except TypeError as e:
            raise ConfigurationError(
                message=f"schedule '{schedule.name}' args/kwargs do not match task '{schedule.task_name}' signature",
                code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=[f'signature bind error: {e}'],
                help_text='update TaskSchedule args/kwargs to match task function parameters',
            ) from e

    def _validate_schedule_serializable(self, schedule: TaskSchedule) -> None:
        """Dry-run the kwargs wire encoding at startup.

        ``sig.bind`` only checks parameter names/arity, not the strict-serde
        wire contract. Without this, a schedule whose kwarg targets an
        engine-injected parameter (``workflow_ctx``/``workflow_meta``), a
        TaskResult-typed parameter, or a value that cannot be JSON-serialized
        passes startup validation and then fails ``ENQUEUE_FAILED`` on every
        tick. Mirror the enqueue-time encode/serialize so the bad schedule is
        rejected once, at startup, instead of forever.
        """
        if not schedule.kwargs:
            return
        task = self.app.tasks.get(schedule.task_name)
        if task is None:
            return  # missing-task case already raised above
        task_fn = underlying_task_fn(task)
        try:
            encoded = encode_kwargs(task_fn, schedule.kwargs)
        except (StrictJsonError, _PydanticValidationError) as exc:
            raise ConfigurationError(
                message=f"schedule '{schedule.name}' kwargs are not encodable for task '{schedule.task_name}'",
                code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=[f'encode error: {exc}'],
                help_text='schedules are kwargs-only and must satisfy the task\'s typed signature; do not supply engine-injected (workflow_ctx/workflow_meta) or TaskResult-typed kwargs',
            ) from exc
        ser = dumps_json(encoded)
        if is_err(ser):
            raise ConfigurationError(
                message=f"schedule '{schedule.name}' kwargs are not JSON-serializable for task '{schedule.task_name}'",
                code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=[f'serialization error: {ser.err_value}'],
                help_text='ensure all scheduled kwargs serialize to JSON',
            )

    async def _enqueue_scheduled_task(
        self,
        schedule: TaskSchedule,
        *,
        slot_time: datetime,
    ) -> BrokerResult[str]:
        """Enqueue a scheduled task via the broker.

        Uses a deterministic task_id derived from the schedule name and slot_time.
        sent_at is set to slot_time (the logical schedule slot), not wall clock.
        This ensures crash-and-retry produces the same task_id + same SHA,
        making the INSERT idempotent via ON CONFLICT DO NOTHING.

        Returns Ok(task_id) on success, Err(BrokerOperationError) on broker failure.
        Raises ValueError/RuntimeError for programming errors (missing task, bad queue).
        """
        if not self.broker:
            raise RuntimeError('Broker not initialized')

        if schedule.task_name not in self.app.tasks:
            raise ValueError(
                f"Task '{schedule.task_name}' not registered. "
                f'Available tasks: {list(self.app.tasks.keys())}',
            )

        validated_queue_name = self._resolve_schedule_queue(schedule)

        from horsies.core.task_decorator import effective_priority

        priority = effective_priority(self.app, validated_queue_name)

        task_id = schedule_slot_task_id(schedule.name, slot_time)

        # Pre-serialize args/kwargs.
        # Strict-serde: positional args have no typed wire representation;
        # the scheduler rejects schedules carrying any. Schedules must
        # use kwargs-only (the same contract `.send()` enforces).
        args_json: str | None = None
        if schedule.args:
            return Err(BrokerOperationError(
                code=BrokerErrorCode.ENQUEUE_FAILED,
                message=(
                    f"Schedule {schedule.name!r} carries "
                    f"{len(schedule.args)} positional arg(s); strict-serde "
                    f"rejects positional args. Use kwargs-only."
                ),
                retryable=False,
            ))

        kwargs_json: str | None = None
        if schedule.kwargs:
            # Strict-serde phase 3 routes through encode_kwargs.
            task = self.app.tasks.get(schedule.task_name)
            task_fn = underlying_task_fn(task)
            try:
                encoded_kwargs = encode_kwargs(task_fn, schedule.kwargs)
            except (StrictJsonError, _PydanticValidationError) as exc:
                return Err(BrokerOperationError(
                    code=BrokerErrorCode.ENQUEUE_FAILED,
                    message=f"Failed to encode kwargs for schedule '{schedule.name}': {exc}",
                    retryable=False,
                    exception=exc,
                ))
            kwargs_r = dumps_json(encoded_kwargs)
            if is_err(kwargs_r):
                return Err(BrokerOperationError(
                    code=BrokerErrorCode.ENQUEUE_FAILED,
                    message=f"Failed to serialize encoded kwargs for schedule '{schedule.name}': {kwargs_r.err_value}",
                    retryable=False,
                    exception=kwargs_r.err_value if isinstance(kwargs_r.err_value, BaseException) else None,
                ))
            kwargs_json = kwargs_r.ok_value

        # Deterministic sent_at — same slot produces same SHA across ticks.
        sent_at = slot_time

        enqueue_sha = enqueue_fingerprint(
            task_name=schedule.task_name,
            queue_name=validated_queue_name,
            priority=priority,
            args_json=args_json,
            kwargs_json=kwargs_json,
            sent_at=sent_at,
            good_until=None,
            enqueue_delay_seconds=None,
            task_options=None,
        )

        return await self.broker.enqueue_async(
            task_name=schedule.task_name,
            queue_name=validated_queue_name,
            task_id=task_id,
            enqueue_sha=enqueue_sha,
            args_json=args_json,
            kwargs_json=kwargs_json,
            priority=priority,
            sent_at=sent_at,
        )
