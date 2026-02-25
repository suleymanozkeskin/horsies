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
from horsies.core.brokers.result_types import BrokerResult
from horsies.core.types.result import is_err
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
        """Clean shutdown of scheduler."""
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
        """Main scheduler loop."""
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
        """
        if not self.state_manager:
            raise RuntimeError('State manager not initialized')

        now = datetime.now(timezone.utc)
        configured_names = {schedule.name for schedule in self.schedule_config.schedules}

        # Remove stale DB state entries for schedules that no longer exist in config.
        try:
            existing_states = await self.state_manager.get_all_states()
            removed = [
                state.schedule_name
                for state in existing_states
                if state.schedule_name not in configured_names
            ]
            for name in removed:
                await self.state_manager.delete_state(name)
            if removed:
                logger.info(
                    f'Pruned {len(removed)} stale schedule_state row(s): {removed}'
                )
        except Exception as e:
            logger.warning(f'Failed to prune stale schedule state rows: {e}')

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
        """Check all schedules and execute those that are due."""
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
        """
        from horsies.core.codec.serde import dumps_json

        ser_result = dumps_json(
            {
                'pattern': schedule.pattern.model_dump(),
                'timezone': schedule.timezone,
            },
        )
        if is_err(ser_result):
            # model_dump() output is always JSON-safe; failure here is a bug
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
                    for due_time in due_runs:
                        result = await self._enqueue_scheduled_task(schedule)
                        if is_err(result):
                            err = result.err_value
                            level = 'transient' if err.retryable else 'permanent'
                            logger.error(
                                f"Schedule '{schedule.name}' catch-up failed at run "
                                f'{len(caught_up) + 1}/{len(due_runs)} ({level}): {err.message}',
                            )
                            break
                        last_task_id = result.ok_value
                        caught_up.append(due_time)
                        logger.info(
                            f"Schedule '{schedule.name}' catch-up: enqueued task {last_task_id} "
                            f'for scheduled run at {due_time}',
                        )

                    if caught_up:
                        # Save progress for runs that succeeded — prevents
                        # double-enqueue on the next tick.
                        last_slot = caught_up[-1]
                        next_run = calculate_next_run(
                            schedule.pattern, last_slot, schedule.timezone,
                        )
                        await self.state_manager.update_after_run(
                            schedule_name=schedule.name,
                            task_id=last_task_id,
                            executed_at=check_time,
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
                        # All failed — don't advance state, will retry next tick
                        await session.rollback()

                else:
                    # Normal execution: single run
                    result = await self._enqueue_scheduled_task(schedule)
                    if is_err(result):
                        err = result.err_value
                        if err.retryable:
                            logger.warning(
                                f"Schedule '{schedule.name}' enqueue failed (transient), "
                                f'will retry next tick: {err.message}',
                            )
                        else:
                            logger.error(
                                f"Schedule '{schedule.name}' enqueue failed (permanent): "
                                f'{err.message}',
                            )
                        await session.rollback()
                        return

                    task_id = result.ok_value
                    logger.info(
                        f"Schedule '{schedule.name}' executed: enqueued task {task_id} "
                        f"for task '{schedule.task_name}'",
                    )

                    next_run = calculate_next_run(
                        schedule.pattern, check_time, schedule.timezone,
                    )
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
        try:
            modules = self.app.get_discovered_task_modules()
        except Exception:
            modules = []

        if not modules:
            logger.warning(
                'No task modules discovered; scheduler may not resolve tasks'
            )
            return

        try:
            self.app.suppress_sends(True)
        except Exception:
            pass

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
            try:
                self.app.suppress_sends(False)
            except Exception:
                pass

    def _resolve_schedule_queue(self, schedule: TaskSchedule) -> str:
        """
        Determine the queue name for a schedule, preferring the schedule's explicit queue,
        otherwise falling back to the task's declared queue (stored on the task wrapper).
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
            self._validate_schedule_signature(sched)

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

    async def _enqueue_scheduled_task(
        self,
        schedule: TaskSchedule,
    ) -> BrokerResult[str]:
        """Enqueue a scheduled task via the broker.

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

        return await self.broker.enqueue_async(
            task_name=schedule.task_name,
            args=schedule.args,
            kwargs=schedule.kwargs,
            queue_name=validated_queue_name,
            priority=priority,
        )
