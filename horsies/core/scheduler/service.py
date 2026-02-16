# horsies/core/scheduler/service.py
from __future__ import annotations
import asyncio
import hashlib
import importlib
import importlib.util
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
        """Initialize broker, state manager, and database schema."""
        if self._initialized:
            return

        # Initialize broker for task enqueuing
        self.broker = self.app.get_broker()
        await self.broker.ensure_schema_initialized()
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
            await self.broker.close_async()
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

        for schedule in self.schedule_config.schedules:
            if not schedule.enabled:
                logger.debug(f'Skipping disabled schedule: {schedule.name}')
                continue

            # Compute current config hash
            config_hash = self._compute_config_hash(schedule)

            # Check if state already exists
            state = await self.state_manager.get_state(schedule.name)

            if state is None:
                # Initialize new schedule
                next_run = calculate_next_run(schedule.pattern, now, schedule.timezone)
                await self.state_manager.initialize_state(
                    schedule.name, next_run, config_hash
                )
                logger.info(
                    f"Initialized new schedule '{schedule.name}', next_run={next_run}"
                )
            else:
                # Check if config has changed
                if state.config_hash != config_hash:
                    logger.warning(
                        f"Schedule '{schedule.name}' configuration changed "
                        f'(pattern or timezone), recalculating next_run_at'
                    )
                    # Recalculate next_run_at from current time
                    next_run = calculate_next_run(
                        schedule.pattern, now, schedule.timezone
                    )
                    await self.state_manager.update_next_run(
                        schedule.name, next_run, config_hash
                    )
                    logger.info(
                        f"Updated schedule '{schedule.name}' due to config change, "
                        f'new next_run={next_run}'
                    )
                else:
                    logger.debug(
                        f"Schedule '{schedule.name}' already initialized, "
                        f'next_run={state.next_run_at}'
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

        config_str = dumps_json(
            {
                'pattern': schedule.pattern.model_dump(),
                'timezone': schedule.timezone,
            }
        )
        return hashlib.sha256(config_str.encode('utf-8')).hexdigest()

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
            state = await self.state_manager.get_state(schedule.name)

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
                    schedule.name, next_run, config_hash
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
                missed_runs: list[datetime] = []
                if schedule.catch_up_missed and state.next_run_at:
                    missed_runs = self._calculate_missed_runs(
                        schedule=schedule,
                        last_scheduled_run=state.next_run_at,
                        current_time=check_time,
                    )

                if missed_runs:
                    logger.warning(
                        f"Schedule '{schedule.name}' missed {len(missed_runs)} run(s), "
                        f'catching up...'
                    )
                    # Enqueue all missed runs
                    last_task_id = ''
                    for missed_time in missed_runs:
                        last_task_id = await self._enqueue_scheduled_task(schedule)
                        logger.info(
                            f"Schedule '{schedule.name}' catch-up: enqueued task {last_task_id} "
                            f'for missed run at {missed_time}'
                        )

                    # After catch-up, calculate next run from the last scheduled slot to preserve cadence
                    last_slot = missed_runs[-1]
                    next_run = calculate_next_run(
                        schedule.pattern, last_slot, schedule.timezone
                    )

                    # Update state with last caught-up task
                    await self.state_manager.update_after_run(
                        schedule_name=schedule.name,
                        task_id=last_task_id,
                        executed_at=check_time,
                        next_run_at=next_run,
                    )
                else:
                    # Normal execution: single run
                    task_id = await self._enqueue_scheduled_task(schedule)
                    logger.info(
                        f"Schedule '{schedule.name}' executed: enqueued task {task_id} "
                        f"for task '{schedule.task_name}'"
                    )

                    # Calculate next run time
                    next_run = calculate_next_run(
                        schedule.pattern, check_time, schedule.timezone
                    )

                    # Update state
                    await self.state_manager.update_after_run(
                        schedule_name=schedule.name,
                        task_id=task_id,
                        executed_at=check_time,
                        next_run_at=next_run,
                    )

                await session.commit()

            except Exception as e:
                logger.error(
                    f"Failed to execute schedule '{schedule.name}': {e}", exc_info=True
                )
                await session.rollback()
                # Don't update state on failure - will retry next check

        # Advisory lock automatically released at transaction end

    def _calculate_missed_runs(
        self,
        schedule: TaskSchedule,
        last_scheduled_run: datetime,
        current_time: datetime,
    ) -> list[datetime]:
        """
        Calculate all missed runs between last scheduled run and current time.

        Args:
            schedule: TaskSchedule configuration
            last_scheduled_run: The last scheduled run time (may not have executed)
            current_time: Current time

        Returns:
            List of missed run times, sorted chronologically
        """
        missed: list[datetime] = []
        threshold = self.schedule_config.check_interval_seconds * 2

        # Only catch up if significantly in the past
        if (current_time - last_scheduled_run).total_seconds() <= threshold:
            return missed

        # Calculate all runs between last_scheduled_run and current_time
        cursor = last_scheduled_run
        while True:
            # Calculate next run from cursor
            next_run = calculate_next_run(schedule.pattern, cursor, schedule.timezone)

            # Stop if next run is in the future
            if next_run > current_time:
                break

            missed.append(next_run)
            cursor = next_run

        return missed

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
        """Ensure required task parameters are satisfied by schedule args/kwargs."""
        task = self.app.tasks.get(schedule.task_name)
        original_fn = getattr(task, '_original_fn', None) if task else None
        if original_fn is None:
            return  # Cannot validate without original function signature

        sig = inspect.signature(original_fn)
        args_provided = list(schedule.args)
        kwargs_provided = schedule.kwargs
        missing: list[str] = []

        consumed_positional = 0
        for param in sig.parameters.values():
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue  # skip *args/**kwargs
            if param.default is not inspect.Parameter.empty:
                continue  # optional

            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                if consumed_positional < len(args_provided):
                    consumed_positional += 1
                    continue
                if param.name in kwargs_provided:
                    continue
                missing.append(param.name)
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                if param.name in kwargs_provided:
                    continue
                missing.append(param.name)

        if missing:
            raise ConfigurationError(
                message=f"schedule '{schedule.name}' is missing required params for task '{schedule.task_name}'",
                code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=[f'missing required parameters: {missing}'],
                help_text='add missing params to args=(...) or kwargs={{...}} in TaskSchedule',
            )

    async def _enqueue_scheduled_task(self, schedule: TaskSchedule) -> str:
        """
        Enqueue a scheduled task via the broker.

        Args:
            schedule: TaskSchedule configuration

        Returns:
            Task ID of enqueued task

        Raises:
            Exception: If task enqueuing fails
        """
        if not self.broker:
            raise RuntimeError('Broker not initialized')

        # Validate that task is registered
        if schedule.task_name not in self.app.tasks:
            raise ValueError(
                f"Task '{schedule.task_name}' not registered. "
                f'Available tasks: {list(self.app.tasks.keys())}'
            )

        # Validate queue_name against app configuration
        try:
            validated_queue_name = self._resolve_schedule_queue(schedule)
        except ValueError as e:
            raise ValueError(
                f"Invalid queue configuration for schedule '{schedule.name}': {e}"
            )

        # Determine priority for the queue
        from horsies.core.task_decorator import effective_priority

        priority = effective_priority(self.app, validated_queue_name)

        # Enqueue task via broker
        task_id = await self.broker.enqueue_async(
            task_name=schedule.task_name,
            args=schedule.args,
            kwargs=schedule.kwargs,
            queue_name=validated_queue_name,
            priority=priority,
        )

        return task_id
