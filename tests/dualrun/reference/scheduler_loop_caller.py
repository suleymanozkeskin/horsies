"""The Python scheduler loop, before the scheduler called the core.

``Scheduler`` (``python/horsies/core/scheduler/service.py``) ran the start, the
tick and the fire in Python: the state statements of
``ScheduleStateManager`` (``horsies.core.scheduler.state``), the slots and
next runs of ``calculate_next_run``, the config digest of
``_compute_config_hash`` and the fire through ``horsies_fire_schedule``.
``Scheduler`` below is that class as it was. The production ``Scheduler`` is
now a facade on ``horsies._core.CoreScheduler``; this class is the reference
of the dual-runs of the scheduler switch (``tests/dualrun/test_schedule_fire.py``)
and of the unit tests of the Python loop (``tests/unit/test_scheduler_service.py``).
"""

from __future__ import annotations
import asyncio
import hashlib
import importlib
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker, SendCommand
from horsies.core.models.retention import resolve_queue_retention_class
from horsies.core.models.schedule import ScheduleConfig, TaskSchedule
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.scheduler.state import (
    DueSchedule,
    FiredSlot,
    ScheduleAdvanced,
    ScheduleFire,
    ScheduleFireOutcomeUnknown,
    ScheduleFireRefused,
    ScheduleStateManager,
    ScheduleStateMissing,
    ScheduleStateMoved,
    ScheduleStateWrite,
    SlotOutcome,
)
from horsies.core.scheduler.validation import (
    PreparedSchedule,
    PreparedSchedules,
    prepare_schedules,
)
from horsies.core.scheduler.calculator import calculate_next_run
from horsies.core.logging import get_logger
from horsies.core.brokers.result_types import broker_error_cause
from horsies.core.types.error_detail import NoCause

from horsies.core.codec.payload_guard import warn_if_payload_large
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.worker.worker import import_by_path

logger = get_logger('scheduler')

# Wall-clock target between missing-row existence checks while every
# state row is present and initialized. The check guards rare conditions
# (startup init failure, external row deletion); running it every tick
# reads the whole schedule-state table each tick — 108 schedules at a
# 1 s tick is ~9M rows/day for an answer that changes only on those rare
# events. Denominated in seconds, not ticks, so the worst-case dormancy
# bound for an externally deleted row does not scale with
# check_interval_seconds; the tick count is derived once at init. While
# any row is missing or a re-init failed, the check reruns every tick
# until healthy.
_EXISTENCE_CHECK_INTERVAL_S = 60.0


def _log_start_writes(
    writes: list[ScheduleStateWrite],
    written: tuple[str, ...],
    stored: dict[str, bytes],
) -> None:
    """Log each row of the start write: created, moved to a changed pattern,
    or left to the row another scheduler wrote first."""
    for write in writes:
        match (write.schedule_name in written, write.schedule_name in stored):
            case (True, False):
                logger.info(
                    f"Initialized new schedule '{write.schedule_name}', "
                    f'next_run={write.next_run_at}',
                )
            case (True, True):
                logger.warning(
                    f"Schedule '{write.schedule_name}' configuration changed "
                    f'(pattern or timezone); new next_run={write.next_run_at}',
                )
            case (False, _):
                logger.debug(
                    f"Schedule '{write.schedule_name}' was written by another "
                    f'scheduler first',
                )


@dataclass(frozen=True, slots=True)
class FirePlan:
    """The slots one fire sends and where it moves the row. Computed from the due read.

    ``slots`` strictly increase from the stored ``next_run_at`` and are at or
    before the database time of the due read; empty when the latest-slot scan
    reached its bound (the fire then only moves the row). ``next_run_at`` is
    after the last slot. ``skip_to`` is where a refused fire moves the row: past
    the first slot (catch-up), or to ``next_run_at`` (latest slot).
    """

    slots: tuple[datetime, ...]
    next_run_at: datetime
    skip_to: datetime


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
        # Set by start(). The tick reads this text and does not encode.
        self._prepared_schedules: PreparedSchedules = {}
        # Set by start(): the send command of each enabled schedule, built
        # once from the prepared text. Every fire of a schedule sends it.
        self._commands: dict[str, SendCommand] = {}
        # 0 -> the next tick runs the missing-row existence check.
        self._ticks_until_existence_check = 0

        # Validate that schedule config exists
        if not app.config.schedule:
            raise ConfigurationError(
                message='app config must have schedule configuration',
                code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=['schedule config is None or not provided in AppConfig'],
                help_text='add schedule=ScheduleConfig(...) to your AppConfig',
            )

        self.schedule_config: ScheduleConfig = app.config.schedule

        # Derived once: run the existence check every this-many ticks so
        # its wall-clock cadence stays ~_EXISTENCE_CHECK_INTERVAL_S
        # regardless of check_interval_seconds.
        self._existence_check_interval_ticks = max(
            1,
            round(
                _EXISTENCE_CHECK_INTERVAL_S
                / self.schedule_config.check_interval_seconds
            ),
        )

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
            RuntimeError: the broker already serves its limit of open event
                loops and this is a new one, or schema initialization failed
                (broker Err unwrapped).
            HorsiesError: broker construction failed (``get_broker``).
            RegistryError | ConfigurationError: schedule validation failed,
                or the broker refused a schedule's send command.
                All propagate through run_forever to the CLI boundary —
                fail-fast at boot, exit 1; nothing has been enqueued yet.
        """
        if self._initialized:
            return

        self.broker = self.app.get_broker()
        match await self.broker.select_resources():
            case Err(loop_limit):
                raise RuntimeError(
                    f'Scheduler start failed: {loop_limit.describe()}',
                )
            case Ok(resources):
                pass
        init_result = await self.broker.ensure_schema_initialized()
        if is_err(init_result):
            err = init_result.err_value
            match broker_error_cause(err):
                case NoCause():
                    raise RuntimeError(
                        f'Schema initialization failed: {err.message}',
                    )
                case cause:
                    raise RuntimeError(
                        f'Schema initialization failed: {err.message}',
                    ) from cause
        logger.info('Broker initialized')

        # Import task modules so task registry is populated for scheduled names
        self._preload_task_modules()

        # The state manager keeps the session factory selected above.
        self.state_manager = ScheduleStateManager(resources.session_factory)
        logger.info('State manager initialized')

        # Validate schedules eagerly (task exists, queue valid), then build
        # the send command of each one.
        self._validate_schedules()
        self._build_commands(self.broker)

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

        Per-tick errors (the due read, the heal, per-schedule escapes) are logged
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
        Store the state row of each enabled schedule that is new or whose
        pattern or timezone changed, in two statements.

        1. One read of every row's name and digest, with the database time.
        2. One write of the new and changed rows, each with its first run
           computed from that database time. A changed row keeps
           run_count, last_run_at and last_task_id.

        Rows of names that are not in this config are kept and logged.

        Errors: a schedule whose first run cannot be computed is left out
        and logged, and the others are stored. A database error of either
        statement is logged and start goes on; the tick loop's existence
        heal (`_ensure_states_exist`) then creates the missing rows, and a
        changed digest waits for the next start.

        Raises:
            RuntimeError: state manager not initialized (programmer error;
                start() wires it before calling this).
        """
        if not self.state_manager:
            raise RuntimeError('State manager not initialized')

        configured_names = {schedule.name for schedule in self.schedule_config.schedules}
        try:
            read = await self.state_manager.read_start_states()
        except Exception as e:
            logger.error(
                f'Failed to read schedule state rows at start: {e}; the tick '
                f'loop creates missing rows',
                exc_info=True,
            )
            return

        # State rows not in this scheduler's config are left untouched:
        # deleting them breaks rolling deploys (the still-running old-config
        # scheduler loses its rows and silently stops firing) and shared-DB
        # topologies (one app's scheduler wiping another app's schedules).
        # Orphan rows are inert — the due read filters to configured
        # names — so we only surface them for operators.
        stored = {row.schedule_name: row.config_hash for row in read.rows}
        orphaned = sorted(stored.keys() - configured_names)
        if orphaned:
            logger.warning(
                f'{len(orphaned)} schedule_state row(s) not in this '
                f"scheduler's config (other scheduler, or removed "
                f'schedules): {orphaned}. Rows are kept; delete '
                f'manually if the schedules are gone for good.'
            )

        writes = self._start_writes(stored, read.database_now)
        if not writes:
            return
        try:
            written = await self.state_manager.store_start_states(writes)
        except Exception as e:
            logger.error(
                f'Failed to store schedule state rows at start: {e}; the tick '
                f'loop creates missing rows',
                exc_info=True,
            )
            return
        _log_start_writes(writes, written, stored)

    def _start_writes(
        self,
        stored: dict[str, bytes],
        database_now: datetime,
    ) -> list[ScheduleStateWrite]:
        """The start write's rows: each enabled schedule whose stored digest
        is missing or differs, with its first run after ``database_now``.

        A schedule whose first run cannot be computed is left out and
        logged. Does no I/O.
        """
        writes: list[ScheduleStateWrite] = []
        failed_schedules: list[str] = []
        for schedule in self.schedule_config.schedules:
            if not schedule.enabled:
                logger.debug(f'Skipping disabled schedule: {schedule.name}')
                continue
            config_hash = self._compute_config_hash(schedule)
            if stored.get(schedule.name) == config_hash:
                logger.debug(f"Schedule '{schedule.name}' already initialized")
                continue
            try:
                next_run = calculate_next_run(
                    schedule.pattern, database_now, schedule.timezone,
                )
            except (ValueError, RuntimeError) as e:
                logger.error(
                    f"Failed to initialize schedule '{schedule.name}': {e}",
                    exc_info=True,
                )
                failed_schedules.append(schedule.name)
                continue
            writes.append(
                ScheduleStateWrite(
                    schedule_name=schedule.name,
                    next_run_at=next_run,
                    config_hash=config_hash,
                ),
            )
        if failed_schedules:
            logger.error(
                f'{len(failed_schedules)} schedule(s) failed to initialize: '
                f'{failed_schedules}; remaining schedules will proceed',
            )
        return writes

    async def _check_and_run_schedules(self) -> None:
        """Check all schedules and execute those that are due.

        The due read returns the database time with the due rows. That
        time is the tick's only clock: the existence heal computes first
        runs from it, and each due schedule computes its slots from it.

        Per-schedule isolation: _check_schedule errors are logged and the
        remaining due schedules still run. The due read and the heal raise
        to run_forever's tick handler (retry next tick); a raise in the
        heal leaves its countdown at zero, so the retried tick re-runs it.
        The heal runs on a ~_EXISTENCE_CHECK_INTERVAL_S wall-clock cadence
        while healthy (every tick while unhealthy), not per tick; the tick
        count is derived at init from check_interval_seconds.

        Raises:
            RuntimeError: scheduler not initialized (programmer error).
            Exception: DB failure of the due read or the heal.
        """
        if not self.state_manager or not self.broker:
            raise RuntimeError('Scheduler not properly initialized')

        enabled_schedules: dict[str, TaskSchedule] = {
            schedule.name: schedule
            for schedule in self.schedule_config.schedules
            if schedule.enabled
        }
        if not enabled_schedules:
            return

        due_read = await self.state_manager.read_due_schedules(
            list(enabled_schedules.keys()),
        )
        database_now = due_read.database_now

        if self._ticks_until_existence_check <= 0:
            healthy = await self._ensure_states_exist(enabled_schedules, database_now)
            self._ticks_until_existence_check = (
                self._existence_check_interval_ticks if healthy else 1
            )
        self._ticks_until_existence_check -= 1

        for due in due_read.due:
            schedule = enabled_schedules.get(due.schedule_name)
            if schedule is None:
                continue

            try:
                await self._check_schedule(schedule, due, database_now)
            except Exception as e:
                logger.error(
                    f"Error checking schedule '{schedule.name}': {e}", exc_info=True
                )

    async def _ensure_states_exist(
        self,
        enabled_schedules: dict[str, TaskSchedule],
        database_now: datetime,
    ) -> bool:
        """Create the state rows missing for enabled schedules (self-heal).

        Closes the startup-init liveness gap: a schedule whose state-row
        creation failed during start() — or whose row was deleted
        externally — was invisible to the due read and stayed dormant
        until a restart. One statement: ``INSERT ... ON CONFLICT DO
        NOTHING``, with no read before it. A row that exists is not
        written; a concurrent scheduler's row stands.

        ``database_now`` is the database time of this tick's due read; each
        missing row gets the first run after it.

        Runs on a cadence, not every tick: while healthy the caller
        re-checks roughly every ``_EXISTENCE_CHECK_INTERVAL_S`` seconds
        (tick count derived at init from ``check_interval_seconds``); an
        unhealthy pass makes the caller re-check every tick until a fully
        healthy pass.

        Returns:
            True when the first run of every enabled schedule was computed
            and the statement ran. A schedule whose first run cannot be
            computed is left out and logged, and the pass is unhealthy.

        Raises:
            RuntimeError: state manager not initialized (programmer error).
            Exception: DB failure of the heal statement — raises to
                run_forever's tick handler (retry next tick).
        """
        if not self.state_manager:
            raise RuntimeError('State manager not initialized')

        writes: list[ScheduleStateWrite] = []
        healthy = True
        for name in sorted(enabled_schedules):
            schedule = enabled_schedules[name]
            try:
                next_run = calculate_next_run(
                    schedule.pattern, database_now, schedule.timezone,
                )
            except (ValueError, RuntimeError) as e:
                healthy = False
                logger.error(
                    f"Failed to compute the first run of schedule '{name}' "
                    f'(will retry next tick): {e}',
                    exc_info=True,
                )
                continue
            writes.append(
                ScheduleStateWrite(
                    schedule_name=name,
                    next_run_at=next_run,
                    config_hash=self._compute_config_hash(schedule),
                ),
            )

        healed = await self.state_manager.heal_missing_states(writes)
        for name in healed:
            logger.warning(
                f"Schedule '{name}' had no state row (failed startup init "
                f'or external delete); created it',
            )
        return healthy

    def _compute_config_hash(self, schedule: TaskSchedule) -> bytes:
        """
        Compute hash of schedule configuration for change detection.

        Includes pattern and timezone to detect when schedule needs recalculation.
        Returns the 32-byte SHA-256 digest that the bytea column stores.

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
        return hashlib.sha256(ser_result.ok_value.encode('utf-8')).digest()

    async def _check_schedule(
        self,
        schedule: TaskSchedule,
        due: DueSchedule,
        database_now: datetime,
    ) -> None:
        """Fire the due slots of one schedule in one statement.

        ``due`` is the schedule's row in this tick's due read and
        ``database_now`` the database time of that read. The slots and the
        next run come from them (``_fire_plan``); ``horsies_fire_schedule``
        then inserts the slot tasks and moves the row in one commit, under a
        lock of the row that serializes concurrent schedulers. No advisory
        lock and no second connection.

        Outcomes:
            - Advanced: the slots are logged by outcome.
            - State missing (the row was deleted after the due read): the heal
              creates it with the first run after ``database_now``.
            - State moved (another scheduler fired first): nothing to do.
            - Outcome unknown (a connection failure): logged; the next tick's
              due read shows whether the row moved.
            - Refused (any other database error, nothing committed): logged;
              a second fire with no slots moves the row to ``skip_to`` and
              records no run, so a refusal that repeats does not stop the
              schedule.

        Raises:
            RuntimeError: scheduler not initialized (programmer error).
            Exception: a calculator failure, a failure of the heal or of the
                second fire, or an argument the fire function refuses as a
                caller defect (SQLSTATE 22023, 22004); the per-schedule seam
                of ``_check_and_run_schedules`` logs it and the next tick
                retries.
        """
        if not self.state_manager or not self.broker:
            raise RuntimeError('Scheduler not properly initialized')

        plan = self._fire_plan(schedule, due.next_run_at, database_now)
        command = self._command(schedule)
        fire = ScheduleFire(
            schedule_name=schedule.name,
            expected_next_run_at=due.next_run_at,
            next_run_at=plan.next_run_at,
            slot_ats=plan.slots,
            command=command,
        )
        match await self.state_manager.fire_schedule(fire):
            case Ok(answer):
                pass
            case Err(failed):
                match failed:
                    case ScheduleFireOutcomeUnknown(cause=cause):
                        logger.warning(
                            f"Schedule '{schedule.name}' fire failed (transient), will "
                            f'retry next tick: {cause}',
                        )
                    case ScheduleFireRefused(cause=cause):
                        logger.error(
                            f"Schedule '{schedule.name}' fire refused (permanent), "
                            f'moving to {plan.skip_to} with no run recorded: {cause}',
                        )
                        await self._move_past_refused(schedule, due, plan, command)
                return
        match answer:
            case ScheduleAdvanced(slots=slots):
                self._log_fired(schedule, slots)
            case ScheduleStateMissing():
                await self._heal_one(schedule, database_now)
            case ScheduleStateMoved():
                logger.debug(
                    f"Schedule '{schedule.name}' was moved by another scheduler; "
                    f'nothing fired',
                )

    def _fire_plan(
        self,
        schedule: TaskSchedule,
        first_due: datetime,
        database_now: datetime,
    ) -> FirePlan:
        """The slots of one fire and the next run after them. Does no I/O.

        ``catch_up_missed``: the due slots in order, at most
        ``max_catch_up_runs``; the rest fire on later ticks. Else the latest
        due slot only: older missed slots are dropped and the schedule
        resumes in the future. When that scan reaches its bound, no slot
        fires and the row moves to where the scan stopped.

        Raises:
            ValueError | RuntimeError: ``calculate_next_run`` failed.
        """
        assert first_due <= database_now, (
            f"schedule '{schedule.name}' is not due: next run {first_due}, "
            f'database time {database_now}'
        )
        if schedule.catch_up_missed:
            due_runs = self._calculate_missed_runs(
                schedule=schedule,
                first_due_run=first_due,
                current_time=database_now,
                max_runs=schedule.max_catch_up_runs,
            )
            assert due_runs, f"schedule '{schedule.name}' is due and has no slot"
            if len(due_runs) > 1:
                logger.warning(
                    f"Schedule '{schedule.name}' has {len(due_runs)} due run(s), "
                    f'catching up with cap={schedule.max_catch_up_runs}',
                )
            return FirePlan(
                slots=tuple(due_runs),
                next_run_at=calculate_next_run(
                    schedule.pattern, due_runs[-1], schedule.timezone,
                ),
                skip_to=calculate_next_run(
                    schedule.pattern, due_runs[0], schedule.timezone,
                ),
            )
        slot_time, next_run, skipped = self._advance_to_latest_due_slot(
            schedule, first_due, database_now,
        )
        if skipped:
            logger.warning(
                f"Schedule '{schedule.name}' missed {skipped} run(s); "
                f'skipping to latest due slot {slot_time} '
                f'(catch_up_missed=False)',
            )
        if next_run <= database_now:
            # Scan bound reached on a very deep backlog: move the row
            # without firing a stale slot; the next tick continues from here.
            return FirePlan(slots=(), next_run_at=next_run, skip_to=next_run)
        return FirePlan(slots=(slot_time,), next_run_at=next_run, skip_to=next_run)

    def _log_fired(self, schedule: TaskSchedule, slots: tuple[FiredSlot, ...]) -> None:
        """Log each slot of an advanced fire by its outcome."""
        for slot in slots:
            match slot.outcome:
                case SlotOutcome.INSERTED:
                    logger.info(
                        f"Schedule '{schedule.name}' executed: enqueued task "
                        f"{slot.task_id} for task '{schedule.task_name}' "
                        f'(scheduled run at {slot.slot_at})',
                    )
                case SlotOutcome.SAME_COMMAND:
                    logger.info(
                        f"Schedule '{schedule.name}' slot {slot.slot_at} already "
                        f'has its task {slot.task_id}; recorded as fired',
                    )
                case SlotOutcome.PAYLOAD_MISMATCH:
                    logger.warning(
                        f"Schedule '{schedule.name}' slot {slot.slot_at} already has "
                        f'task {slot.task_id} with a different command (an earlier '
                        f"horsies version, or a change to the schedule's kwargs or "
                        f"the task's options); the slot counts as fired and is not "
                        f'enqueued again',
                    )
                case SlotOutcome.CONFLICT_UNVERIFIABLE:
                    logger.error(
                        f"Schedule '{schedule.name}' slot {slot.slot_at}: task "
                        f'{slot.task_id} finished while the fire read it; the slot '
                        f'is not enqueued again and not recorded as a run',
                    )

    async def _heal_one(self, schedule: TaskSchedule, database_now: datetime) -> None:
        """Create the state row of a schedule whose row the fire did not find.

        Raises:
            RuntimeError: state manager not initialized (programmer error).
            Exception: the heal statement failed (the per-schedule seam logs it).
        """
        if not self.state_manager:
            raise RuntimeError('State manager not initialized')
        next_run = calculate_next_run(schedule.pattern, database_now, schedule.timezone)
        healed = await self.state_manager.heal_missing_states(
            [
                ScheduleStateWrite(
                    schedule_name=schedule.name,
                    next_run_at=next_run,
                    config_hash=self._compute_config_hash(schedule),
                ),
            ],
        )
        logger.warning(
            f"Schedule state missing for '{schedule.name}' at its fire; "
            f'{"created it" if healed else "another scheduler created it"}, '
            f'next_run={next_run}',
        )

    async def _move_past_refused(
        self,
        schedule: TaskSchedule,
        due: DueSchedule,
        plan: FirePlan,
        command: SendCommand,
    ) -> None:
        """Fire with no slots: move the row to ``plan.skip_to`` and record no run.

        Raises:
            RuntimeError: state manager not initialized (programmer error).
        """
        if not self.state_manager:
            raise RuntimeError('State manager not initialized')
        skip = ScheduleFire(
            schedule_name=schedule.name,
            expected_next_run_at=due.next_run_at,
            next_run_at=plan.skip_to,
            slot_ats=(),
            command=command,
        )
        match await self.state_manager.fire_schedule(skip):
            case Ok(answer):
                match answer:
                    case ScheduleAdvanced():
                        logger.warning(
                            f"Schedule '{schedule.name}' moved past the refused slot to "
                            f'{plan.skip_to}',
                        )
                    case ScheduleStateMissing() | ScheduleStateMoved():
                        logger.debug(
                            f"Schedule '{schedule.name}' row is gone or moved; nothing to skip",
                        )
            case Err(failed):
                logger.error(
                    f"Schedule '{schedule.name}' could not move past the refused "
                    f'slot, will retry next tick: {failed.cause}',
                )

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
        prev_suppress = self.app._suppress_sends  # pyright: ignore[reportPrivateUsage]
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
        task_queue = task.task_queue_name if task is not None else None

        queue_name = (
            schedule.queue_name if schedule.queue_name is not None else task_queue
        )
        return self.app.validate_queue_name(queue_name)

    def _validate_schedules(self) -> None:
        """Fail fast on the first invalid schedule and keep the prepared text.

        Shares :func:`prepare_schedules` with ``Horsies.check()``; ``errors[0]``
        is the first bad schedule in iteration order, preserving the original
        raise-on-first behaviour. ``check`` collects all of them.
        """
        match prepare_schedules(self.app, self.schedule_config):
            case Ok(prepared_schedules):
                self._prepared_schedules = prepared_schedules
            case Err(errors):
                raise errors[0]

    def _prepared_schedule(self, schedule: TaskSchedule) -> PreparedSchedule:
        """Return the wire text that ``start()`` prepared for this schedule.

        Logs the payload size warning when the kwargs text is over
        ``payload.warn_bytes``. Raises RuntimeError when ``start()`` did not
        run: a programming error.
        """
        if schedule.name not in self._prepared_schedules:
            raise RuntimeError(
                f"Schedule '{schedule.name}' has no prepared wire text: start() did not run",
            )
        prepared = self._prepared_schedules[schedule.name]
        if prepared.kwargs_json is not None:
            warn_if_payload_large(
                self.app.config.payload,
                task_name=schedule.task_name,
                kind='kwargs',
                payload_text=prepared.kwargs_json,
            )
        return prepared

    def _build_commands(self, broker: PostgresBroker) -> None:
        """Build the send command of each enabled schedule once, from its prepared text.

        The queue is the schedule's queue or the task's, the priority that
        queue's, and the retention class that queue's mapping in this
        process's configuration. Does no I/O.

        Raises:
            ConfigurationError: the broker refused a schedule's command
                (``CONFIG_INVALID_SCHEDULE``, naming the schedule and the
                refusal). ``prepare_schedules`` already checked the text, so
                this is a value that only the broker's check refuses.
        """
        from horsies.core.task_decorator import effective_priority

        commands: dict[str, SendCommand] = {}
        for schedule in self.schedule_config.schedules:
            prepared = self._prepared_schedules.get(schedule.name)
            if prepared is None:
                continue
            queue_name = self._resolve_schedule_queue(schedule)
            match broker.prepare_send_command(
                task_name=schedule.task_name,
                queue_name=queue_name,
                priority=effective_priority(self.app, queue_name),
                kwargs_json=prepared.kwargs_json,
                task_options=prepared.task_options_json,
                retention_class_key=resolve_queue_retention_class(
                    getattr(self.app.config, 'retention', None),
                    queue_name,
                ),
            ):
                case Ok(command):
                    commands[schedule.name] = command
                case Err(refused):
                    raise ConfigurationError(
                        message=f"schedule '{schedule.name}' cannot be sent",
                        code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                        notes=[refused.message],
                        help_text=(
                            "fix the schedule's kwargs or the task's options"
                        ),
                    )
        self._commands = commands

    def _command(self, schedule: TaskSchedule) -> SendCommand:
        """The send command that ``start()`` built for this schedule.

        Logs the payload size warning when the kwargs text is over
        ``payload.warn_bytes``. Raises RuntimeError when ``start()`` did not
        run: a programming error.
        """
        self._prepared_schedule(schedule)
        command = self._commands.get(schedule.name)
        if command is None:
            raise RuntimeError(
                f"Schedule '{schedule.name}' has no send command: start() did not run",
            )
        return command
