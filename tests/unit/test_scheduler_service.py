"""Tests for Scheduler service-level methods (mock-based, no DB)."""

from __future__ import annotations

from datetime import datetime, time, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.errors import ConfigurationError, ErrorCode, RegistryError
from horsies.core.models.schedule import (
    DailySchedule,
    IntervalSchedule,
    MonthlySchedule,
    ScheduleConfig,
    TaskSchedule,
    Weekday,
    WeeklySchedule,
)
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import WorkflowContext
from horsies.core.scheduler.service import Scheduler
from horsies.core.scheduler.validation import (
    _serializable_error,
    _signature_error,
)
from horsies.core.types.result import Err, Ok, is_err


def _utc(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
) -> datetime:
    """Helper to construct a UTC-aware datetime."""
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def _make_app(
    schedule_config: ScheduleConfig | None = None,
    tasks: dict[str, MagicMock] | None = None,
) -> MagicMock:
    """Build a minimal mock Horsies app for Scheduler tests.

    ``config.payload`` is a REAL PayloadPolicy: the payload guardrail
    compares sizes against it, and int-vs-MagicMock comparison raises.
    """
    app = MagicMock()
    app.config.schedule = schedule_config
    app.config.payload = PayloadPolicy()
    app.tasks = tasks or {}
    # Default validate_queue_name returns 'default'
    app.validate_queue_name = MagicMock(return_value='default')
    # Default queue mode so effective_priority returns 100
    app.config.queue_mode.name = 'DEFAULT'
    return app


def _make_task_mock(fn: object) -> MagicMock:
    """Create a mock task object with _original_fn pointing to a real function."""
    task = MagicMock()
    task._original_fn = fn
    task.task_queue_name = None
    return task


# =============================================================================
# Scheduler.__init__
# =============================================================================


@pytest.mark.unit
class TestSchedulerInit:
    """Tests for Scheduler initialization."""

    def test_missing_config_raises_configuration_error(self) -> None:
        """App without schedule config raises ConfigurationError HRS-205."""
        app = _make_app(schedule_config=None)

        with pytest.raises(ConfigurationError) as exc_info:
            Scheduler(app)

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'schedule configuration' in exc.message
        assert exc.help_text is not None

    def test_valid_config_initializes(self) -> None:
        """Valid schedule config allows Scheduler construction."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='test',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
            ],
        )
        app = _make_app(schedule_config=config)

        scheduler = Scheduler(app)

        assert scheduler.schedule_config is config
        assert scheduler._initialized is False


# =============================================================================
# validation._signature_error
# =============================================================================


@pytest.mark.unit
class TestSignatureError:
    """Tests for validation._signature_error (isolated signature bind).

    Args tolerance is exercised here in isolation; the integrated
    collect_schedule_errors path rejects positional args upstream (see
    test_schedule_validation.py).
    """

    def _make_app_with_task(self, task: MagicMock) -> MagicMock:
        """Build a mock app exposing the given task as 'task_a'."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='task_a',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        return _make_app(schedule_config=config, tasks={'task_a': task})

    def test_no_required_params_ok(self) -> None:
        """Function with no required params passes validation."""
        def no_params() -> str:
            return 'ok'

        app = self._make_app_with_task(_make_task_mock(no_params))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
        )

        assert _signature_error(app, schedule) is None

    def test_missing_required_returns_configuration_error(self) -> None:
        """Missing required param returns ConfigurationError HRS-205."""
        def needs_value(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(needs_value))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
        )

        error = _signature_error(app, schedule)
        assert error is not None
        assert error.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'kwargs do not match' in error.message

    def test_unexpected_kwarg_returns_configuration_error(self) -> None:
        """Unexpected kwargs are rejected."""

        def needs_value(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(needs_value))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'unknown': 42},
        )

        error = _signature_error(app, schedule)
        assert error is not None
        assert error.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'kwargs do not match' in error.message

    def test_satisfied_by_kwargs(self) -> None:
        """Required param satisfied by keyword args passes."""
        def needs_value(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(needs_value))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'value': 42},
        )

        assert _signature_error(app, schedule) is None

    def test_multiple_params_satisfied_by_kwargs(self) -> None:
        """Multiple required params satisfied entirely by kwargs."""
        def needs_both(a: int, b: str) -> str:
            return f'{a}{b}'

        app = self._make_app_with_task(_make_task_mock(needs_both))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'a': 1, 'b': 'hello'},
        )

        assert _signature_error(app, schedule) is None


# =============================================================================
# validation._serializable_error
# =============================================================================


@pytest.mark.unit
class TestSerializableError:
    """Schedules whose kwargs pass sig.bind but cannot be wire-encoded must be
    rejected at startup, not fail ENQUEUE_FAILED on every tick."""

    def _make_app_with_task(self, task: MagicMock) -> MagicMock:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='task_a',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        return _make_app(schedule_config=config, tasks={'task_a': task})

    def _schedule(self, kwargs: dict[str, object]) -> TaskSchedule:
        return TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs=kwargs,
        )

    def test_no_kwargs_ok(self) -> None:
        def f() -> str:
            return 'ok'

        app = self._make_app_with_task(_make_task_mock(f))
        assert _serializable_error(app, self._schedule({})) is None

    def test_plain_kwargs_ok(self) -> None:
        def f(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(f))
        assert _serializable_error(app, self._schedule({'value': 5})) is None

    def test_engine_injected_kwarg_rejected(self) -> None:
        def f(x: int, workflow_ctx: WorkflowContext | None = None) -> str:
            return str(x)

        app = self._make_app_with_task(_make_task_mock(f))
        error = _serializable_error(app, self._schedule({'x': 1, 'workflow_ctx': None}))
        assert error is not None
        assert error.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'not encodable' in error.message

    def test_taskresult_typed_kwarg_rejected(self) -> None:
        def f(dep: TaskResult[int, TaskError]) -> str:
            return 'ok'

        app = self._make_app_with_task(_make_task_mock(f))
        error = _serializable_error(app, self._schedule({'dep': TaskResult(ok=1)}))
        assert error is not None
        assert error.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'not encodable' in error.message


# =============================================================================
# _compute_config_hash
# =============================================================================


@pytest.mark.unit
class TestComputeConfigHash:
    """Tests for Scheduler._compute_config_hash."""

    def _make_scheduler(self) -> Scheduler:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        return Scheduler(app)

    def test_deterministic(self) -> None:
        """Same schedule produces same hash."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
        )

        hash1 = scheduler._compute_config_hash(schedule)
        hash2 = scheduler._compute_config_hash(schedule)

        assert hash1 == hash2

    def test_changes_on_pattern_change(self) -> None:
        """Different pattern produces different hash."""
        scheduler = self._make_scheduler()
        schedule_a = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
        )
        schedule_b = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=20),
        )

        hash_a = scheduler._compute_config_hash(schedule_a)
        hash_b = scheduler._compute_config_hash(schedule_b)

        assert hash_a != hash_b

    def test_changes_on_timezone_change(self) -> None:
        """Different timezone produces different hash."""
        scheduler = self._make_scheduler()
        schedule_utc = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
            timezone='UTC',
        )
        schedule_ny = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
            timezone='America/New_York',
        )

        hash_utc = scheduler._compute_config_hash(schedule_utc)
        hash_ny = scheduler._compute_config_hash(schedule_ny)

        assert hash_utc != hash_ny

    def test_time_based_patterns_serialize(self) -> None:
        """Regression: time-based patterns (Daily/Weekly/Monthly) carry a
        datetime.time. _compute_config_hash must serialize them, not raise —
        strict dumps_json rejects a live datetime.time, so model_dump(mode='json')
        is required. Interval-only tests previously hid this."""
        scheduler = self._make_scheduler()
        patterns = [
            DailySchedule(time=time(3, 0)),
            WeeklySchedule(days=[Weekday.MONDAY, Weekday.FRIDAY], time=time(9, 0)),
            MonthlySchedule(day=15, time=time(17, 30)),
        ]
        for pattern in patterns:
            schedule = TaskSchedule(name='t', task_name='my_task', pattern=pattern)
            digest = scheduler._compute_config_hash(schedule)
            assert isinstance(digest, str) and len(digest) == 64  # sha256 hex

    def test_time_based_pattern_hash_is_deterministic(self) -> None:
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='t', task_name='my_task', pattern=DailySchedule(time=time(3, 0)),
        )
        assert scheduler._compute_config_hash(schedule) == scheduler._compute_config_hash(schedule)


# =============================================================================
# _schedule_advisory_key
# =============================================================================


@pytest.mark.unit
class TestScheduleAdvisoryKey:
    """Tests for Scheduler._schedule_advisory_key."""

    def _make_scheduler(self) -> Scheduler:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        return Scheduler(app)

    def test_deterministic(self) -> None:
        """Same name produces same key."""
        scheduler = self._make_scheduler()

        key1 = scheduler._schedule_advisory_key('test_schedule')
        key2 = scheduler._schedule_advisory_key('test_schedule')

        assert key1 == key2

    def test_different_names_produce_different_keys(self) -> None:
        """Different schedule names produce different keys."""
        scheduler = self._make_scheduler()

        key_a = scheduler._schedule_advisory_key('schedule_a')
        key_b = scheduler._schedule_advisory_key('schedule_b')

        assert key_a != key_b

    def test_returns_int(self) -> None:
        """Advisory key is an integer (for pg_advisory_xact_lock)."""
        scheduler = self._make_scheduler()

        key = scheduler._schedule_advisory_key('test')

        assert isinstance(key, int)


# =============================================================================
# _calculate_missed_runs
# =============================================================================


@pytest.mark.unit
class TestCalculateMissedRuns:
    """Tests for Scheduler._calculate_missed_runs."""

    def _make_scheduler(self) -> Scheduler:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        return Scheduler(app)

    def test_includes_first_due_run(self) -> None:
        """Catch-up includes the first overdue scheduled slot."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=2),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 1)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=10)

        assert result == [last]

    def test_gap_returns_correct_due_list(self) -> None:
        """Gap produces all due runs including first due slot."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 17)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=10)

        assert len(result) == 4
        assert result[0] == _utc(2025, 6, 1, 12, 0, 0)
        assert result[1] == _utc(2025, 6, 1, 12, 0, 5)
        assert result[2] == _utc(2025, 6, 1, 12, 0, 10)
        assert result[3] == _utc(2025, 6, 1, 12, 0, 15)

    def test_chronologically_ordered(self) -> None:
        """Due runs are in ascending chronological order."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=3),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 12)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=10)

        for i in range(1, len(result)):
            assert result[i] > result[i - 1]

    def test_respects_max_runs_cap(self) -> None:
        """Catch-up caps due runs for a single tick."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=1),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 5)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=3)

        assert result == [
            _utc(2025, 6, 1, 12, 0, 0),
            _utc(2025, 6, 1, 12, 0, 1),
            _utc(2025, 6, 1, 12, 0, 2),
        ]


# =============================================================================
# _advance_to_latest_due_slot (catch_up_missed=False skip semantics)
# =============================================================================


@pytest.mark.unit
class TestAdvanceToLatestDueSlot:
    """Tests for Scheduler._advance_to_latest_due_slot."""

    def _make_scheduler(self) -> Scheduler:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        return Scheduler(app)

    def test_single_due_slot_is_not_skipped(self) -> None:
        """Normal operation: slot due by under one period fires unchanged."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 1)

        slot, next_run, skipped = scheduler._advance_to_latest_due_slot(
            schedule, first_due, now,
        )

        assert slot == first_due
        assert skipped == 0
        assert next_run == _utc(2025, 6, 1, 12, 0, 5)
        assert next_run > now

    def test_backlog_skips_to_latest_due_slot(self) -> None:
        """Downtime backlog: only the latest due slot remains."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 52)  # slots 0..50 due (11 slots)

        slot, next_run, skipped = scheduler._advance_to_latest_due_slot(
            schedule, first_due, now,
        )

        assert slot == _utc(2025, 6, 1, 12, 0, 50)
        assert skipped == 10
        assert next_run == _utc(2025, 6, 1, 12, 0, 55)
        assert next_run > now

    def test_scan_cap_returns_partial_progress(self) -> None:
        """Deep backlog beyond the cap returns a still-due next_run."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=1),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 30)

        with patch.object(Scheduler, '_MAX_SKIP_SCAN_PER_TICK', 3):
            slot, next_run, skipped = scheduler._advance_to_latest_due_slot(
                schedule, first_due, now,
            )

        assert skipped == 3
        assert slot == _utc(2025, 6, 1, 12, 0, 3)
        assert next_run == _utc(2025, 6, 1, 12, 0, 4)
        assert next_run <= now, 'cap path must signal an unconverged backlog'


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckScheduleSkipsMissedRuns:
    """catch_up_missed=False fires at most one run after downtime."""

    def _make_harness(
        self,
        *,
        pattern: IntervalSchedule,
        next_run_at: datetime,
    ) -> tuple[Scheduler, TaskSchedule, AsyncMock, MagicMock]:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(name='s', task_name='my_task', pattern=pattern),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session = AsyncMock()
        broker = MagicMock()
        broker.session_factory.return_value.__aenter__ = AsyncMock(
            return_value=session,
        )
        broker.session_factory.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = next_run_at
        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        enqueue_mock = AsyncMock(return_value=Ok('task-1'))
        scheduler._enqueue_scheduled_task = enqueue_mock  # type: ignore[method-assign]
        return scheduler, config.schedules[0], enqueue_mock, state_manager

    async def test_downtime_backlog_fires_latest_slot_once(self) -> None:
        """10h-stale hourly-equivalent state enqueues exactly one run."""
        scheduler, schedule, enqueue_mock, state_manager = self._make_harness(
            pattern=IntervalSchedule(seconds=5),
            next_run_at=_utc(2025, 6, 1, 12, 0, 0),
        )
        now = _utc(2025, 6, 1, 12, 0, 52)

        await scheduler._check_schedule(schedule, now)

        enqueue_mock.assert_awaited_once()
        assert enqueue_mock.await_args.kwargs['slot_time'] == _utc(
            2025, 6, 1, 12, 0, 50,
        )
        update_kwargs = state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['next_run_at'] == _utc(2025, 6, 1, 12, 0, 55)
        assert update_kwargs['next_run_at'] > now

    async def test_deep_backlog_persists_progress_without_firing(self) -> None:
        """Scan-cap path advances state without enqueuing a stale slot."""
        scheduler, schedule, enqueue_mock, state_manager = self._make_harness(
            pattern=IntervalSchedule(seconds=1),
            next_run_at=_utc(2025, 6, 1, 12, 0, 0),
        )
        now = _utc(2025, 6, 1, 12, 0, 30)

        with patch.object(Scheduler, '_MAX_SKIP_SCAN_PER_TICK', 3):
            await scheduler._check_schedule(schedule, now)

        enqueue_mock.assert_not_awaited()
        state_manager.update_after_run.assert_not_awaited()
        update_kwargs = state_manager.update_next_run.await_args.kwargs
        assert update_kwargs['next_run_at'] == _utc(2025, 6, 1, 12, 0, 4)


# =============================================================================
# _validate_schedules
# =============================================================================


@pytest.mark.unit
class TestValidateSchedules:
    """Tests for Scheduler._validate_schedules."""

    def test_invalid_timezone_raises_configuration_error(self) -> None:
        """Regression: a bad timezone fails at startup validation (exit 1)
        instead of failing calculate_next_run on every tick — the
        missing-row self-heal would otherwise retry it forever."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    timezone='Not/AZone',
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        with pytest.raises(ConfigurationError) as exc_info:
            scheduler._validate_schedules()

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'invalid timezone' in exc.message

    def test_unregistered_task_raises_registry_error(self) -> None:
        """Schedule referencing unregistered task raises RegistryError HRS-300."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='nonexistent_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={})
        scheduler = Scheduler(app)

        with pytest.raises(RegistryError) as exc_info:
            scheduler._validate_schedules()

        exc = exc_info.value
        assert exc.code == ErrorCode.TASK_NOT_REGISTERED
        assert 'not registered' in exc.message

    def test_disabled_schedule_skipped(self) -> None:
        """Disabled schedules are not validated."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='disabled',
                    task_name='nonexistent_task',
                    pattern=IntervalSchedule(seconds=5),
                    enabled=False,
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={})
        scheduler = Scheduler(app)

        # Should not raise even though task doesn't exist
        scheduler._validate_schedules()

    def test_invalid_queue_raises_configuration_error(self) -> None:
        """Invalid queue configuration raises ConfigurationError HRS-205."""
        def no_params() -> str:
            return 'ok'

        task_mock = _make_task_mock(no_params)

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    queue_name='invalid_queue',
                ),
            ],
        )
        app = _make_app(
            schedule_config=config,
            tasks={'my_task': task_mock},
        )
        # Make validate_queue_name raise to simulate invalid queue
        app.validate_queue_name = MagicMock(
            side_effect=ConfigurationError(
                message='invalid queue',
                code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
            ),
        )
        scheduler = Scheduler(app)

        with pytest.raises(ConfigurationError) as exc_info:
            scheduler._validate_schedules()

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'invalid queue configuration' in exc.message


# =============================================================================
# _initialize_schedules
# =============================================================================


@pytest.mark.unit
class TestInitializeSchedules:
    """Tests for Scheduler._initialize_schedules."""

    @pytest.mark.asyncio
    async def test_foreign_schedule_state_rows_are_kept(self) -> None:
        """Startup must not delete state rows outside this config.

        Spec change (0.1.7): auto-pruning broke rolling deploys (the
        old-config scheduler lost its rows and silently stopped firing)
        and shared-database topologies. Orphan rows are kept and only
        logged.
        """
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='kept',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        foreign_state = MagicMock()
        foreign_state.schedule_name = 'other_schedulers_row'
        kept_state = MagicMock()
        kept_state.schedule_name = 'kept'

        state_manager = MagicMock()
        state_manager.get_all_states = AsyncMock(
            return_value=[foreign_state, kept_state],
        )
        state_manager.get_state = AsyncMock(return_value=None)
        state_manager.initialize_state = AsyncMock()
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        state_manager.get_all_states.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_one_bad_schedule_does_not_block_others(self) -> None:
        """If calculate_next_run fails for one schedule, the rest still initialize."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='good_first',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
                TaskSchedule(
                    name='bad',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
                TaskSchedule(
                    name='good_last',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.get_all_states = AsyncMock(return_value=[])
        state_manager.get_state = AsyncMock(return_value=None)

        call_count = 0

        async def _init_side_effect(
            name: str, next_run: datetime, config_hash: str, **kwargs: object,
        ) -> None:
            nonlocal call_count
            call_count += 1
            if name == 'bad':
                raise RuntimeError('simulated calculator failure')

        state_manager.initialize_state = AsyncMock(side_effect=_init_side_effect)
        scheduler.state_manager = state_manager

        # Should not raise — bad schedule is isolated
        await scheduler._initialize_schedules()

        # Both good schedules initialized; bad one attempted but failed
        assert call_count == 3
        assert state_manager.initialize_state.await_count == 3


# =============================================================================
# _check_schedule transactional session usage
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckScheduleSessionUsage:
    """Ensure lock-protected checks use one DB session for state read/write."""

    async def test_state_reads_and_updates_use_lock_session(self) -> None:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session = AsyncMock()
        broker = MagicMock()
        broker.session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
        broker.session_factory.return_value.__aexit__ = AsyncMock(return_value=False)
        scheduler.broker = broker

        state = MagicMock()
        now = _utc(2025, 6, 1, 12, 0, 0)
        state.next_run_at = _utc(2025, 6, 1, 11, 59, 59)
        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        scheduler.state_manager = state_manager

        scheduler._enqueue_scheduled_task = AsyncMock(return_value=Ok('task-1'))  # type: ignore[method-assign]

        await scheduler._check_schedule(config.schedules[0], now)

        state_manager.get_state.assert_awaited_once_with('s', session=session)
        assert state_manager.update_after_run.await_count == 1
        update_kwargs = state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['session'] is session

    async def test_missing_state_initializes_using_lock_session(self) -> None:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session = AsyncMock()
        broker = MagicMock()
        broker.session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
        broker.session_factory.return_value.__aexit__ = AsyncMock(return_value=False)
        scheduler.broker = broker

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=None)
        state_manager.initialize_state = AsyncMock()
        scheduler.state_manager = state_manager

        await scheduler._check_schedule(config.schedules[0], _utc(2025, 6, 1, 12, 0, 0))

        state_manager.get_state.assert_awaited_once()
        assert state_manager.initialize_state.await_count == 1
        init_kwargs = state_manager.initialize_state.await_args.kwargs
        assert init_kwargs['session'] is session


# =============================================================================
# Scheduler.start
# =============================================================================


def _make_broker_mock(
    *,
    schema_ok: bool = True,
    close_ok: bool = True,
) -> MagicMock:
    """Build a mock PostgresBroker for Scheduler tests."""
    broker = MagicMock()
    if schema_ok:
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(None))
    else:
        broker.ensure_schema_initialized = AsyncMock(
            return_value=Err(BrokerOperationError(
                code=BrokerErrorCode.SCHEMA_INIT_FAILED,
                message='schema init failed',
                retryable=False,
                exception=RuntimeError('db down'),
            )),
        )
    if close_ok:
        broker.close_async = AsyncMock(return_value=Ok(None))
    else:
        broker.close_async = AsyncMock(
            return_value=Err(BrokerOperationError(
                code=BrokerErrorCode.CLOSE_FAILED,
                message='close failed',
                retryable=False,
            )),
        )
    broker.session_factory = MagicMock()
    return broker


@pytest.mark.unit
@pytest.mark.asyncio
class TestSchedulerStart:
    """Tests for Scheduler.start()."""

    async def test_already_initialized_returns_immediately(self) -> None:
        """If _initialized is True, start() is a no-op."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler._initialized = True

        # Should not call get_broker or anything else
        await scheduler.start()

        app.get_broker.assert_not_called()

    async def test_schema_init_failure_raises_runtime_error(self) -> None:
        """Schema init failure raises RuntimeError with chained exception."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        broker = _make_broker_mock(schema_ok=False)
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        with pytest.raises(RuntimeError, match='Schema initialization failed'):
            await scheduler.start()

    async def test_happy_path_initializes_all_components(self) -> None:
        """Successful start: broker, preload, state_manager, validate, init schedules."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': _make_task_mock(lambda: 'ok')})
        broker = _make_broker_mock()
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        # Patch methods that need DB or real modules
        scheduler._preload_task_modules = MagicMock()  # type: ignore[method-assign]
        scheduler._validate_schedules = MagicMock()  # type: ignore[method-assign]
        scheduler._initialize_schedules = AsyncMock()  # type: ignore[method-assign]

        await scheduler.start()

        assert scheduler._initialized is True
        assert scheduler.broker is broker
        assert scheduler.state_manager is not None
        scheduler._preload_task_modules.assert_called_once()
        scheduler._validate_schedules.assert_called_once()
        scheduler._initialize_schedules.assert_awaited_once()

    async def test_start_calls_preload_before_validate(self) -> None:
        """Preload runs before validate to ensure task registry is populated."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': _make_task_mock(lambda: 'ok')})
        broker = _make_broker_mock()
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        call_order: list[str] = []
        scheduler._preload_task_modules = MagicMock(side_effect=lambda: call_order.append('preload'))  # type: ignore[method-assign]
        scheduler._validate_schedules = MagicMock(side_effect=lambda: call_order.append('validate'))  # type: ignore[method-assign]
        scheduler._initialize_schedules = AsyncMock(side_effect=lambda: call_order.append('init'))  # type: ignore[method-assign]

        await scheduler.start()

        assert call_order == ['preload', 'validate', 'init']


# =============================================================================
# Scheduler.stop
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestSchedulerStop:
    """Tests for Scheduler.stop()."""

    async def test_stop_sets_event_and_closes_broker(self) -> None:
        """Happy path: stop event set, broker closed."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock(close_ok=True)

        await scheduler.stop()

        assert scheduler._stop.is_set()
        scheduler.broker.close_async.assert_awaited_once()

    async def test_stop_broker_close_error_logs_without_raising(self) -> None:
        """Broker close failure logs error but does not raise."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock(close_ok=False)

        # Should not raise
        await scheduler.stop()

        assert scheduler._stop.is_set()
        scheduler.broker.close_async.assert_awaited_once()

    async def test_stop_no_broker_only_sets_event(self) -> None:
        """No broker → just sets stop event, no crash."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.broker = None

        await scheduler.stop()

        assert scheduler._stop.is_set()


# =============================================================================
# Scheduler.request_stop
# =============================================================================


@pytest.mark.unit
class TestRequestStop:
    """Tests for Scheduler.request_stop()."""

    def test_request_stop_sets_stop_event(self) -> None:
        """request_stop sets the internal _stop event."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        assert not scheduler._stop.is_set()

        scheduler.request_stop()

        assert scheduler._stop.is_set()


# =============================================================================
# Scheduler.run_forever
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestRunForever:
    """Tests for Scheduler.run_forever()."""

    async def test_stop_signal_breaks_loop(self) -> None:
        """Pre-set stop event → loop exits after start()."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        # Stub start and stop to avoid real broker/DB
        scheduler.start = AsyncMock()  # type: ignore[method-assign]
        scheduler.stop = AsyncMock()  # type: ignore[method-assign]

        # Pre-set stop signal so wait_for returns immediately
        scheduler._stop.set()

        await scheduler.run_forever()

        scheduler.start.assert_awaited_once()
        scheduler.stop.assert_awaited_once()

    async def test_schedule_error_does_not_crash_loop(self) -> None:
        """Error in _check_and_run_schedules is caught; loop continues."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
            check_interval_seconds=1,
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        scheduler.start = AsyncMock()  # type: ignore[method-assign]
        scheduler.stop = AsyncMock()  # type: ignore[method-assign]

        call_count = 0

        async def _check_side_effect() -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError('simulated error')
            # On second call, signal stop
            scheduler._stop.set()

        scheduler._check_and_run_schedules = AsyncMock(side_effect=_check_side_effect)  # type: ignore[method-assign]

        await scheduler.run_forever()

        # Called at least twice: first raises, second stops
        assert call_count >= 2

    async def test_timeout_continues_loop(self) -> None:
        """wait_for timeout continues to next iteration."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
            check_interval_seconds=1,
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        scheduler.start = AsyncMock()  # type: ignore[method-assign]
        scheduler.stop = AsyncMock()  # type: ignore[method-assign]

        iteration_count = 0

        async def _check_side_effect() -> None:
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 2:
                scheduler._stop.set()

        scheduler._check_and_run_schedules = AsyncMock(side_effect=_check_side_effect)  # type: ignore[method-assign]

        await scheduler.run_forever()

        # At least 2 iterations before stop
        assert iteration_count >= 2


# =============================================================================
# _initialize_schedules — additional paths
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestInitializeSchedulesAdditionalPaths:
    """Additional tests for Scheduler._initialize_schedules."""

    async def test_no_state_manager_raises_runtime_error(self) -> None:
        """state_manager=None → RuntimeError."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.state_manager = None

        with pytest.raises(RuntimeError, match='State manager not initialized'):
            await scheduler._initialize_schedules()

    async def test_prune_exception_logs_warning_continues(self) -> None:
        """get_all_states raises → logs warning, initialization still runs."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.get_all_states = AsyncMock(side_effect=RuntimeError('db error'))
        state_manager.get_state = AsyncMock(return_value=None)
        state_manager.initialize_state = AsyncMock()
        scheduler.state_manager = state_manager

        # Should not raise
        await scheduler._initialize_schedules()

        # Schedule 's' should still initialize despite prune failure
        state_manager.initialize_state.assert_awaited_once()

    async def test_disabled_schedule_skipped_during_init(self) -> None:
        """Disabled schedules are not passed to _initialize_single_schedule."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='enabled_one',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
                TaskSchedule(
                    name='disabled_one',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    enabled=False,
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.get_all_states = AsyncMock(return_value=[])
        state_manager.get_state = AsyncMock(return_value=None)
        state_manager.initialize_state = AsyncMock()
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        # Only 'enabled_one' should be initialized
        assert state_manager.initialize_state.await_count == 1
        init_args = state_manager.initialize_state.await_args
        assert init_args[0][0] == 'enabled_one'


# =============================================================================
# _initialize_single_schedule — additional paths
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestInitializeSingleScheduleAdditionalPaths:
    """Additional tests for Scheduler._initialize_single_schedule."""

    async def test_no_state_manager_raises_runtime_error(self) -> None:
        """state_manager=None → RuntimeError."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.state_manager = None

        with pytest.raises(RuntimeError, match='State manager not initialized'):
            await scheduler._initialize_single_schedule(
                config.schedules[0],
                _utc(2025, 6, 1, 12, 0, 0),
            )

    async def test_config_hash_changed_recalculates_next_run(self) -> None:
        """Existing state with stale hash → update_next_run called with new hash."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        # Existing state with old hash
        old_state = MagicMock()
        old_state.config_hash = 'stale_hash_value'
        old_state.next_run_at = _utc(2025, 6, 1, 11, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=old_state)
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        now = _utc(2025, 6, 1, 12, 0, 0)
        await scheduler._initialize_single_schedule(config.schedules[0], now)

        # Should have called update_next_run with new hash
        state_manager.update_next_run.assert_awaited_once()
        update_args = state_manager.update_next_run.await_args
        assert update_args[0][0] == 's'  # schedule_name
        # The new config_hash should differ from the stale one
        new_hash = update_args[0][2]
        assert new_hash != 'stale_hash_value'


# =============================================================================
# _check_and_run_schedules
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckAndRunSchedules:
    """Tests for Scheduler._check_and_run_schedules."""

    async def test_not_initialized_raises_runtime_error(self) -> None:
        """state_manager or broker is None → RuntimeError."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.state_manager = None
        scheduler.broker = None

        with pytest.raises(RuntimeError, match='not properly initialized'):
            await scheduler._check_and_run_schedules()

    async def test_no_enabled_schedules_returns_early(self) -> None:
        """All disabled → returns without querying state."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    enabled=False,
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.get_due_states = AsyncMock()
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        await scheduler._check_and_run_schedules()

        # get_due_states should not be called since no enabled schedules
        state_manager.get_due_states.assert_not_awaited()

    async def test_iterates_due_states_and_checks_each(self) -> None:
        """Two due states → _check_schedule called for each."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='sched_a',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
                TaskSchedule(
                    name='sched_b',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        due_a = MagicMock()
        due_a.schedule_name = 'sched_a'
        due_b = MagicMock()
        due_b.schedule_name = 'sched_b'

        state_manager = MagicMock()
        state_manager.get_existing_names = AsyncMock(
            return_value={'sched_a', 'sched_b'},
        )
        state_manager.get_due_states = AsyncMock(return_value=[due_a, due_b])
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        checked: list[str] = []

        async def _track_check(schedule: object, now: object) -> None:
            checked.append(schedule.name)  # type: ignore[union-attr]

        scheduler._check_schedule = AsyncMock(side_effect=_track_check)  # type: ignore[method-assign]

        await scheduler._check_and_run_schedules()

        assert checked == ['sched_a', 'sched_b']

    async def test_check_schedule_error_does_not_block_others(self) -> None:
        """First _check_schedule raises → second still called."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='sched_a',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
                TaskSchedule(
                    name='sched_b',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        due_a = MagicMock()
        due_a.schedule_name = 'sched_a'
        due_b = MagicMock()
        due_b.schedule_name = 'sched_b'

        state_manager = MagicMock()
        state_manager.get_existing_names = AsyncMock(
            return_value={'sched_a', 'sched_b'},
        )
        state_manager.get_due_states = AsyncMock(return_value=[due_a, due_b])
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        checked: list[str] = []

        async def _fail_first(schedule: object, now: object) -> None:
            name = schedule.name  # type: ignore[union-attr]
            checked.append(name)
            if name == 'sched_a':
                raise RuntimeError('simulated failure')

        scheduler._check_schedule = AsyncMock(side_effect=_fail_first)  # type: ignore[method-assign]

        await scheduler._check_and_run_schedules()

        assert checked == ['sched_a', 'sched_b']


# =============================================================================
# _compute_config_hash — serialization failure
# =============================================================================


@pytest.mark.unit
class TestComputeConfigHashSerializationFailure:
    """Tests for _compute_config_hash when dumps_json fails."""

    def test_serialization_failure_raises_runtime_error(self) -> None:
        """dumps_json returning Err raises RuntimeError."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        with patch(
            'horsies.core.codec.json_io.dumps_json',
            return_value=Err(ValueError('bad json')),
        ):
            with pytest.raises(RuntimeError, match='Failed to serialize config'):
                scheduler._compute_config_hash(config.schedules[0])


# =============================================================================
# _check_schedule — additional paths
# =============================================================================


def _make_session_and_broker() -> tuple[AsyncMock, MagicMock]:
    """Create a mock session + broker wired through session_factory context manager."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    broker = MagicMock()
    broker.session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
    broker.session_factory.return_value.__aexit__ = AsyncMock(return_value=False)
    return session, broker


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckScheduleAdditionalPaths:
    """Additional tests for Scheduler._check_schedule."""

    async def test_not_initialized_raises_runtime_error(self) -> None:
        """state_manager=None → RuntimeError."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.state_manager = None
        scheduler.broker = None

        with pytest.raises(RuntimeError, match='not properly initialized'):
            await scheduler._check_schedule(config.schedules[0], _utc(2025, 6, 1, 12, 0, 0))

    async def test_not_due_commits_and_returns(self) -> None:
        """should_run_now returns False → commit, no enqueue."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        # State exists but not due yet
        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 13, 0, 0)  # future

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        scheduler.state_manager = state_manager

        now = _utc(2025, 6, 1, 12, 0, 0)  # before next_run_at

        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=False,
        ):
            await scheduler._check_schedule(config.schedules[0], now)

        session.commit.assert_awaited_once()
        # No enqueue should have been attempted
        assert not hasattr(scheduler, '_enqueue_scheduled_task_called')

    async def test_catch_up_multiple_runs_all_succeed(self) -> None:
        """Catch-up with 3 due runs, all succeed → state updated with last slot."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=True,
            max_catch_up_runs=10,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        # State: due since 12:00:00, now is 12:00:17 → 4 slots
        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)
        state.config_hash = scheduler._compute_config_hash(schedule)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        scheduler.state_manager = state_manager

        enqueue_ids: list[str] = []

        async def _mock_enqueue(sched: object, *, slot_time: datetime) -> Ok[str]:
            task_id = f'task-{len(enqueue_ids)}'
            enqueue_ids.append(task_id)
            return Ok(task_id)

        scheduler._enqueue_scheduled_task = AsyncMock(side_effect=_mock_enqueue)  # type: ignore[method-assign]

        now = _utc(2025, 6, 1, 12, 0, 17)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        # Multiple runs enqueued
        assert len(enqueue_ids) >= 3
        # State updated after run
        state_manager.update_after_run.assert_awaited_once()
        update_kwargs = state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['task_id'] == enqueue_ids[-1]
        session.commit.assert_awaited_once()

    async def test_catch_up_partial_failure_commits_progress(self) -> None:
        """Catch-up: 2nd run fails → 1st saved, warning logged."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=True,
            max_catch_up_runs=10,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        scheduler.state_manager = state_manager

        call_idx = 0

        async def _fail_second(sched: object, *, slot_time: datetime) -> Ok[str] | Err[BrokerOperationError]:
            nonlocal call_idx
            call_idx += 1
            if call_idx == 1:
                return Ok('task-0')
            return Err(BrokerOperationError(
                code=BrokerErrorCode.ENQUEUE_FAILED,
                message='transient error',
                retryable=True,
            ))

        scheduler._enqueue_scheduled_task = AsyncMock(side_effect=_fail_second)  # type: ignore[method-assign]

        now = _utc(2025, 6, 1, 12, 0, 17)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        # State should still be updated for the one that succeeded
        state_manager.update_after_run.assert_awaited_once()
        update_kwargs = state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['task_id'] == 'task-0'
        session.commit.assert_awaited_once()

    async def test_catch_up_all_failed_rolls_back(self) -> None:
        """Catch-up: 1st enqueue fails → rollback, no state update."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=True,
            max_catch_up_runs=10,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        scheduler.state_manager = state_manager

        scheduler._enqueue_scheduled_task = AsyncMock(  # type: ignore[method-assign]
            return_value=Err(BrokerOperationError(
                code=BrokerErrorCode.ENQUEUE_FAILED,
                message='permanent error',
                retryable=False,
            )),
        )

        now = _utc(2025, 6, 1, 12, 0, 17)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        # No state update — all failed
        state_manager.update_after_run.assert_not_awaited()
        session.rollback.assert_awaited_once()

    async def test_normal_run_transient_failure_rollback(self) -> None:
        """Normal run (no catch-up): retryable Err → warning, rollback."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=False,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        scheduler.state_manager = state_manager

        scheduler._enqueue_scheduled_task = AsyncMock(  # type: ignore[method-assign]
            return_value=Err(BrokerOperationError(
                code=BrokerErrorCode.ENQUEUE_FAILED,
                message='connection reset',
                retryable=True,
            )),
        )

        now = _utc(2025, 6, 1, 12, 0, 5)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        state_manager.update_after_run.assert_not_awaited()
        session.rollback.assert_awaited_once()

    async def test_normal_run_advances_next_run_from_slot_time(self) -> None:
        """A late tick fires the latest due slot, keeping slot alignment.

        catch_up_missed=False (spec change, 0.1.7): older missed slots are
        dropped instead of replayed one per tick; the fired slot and the new
        next_run_at stay aligned to the original anchor.
        """
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=False,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        due_slot = _utc(2025, 6, 1, 12, 0, 0)
        state = MagicMock()
        state.next_run_at = due_slot

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        scheduler.state_manager = state_manager

        slot_times: list[datetime] = []

        async def _mock_enqueue(sched: object, *, slot_time: datetime) -> Ok[str]:
            slot_times.append(slot_time)
            return Ok('task-0')

        scheduler._enqueue_scheduled_task = AsyncMock(side_effect=_mock_enqueue)  # type: ignore[method-assign]

        now = _utc(2025, 6, 1, 12, 0, 17)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        # Slots 12:00:00..12:00:15 were due; only the latest fires, and the
        # anchor is preserved (12:00:15 is still a multiple of the period).
        assert slot_times == [_utc(2025, 6, 1, 12, 0, 15)]
        state_manager.update_after_run.assert_awaited_once()
        update_kwargs = state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['next_run_at'] == _utc(2025, 6, 1, 12, 0, 20)
        assert update_kwargs['next_run_at'] > now
        session.commit.assert_awaited_once()

    async def test_normal_run_permanent_failure_skips_slot(self) -> None:
        """Non-retryable Err advances past the doomed slot (no wedge).

        Spec change (0.1.7): a permanent enqueue error (e.g.
        PAYLOAD_MISMATCH after a deploy changed kwargs) used to roll back
        without advancing next_run_at, so the same slot was retried every
        tick forever.
        """
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=False,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        scheduler._enqueue_scheduled_task = AsyncMock(  # type: ignore[method-assign]
            return_value=Err(BrokerOperationError(
                code=BrokerErrorCode.ENQUEUE_FAILED,
                message='permanent failure',
                retryable=False,
            )),
        )

        now = _utc(2025, 6, 1, 12, 0, 5)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        state_manager.update_after_run.assert_not_awaited()
        update_kwargs = state_manager.update_next_run.await_args.kwargs
        assert update_kwargs['next_run_at'] == _utc(2025, 6, 1, 12, 0, 10)
        session.commit.assert_awaited_once()

    async def test_normal_run_transient_failure_rollback(self) -> None:
        """Retryable Err → rollback, slot retried next tick."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=False,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        scheduler._enqueue_scheduled_task = AsyncMock(  # type: ignore[method-assign]
            return_value=Err(BrokerOperationError(
                code=BrokerErrorCode.ENQUEUE_FAILED,
                message='transient failure',
                retryable=True,
            )),
        )

        now = _utc(2025, 6, 1, 12, 0, 5)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        state_manager.update_after_run.assert_not_awaited()
        state_manager.update_next_run.assert_not_awaited()
        session.rollback.assert_awaited_once()

    async def test_catch_up_permanent_failure_skips_doomed_slot(self) -> None:
        """Catch-up: a permanently failing slot is consumed, not retried."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=True,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        state_manager.update_after_run = AsyncMock()
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        # First slot enqueues, second fails permanently, third never tried.
        scheduler._enqueue_scheduled_task = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                Ok('task-0'),
                Err(BrokerOperationError(
                    code=BrokerErrorCode.PAYLOAD_MISMATCH,
                    message='sha mismatch',
                    retryable=False,
                )),
            ],
        )

        now = _utc(2025, 6, 1, 12, 0, 12)  # due slots: 0, 5, 10
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            await scheduler._check_schedule(schedule, now)

        # Progress recorded past the doomed slot (12:00:05) → next 12:00:10.
        update_kwargs = state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['task_id'] == 'task-0'
        assert update_kwargs['next_run_at'] == _utc(2025, 6, 1, 12, 0, 10)
        session.commit.assert_awaited_once()

    async def test_unexpected_exception_rollback(self) -> None:
        """Unexpected exception during enqueue → error logged, rollback."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            catch_up_missed=False,
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        session, broker = _make_session_and_broker()
        scheduler.broker = broker

        state = MagicMock()
        state.next_run_at = _utc(2025, 6, 1, 12, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=state)
        scheduler.state_manager = state_manager

        scheduler._enqueue_scheduled_task = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError('unexpected boom'),
        )

        now = _utc(2025, 6, 1, 12, 0, 5)
        with patch(
            'horsies.core.scheduler.service.should_run_now',
            return_value=True,
        ):
            # Should not raise — caught inside _check_schedule
            await scheduler._check_schedule(schedule, now)

        session.rollback.assert_awaited_once()


# =============================================================================
# _calculate_missed_runs — additional paths
# =============================================================================


@pytest.mark.unit
class TestCalculateMissedRunsAdditionalPaths:
    """Additional tests for Scheduler._calculate_missed_runs."""

    def _make_scheduler(self) -> Scheduler:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        return Scheduler(app)

    def test_future_first_due_returns_empty(self) -> None:
        """first_due_run > current_time → empty list."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 13, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 0)

        result = scheduler._calculate_missed_runs(schedule, first_due, now, max_runs=10)

        assert result == []

    def test_non_monotonic_next_run_breaks(self) -> None:
        """calculate_next_run returning same time → logs error, breaks."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 20)

        with patch(
            'horsies.core.scheduler.service.calculate_next_run',
            return_value=first_due,  # non-monotonic: same as cursor
        ):
            result = scheduler._calculate_missed_runs(schedule, first_due, now, max_runs=10)

        # Only the first slot, then break due to non-monotonic
        assert result == [first_due]


# =============================================================================
# _preload_task_modules
# =============================================================================


@pytest.mark.unit
class TestPreloadTaskModules:
    """Tests for Scheduler._preload_task_modules."""

    def _make_scheduler(self) -> Scheduler:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        return Scheduler(app)

    def test_no_modules_discovered_logs_warning(self) -> None:
        """Empty module list → warning logged, returns early."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(return_value=[])

        with patch('horsies.core.scheduler.service.logger') as mock_logger:
            scheduler._preload_task_modules()

        mock_logger.warning.assert_called_once()
        assert 'No task modules discovered' in mock_logger.warning.call_args[0][0]

    def test_discover_raises_propagates(self) -> None:
        """get_discovered_task_modules raises → error propagates to caller."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            side_effect=RuntimeError('discovery broken'),
        )

        with pytest.raises(RuntimeError, match='discovery broken'):
            scheduler._preload_task_modules()

    def test_file_path_import_calls_import_by_path(self) -> None:
        """Module ending with .py → import_by_path called."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['tasks/my_tasks.py'],
        )

        with patch('horsies.core.scheduler.service.import_by_path') as mock_import:
            scheduler._preload_task_modules()

        mock_import.assert_called_once()
        # The path is os.path.abspath'd
        assert mock_import.call_args[0][0].endswith('tasks/my_tasks.py')

    def test_module_name_import_calls_importlib(self) -> None:
        """Dotted module name → importlib.import_module called."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )

        with patch('horsies.core.scheduler.service.importlib.import_module') as mock_import:
            scheduler._preload_task_modules()

        mock_import.assert_called_once_with('myapp.tasks')

    def test_import_failure_logs_warning_continues(self) -> None:
        """One module fails → warning logged, next module still imported."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['bad_module', 'good_module'],
        )

        import_calls: list[str] = []

        def _track_import(name: str) -> None:
            import_calls.append(name)
            if name == 'bad_module':
                raise ImportError('cannot import bad_module')

        with patch(
            'horsies.core.scheduler.service.importlib.import_module',
            side_effect=_track_import,
        ):
            scheduler._preload_task_modules()

        assert import_calls == ['bad_module', 'good_module']


# =============================================================================
# validation._signature_error — no _original_fn
# =============================================================================


@pytest.mark.unit
class TestSignatureErrorNoOriginalFn:
    """Test for _signature_error when task has no _original_fn."""

    def test_no_original_fn_returns_none(self) -> None:
        """Task without _original_fn → no error (cannot validate)."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='task_a',
                    pattern=IntervalSchedule(seconds=5),
                    kwargs={'anything': 42},
                ),
            ],
        )
        task_mock = MagicMock()
        task_mock._original_fn = None
        app = _make_app(schedule_config=config, tasks={'task_a': task_mock})

        # No error despite kwargs that wouldn't match any real function
        assert _signature_error(app, config.schedules[0]) is None


# =============================================================================
# _enqueue_scheduled_task
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestEnqueueScheduledTask:
    """Tests for Scheduler._enqueue_scheduled_task."""

    def _make_scheduler_with_task(
        self,
        *,
        task_name: str = 'my_task',
    ) -> Scheduler:
        """Build scheduler with a registered task and mock broker."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name=task_name,
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        task_mock = _make_task_mock(lambda: 'ok')
        app = _make_app(schedule_config=config, tasks={task_name: task_mock})
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock()
        scheduler.broker.enqueue_async = AsyncMock(return_value=Ok('enqueued-id'))
        return scheduler

    async def test_oversized_schedule_kwargs_rejected_before_broker(self) -> None:
        """payload.reject_bytes fails the slot closed; the broker is never
        called. Wiring test for the guardrail in _enqueue_scheduled_task —
        reverting that guard in place fails here."""
        from horsies.core.codec.payload_guard import reset_payload_warnings
        reset_payload_warnings()

        def blob_fn(*, blob: str) -> None:
            return None

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    kwargs={'blob': 'x' * 2048},
                ),
            ],
        )
        task_mock = _make_task_mock(blob_fn)
        app = _make_app(schedule_config=config, tasks={'my_task': task_mock})
        app.config.payload = PayloadPolicy(warn_bytes=64, reject_bytes=256)
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock()
        scheduler.broker.enqueue_async = AsyncMock(return_value=Ok('enqueued-id'))

        result = await scheduler._enqueue_scheduled_task(
            config.schedules[0],
            slot_time=_utc(2025, 6, 1, 12, 0, 0),
        )

        assert is_err(result)
        assert 'payload.reject_bytes' in result.err_value.message
        scheduler.broker.enqueue_async.assert_not_awaited()

    async def test_no_broker_raises_runtime_error(self) -> None:
        """broker=None → RuntimeError."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': MagicMock()})
        scheduler = Scheduler(app)
        scheduler.broker = None

        with pytest.raises(RuntimeError, match='Broker not initialized'):
            await scheduler._enqueue_scheduled_task(
                config.schedules[0],
                slot_time=_utc(2025, 6, 1, 12, 0, 0),
            )

    async def test_unregistered_task_raises_value_error(self) -> None:
        """Task not in app.tasks → ValueError."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='nonexistent',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={})
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock()

        with pytest.raises(ValueError, match='not registered'):
            await scheduler._enqueue_scheduled_task(
                config.schedules[0],
                slot_time=_utc(2025, 6, 1, 12, 0, 0),
            )

    async def test_kwargs_serialization_failure_returns_err(self) -> None:
        """dumps_json returning Err on encoded kwargs → Err(BrokerOperationError).

        Strict-serde phase 3 routes scheduler kwargs through `encode_kwargs`
        then `dumps_json`; this test patches the final stringification step
        (the only post-encoding failure surface) to verify error wrapping.
        """
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'key': 'val'},
        )
        config = ScheduleConfig(schedules=[schedule])
        task_mock = _make_task_mock(lambda key: key)
        app = _make_app(schedule_config=config, tasks={'my_task': task_mock})
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock()

        with patch(
            'horsies.core.scheduler.service.dumps_json',
            return_value=Err(TypeError('bad kwargs')),
        ):
            result = await scheduler._enqueue_scheduled_task(
                schedule,
                slot_time=_utc(2025, 6, 1, 12, 0, 0),
            )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.ENQUEUE_FAILED
        assert 'kwargs' in result.err_value.message

    async def test_happy_path_calls_broker_with_correct_params(self) -> None:
        """Full success: broker.enqueue_async called with deterministic task_id and sent_at=slot_time."""
        # Strict-serde: positional args are rejected by the scheduler;
        # kwargs-only is the documented usage.
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'a': 1, 'b': 2, 'key': 'val'},
        )
        config = ScheduleConfig(schedules=[schedule])
        task_mock = _make_task_mock(lambda a, b, key: None)
        app = _make_app(schedule_config=config, tasks={'my_task': task_mock})
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock()
        scheduler.broker.enqueue_async = AsyncMock(return_value=Ok('task-abc'))

        slot = _utc(2025, 6, 1, 12, 0, 0)
        result = await scheduler._enqueue_scheduled_task(schedule, slot_time=slot)

        assert not is_err(result)
        assert result.ok_value == 'task-abc'

        # Verify broker was called with sent_at = slot_time
        enqueue_kwargs = scheduler.broker.enqueue_async.await_args.kwargs
        assert enqueue_kwargs['sent_at'] == slot
        assert enqueue_kwargs['task_name'] == 'my_task'
        assert enqueue_kwargs['task_id'] is not None

    async def test_no_args_skips_serialization(self) -> None:
        """Schedule with no args/kwargs → args_json=None, kwargs_json=None."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        config = ScheduleConfig(schedules=[schedule])
        task_mock = _make_task_mock(lambda: 'ok')
        app = _make_app(schedule_config=config, tasks={'my_task': task_mock})
        scheduler = Scheduler(app)
        scheduler.broker = _make_broker_mock()
        scheduler.broker.enqueue_async = AsyncMock(return_value=Ok('task-xyz'))

        slot = _utc(2025, 6, 1, 12, 0, 0)
        result = await scheduler._enqueue_scheduled_task(schedule, slot_time=slot)

        assert not is_err(result)
        enqueue_kwargs = scheduler.broker.enqueue_async.await_args.kwargs
        assert enqueue_kwargs['args_json'] is None
        assert enqueue_kwargs['kwargs_json'] is None


# =============================================================================
# Remaining coverage gap tests
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestInitializeSingleScheduleAlreadyInitialized:
    """Cover the else branch when config hash matches (L234)."""

    async def test_state_exists_hash_matches_no_update(self) -> None:
        """Existing state with matching hash → no-op (debug log only)."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        # Compute the real hash to simulate matching state
        real_hash = scheduler._compute_config_hash(schedule)

        existing_state = MagicMock()
        existing_state.config_hash = real_hash
        existing_state.next_run_at = _utc(2025, 6, 1, 13, 0, 0)

        state_manager = MagicMock()
        state_manager.get_state = AsyncMock(return_value=existing_state)
        state_manager.initialize_state = AsyncMock()
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        await scheduler._initialize_single_schedule(
            schedule,
            _utc(2025, 6, 1, 12, 0, 0),
        )

        # Neither initialize nor update should be called
        state_manager.initialize_state.assert_not_awaited()
        state_manager.update_next_run.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckAndRunSchedulesOrphanDueState:
    """Cover the 'continue' when due state has no matching enabled schedule (L263)."""

    async def test_due_state_without_matching_schedule_skipped(self) -> None:
        """Due state for unknown schedule_name → skipped via continue."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='real',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        orphan_state = MagicMock()
        orphan_state.schedule_name = 'deleted_schedule'
        real_state = MagicMock()
        real_state.schedule_name = 'real'

        state_manager = MagicMock()
        state_manager.get_existing_names = AsyncMock(return_value={'real'})
        state_manager.get_due_states = AsyncMock(return_value=[orphan_state, real_state])
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        checked: list[str] = []

        async def _track(schedule: object, now: object) -> None:
            checked.append(schedule.name)  # type: ignore[union-attr]

        scheduler._check_schedule = AsyncMock(side_effect=_track)  # type: ignore[method-assign]

        await scheduler._check_and_run_schedules()

        # Only 'real' should be checked, orphan is skipped
        assert checked == ['real']


@pytest.mark.unit
@pytest.mark.asyncio
class TestEnsureStatesExist:
    """Tick-loop self-heal: missing state rows are re-initialized.

    Regression for the startup-init liveness gap: a schedule whose row
    creation failed at start() was invisible to get_due_states and stayed
    dormant until restart.
    """

    def _scheduler_with_mock_manager(
        self, names: list[str],
    ) -> tuple[Scheduler, MagicMock]:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name=name,
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                )
                for name in names
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        state_manager = MagicMock()
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()
        return scheduler, state_manager

    async def test_missing_row_reinitialized_on_tick(self) -> None:
        """A schedule without a state row gets initialized by the tick."""
        scheduler, state_manager = self._scheduler_with_mock_manager(
            ['healthy', 'dormant'],
        )
        state_manager.get_existing_names = AsyncMock(return_value={'healthy'})
        state_manager.get_due_states = AsyncMock(return_value=[])
        inited: list[str] = []

        async def _track_init(schedule: object, now: object) -> None:
            inited.append(schedule.name)  # type: ignore[union-attr]

        scheduler._initialize_single_schedule = AsyncMock(  # type: ignore[method-assign]
            side_effect=_track_init,
        )

        await scheduler._check_and_run_schedules()

        assert inited == ['dormant']
        state_manager.get_due_states.assert_awaited_once()

    async def test_init_failure_logged_and_tick_proceeds(self) -> None:
        """A failing re-init neither raises nor blocks the rest of the tick."""
        scheduler, state_manager = self._scheduler_with_mock_manager(
            ['bad', 'also_missing'],
        )
        state_manager.get_existing_names = AsyncMock(return_value=set())
        state_manager.get_due_states = AsyncMock(return_value=[])
        attempted: list[str] = []

        async def _flaky_init(schedule: object, now: object) -> None:
            attempted.append(schedule.name)  # type: ignore[union-attr]
            if schedule.name == 'also_missing':  # type: ignore[union-attr]
                raise RuntimeError('db error')

        scheduler._initialize_single_schedule = AsyncMock(  # type: ignore[method-assign]
            side_effect=_flaky_init,
        )

        await scheduler._check_and_run_schedules()

        # Both missing schedules attempted (sorted order), failure contained,
        # and the tick still reached the due-states read.
        assert attempted == ['also_missing', 'bad']
        state_manager.get_due_states.assert_awaited_once()

    async def test_no_missing_rows_no_init(self) -> None:
        """All rows present: no initialization happens."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a', 'b'])
        state_manager.get_existing_names = AsyncMock(return_value={'a', 'b'})
        state_manager.get_due_states = AsyncMock(return_value=[])
        scheduler._initialize_single_schedule = AsyncMock()  # type: ignore[method-assign]

        await scheduler._check_and_run_schedules()

        scheduler._initialize_single_schedule.assert_not_awaited()

    async def test_existence_read_failure_raises_to_tick_seam(self) -> None:
        """A DB error on the existence read escapes to run_forever's
        per-tick handler (the whole tick is retried next interval)."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.get_existing_names = AsyncMock(
            side_effect=RuntimeError('db down'),
        )
        state_manager.get_due_states = AsyncMock(return_value=[])

        with pytest.raises(RuntimeError, match='db down'):
            await scheduler._check_and_run_schedules()

        state_manager.get_due_states.assert_not_awaited()

    async def test_healthy_check_skipped_until_interval_elapses(self) -> None:
        """While healthy, only the first tick reads existence; subsequent
        ticks inside the interval skip it. Due states are read every tick."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.get_existing_names = AsyncMock(return_value={'a'})
        state_manager.get_due_states = AsyncMock(return_value=[])

        for _tick in range(5):
            await scheduler._check_and_run_schedules()

        assert state_manager.get_existing_names.await_count == 1
        assert state_manager.get_due_states.await_count == 5

    async def test_healthy_check_reruns_after_interval(self) -> None:
        """The existence check runs again once the tick interval elapses."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.get_existing_names = AsyncMock(return_value={'a'})
        state_manager.get_due_states = AsyncMock(return_value=[])

        for _tick in range(scheduler._existence_check_interval_ticks + 1):
            await scheduler._check_and_run_schedules()

        assert state_manager.get_existing_names.await_count == 2

    async def test_interval_ticks_derived_from_check_interval(self) -> None:
        """The tick count preserves the ~60 s wall-clock cadence across
        tick configs; a tick longer than the target floors at 1."""
        for seconds, expected_ticks in ((1, 60), (10, 6), (60, 1)):
            config = ScheduleConfig(
                schedules=[
                    TaskSchedule(
                        name='a',
                        task_name='my_task',
                        pattern=IntervalSchedule(seconds=5),
                    ),
                ],
                check_interval_seconds=seconds,
            )
            app = _make_app(schedule_config=config)
            scheduler = Scheduler(app)
            assert (
                scheduler._existence_check_interval_ticks == expected_ticks
            ), f'check_interval_seconds={seconds}'

    async def test_unhealthy_pass_rechecks_next_tick(self) -> None:
        """A failed re-init keeps the check running every tick until a
        fully healthy pass, then the interval cadence resumes."""
        scheduler, state_manager = self._scheduler_with_mock_manager(
            ['flaky', 'ok'],
        )
        state_manager.get_existing_names = AsyncMock(
            side_effect=[{'ok'}, {'ok'}, {'ok', 'flaky'}, {'ok', 'flaky'}],
        )
        state_manager.get_due_states = AsyncMock(return_value=[])
        init_attempts = 0

        async def _fail_once_then_succeed(schedule: object, now: object) -> None:
            nonlocal init_attempts
            init_attempts += 1
            if init_attempts == 1:
                raise RuntimeError('db error')

        scheduler._initialize_single_schedule = AsyncMock(  # type: ignore[method-assign]
            side_effect=_fail_once_then_succeed,
        )

        # Tick 1: missing + init fails -> unhealthy, recheck next tick.
        # Tick 2: missing + init succeeds -> healthy, cadence resumes.
        # Ticks 3-4: inside the interval -> no existence reads.
        for _tick in range(4):
            await scheduler._check_and_run_schedules()

        assert state_manager.get_existing_names.await_count == 2
        assert init_attempts == 2

    async def test_existence_read_failure_rechecks_on_retried_tick(self) -> None:
        """A raise on the existence read leaves the countdown armed: the
        retried tick runs the check again instead of skipping it."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.get_existing_names = AsyncMock(
            side_effect=[RuntimeError('db down'), {'a'}],
        )
        state_manager.get_due_states = AsyncMock(return_value=[])

        with pytest.raises(RuntimeError, match='db down'):
            await scheduler._check_and_run_schedules()
        await scheduler._check_and_run_schedules()

        assert state_manager.get_existing_names.await_count == 2
        state_manager.get_due_states.assert_awaited_once()


@pytest.mark.unit
class TestPreloadTaskModulesAdditionalPaths:
    """Cover suppress_sends exceptions and file path import failure."""

    def test_suppress_sends_true_failure_propagates(self) -> None:
        """suppress_sends(True) raises → error propagates to caller."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )
        scheduler.app.suppress_sends = MagicMock(
            side_effect=AttributeError('no suppress_sends'),
        )

        with pytest.raises(AttributeError, match='no suppress_sends'):
            scheduler._preload_task_modules()

    def test_suppress_sends_false_failure_propagates(self) -> None:
        """suppress_sends(False) raises in finally → error propagates."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )

        def _suppress_side_effect(value: bool) -> None:
            if not value:
                raise AttributeError('no suppress_sends')

        scheduler.app.suppress_sends = MagicMock(
            side_effect=_suppress_side_effect,
        )
        # The finally-restore passes the saved prior flag (spec change,
        # 0.1.7); pin it to False so the failing restore path is exercised.
        scheduler.app._suppress_sends = False

        with pytest.raises(AttributeError, match='no suppress_sends'):
            with patch(
                'horsies.core.scheduler.service.importlib.import_module',
            ):
                scheduler._preload_task_modules()

    def test_file_path_import_failure_logs_warning(self) -> None:
        """File ending with .py that fails import → warning logged, continues."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['tasks/bad.py', 'myapp.good'],
        )

        import_calls: list[str] = []

        with (
            patch(
                'horsies.core.scheduler.service.import_by_path',
                side_effect=ImportError('cannot import'),
            ),
            patch(
                'horsies.core.scheduler.service.importlib.import_module',
                side_effect=lambda name: import_calls.append(name),
            ),
        ):
            scheduler._preload_task_modules()

        # Despite .py import failing, module import was still attempted
        assert import_calls == ['myapp.good']

    def test_sys_path_insert_when_cwd_missing(self) -> None:
        """When cwd is not in sys.path, it gets inserted."""
        import sys

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )

        fake_cwd = '/unlikely/test/dir/xyz'
        original_sys_path = sys.path.copy()

        try:
            # Ensure fake cwd is not in sys.path
            sys.path = [p for p in sys.path if p != fake_cwd]

            with (
                patch('os.getcwd', return_value=fake_cwd),
                patch(
                    'horsies.core.scheduler.service.importlib.import_module',
                ),
            ):
                scheduler._preload_task_modules()

            assert fake_cwd in sys.path
        finally:
            sys.path = original_sys_path


@pytest.mark.unit
class TestValidateSchedulesCallsSignatureValidation:
    """Cover L588: _validate_schedules calls _validate_schedule_signature."""

    def test_valid_task_and_queue_calls_signature_validation(self) -> None:
        """A valid (kwargs-only) schedule reaches signature validation."""
        def my_fn(x: int) -> str:
            return str(x)

        task_mock = _make_task_mock(my_fn)

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    kwargs={'x': 42},
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': task_mock})
        scheduler = Scheduler(app)

        # Should not raise — kwargs match signature
        scheduler._validate_schedules()
