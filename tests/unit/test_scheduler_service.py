"""Tests for Scheduler service-level methods (mock-based, no DB)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.errors import ConfigurationError, ErrorCode, RegistryError
from horsies.core.models.schedule import (
    IntervalSchedule,
    ScheduleConfig,
    TaskSchedule,
)
from horsies.core.scheduler.service import Scheduler


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
    """Build a minimal mock Horsies app for Scheduler tests."""
    app = MagicMock()
    app.config.schedule = schedule_config
    app.tasks = tasks or {}
    # Default validate_queue_name returns 'default'
    app.validate_queue_name = MagicMock(return_value='default')
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
        """App without schedule config raises ConfigurationError E205."""
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
# _validate_schedule_signature
# =============================================================================


@pytest.mark.unit
class TestValidateScheduleSignature:
    """Tests for Scheduler._validate_schedule_signature."""

    def _make_scheduler(
        self,
        tasks: dict[str, MagicMock],
    ) -> Scheduler:
        """Helper to build Scheduler with given tasks."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='task_a',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks=tasks)
        return Scheduler(app)

    def test_no_required_params_ok(self) -> None:
        """Function with no required params passes validation."""
        def no_params() -> str:
            return 'ok'

        task_mock = _make_task_mock(no_params)
        scheduler = self._make_scheduler({'task_a': task_mock})
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
        )

        # Should not raise
        scheduler._validate_schedule_signature(schedule)

    def test_missing_required_raises_configuration_error(self) -> None:
        """Missing required param raises ConfigurationError E205."""
        def needs_value(value: int) -> str:
            return str(value)

        task_mock = _make_task_mock(needs_value)
        scheduler = self._make_scheduler({'task_a': task_mock})
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
        )

        with pytest.raises(ConfigurationError) as exc_info:
            scheduler._validate_schedule_signature(schedule)

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'args/kwargs do not match' in exc.message

    def test_unexpected_kwarg_raises_configuration_error(self) -> None:
        """Unexpected kwargs are rejected at scheduler startup."""

        def needs_value(value: int) -> str:
            return str(value)

        task_mock = _make_task_mock(needs_value)
        scheduler = self._make_scheduler({'task_a': task_mock})
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'unknown': 42},
        )

        with pytest.raises(ConfigurationError) as exc_info:
            scheduler._validate_schedule_signature(schedule)

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'args/kwargs do not match' in exc.message

    def test_satisfied_by_args(self) -> None:
        """Required param satisfied by positional args passes."""
        def needs_value(value: int) -> str:
            return str(value)

        task_mock = _make_task_mock(needs_value)
        scheduler = self._make_scheduler({'task_a': task_mock})
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            args=(42,),
        )

        # Should not raise
        scheduler._validate_schedule_signature(schedule)

    def test_satisfied_by_kwargs(self) -> None:
        """Required param satisfied by keyword args passes."""
        def needs_value(value: int) -> str:
            return str(value)

        task_mock = _make_task_mock(needs_value)
        scheduler = self._make_scheduler({'task_a': task_mock})
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'value': 42},
        )

        # Should not raise
        scheduler._validate_schedule_signature(schedule)

    def test_mixed_args_and_kwargs(self) -> None:
        """Multiple required params satisfied by mix of args and kwargs."""
        def needs_both(a: int, b: str) -> str:
            return f'{a}{b}'

        task_mock = _make_task_mock(needs_both)
        scheduler = self._make_scheduler({'task_a': task_mock})
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            args=(1,),
            kwargs={'b': 'hello'},
        )

        # Should not raise
        scheduler._validate_schedule_signature(schedule)


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
# _validate_schedules
# =============================================================================


@pytest.mark.unit
class TestValidateSchedules:
    """Tests for Scheduler._validate_schedules."""

    def test_unregistered_task_raises_registry_error(self) -> None:
        """Schedule referencing unregistered task raises RegistryError E300."""
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
        """Invalid queue configuration raises ConfigurationError E205."""
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
    async def test_prunes_removed_schedule_state_rows(self) -> None:
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

        stale_state = MagicMock()
        stale_state.schedule_name = 'removed'
        kept_state = MagicMock()
        kept_state.schedule_name = 'kept'

        state_manager = MagicMock()
        state_manager.get_all_states = AsyncMock(return_value=[stale_state, kept_state])
        state_manager.delete_state = AsyncMock(return_value=True)
        state_manager.get_state = AsyncMock(return_value=None)
        state_manager.initialize_state = AsyncMock()
        state_manager.update_next_run = AsyncMock()
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        state_manager.get_all_states.assert_awaited_once()
        state_manager.delete_state.assert_awaited_once_with('removed')


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

        scheduler._enqueue_scheduled_task = AsyncMock(return_value='task-1')  # type: ignore[method-assign]

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
