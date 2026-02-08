"""Tests for schedule model classes, validators, defaults, and boundaries."""

from __future__ import annotations

from datetime import time as datetime_time

import pytest
from pydantic import ValidationError

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.schedule import (
    DailySchedule,
    HourlySchedule,
    IntervalSchedule,
    MonthlySchedule,
    ScheduleConfig,
    TaskSchedule,
    Weekday,
    WeeklySchedule,
)


# =============================================================================
# IntervalSchedule
# =============================================================================


@pytest.mark.unit
class TestIntervalSchedule:
    """Tests for IntervalSchedule model."""

    def test_valid_seconds_only(self) -> None:
        """Seconds-only interval constructs correctly."""
        schedule = IntervalSchedule(seconds=30)

        assert schedule.seconds == 30
        assert schedule.minutes is None
        assert schedule.hours is None
        assert schedule.days is None
        assert schedule.type == 'interval'

    def test_valid_all_fields(self) -> None:
        """All time units can be combined."""
        schedule = IntervalSchedule(seconds=10, minutes=5, hours=2, days=1)

        assert schedule.seconds == 10
        assert schedule.minutes == 5
        assert schedule.hours == 2
        assert schedule.days == 1

    def test_total_seconds_single_unit(self) -> None:
        """total_seconds returns correct value for single unit."""
        assert IntervalSchedule(seconds=45).total_seconds() == 45
        assert IntervalSchedule(minutes=2).total_seconds() == 120
        assert IntervalSchedule(hours=1).total_seconds() == 3600
        assert IntervalSchedule(days=1).total_seconds() == 86400

    def test_total_seconds_combined(self) -> None:
        """total_seconds sums all units correctly."""
        schedule = IntervalSchedule(seconds=10, minutes=5, hours=1, days=1)

        expected = 10 + (5 * 60) + (1 * 3600) + (1 * 86400)
        assert schedule.total_seconds() == expected

    def test_empty_raises_configuration_error(self) -> None:
        """No time units raises ConfigurationError E205."""
        with pytest.raises(ConfigurationError) as exc_info:
            IntervalSchedule()

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'at least one time unit' in exc.message
        assert any('all time units' in note for note in exc.notes)
        assert exc.help_text is not None
        assert 'seconds' in exc.help_text

    def test_seconds_below_minimum_raises_validation_error(self) -> None:
        """seconds=0 violates ge=1 constraint via pydantic ValidationError."""
        with pytest.raises(ValidationError):
            IntervalSchedule(seconds=0)

    def test_seconds_above_maximum_raises_validation_error(self) -> None:
        """seconds=86401 violates le=86400 constraint."""
        with pytest.raises(ValidationError):
            IntervalSchedule(seconds=86401)

    def test_days_above_maximum_raises_validation_error(self) -> None:
        """days=366 violates le=365 constraint."""
        with pytest.raises(ValidationError):
            IntervalSchedule(days=366)


# =============================================================================
# HourlySchedule
# =============================================================================


@pytest.mark.unit
class TestHourlySchedule:
    """Tests for HourlySchedule model."""

    def test_valid_with_default_second(self) -> None:
        """HourlySchedule with just minute uses default second=0."""
        schedule = HourlySchedule(minute=30)

        assert schedule.minute == 30
        assert schedule.second == 0
        assert schedule.type == 'hourly'

    def test_valid_with_explicit_second(self) -> None:
        """Both minute and second can be set."""
        schedule = HourlySchedule(minute=15, second=45)

        assert schedule.minute == 15
        assert schedule.second == 45

    def test_minute_below_minimum_raises_validation_error(self) -> None:
        """minute=-1 violates ge=0 constraint."""
        with pytest.raises(ValidationError):
            HourlySchedule(minute=-1)

    def test_minute_above_maximum_raises_validation_error(self) -> None:
        """minute=60 violates le=59 constraint."""
        with pytest.raises(ValidationError):
            HourlySchedule(minute=60)

    def test_second_above_maximum_raises_validation_error(self) -> None:
        """second=60 violates le=59 constraint."""
        with pytest.raises(ValidationError):
            HourlySchedule(minute=0, second=60)


# =============================================================================
# DailySchedule
# =============================================================================


@pytest.mark.unit
class TestDailySchedule:
    """Tests for DailySchedule model."""

    def test_valid_construction(self) -> None:
        """DailySchedule constructs with a time value."""
        schedule = DailySchedule(time=datetime_time(15, 30, 0))

        assert schedule.time == datetime_time(15, 30, 0)
        assert schedule.type == 'daily'

    def test_time_is_required(self) -> None:
        """time field is required; omitting raises ValidationError."""
        with pytest.raises(ValidationError):
            DailySchedule()  # type: ignore[call-arg]


# =============================================================================
# WeeklySchedule
# =============================================================================


@pytest.mark.unit
class TestWeeklySchedule:
    """Tests for WeeklySchedule model."""

    def test_valid_construction(self) -> None:
        """WeeklySchedule with days and time."""
        schedule = WeeklySchedule(
            days=[Weekday.MONDAY, Weekday.FRIDAY],
            time=datetime_time(9, 0, 0),
        )

        assert schedule.days == [Weekday.MONDAY, Weekday.FRIDAY]
        assert schedule.time == datetime_time(9, 0, 0)
        assert schedule.type == 'weekly'

    def test_duplicate_days_raises_configuration_error(self) -> None:
        """Duplicate days raises ConfigurationError E205."""
        with pytest.raises(ConfigurationError) as exc_info:
            WeeklySchedule(
                days=[Weekday.MONDAY, Weekday.MONDAY],
                time=datetime_time(9, 0, 0),
            )

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'duplicate days' in exc.message
        assert exc.help_text is not None

    def test_empty_days_raises_validation_error(self) -> None:
        """Empty days list violates min_length=1 constraint."""
        with pytest.raises(ValidationError):
            WeeklySchedule(days=[], time=datetime_time(9, 0, 0))

    def test_all_seven_days_valid(self) -> None:
        """All 7 weekdays accepted without error."""
        all_days = list(Weekday)
        schedule = WeeklySchedule(
            days=all_days,
            time=datetime_time(12, 0, 0),
        )

        assert len(schedule.days) == 7


# =============================================================================
# MonthlySchedule
# =============================================================================


@pytest.mark.unit
class TestMonthlySchedule:
    """Tests for MonthlySchedule model."""

    def test_valid_construction(self) -> None:
        """MonthlySchedule with day and time."""
        schedule = MonthlySchedule(day=15, time=datetime_time(3, 0, 0))

        assert schedule.day == 15
        assert schedule.time == datetime_time(3, 0, 0)
        assert schedule.type == 'monthly'

    def test_day_below_minimum_raises_validation_error(self) -> None:
        """day=0 violates ge=1 constraint."""
        with pytest.raises(ValidationError):
            MonthlySchedule(day=0, time=datetime_time(0, 0, 0))

    def test_day_above_maximum_raises_validation_error(self) -> None:
        """day=32 violates le=31 constraint."""
        with pytest.raises(ValidationError):
            MonthlySchedule(day=32, time=datetime_time(0, 0, 0))


# =============================================================================
# TaskSchedule
# =============================================================================


@pytest.mark.unit
class TestTaskSchedule:
    """Tests for TaskSchedule model."""

    def test_defaults(self) -> None:
        """Verify default values for optional fields."""
        schedule = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
        )

        assert schedule.enabled is True
        assert schedule.catch_up_missed is False
        assert schedule.timezone == 'UTC'
        assert schedule.args == ()
        assert schedule.kwargs == {}
        assert schedule.queue_name is None

    def test_all_fields(self) -> None:
        """All fields can be set explicitly."""
        schedule = TaskSchedule(
            name='full',
            task_name='my_task',
            pattern=IntervalSchedule(minutes=5),
            args=(1, 'two'),
            kwargs={'key': 'value'},
            queue_name='high',
            enabled=False,
            timezone='America/New_York',
            catch_up_missed=True,
        )

        assert schedule.name == 'full'
        assert schedule.task_name == 'my_task'
        assert schedule.args == (1, 'two')
        assert schedule.kwargs == {'key': 'value'}
        assert schedule.queue_name == 'high'
        assert schedule.enabled is False
        assert schedule.timezone == 'America/New_York'
        assert schedule.catch_up_missed is True

    def test_name_is_required(self) -> None:
        """name field is required."""
        with pytest.raises(ValidationError):
            TaskSchedule(  # type: ignore[call-arg]
                task_name='my_task',
                pattern=IntervalSchedule(seconds=10),
            )


# =============================================================================
# ScheduleConfig
# =============================================================================


@pytest.mark.unit
class TestScheduleConfig:
    """Tests for ScheduleConfig model."""

    def test_defaults(self) -> None:
        """Verify default values."""
        config = ScheduleConfig()

        assert config.enabled is True
        assert config.schedules == []
        assert config.check_interval_seconds == 1

    def test_duplicate_names_raises_configuration_error(self) -> None:
        """Duplicate schedule names raises ConfigurationError E205."""
        with pytest.raises(ConfigurationError) as exc_info:
            ScheduleConfig(
                schedules=[
                    TaskSchedule(
                        name='dup',
                        task_name='task_a',
                        pattern=IntervalSchedule(seconds=5),
                    ),
                    TaskSchedule(
                        name='dup',
                        task_name='task_b',
                        pattern=IntervalSchedule(seconds=10),
                    ),
                ],
            )

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'duplicate schedule names' in exc.message
        assert exc.help_text is not None

    def test_check_interval_below_minimum_raises_validation_error(self) -> None:
        """check_interval_seconds=0 violates ge=1 constraint."""
        with pytest.raises(ValidationError):
            ScheduleConfig(check_interval_seconds=0)

    def test_check_interval_above_maximum_raises_validation_error(self) -> None:
        """check_interval_seconds=61 violates le=60 constraint."""
        with pytest.raises(ValidationError):
            ScheduleConfig(check_interval_seconds=61)

    def test_empty_schedules_is_valid(self) -> None:
        """Empty schedule list is valid (scheduler just has nothing to do)."""
        config = ScheduleConfig(schedules=[])

        assert config.schedules == []


# =============================================================================
# Weekday Enum
# =============================================================================


@pytest.mark.unit
class TestWeekdayEnum:
    """Tests for Weekday enum."""

    def test_all_seven_values(self) -> None:
        """Weekday has exactly 7 members with expected values."""
        expected = {
            'MONDAY': 'monday',
            'TUESDAY': 'tuesday',
            'WEDNESDAY': 'wednesday',
            'THURSDAY': 'thursday',
            'FRIDAY': 'friday',
            'SATURDAY': 'saturday',
            'SUNDAY': 'sunday',
        }
        assert len(Weekday) == 7
        for name, value in expected.items():
            assert Weekday[name].value == value

    def test_is_string_enum(self) -> None:
        """Weekday inherits from str, so members are strings."""
        assert isinstance(Weekday.MONDAY, str)
        assert Weekday.MONDAY == 'monday'
