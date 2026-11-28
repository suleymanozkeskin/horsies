"""Tests for schedule model classes, validators, defaults, and boundaries."""

from __future__ import annotations

from collections.abc import Callable
from datetime import time as datetime_time
from datetime import timedelta, timezone

import pytest
from pydantic import ValidationError

from horsies.core.errors import (
    ConfigurationError,
    HorsiesErrorCode,
    MultipleValidationErrors,
)
from horsies.core.models.schedule import (
    INTERVAL_EVERY_SECONDS_MAX,
    INTERVAL_EVERY_SECONDS_MIN,
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

    def test_valid_construction(self) -> None:
        """An interval holds its length in `every`."""
        schedule = IntervalSchedule(every=timedelta(seconds=30))

        assert schedule.every == timedelta(seconds=30)
        assert schedule.kind == 'interval'

    @pytest.mark.parametrize(
        'every',
        [
            pytest.param(
                timedelta(seconds=INTERVAL_EVERY_SECONDS_MIN),
                id='one_second',
            ),
            pytest.param(timedelta(days=1, hours=2, minutes=5, seconds=10), id='mixed'),
            pytest.param(timedelta(seconds=INTERVAL_EVERY_SECONDS_MAX), id='365_days'),
        ],
    )
    def test_a_length_inside_the_bound_is_accepted(self, every: timedelta) -> None:
        """The bound ends, 1 second and 365 days, are lengths of an interval."""
        assert IntervalSchedule(every=every).every == every

    def test_the_bound_is_1_second_to_365_days(self) -> None:
        """The bound constants hold 1 second and 365 days, in seconds."""
        assert INTERVAL_EVERY_SECONDS_MIN == 1
        assert INTERVAL_EVERY_SECONDS_MAX == timedelta(days=365) // timedelta(seconds=1)

    @pytest.mark.parametrize(
        ('every', 'note'),
        [
            pytest.param(timedelta(0), 'every: 0:00:00', id='zero'),
            pytest.param(
                timedelta(seconds=-5),
                'every: -1 day, 23:59:55',
                id='negative',
            ),
            pytest.param(
                timedelta(days=365, seconds=1),
                'every: 365 days, 0:00:01',
                id='above_365_days',
            ),
        ],
    )
    def test_a_length_outside_the_bound_is_refused(
        self, every: timedelta, note: str,
    ) -> None:
        """Zero, a negative length and a length above 365 days are refused."""
        with pytest.raises(ConfigurationError) as exc_info:
            IntervalSchedule(every=every)

        exc = exc_info.value
        assert exc.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
        assert exc.message == 'IntervalSchedule every is outside 1 second to 365 days'
        assert exc.notes == [note]
        assert exc.help_text == 'give every from 1 second to 365 days'

    def test_a_length_with_microseconds_is_refused(self) -> None:
        """Slots fire at whole seconds, so a fraction of a second is refused."""
        with pytest.raises(ConfigurationError) as exc_info:
            IntervalSchedule(every=timedelta(seconds=1, microseconds=500))

        exc = exc_info.value
        assert exc.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
        assert exc.message == 'IntervalSchedule every has microseconds'
        assert exc.notes == ['every: 0:00:01.000500']
        assert exc.help_text == (
            'schedules fire at whole seconds; give every in whole seconds'
        )

    def test_a_length_that_breaks_both_rules_reports_both(self) -> None:
        """A length below 1 second with microseconds breaks both rules."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            IntervalSchedule(every=timedelta(microseconds=500))

        messages = sorted(error.message for error in exc_info.value.report.refusals)
        assert messages == [
            'IntervalSchedule every has microseconds',
            'IntervalSchedule every is outside 1 second to 365 days',
        ]

    def test_every_is_required(self) -> None:
        """every field is required; omitting raises ValidationError."""
        with pytest.raises(ValidationError):
            IntervalSchedule()  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        ('every', 'text'),
        [
            pytest.param(timedelta(seconds=30), 'PT30S', id='seconds'),
            pytest.param(timedelta(seconds=90), 'PT1M30S', id='seconds_split'),
            pytest.param(timedelta(minutes=90), 'PT1H30M', id='minutes_split'),
            pytest.param(timedelta(days=1), 'P1D', id='one_day'),
            pytest.param(timedelta(days=1, hours=2), 'P1DT2H', id='day_and_hours'),
            pytest.param(timedelta(days=1, seconds=1), 'P1DT1S', id='day_and_second'),
            pytest.param(timedelta(days=365), 'P365D', id='365_days'),
        ],
    )
    def test_the_json_text_puts_the_largest_unit_first_and_writes_no_zero_part(
        self, every: timedelta, text: str,
    ) -> None:
        """The JSON form of every is ISO 8601 text of days, hours, minutes, seconds."""
        schedule = IntervalSchedule(every=every)

        assert schedule.model_dump(mode='json') == {'kind': 'interval', 'every': text}
        assert schedule.model_dump() == {'kind': 'interval', 'every': every}

    def test_a_json_text_reads_back_to_its_length(self) -> None:
        """The text that the model writes reads back to the same length."""
        schedule = IntervalSchedule.model_validate_json(
            '{"kind":"interval","every":"P1DT2H"}',
        )

        assert schedule.every == timedelta(days=1, hours=2)

    @pytest.mark.parametrize(
        'pattern',
        [
            pytest.param({'seconds': 30}, id='four_parts'),
            pytest.param({'every': 'PT30S', 'minutes': 1}, id='every_and_a_part'),
            pytest.param({'type': 'interval', 'every': 'PT30S'}, id='tag_type'),
        ],
    )
    def test_the_old_keys_are_refused(self, pattern: dict[str, object]) -> None:
        """The four parts and the tag `type` are unknown keys."""
        with pytest.raises(ValidationError):
            IntervalSchedule.model_validate(pattern)


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
        assert schedule.kind == 'hourly'

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
        assert schedule.kind == 'daily'

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
        """WeeklySchedule with weekdays and time."""
        schedule = WeeklySchedule(
            weekdays=[Weekday.MONDAY, Weekday.FRIDAY],
            time=datetime_time(9, 0, 0),
        )

        assert schedule.weekdays == [Weekday.MONDAY, Weekday.FRIDAY]
        assert schedule.time == datetime_time(9, 0, 0)
        assert schedule.kind == 'weekly'

    def test_duplicate_weekdays_raises_configuration_error(self) -> None:
        """Duplicate weekdays raises ConfigurationError HRS-205."""
        with pytest.raises(ConfigurationError) as exc_info:
            WeeklySchedule(
                weekdays=[Weekday.MONDAY, Weekday.MONDAY],
                time=datetime_time(9, 0, 0),
            )

        exc = exc_info.value
        assert exc.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
        assert exc.message == 'WeeklySchedule has duplicate weekdays'
        assert exc.notes == ["weekdays: ['monday', 'monday']"]
        assert exc.help_text == 'each weekday should appear only once in the list'

    def test_empty_weekdays_raises_validation_error(self) -> None:
        """Empty weekdays list violates min_length=1 constraint."""
        with pytest.raises(ValidationError):
            WeeklySchedule(weekdays=[], time=datetime_time(9, 0, 0))

    def test_all_seven_weekdays_valid(self) -> None:
        """All 7 weekdays accepted without error."""
        all_days = list(Weekday)
        schedule = WeeklySchedule(
            weekdays=all_days,
            time=datetime_time(12, 0, 0),
        )

        assert len(schedule.weekdays) == 7

    def test_the_old_key_days_is_refused(self) -> None:
        """`days` is an unknown key: the weekdays are in `weekdays`."""
        with pytest.raises(ValidationError):
            WeeklySchedule.model_validate({'days': ['monday'], 'time': '09:00:00'})


# =============================================================================
# MonthlySchedule
# =============================================================================


@pytest.mark.unit
class TestMonthlySchedule:
    """Tests for MonthlySchedule model."""

    def test_valid_construction(self) -> None:
        """MonthlySchedule with day_of_month and time."""
        schedule = MonthlySchedule(day_of_month=15, time=datetime_time(3, 0, 0))

        assert schedule.day_of_month == 15
        assert schedule.time == datetime_time(3, 0, 0)
        assert schedule.kind == 'monthly'

    def test_day_of_month_below_minimum_raises_validation_error(self) -> None:
        """day_of_month=0 violates ge=1 constraint."""
        with pytest.raises(ValidationError):
            MonthlySchedule(day_of_month=0, time=datetime_time(0, 0, 0))

    def test_day_of_month_above_maximum_raises_validation_error(self) -> None:
        """day_of_month=32 violates le=31 constraint."""
        with pytest.raises(ValidationError):
            MonthlySchedule(day_of_month=32, time=datetime_time(0, 0, 0))

    def test_the_old_key_day_is_refused(self) -> None:
        """`day` is an unknown key: the day is in `day_of_month`."""
        with pytest.raises(ValidationError):
            MonthlySchedule.model_validate({'day': 15, 'time': '03:00:00'})


# =============================================================================
# Time of day of DailySchedule, WeeklySchedule and MonthlySchedule
# =============================================================================


type _TimePattern = DailySchedule | WeeklySchedule | MonthlySchedule


def _daily(time_of_day: datetime_time) -> _TimePattern:
    return DailySchedule(time=time_of_day)


def _weekly(time_of_day: datetime_time) -> _TimePattern:
    return WeeklySchedule(weekdays=[Weekday.MONDAY], time=time_of_day)


def _monthly(time_of_day: datetime_time) -> _TimePattern:
    return MonthlySchedule(day_of_month=1, time=time_of_day)


_TIME_PATTERNS = [
    pytest.param(_daily, 'DailySchedule', id='daily'),
    pytest.param(_weekly, 'WeeklySchedule', id='weekly'),
    pytest.param(_monthly, 'MonthlySchedule', id='monthly'),
]


@pytest.mark.unit
@pytest.mark.parametrize(('build', 'pattern_name'), _TIME_PATTERNS)
class TestTimeOfDay:
    """The zone of a schedule comes only from its `timezone`, and slots fire at whole seconds."""

    def test_a_whole_second_time_without_tzinfo_is_accepted(
        self, build: Callable[[datetime_time], _TimePattern], pattern_name: str,
    ) -> None:
        pattern = build(datetime_time(9, 30, 15))

        assert pattern.time == datetime_time(9, 30, 15)
        assert pattern_name == type(pattern).__name__

    def test_a_time_with_tzinfo_is_refused_with_the_field_that_holds_the_zone(
        self, build: Callable[[datetime_time], _TimePattern], pattern_name: str,
    ) -> None:
        offset = timezone(timedelta(hours=2))

        with pytest.raises(ConfigurationError) as exc_info:
            build(datetime_time(9, 0, 0, tzinfo=offset))

        exc = exc_info.value
        assert exc.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
        assert exc.message == f'{pattern_name} time has a time zone'
        assert exc.notes == ['time: 09:00:00+02:00']
        assert exc.help_text == (
            "give the time without tzinfo and set the zone in the schedule's "
            "'timezone' field"
        )

    def test_a_time_with_microseconds_is_refused(
        self, build: Callable[[datetime_time], _TimePattern], pattern_name: str,
    ) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            build(datetime_time(9, 0, 0, 500))

        exc = exc_info.value
        assert exc.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
        assert exc.message == f'{pattern_name} time has microseconds'
        assert exc.notes == ['time: 09:00:00.000500']
        assert exc.help_text == (
            'schedules fire at whole seconds; give the time without microseconds'
        )

    def test_a_time_that_breaks_both_rules_reports_both(
        self, build: Callable[[datetime_time], _TimePattern], pattern_name: str,
    ) -> None:
        with pytest.raises(MultipleValidationErrors) as exc_info:
            build(datetime_time(9, 0, 0, 500, tzinfo=timezone.utc))

        messages = [error.message for error in exc_info.value.report.refusals]
        assert messages == [
            f'{pattern_name} time has a time zone',
            f'{pattern_name} time has microseconds',
        ]


@pytest.mark.unit
class TestTimeOfDayFromText:
    """A config read from text holds the same rules."""

    def test_a_time_text_with_an_offset_is_refused(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            DailySchedule.model_validate({'time': '09:00:00+02:00'})

        assert exc_info.value.message == 'DailySchedule time has a time zone'

    def test_a_time_text_with_a_fraction_of_a_second_is_refused(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            WeeklySchedule.model_validate(
                {'weekdays': ['monday'], 'time': '09:00:00.000500'},
            )

        assert exc_info.value.message == 'WeeklySchedule time has microseconds'


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
            pattern=IntervalSchedule(every=timedelta(seconds=10)),
        )

        assert schedule.enabled is True
        assert schedule.catch_up_missed is False
        assert schedule.max_catch_up_runs == 100
        assert schedule.timezone == 'UTC'
        assert schedule.kwargs == {}
        assert schedule.queue is None

    def test_all_fields(self) -> None:
        """All fields can be set explicitly."""
        schedule = TaskSchedule(
            name='full',
            task_name='my_task',
            pattern=IntervalSchedule(every=timedelta(minutes=5)),
            kwargs={'key': 'value'},
            queue='high',
            enabled=False,
            timezone='America/New_York',
            catch_up_missed=True,
            max_catch_up_runs=250,
        )

        assert schedule.name == 'full'
        assert schedule.task_name == 'my_task'
        assert schedule.kwargs == {'key': 'value'}
        assert schedule.queue == 'high'
        assert schedule.enabled is False
        assert schedule.timezone == 'America/New_York'
        assert schedule.catch_up_missed is True
        assert schedule.max_catch_up_runs == 250

    def test_positional_args_rejected_at_construction(self) -> None:
        """Schedules are kwargs-only; an args field is rejected (extra='forbid')."""
        with pytest.raises(ValidationError):
            TaskSchedule(
                name='s',
                task_name='my_task',
                pattern=IntervalSchedule(every=timedelta(seconds=10)),
                args=(1,),  # type: ignore[call-arg]
            )

    def test_the_old_key_queue_name_is_refused(self) -> None:
        """`queue_name` is an unknown key: the queue is in `queue`."""
        with pytest.raises(ValidationError):
            TaskSchedule.model_validate(
                {
                    'name': 's',
                    'task_name': 'my_task',
                    'pattern': {'kind': 'interval', 'every': 'PT10S'},
                    'queue_name': 'high',
                },
            )

    def test_name_is_required(self) -> None:
        """name field is required."""
        with pytest.raises(ValidationError):
            TaskSchedule(  # type: ignore[call-arg]
                task_name='my_task',
                pattern=IntervalSchedule(every=timedelta(seconds=10)),
            )

    def test_max_catch_up_runs_validation(self) -> None:
        """max_catch_up_runs must stay within configured bounds."""
        with pytest.raises(ValidationError):
            TaskSchedule(
                name='test',
                task_name='my_task',
                pattern=IntervalSchedule(every=timedelta(seconds=10)),
                max_catch_up_runs=0,
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
        """Duplicate schedule names raises ConfigurationError HRS-205."""
        with pytest.raises(ConfigurationError) as exc_info:
            ScheduleConfig(
                schedules=[
                    TaskSchedule(
                        name='dup',
                        task_name='task_a',
                        pattern=IntervalSchedule(every=timedelta(seconds=5)),
                    ),
                    TaskSchedule(
                        name='dup',
                        task_name='task_b',
                        pattern=IntervalSchedule(every=timedelta(seconds=10)),
                    ),
                ],
            )

        exc = exc_info.value
        assert exc.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
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
