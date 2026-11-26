"""Tests for CronSchedule: typed terms, validation and serde."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    MultipleValidationErrors,
)
from horsies.core.models.schedule import (
    BothDays,
    ByMonthDay,
    ByWeekday,
    CronEnumRange,
    CronEnumStep,
    CronEnumValues,
    CronEvery,
    CronRange,
    CronSchedule,
    CronStep,
    CronValues,
    EitherDay,
    EveryDay,
    Month,
    TaskSchedule,
    Weekday,
)


# =============================================================================
# Construction
# =============================================================================


@pytest.mark.unit
class TestCronScheduleConstruction:
    """Valid construction and field access."""

    def test_minimal_valid(self) -> None:
        """A wildcard-everything cron schedule constructs with kind 'cron'."""
        schedule = CronSchedule(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=EveryDay(),
        )

        assert schedule.kind == 'cron'
        assert schedule.day.kind == 'every_day'

    def test_mixed_terms_in_one_field(self) -> None:
        """A field accepts multiple terms whose match sets union."""
        schedule = CronSchedule(
            minute=[CronValues(values=[0]), CronStep(step=15)],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=EveryDay(),
        )

        assert len(schedule.minute) == 2

    def test_enum_range_and_step(self) -> None:
        """Month accepts enum range/step terms."""
        schedule = CronSchedule(
            minute=[CronValues(values=[0])],
            hour=[CronValues(values=[0])],
            month=[CronEnumRange[Month](start=Month.JANUARY, end=Month.MARCH)],
            day=EveryDay(),
        )

        assert schedule.month[0].kind == 'enum_range'


# =============================================================================
# Validation — single error (raises ConfigurationError directly)
# =============================================================================


@pytest.mark.unit
class TestCronScheduleValidationSingle:
    """Single-error validation paths raise ConfigurationError with the schedule code."""

    def _assert_config_error(self, **kwargs: object) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            CronSchedule(**kwargs)  # type: ignore[arg-type]
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_SCHEDULE

    def test_minute_out_of_domain(self) -> None:
        """Minute value above 59 is rejected."""
        self._assert_config_error(
            minute=[CronValues(values=[99])],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=EveryDay(),
        )

    def test_hour_out_of_domain(self) -> None:
        """Hour value above 23 is rejected."""
        self._assert_config_error(
            minute=[CronEvery()],
            hour=[CronValues(values=[24])],
            month=[CronEvery()],
            day=EveryDay(),
        )

    def test_day_of_month_out_of_domain(self) -> None:
        """Day-of-month above 31 is rejected."""
        self._assert_config_error(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=ByMonthDay(day_of_month=[CronValues(values=[32])]),
        )

    def test_numeric_range_start_after_end(self) -> None:
        """A numeric range with start > end is rejected."""
        self._assert_config_error(
            minute=[CronRange(start=30, end=10)],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=EveryDay(),
        )

    def test_enum_range_wraparound_rejected(self) -> None:
        """A weekday range that wraps (FRI-MON) is rejected; use explicit values."""
        self._assert_config_error(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=ByWeekday(
                day_of_week=[
                    CronEnumRange[Weekday](start=Weekday.FRIDAY, end=Weekday.MONDAY)
                ]
            ),
        )

    def test_month_range_wraparound_rejected(self) -> None:
        """A month range that wraps (DEC-FEB) is rejected."""
        self._assert_config_error(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEnumRange[Month](start=Month.DECEMBER, end=Month.FEBRUARY)],
            day=EveryDay(),
        )


# =============================================================================
# Validation — step span (intentionally stricter than some cron implementations)
# =============================================================================


@pytest.mark.unit
class TestCronScheduleStepSpan:
    """Step must be <= the field span; a larger step collapses to one value."""

    def test_minute_step_at_span_allowed(self) -> None:
        """Minute step equal to the span (59) is allowed -> {0, 59}."""
        schedule = CronSchedule(
            minute=[CronStep(step=59)],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=EveryDay(),
        )
        assert schedule.minute[0].kind == 'step'

    def test_minute_step_over_span_rejected(self) -> None:
        """Minute step above the span (60) is rejected."""
        with pytest.raises(ConfigurationError) as exc_info:
            CronSchedule(
                minute=[CronStep(step=60)],
                hour=[CronEvery()],
                month=[CronEvery()],
                day=EveryDay(),
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_SCHEDULE

    def test_day_of_month_step_at_span_allowed(self) -> None:
        """Day-of-month step at span (30) is allowed -> {1, 31}."""
        schedule = CronSchedule(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEvery()],
            day=ByMonthDay(day_of_month=[CronStep(step=30)]),
        )
        assert isinstance(schedule.day, ByMonthDay)

    def test_day_of_month_step_over_span_rejected(self) -> None:
        """Day-of-month step above span (31) is rejected."""
        with pytest.raises(ConfigurationError):
            CronSchedule(
                minute=[CronEvery()],
                hour=[CronEvery()],
                month=[CronEvery()],
                day=ByMonthDay(day_of_month=[CronStep(step=31)]),
            )

    def test_month_step_at_span_allowed(self) -> None:
        """Month step at span (11) is allowed -> {JAN, DEC}."""
        schedule = CronSchedule(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEnumStep[Month](step=11)],
            day=EveryDay(),
        )
        assert schedule.month[0].kind == 'enum_step'

    def test_month_step_over_span_rejected(self) -> None:
        """Month step above span (12) is rejected."""
        with pytest.raises(ConfigurationError):
            CronSchedule(
                minute=[CronEvery()],
                hour=[CronEvery()],
                month=[CronEnumStep[Month](step=12)],
                day=EveryDay(),
            )


# =============================================================================
# Validation — satisfiability
# =============================================================================


@pytest.mark.unit
class TestCronScheduleSatisfiability:
    """Reject month x day-of-month combinations that can never occur."""

    def test_february_30_rejected(self) -> None:
        """February + day 30 can never fire."""
        with pytest.raises(ConfigurationError) as exc_info:
            CronSchedule(
                minute=[CronEvery()],
                hour=[CronEvery()],
                month=[CronEnumValues[Month](values=[Month.FEBRUARY])],
                day=ByMonthDay(day_of_month=[CronValues(values=[30])]),
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_SCHEDULE

    def test_february_29_allowed(self) -> None:
        """February + day 29 is valid (leap years exist)."""
        schedule = CronSchedule(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEnumValues[Month](values=[Month.FEBRUARY])],
            day=ByMonthDay(day_of_month=[CronValues(values=[29])]),
        )
        assert isinstance(schedule.day, ByMonthDay)

    def test_april_31_rejected(self) -> None:
        """April (30 days) + day 31 can never fire."""
        with pytest.raises(ConfigurationError):
            CronSchedule(
                minute=[CronEvery()],
                hour=[CronEvery()],
                month=[CronEnumValues[Month](values=[Month.APRIL])],
                day=ByMonthDay(day_of_month=[CronValues(values=[31])]),
            )

    def test_either_day_impossible_dom_still_valid(self) -> None:
        """EitherDay fires via its weekday branch even if day-of-month is impossible."""
        schedule = CronSchedule(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEnumValues[Month](values=[Month.FEBRUARY])],
            day=EitherDay(
                day_of_month=[CronValues(values=[30])],
                day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
            ),
        )
        assert isinstance(schedule.day, EitherDay)

    def test_both_days_impossible_dom_rejected(self) -> None:
        """BothDays requires the day-of-month to be reachable in the month."""
        with pytest.raises(ConfigurationError):
            CronSchedule(
                minute=[CronEvery()],
                hour=[CronEvery()],
                month=[CronEnumValues[Month](values=[Month.FEBRUARY])],
                day=BothDays(
                    day_of_month=[CronValues(values=[30])],
                    day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
                ),
            )


# =============================================================================
# Validation — multiple errors and pydantic-level rejections
# =============================================================================


@pytest.mark.unit
class TestCronScheduleValidationMultiAndPydantic:
    """Multi-error collection and field-level pydantic rejections."""

    def test_two_domain_errors_collected(self) -> None:
        """Two out-of-domain fields raise MultipleValidationErrors, not one."""
        with pytest.raises(MultipleValidationErrors):
            CronSchedule(
                minute=[CronValues(values=[99])],
                hour=[CronValues(values=[99])],
                month=[CronEvery()],
                day=EveryDay(),
            )

    def test_empty_field_rejected(self) -> None:
        """An empty term list violates min_length at the pydantic layer."""
        with pytest.raises(ValidationError):
            CronSchedule(
                minute=[],
                hour=[CronEvery()],
                month=[CronEvery()],
                day=EveryDay(),
            )

    def test_step_zero_rejected_at_construction(self) -> None:
        """CronStep(step=0) violates ge=1 on the term itself."""
        with pytest.raises(ValidationError):
            CronStep(step=0)

    def test_weekday_term_in_month_field_rejected(self) -> None:
        """A weekday-typed enum term cannot populate the month field.

        The pyright ignore is intentional: this is *also* a static type error
        (the field-specific aliases forbid it), and the test asserts the runtime
        layer rejects it too.
        """
        with pytest.raises(ValidationError):
            CronSchedule(
                minute=[CronEvery()],
                hour=[CronEvery()],
                month=[CronEnumValues[Weekday](values=[Weekday.MONDAY])],  # type: ignore[list-item]
                day=EveryDay(),
            )


# =============================================================================
# Serde / round-trip
# =============================================================================


@pytest.mark.unit
class TestCronScheduleSerde:
    """JSON-mode dump/validate round-trips, including through the pattern union."""

    def _rich(self) -> CronSchedule:
        return CronSchedule(
            minute=[CronValues(values=[0, 30])],
            hour=[CronStep(step=4)],
            month=[CronEnumRange[Month](start=Month.JANUARY, end=Month.MARCH)],
            day=EitherDay(
                day_of_month=[CronValues(values=[13])],
                day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
            ),
        )

    def test_json_dump_carries_discriminators(self) -> None:
        """model_dump(mode='json') emits the term/day discriminators."""
        dumped = self._rich().model_dump(mode='json')

        assert dumped['kind'] == 'cron'
        assert dumped['month'][0]['kind'] == 'enum_range'
        assert dumped['day']['kind'] == 'either_day'

    def test_direct_round_trip(self) -> None:
        """A CronSchedule survives model_dump(mode='json') -> model_validate."""
        original = self._rich()

        restored = CronSchedule.model_validate(original.model_dump(mode='json'))

        assert restored == original

    def test_task_schedule_round_trip(self) -> None:
        """A serialized cron pattern round-trips through TaskSchedule's union."""
        task = TaskSchedule(
            name='nightly',
            task_name='do_work',
            pattern=self._rich(),
        )

        dumped = task.model_dump(mode='json')
        restored = TaskSchedule.model_validate(dumped)

        assert isinstance(restored.pattern, CronSchedule)
        assert restored.pattern == self._rich()

    def test_month_enum_serializes_as_string(self) -> None:
        """Month serializes to its string value (hash-stable for config diffing)."""
        dumped = CronSchedule(
            minute=[CronEvery()],
            hour=[CronEvery()],
            month=[CronEnumValues[Month](values=[Month.JUNE])],
            day=EveryDay(),
        ).model_dump(mode='json')

        assert dumped['month'][0]['values'] == ['june']
