"""Tests for schedule calculator functions (pure, deterministic)."""

from __future__ import annotations

from datetime import datetime, time as datetime_time, timezone

import pytest

from horsies.core.models.schedule import (
    DailySchedule,
    HourlySchedule,
    IntervalSchedule,
    MonthlySchedule,
    Weekday,
    WeeklySchedule,
)
from horsies.core.scheduler.calculator import calculate_next_run, should_run_now


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    """Helper to construct a UTC-aware datetime."""
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


# =============================================================================
# IntervalSchedule calculator
# =============================================================================


@pytest.mark.unit
class TestCalculateNextRunInterval:
    """Tests for calculate_next_run with IntervalSchedule."""

    def test_seconds_only(self) -> None:
        """Adds interval seconds to from_time."""
        from_time = _utc(2025, 6, 1, 12, 0, 0)
        pattern = IntervalSchedule(seconds=30)

        result = calculate_next_run(pattern, from_time)

        assert result == _utc(2025, 6, 1, 12, 0, 30)

    def test_combined_units(self) -> None:
        """Combined days+hours+minutes+seconds are summed correctly."""
        from_time = _utc(2025, 6, 1, 0, 0, 0)
        pattern = IntervalSchedule(days=1, hours=2, minutes=30, seconds=15)

        result = calculate_next_run(pattern, from_time)

        expected = _utc(2025, 6, 2, 2, 30, 15)
        assert result == expected

    def test_result_is_utc(self) -> None:
        """Result is always UTC-aware regardless of timezone parameter."""
        from_time = _utc(2025, 6, 1, 12, 0, 0)
        pattern = IntervalSchedule(hours=1)

        result = calculate_next_run(pattern, from_time, tz_str='America/New_York')

        assert result.tzinfo == timezone.utc


# =============================================================================
# HourlySchedule calculator
# =============================================================================


@pytest.mark.unit
class TestCalculateNextRunHourly:
    """Tests for calculate_next_run with HourlySchedule."""

    def test_before_target_minute_returns_same_hour(self) -> None:
        """When current time is before target minute, returns same hour."""
        from_time = _utc(2025, 6, 1, 14, 10, 0)
        pattern = HourlySchedule(minute=30)

        result = calculate_next_run(pattern, from_time)

        assert result == _utc(2025, 6, 1, 14, 30, 0)

    def test_after_target_minute_returns_next_hour(self) -> None:
        """When current time is past target minute, returns next hour."""
        from_time = _utc(2025, 6, 1, 14, 45, 0)
        pattern = HourlySchedule(minute=30)

        result = calculate_next_run(pattern, from_time)

        assert result == _utc(2025, 6, 1, 15, 30, 0)


# =============================================================================
# DailySchedule calculator
# =============================================================================


@pytest.mark.unit
class TestCalculateNextRunDaily:
    """Tests for calculate_next_run with DailySchedule."""

    def test_before_target_time_returns_today(self) -> None:
        """When current time is before target, returns today."""
        from_time = _utc(2025, 6, 1, 8, 0, 0)
        pattern = DailySchedule(time=datetime_time(15, 0, 0))

        result = calculate_next_run(pattern, from_time)

        assert result.day == 1
        assert result.hour == 15

    def test_after_target_time_returns_tomorrow(self) -> None:
        """When current time is past target, returns tomorrow."""
        from_time = _utc(2025, 6, 1, 16, 0, 0)
        pattern = DailySchedule(time=datetime_time(15, 0, 0))

        result = calculate_next_run(pattern, from_time)

        assert result.day == 2
        assert result.hour == 15


# =============================================================================
# WeeklySchedule calculator
# =============================================================================


@pytest.mark.unit
class TestCalculateNextRunWeekly:
    """Tests for calculate_next_run with WeeklySchedule."""

    def test_today_before_time_returns_today(self) -> None:
        """2025-06-02 is Monday; before target time returns same day."""
        # 2025-06-02 is Monday
        from_time = _utc(2025, 6, 2, 8, 0, 0)
        pattern = WeeklySchedule(
            days=[Weekday.MONDAY],
            time=datetime_time(17, 0, 0),
        )

        result = calculate_next_run(pattern, from_time)

        assert result.day == 2
        assert result.hour == 17

    def test_today_after_time_wraps_to_next_week(self) -> None:
        """Monday after target time with only Monday scheduled wraps to next Monday."""
        from_time = _utc(2025, 6, 2, 18, 0, 0)
        pattern = WeeklySchedule(
            days=[Weekday.MONDAY],
            time=datetime_time(17, 0, 0),
        )

        result = calculate_next_run(pattern, from_time)

        assert result.day == 9  # next Monday
        assert result.hour == 17

    def test_next_matching_day_this_week(self) -> None:
        """Monday from_time with Wednesday scheduled returns Wednesday."""
        from_time = _utc(2025, 6, 2, 18, 0, 0)  # Monday evening
        pattern = WeeklySchedule(
            days=[Weekday.WEDNESDAY],
            time=datetime_time(9, 0, 0),
        )

        result = calculate_next_run(pattern, from_time)

        assert result.day == 4  # Wednesday
        assert result.hour == 9

    def test_wrap_to_next_week_first_day(self) -> None:
        """Friday from_time with Monday scheduled wraps to next week Monday."""
        from_time = _utc(2025, 6, 6, 18, 0, 0)  # Friday evening
        pattern = WeeklySchedule(
            days=[Weekday.MONDAY],
            time=datetime_time(9, 0, 0),
        )

        result = calculate_next_run(pattern, from_time)

        assert result.day == 9  # next Monday
        assert result.weekday() == 0  # Monday


# =============================================================================
# MonthlySchedule calculator
# =============================================================================


@pytest.mark.unit
class TestCalculateNextRunMonthly:
    """Tests for calculate_next_run with MonthlySchedule."""

    def test_this_month_before_target(self) -> None:
        """Before target day in current month returns current month."""
        from_time = _utc(2025, 6, 1, 0, 0, 0)
        pattern = MonthlySchedule(day=15, time=datetime_time(12, 0, 0))

        result = calculate_next_run(pattern, from_time)

        assert result.month == 6
        assert result.day == 15
        assert result.hour == 12

    def test_next_month_after_target(self) -> None:
        """After target day in current month returns next month."""
        from_time = _utc(2025, 6, 20, 0, 0, 0)
        pattern = MonthlySchedule(day=15, time=datetime_time(12, 0, 0))

        result = calculate_next_run(pattern, from_time)

        assert result.month == 7
        assert result.day == 15

    def test_day_doesnt_exist_skips_to_next_valid_month(self) -> None:
        """day=31 in February skips to March 31."""
        from_time = _utc(2025, 2, 1, 0, 0, 0)
        pattern = MonthlySchedule(day=31, time=datetime_time(0, 0, 0))

        result = calculate_next_run(pattern, from_time)

        # February has no 31st, so should skip to March 31
        assert result.month == 3
        assert result.day == 31

    def test_year_boundary_december_to_january(self) -> None:
        """Monthly schedule wraps from December to January next year."""
        from_time = _utc(2025, 12, 20, 0, 0, 0)
        pattern = MonthlySchedule(day=15, time=datetime_time(10, 0, 0))

        result = calculate_next_run(pattern, from_time)

        assert result.year == 2026
        assert result.month == 1
        assert result.day == 15

    def test_day_30_skips_february_lands_on_march(self) -> None:
        """day=30 skips February (28/29 days), lands on March 30."""
        from_time = _utc(2025, 1, 31, 0, 0, 0)
        pattern = MonthlySchedule(day=30, time=datetime_time(12, 0, 0))

        result = calculate_next_run(pattern, from_time)

        # January 30 is before from_time (Jan 31), so next month.
        # February has no 30th, so skips to March 30.
        assert result.month == 3
        assert result.day == 30

    def test_same_day_before_time_returns_current_month(self) -> None:
        """On the target day but before the target time returns current month."""
        from_time = _utc(2025, 6, 15, 8, 0, 0)
        pattern = MonthlySchedule(day=15, time=datetime_time(12, 0, 0))

        result = calculate_next_run(pattern, from_time)

        assert result.month == 6
        assert result.day == 15
        assert result.hour == 12


# =============================================================================
# WeeklySchedule — additional boundary cases
# =============================================================================


@pytest.mark.unit
class TestCalculateNextRunWeeklyBoundary:
    """Additional boundary cases for weekly schedule."""

    def test_multiple_days_picks_nearest(self) -> None:
        """Multiple scheduled days picks the nearest future day."""
        # 2025-06-03 is Tuesday
        from_time = _utc(2025, 6, 3, 18, 0, 0)
        pattern = WeeklySchedule(
            days=[Weekday.MONDAY, Weekday.THURSDAY, Weekday.SATURDAY],
            time=datetime_time(9, 0, 0),
        )

        result = calculate_next_run(pattern, from_time)

        # Next matching day after Tuesday evening is Thursday (June 5)
        assert result.day == 5
        assert result.weekday() == 3  # Thursday

    def test_saturday_wraps_to_monday_next_week(self) -> None:
        """Saturday from_time with Mon+Wed scheduled wraps to Monday."""
        # 2025-06-07 is Saturday
        from_time = _utc(2025, 6, 7, 18, 0, 0)
        pattern = WeeklySchedule(
            days=[Weekday.MONDAY, Weekday.WEDNESDAY],
            time=datetime_time(9, 0, 0),
        )

        result = calculate_next_run(pattern, from_time)

        assert result.day == 9  # Monday June 9
        assert result.weekday() == 0  # Monday


# =============================================================================
# Error cases
# =============================================================================


@pytest.mark.unit
class TestCalculateNextRunErrors:
    """Tests for calculate_next_run error paths."""

    def test_naive_datetime_raises_value_error(self) -> None:
        """Naive (no tzinfo) from_time raises ValueError."""
        naive_time = datetime(2025, 6, 1, 12, 0, 0)
        pattern = IntervalSchedule(seconds=10)

        with pytest.raises(ValueError, match='timezone-aware'):
            calculate_next_run(pattern, naive_time)

    def test_invalid_timezone_raises_value_error(self) -> None:
        """Invalid timezone string raises ValueError."""
        from_time = _utc(2025, 6, 1, 12, 0, 0)
        pattern = IntervalSchedule(seconds=10)

        with pytest.raises(ValueError, match='Invalid timezone'):
            calculate_next_run(pattern, from_time, tz_str='Not/A/Timezone')


# =============================================================================
# should_run_now
# =============================================================================


@pytest.mark.unit
class TestShouldRunNow:
    """Tests for should_run_now function."""

    def test_none_returns_true(self) -> None:
        """None next_run_at means first run, should execute."""
        assert should_run_now(None, _utc(2025, 6, 1)) is True

    def test_past_returns_true(self) -> None:
        """next_run_at in the past should execute."""
        next_run = _utc(2025, 6, 1, 11, 0, 0)
        check = _utc(2025, 6, 1, 12, 0, 0)

        assert should_run_now(next_run, check) is True

    def test_future_returns_false(self) -> None:
        """next_run_at in the future should not execute."""
        next_run = _utc(2025, 6, 1, 13, 0, 0)
        check = _utc(2025, 6, 1, 12, 0, 0)

        assert should_run_now(next_run, check) is False

    def test_exact_time_returns_true(self) -> None:
        """Exact match (next_run_at == check_time) should execute (<=)."""
        exact = _utc(2025, 6, 1, 12, 0, 0)

        assert should_run_now(exact, exact) is True
