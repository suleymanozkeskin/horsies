# horsies/core/scheduler/calculator.py
from __future__ import annotations
import calendar
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional
from horsies.core.models.schedule import (
    SchedulePattern,
    IntervalSchedule,
    HourlySchedule,
    DailySchedule,
    WeeklySchedule,
    MonthlySchedule,
    Weekday,
)

WEEKDAY_MAP = {
    Weekday.MONDAY: 0,
    Weekday.TUESDAY: 1,
    Weekday.WEDNESDAY: 2,
    Weekday.THURSDAY: 3,
    Weekday.FRIDAY: 4,
    Weekday.SATURDAY: 5,
    Weekday.SUNDAY: 6,
}


def calculate_next_run(
    pattern: SchedulePattern, from_time: datetime, tz_str: str = 'UTC'
) -> datetime:
    """
    Calculate the next run time for a schedule pattern.

    Args:
        pattern: Schedule pattern (interval, hourly, daily, weekly, monthly)
        from_time: Calculate next run after this time (should be UTC-aware)
        tz_str: Timezone for schedule evaluation (e.g., "UTC", "America/New_York")

    Returns:
        Next run time as UTC-aware datetime

    Raises:
        ValueError: If timezone is invalid or pattern type is unknown
    """
    # Ensure from_time is UTC-aware
    if from_time.tzinfo is None:
        raise ValueError('from_time must be timezone-aware')

    # Validate timezone string
    try:
        tz = ZoneInfo(tz_str)
    except Exception as e:
        raise ValueError(f"Invalid timezone '{tz_str}': {e}")

    # Convert from_time to target timezone for schedule calculations
    local_time = from_time.astimezone(tz)

    # Calculate next run based on pattern type (exhaustive match-case)
    match pattern:
        case IntervalSchedule():
            next_run = _calculate_interval(pattern, from_time)
        case HourlySchedule():
            next_run = _calculate_hourly(pattern, local_time, tz)
        case DailySchedule():
            next_run = _calculate_daily(pattern, local_time, tz)
        case WeeklySchedule():
            next_run = _calculate_weekly(pattern, local_time, tz)
        case MonthlySchedule():
            next_run = _calculate_monthly(pattern, local_time, tz)

    # Ensure result is UTC-aware
    if next_run.tzinfo is None:
        raise RuntimeError('Calculated next_run is not timezone-aware')

    return next_run.astimezone(timezone.utc)


def _calculate_interval(pattern: IntervalSchedule, from_time: datetime) -> datetime:
    """Calculate next run for interval-based schedule."""
    total_seconds = pattern.total_seconds()
    next_run = from_time + timedelta(seconds=total_seconds)
    return next_run


def _calculate_hourly(
    pattern: HourlySchedule, local_time: datetime, tz: ZoneInfo
) -> datetime:
    """Calculate next run for hourly schedule with DST-safe local resolution."""
    for hour_offset in range(0, 6):
        hour_base = local_time + timedelta(hours=hour_offset)
        candidate = _resolve_local_datetime(
            date_value=hour_base.date(),
            hour=hour_base.hour,
            minute=pattern.minute,
            second=pattern.second,
            tz=tz,
        )
        if candidate is None or candidate <= local_time:
            continue
        return candidate

    raise RuntimeError('Could not calculate next hourly run within 6 hours')


def _calculate_daily(
    pattern: DailySchedule, local_time: datetime, tz: ZoneInfo
) -> datetime:
    """Calculate next run for daily schedule, skipping nonexistent local times."""
    for day_offset in range(0, 8):
        candidate_date = (local_time + timedelta(days=day_offset)).date()
        candidate = _resolve_local_datetime(
            date_value=candidate_date,
            hour=pattern.time.hour,
            minute=pattern.time.minute,
            second=pattern.time.second,
            tz=tz,
        )
        if candidate is None or candidate <= local_time:
            continue
        return candidate

    raise RuntimeError('Could not calculate next daily run within 7 days')


def _calculate_weekly(
    pattern: WeeklySchedule, local_time: datetime, tz: ZoneInfo
) -> datetime:
    """Calculate next run for weekly schedule, skipping nonexistent local times."""
    target_weekdays = {WEEKDAY_MAP[d] for d in pattern.days}
    for day_offset in range(0, 15):
        candidate_date = (local_time + timedelta(days=day_offset)).date()
        if candidate_date.weekday() not in target_weekdays:
            continue

        candidate = _resolve_local_datetime(
            date_value=candidate_date,
            hour=pattern.time.hour,
            minute=pattern.time.minute,
            second=pattern.time.second,
            tz=tz,
        )
        if candidate is None or candidate <= local_time:
            continue
        return candidate

    raise RuntimeError('Could not calculate next weekly run within 2 weeks')


def _calculate_monthly(
    pattern: MonthlySchedule, local_time: datetime, tz: ZoneInfo
) -> datetime:
    """Calculate next run for monthly schedule, skipping missing days and DST gaps."""
    for month_offset in range(0, 25):
        year, month = _add_months(local_time.year, local_time.month, month_offset)
        if pattern.day > calendar.monthrange(year, month)[1]:
            continue

        candidate = _resolve_local_datetime(
            date_value=date(year, month, pattern.day),
            hour=pattern.time.hour,
            minute=pattern.time.minute,
            second=pattern.time.second,
            tz=tz,
        )
        if candidate is None or candidate <= local_time:
            continue
        return candidate

    raise RuntimeError('Could not calculate next monthly run within 24 months')


def _add_months(year: int, month: int, month_offset: int) -> tuple[int, int]:
    base = (year * 12) + (month - 1) + month_offset
    return (base // 12, (base % 12) + 1)


def _resolve_local_datetime(
    date_value: date,
    hour: int,
    minute: int,
    second: int,
    tz: ZoneInfo,
) -> Optional[datetime]:
    """
    Resolve local wall-clock date/time into a real zoned datetime.

    Returns None for nonexistent local times (spring-forward gaps).
    For ambiguous local times (fall-back), returns the earliest instant.
    """
    naive = datetime(
        year=date_value.year,
        month=date_value.month,
        day=date_value.day,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=0,
    )
    valid: list[datetime] = []
    for fold in (0, 1):
        candidate = naive.replace(tzinfo=tz, fold=fold)
        roundtrip = candidate.astimezone(timezone.utc).astimezone(tz)
        if roundtrip.replace(tzinfo=None) == naive:
            valid.append(candidate)

    if not valid:
        return None

    valid.sort(key=lambda dt: dt.astimezone(timezone.utc))
    return valid[0]


def should_run_now(next_run_at: Optional[datetime], check_time: datetime) -> bool:
    """
    Determine if a schedule should run at the current check time.

    Args:
        next_run_at: Scheduled next run time (UTC-aware)
        check_time: Current time to check against (UTC-aware)

    Returns:
        True if schedule should run now
    """
    if next_run_at is None:
        # First run - should execute
        return True

    # Run if next_run_at is at or before current time
    return next_run_at <= check_time
