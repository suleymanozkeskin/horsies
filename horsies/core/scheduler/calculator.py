# horsies/core/scheduler/calculator.py
from __future__ import annotations
from datetime import datetime, timedelta, timezone
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
    """Calculate next run for hourly schedule."""
    # Start with current hour at the target minute/second
    candidate = local_time.replace(
        minute=pattern.minute, second=pattern.second, microsecond=0
    )

    # If we've already passed this time in the current hour, move to next hour
    if candidate <= local_time:
        candidate = candidate + timedelta(hours=1)

    return candidate


def _calculate_daily(
    pattern: DailySchedule, local_time: datetime, tz: ZoneInfo
) -> datetime:
    """Calculate next run for daily schedule, tolerating DST transitions."""
    for day_offset in (0, 1, 2):  # try today, tomorrow, day after (in case of DST gaps)
        candidate_date = local_time + timedelta(days=day_offset)
        try:
            candidate = candidate_date.replace(
                hour=pattern.time.hour,
                minute=pattern.time.minute,
                second=pattern.time.second,
                microsecond=0,
            )
        except Exception:
            continue  # invalid local time (e.g., DST gap), try next day
        if candidate <= local_time:
            continue
        return candidate
    # Fallback: roll forward one more day if all attempts failed
    return (local_time + timedelta(days=1)).replace(
        hour=pattern.time.hour,
        minute=pattern.time.minute,
        second=pattern.time.second,
        microsecond=0,
    )


def _calculate_weekly(
    pattern: WeeklySchedule, local_time: datetime, tz: ZoneInfo
) -> datetime:
    """Calculate next run for weekly schedule, tolerating DST transitions."""
    # Map Weekday enum to Python weekday() values (0=Monday, 6=Sunday)
    weekday_map = {
        Weekday.MONDAY: 0,
        Weekday.TUESDAY: 1,
        Weekday.WEDNESDAY: 2,
        Weekday.THURSDAY: 3,
        Weekday.FRIDAY: 4,
        Weekday.SATURDAY: 5,
        Weekday.SUNDAY: 6,
    }

    target_weekdays = sorted([weekday_map[d] for d in pattern.days])
    current_weekday = local_time.weekday()

    # Start with today at the target time
    try:
        candidate = local_time.replace(
            hour=pattern.time.hour,
            minute=pattern.time.minute,
            second=pattern.time.second,
            microsecond=0,
        )
    except Exception:
        candidate = local_time

    # Find next matching weekday
    if current_weekday in target_weekdays and candidate > local_time:
        # Today matches and time hasn't passed yet
        return candidate

    # Find next target weekday
    days_ahead = None
    for target_day in target_weekdays:
        if target_day > current_weekday:
            days_ahead = target_day - current_weekday
            break

    # If no future day this week, wrap to first day next week
    if days_ahead is None:
        days_ahead = (7 - current_weekday) + target_weekdays[0]

    candidate = candidate + timedelta(days=days_ahead)

    # If the target time is invalid/ambiguous (DST), retry on the computed day by rebuilding datetime
    for _ in range(2):
        try:
            adjusted = candidate.replace(
                hour=pattern.time.hour,
                minute=pattern.time.minute,
                second=pattern.time.second,
                microsecond=0,
            )
            return adjusted
        except Exception:
            candidate = candidate + timedelta(days=1)
    return candidate


def _calculate_monthly(
    pattern: MonthlySchedule, local_time: datetime, tz: ZoneInfo
) -> datetime:
    """Calculate next run for monthly schedule, tolerating missing days and DST."""
    # Start with current month at the target day and time
    try:
        candidate = local_time.replace(
            day=pattern.day,
            hour=pattern.time.hour,
            minute=pattern.time.minute,
            second=pattern.time.second,
            microsecond=0,
        )
    except ValueError:
        # Day doesn't exist in current month (e.g., day=31 in February)
        # Skip to next month
        candidate = _next_valid_monthly_date(local_time, pattern, tz)
        return candidate
    except Exception:
        # DST-related invalid time: rebuild on the same date ignoring current time component
        candidate = _next_valid_monthly_date(
            local_time - timedelta(days=1), pattern, tz
        )
        return candidate

    # If we've already passed this time this month, move to next month
    if candidate <= local_time:
        candidate = _next_valid_monthly_date(local_time, pattern, tz)

    return candidate


def _next_valid_monthly_date(
    local_time: datetime, pattern: MonthlySchedule, tz: ZoneInfo
) -> datetime:
    """Find next valid monthly date, skipping months where day doesn't exist."""
    # Start with next month
    if local_time.month == 12:
        candidate_year = local_time.year + 1
        candidate_month = 1
    else:
        candidate_year = local_time.year
        candidate_month = local_time.month + 1

    # Try up to 12 months ahead to find valid date
    for _ in range(12):
        try:
            candidate = datetime(
                year=candidate_year,
                month=candidate_month,
                day=pattern.day,
                hour=pattern.time.hour,
                minute=pattern.time.minute,
                second=pattern.time.second,
                microsecond=0,
                tzinfo=tz,
            )
            return candidate
        except ValueError:
            # Day doesn't exist in this month, try next month
            if candidate_month == 12:
                candidate_year += 1
                candidate_month = 1
            else:
                candidate_month += 1

    # Should never reach here unless pattern.day is invalid (>31)
    raise ValueError(
        f'Could not find valid date for day={pattern.day} within 12 months'
    )


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
