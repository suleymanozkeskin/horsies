---
title: Schedule Patterns
summary: Type-safe patterns for Interval, Hourly, Daily, Weekly, Monthly schedules.
related: [scheduler-overview, schedule-config]
tags: [scheduling, patterns, interval, cron]
---

## Available Patterns

| Pattern | Use Case |
| ------- | -------- |
| `IntervalSchedule` | Every N seconds/minutes/hours/days |
| `HourlySchedule` | Every hour at specific minute |
| `DailySchedule` | Every day at specific time |
| `WeeklySchedule` | Specific days at specific time |
| `MonthlySchedule` | Specific day of month at specific time |

## IntervalSchedule

Run repeatedly at a fixed interval.

```python
from horsies.core.models.schedule import IntervalSchedule

# Every 30 seconds
IntervalSchedule(seconds=30)

# Every 5 minutes
IntervalSchedule(minutes=5)

# Every 2 hours
IntervalSchedule(hours=2)

# Every day
IntervalSchedule(days=1)

# Combined: every 1 hour 30 minutes
IntervalSchedule(hours=1, minutes=30)
```

At least one time unit must be specified. Values are added together.

## HourlySchedule

Run every hour at a specific minute (and optionally second).

```python
from horsies.core.models.schedule import HourlySchedule

# Every hour at XX:00:00
HourlySchedule(minute=0)

# Every hour at XX:30:00
HourlySchedule(minute=30)

# Every hour at XX:15:45
HourlySchedule(minute=15, second=45)
```

Fields:

| Field | Range | Default |
| ----- | ----- | ------- |
| `minute` | 0-59 | required |
| `second` | 0-59 | 0 |

## DailySchedule

Run every day at a specific time.

```python
from datetime import time
from horsies.core.models.schedule import DailySchedule

# Every day at 3:00 AM
DailySchedule(time=time(3, 0, 0))

# Every day at 6:30 PM
DailySchedule(time=time(18, 30, 0))

# Every day at midnight
DailySchedule(time=time(0, 0, 0))
```

The `time` field is a Python `datetime.time` object.

## WeeklySchedule

Run on specific days of the week at a specific time.

```python
from datetime import time
from horsies.core.models.schedule import WeeklySchedule, Weekday

# Monday and Friday at 9 AM
WeeklySchedule(
    days=[Weekday.MONDAY, Weekday.FRIDAY],
    time=time(9, 0, 0),
)

# Every weekday at 8 AM
WeeklySchedule(
    days=[
        Weekday.MONDAY,
        Weekday.TUESDAY,
        Weekday.WEDNESDAY,
        Weekday.THURSDAY,
        Weekday.FRIDAY,
    ],
    time=time(8, 0, 0),
)

# Weekends at noon
WeeklySchedule(
    days=[Weekday.SATURDAY, Weekday.SUNDAY],
    time=time(12, 0, 0),
)
```

Weekday values:

- `Weekday.MONDAY`
- `Weekday.TUESDAY`
- `Weekday.WEDNESDAY`
- `Weekday.THURSDAY`
- `Weekday.FRIDAY`
- `Weekday.SATURDAY`
- `Weekday.SUNDAY`

## MonthlySchedule

Run on a specific day of the month at a specific time.

```python
from datetime import time
from horsies.core.models.schedule import MonthlySchedule

# 1st of month at midnight
MonthlySchedule(day=1, time=time(0, 0, 0))

# 15th of month at noon
MonthlySchedule(day=15, time=time(12, 0, 0))

# Last valid day handling
MonthlySchedule(day=31, time=time(23, 59, 0))
# In February, runs on 28th (or 29th)
```

| Field | Range | Description |
| ----- | ----- | ----------- |
| `day` | 1-31 | Day of month |
| `time` | time | Time of day |

## Pattern Selection Guide

| Need | Pattern |
| ---- | ------- |
| Run every N minutes/hours | `IntervalSchedule` |
| Run at specific minute each hour | `HourlySchedule` |
| Run at same time every day | `DailySchedule` |
| Run on specific weekdays | `WeeklySchedule` |
| Run monthly on specific date | `MonthlySchedule` |

## Examples in Context

```python
from datetime import time
from horsies.core.models.schedule import (
    ScheduleConfig,
    TaskSchedule,
    IntervalSchedule,
    DailySchedule,
    WeeklySchedule,
)

config = AppConfig(
    broker=PostgresConfig(...),
    schedule=ScheduleConfig(
        enabled=True,
        schedules=[
            # Heartbeat every 30 seconds
            TaskSchedule(
                name="heartbeat",
                task_name="send_heartbeat",
                pattern=IntervalSchedule(seconds=30),
            ),
            # Daily cleanup at 3 AM UTC
            TaskSchedule(
                name="daily-cleanup",
                task_name="cleanup_old_data",
                pattern=DailySchedule(time=time(3, 0, 0)),
                timezone="UTC",
            ),
            # Weekly report on Mondays at 9 AM Eastern
            TaskSchedule(
                name="weekly-report",
                task_name="generate_weekly_report",
                pattern=WeeklySchedule(
                    days=[Weekday.MONDAY],
                    time=time(9, 0, 0),
                ),
                timezone="America/New_York",
            ),
        ],
    ),
)
```

## Why Not Cron?

Horsies uses typed patterns instead of cron strings:

| Cron | Horsies |
| ---- | ------- |
| `*/5 * * * *` | `IntervalSchedule(minutes=5)` |
| `0 3 * * *` | `DailySchedule(time=time(3, 0, 0))` |
| `0 9 * * 1` | `WeeklySchedule(days=[Weekday.MONDAY], time=time(9, 0))` |

Benefits:

- Readable
- Type checking at definition time
- Clear validation errors
- IDE autocomplete
- No string parsing bugs
