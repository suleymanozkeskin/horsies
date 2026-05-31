---
title: Schedule Patterns
summary: Type-safe patterns for Interval, Hourly, Daily, Weekly, Monthly, and full cron-style schedules.
related: [scheduler-overview, schedule-config]
tags: [scheduling, patterns, interval, cron]
---

## Available Patterns

| Pattern | Use Case |
| ------- | -------- |
| `IntervalSchedule` | Every N seconds/minutes/hours/days from scheduler start |
| `HourlySchedule` | Every hour at specific minute |
| `DailySchedule` | Every day at specific time |
| `WeeklySchedule` | Specific days at specific time |
| `MonthlySchedule` | Specific day of month at specific time |
| `CronSchedule` | Full 5-field cron-style expressivity, typed |

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
# Months with fewer than 31 days are skipped (e.g. February, April)
```

| Field | Range | Description |
| ----- | ----- | ----------- |
| `day` | 1-31 | Day of month |
| `time` | time | Time of day |

## CronSchedule

Express anything a 5-field crontab (`minute hour day-of-month month day-of-week`)
can express, through typed fields instead of a cron string. Fires at second `:00`
(minute granularity).

A `CronSchedule` has four parts:

| Field | Type | Domain |
| ----- | ---- | ------ |
| `minute` | `list[CronNumericTerm]` | 0-59 |
| `hour` | `list[CronNumericTerm]` | 0-23 |
| `month` | `list[CronMonthTerm]` | `Month.JANUARY`-`Month.DECEMBER` |
| `day` | `DaySelector` | day-of-month and/or day-of-week |

### Field terms

Each field holds a list of terms; the field matches the union of all its terms.

Numeric terms (`minute`, `hour`, day-of-month):

| Term | Cron | Meaning |
| ---- | ---- | ------- |
| `CronEvery()` | `*` | Every value in the domain |
| `CronStep(step=n)` | `*/n` | Every nth value, anchored at the domain start |
| `CronValues(values=[...])` | `a,b,c` | An explicit set |
| `CronRange(start=a, end=b, step=s)` | `a-b` / `a-b/s` | An inclusive range |

Enum terms (`month`, day-of-week), parameterized by `Month` or `Weekday`:

| Term | Cron | Meaning |
| ---- | ---- | ------- |
| `CronEvery()` | `*` | Every value |
| `CronEnumStep[T](step=n)` | `*/n` | Every nth, by canonical order |
| `CronEnumValues[T](values=[...])` | `a,b,c` | An explicit set |
| `CronEnumRange[T](start=a, end=b, step=s)` | `a-b` | An inclusive range |

`CronStep` is anchored at the domain start. `CronStep(step=4)` on `hour` matches
`0, 4, 8, 12, 16, 20` — aligned to the clock, not to scheduler start.

### Wall-clock alignment and staggering

This is what `CronSchedule` provides that `IntervalSchedule` cannot. `IntervalSchedule(hours=4)`
runs every 4 hours measured from scheduler start, so it drifts and cannot stagger.
`CronSchedule` aligns to the clock and lets a minute offset spread load:

```python
from horsies.core.models.schedule import (
    CronSchedule,
    CronValues,
    CronStep,
    CronEvery,
    EveryDay,
)

# Every 4 hours at minute 15: 00:15, 04:15, 08:15, 12:15, 16:15, 20:15
CronSchedule(
    minute=[CronValues(values=[15])],
    hour=[CronStep(step=4)],
    month=[CronEvery()],
    day=EveryDay(),
)
```

The minute offset (`15`) staggers this job away from jobs anchored at `:00`, avoiding
synchronized bursts — impossible to express with an anchored interval.

### Day selection

Day-of-month and day-of-week are combined into one explicit `DaySelector`, removing
cron's silent "OR when both are set" ambiguity:

| Selector | Meaning |
| -------- | ------- |
| `EveryDay()` | Every day |
| `ByMonthDay(day_of_month=[...])` | Match day-of-month only |
| `ByWeekday(day_of_week=[...])` | Match day-of-week only |
| `EitherDay(day_of_month=[...], day_of_week=[...])` | Match either (OR) |
| `BothDays(day_of_month=[...], day_of_week=[...])` | Match both (AND) |

```python
from horsies.core.models.schedule import (
    CronSchedule,
    CronValues,
    CronEvery,
    CronEnumValues,
    CronEnumRange,
    ByWeekday,
    EitherDay,
    BothDays,
    Weekday,
)

# Weekdays at 09:00 (Monday-Friday)
CronSchedule(
    minute=[CronValues(values=[0])],
    hour=[CronValues(values=[9])],
    month=[CronEvery()],
    day=ByWeekday(
        day_of_week=[CronEnumRange[Weekday](start=Weekday.MONDAY, end=Weekday.FRIDAY)],
    ),
)

# The 13th OR any Friday, at midnight (whichever comes first)
CronSchedule(
    minute=[CronValues(values=[0])],
    hour=[CronValues(values=[0])],
    month=[CronEvery()],
    day=EitherDay(
        day_of_month=[CronValues(values=[13])],
        day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
    ),
)

# Friday the 13th only, at midnight (day-of-month AND weekday)
CronSchedule(
    minute=[CronValues(values=[0])],
    hour=[CronValues(values=[0])],
    month=[CronEvery()],
    day=BothDays(
        day_of_month=[CronValues(values=[13])],
        day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
    ),
)
```

### Cron-to-typed mapping

| Cron | CronSchedule |
| ---- | ------------ |
| `0 */4 * * *` | `minute=[CronValues(values=[0])], hour=[CronStep(step=4)], month=[CronEvery()], day=EveryDay()` |
| `15 */4 * * *` | `minute=[CronValues(values=[15])], hour=[CronStep(step=4)], ...` |
| `0 9 * * 1-5` | `... hour=[CronValues(values=[9])], day=ByWeekday(day_of_week=[CronEnumRange[Weekday](start=Weekday.MONDAY, end=Weekday.FRIDAY)])` |
| `0 0 13 * 5` | `... day=EitherDay(day_of_month=[CronValues(values=[13])], day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])])` |
| `0 0 1 1-3 *` | `... month=[CronEnumRange[Month](start=Month.JANUARY, end=Month.MARCH)], day=ByMonthDay(day_of_month=[CronValues(values=[1])])` |

Note: standard cron ORs day-of-month and day-of-week when both are set (`0 0 13 * 5`
means "13th or Friday"). `CronSchedule` requires choosing `EitherDay` or `BothDays`.

## Things to Avoid

Field domains, range ordering, step span, and satisfiability are checked when the
`CronSchedule` is constructed; each violation raises `ConfigurationError`.

**Don't use a wrap-around enum range — use explicit values.** Ranges require
`start <= end` in canonical order (Monday-Sunday, January-December).

```python
# Wrong: wrap-around range -> ConfigurationError when the schedule is built
CronSchedule(
    minute=[CronValues(values=[0])], hour=[CronValues(values=[0])], month=[CronEvery()],
    day=ByWeekday(day_of_week=[
        CronEnumRange[Weekday](start=Weekday.FRIDAY, end=Weekday.MONDAY),
    ]),
)

# Correct
CronSchedule(
    minute=[CronValues(values=[0])], hour=[CronValues(values=[0])], month=[CronEvery()],
    day=ByWeekday(day_of_week=[CronEnumValues[Weekday](values=[
        Weekday.FRIDAY, Weekday.SATURDAY, Weekday.SUNDAY, Weekday.MONDAY,
    ])]),
)
```

**Don't use a step larger than the field span.** A step above the span selects only
the first value, so it is rejected. The span is `max - min`: 59 for minute, 23 for
hour, 30 for day-of-month, 11 for month, 6 for day-of-week.

```python
# Wrong: step 24 on hour collapses to {0} -> ConfigurationError
CronSchedule(
    minute=[CronEvery()], hour=[CronStep(step=24)], month=[CronEvery()], day=EveryDay(),
)

# Correct: pick the values explicitly
CronSchedule(
    minute=[CronEvery()], hour=[CronValues(values=[0])], month=[CronEvery()], day=EveryDay(),
)
```

**Don't construct an unsatisfiable month/day combination.** It is rejected at
construction, not silently skipped forever.

```python
# Wrong: February never has a 30th -> ConfigurationError
CronSchedule(
    minute=[CronEvery()], hour=[CronEvery()],
    month=[CronEnumValues[Month](values=[Month.FEBRUARY])],
    day=ByMonthDay(day_of_month=[CronValues(values=[30])]),
)
```

## Pattern Selection Guide

| Need | Pattern |
| ---- | ------- |
| Run every N minutes/hours from scheduler start | `IntervalSchedule` |
| Run at specific minute each hour | `HourlySchedule` |
| Run at same time every day | `DailySchedule` |
| Run on specific weekdays | `WeeklySchedule` |
| Run monthly on specific date | `MonthlySchedule` |
| Clock-aligned "every N hours", staggered, or any cron expression | `CronSchedule` |

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

## No Cron Strings

Horsies expresses cron's full power through typed fields — never a cron string. For
common cases the calendar patterns are shortest; for full cron expressivity use
`CronSchedule`:

| Cron | Horsies |
| ---- | ------- |
| `0 3 * * *` | `DailySchedule(time=time(3, 0, 0))` |
| `0 9 * * 1` | `WeeklySchedule(days=[Weekday.MONDAY], time=time(9, 0))` |
| `15 */4 * * *` | `CronSchedule(minute=[CronValues(values=[15])], hour=[CronStep(step=4)], month=[CronEvery()], day=EveryDay())` |

Benefits:

- Type checking at definition time
- Clear validation errors at construction
- IDE autocomplete
- No string parsing bugs
- Ambiguities (day-of-month vs day-of-week) are explicit, not silent
