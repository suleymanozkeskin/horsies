---
title: Schedule Patterns
summary: Type-safe patterns for Interval, Hourly, Daily, Weekly, Monthly schedules.
related: [scheduler-overview, schedule-config]
tags: [scheduling, patterns, interval]
---

## Available Patterns

| Pattern | Use Case |
| ------- | -------- |
| `SchedulePattern::Interval` | Every N seconds/minutes/hours/days |
| `SchedulePattern::Hourly` | Every hour at specific minute |
| `SchedulePattern::Daily` | Every day at specific time |
| `SchedulePattern::Weekly` | Specific days at specific time |
| `SchedulePattern::Monthly` | Specific day of month at specific time |

## IntervalSchedule

Run repeatedly at a fixed interval.

```rust
use horsies::{SchedulePattern, IntervalSchedule};

// Every 30 seconds
SchedulePattern::Interval(IntervalSchedule {
    seconds: Some(30), minutes: None, hours: None, days: None,
})

// Every 5 minutes
SchedulePattern::Interval(IntervalSchedule {
    seconds: None, minutes: Some(5), hours: None, days: None,
})

// Every 2 hours
SchedulePattern::Interval(IntervalSchedule {
    seconds: None, minutes: None, hours: Some(2), days: None,
})

// Every day
SchedulePattern::Interval(IntervalSchedule {
    seconds: None, minutes: None, hours: None, days: Some(1),
})

// Combined: every 1 hour 30 minutes
SchedulePattern::Interval(IntervalSchedule {
    seconds: None, minutes: Some(30), hours: Some(1), days: None,
})
```

At least one time unit must be specified. Values are added together.

## HourlySchedule

Run every hour at a specific minute (and optionally second).

```rust
use horsies::{SchedulePattern, HourlySchedule};

// Every hour at XX:00:00
SchedulePattern::Hourly(HourlySchedule { minute: 0, second: 0 })

// Every hour at XX:30:00
SchedulePattern::Hourly(HourlySchedule { minute: 30, second: 0 })

// Every hour at XX:15:45
SchedulePattern::Hourly(HourlySchedule { minute: 15, second: 45 })
```

Fields:

| Field | Range | Default |
| ----- | ----- | ------- |
| `minute` | 0-59 | required |
| `second` | 0-59 | 0 |

## DailySchedule

Run every day at a specific time.

```rust
use horsies::{SchedulePattern, DailySchedule};
use chrono::NaiveTime;

// Every day at 3:00 AM
SchedulePattern::Daily(DailySchedule {
    time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
})

// Every day at 6:30 PM
SchedulePattern::Daily(DailySchedule {
    time: NaiveTime::from_hms_opt(18, 30, 0).unwrap(),
})

// Every day at midnight
SchedulePattern::Daily(DailySchedule {
    time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
})
```

The `time` field is a `chrono::NaiveTime` value.

## WeeklySchedule

Run on specific days of the week at a specific time.

```rust
use horsies::{SchedulePattern, WeeklySchedule, Weekday};
use chrono::NaiveTime;

// Monday and Friday at 9 AM
SchedulePattern::Weekly(WeeklySchedule {
    days: vec![Weekday::Monday, Weekday::Friday],
    time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
})

// Every weekday at 8 AM
SchedulePattern::Weekly(WeeklySchedule {
    days: vec![
        Weekday::Monday,
        Weekday::Tuesday,
        Weekday::Wednesday,
        Weekday::Thursday,
        Weekday::Friday,
    ],
    time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
})

// Weekends at noon
SchedulePattern::Weekly(WeeklySchedule {
    days: vec![Weekday::Saturday, Weekday::Sunday],
    time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
})
```

Weekday values:

- `Weekday::Monday`
- `Weekday::Tuesday`
- `Weekday::Wednesday`
- `Weekday::Thursday`
- `Weekday::Friday`
- `Weekday::Saturday`
- `Weekday::Sunday`

## MonthlySchedule

Run on a specific day of the month at a specific time.

```rust
use horsies::{SchedulePattern, MonthlySchedule};
use chrono::NaiveTime;

// 1st of month at midnight
SchedulePattern::Monthly(MonthlySchedule {
    day: 1,
    time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
})

// 15th of month at noon
SchedulePattern::Monthly(MonthlySchedule {
    day: 15,
    time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
})

// Last valid day handling
SchedulePattern::Monthly(MonthlySchedule {
    day: 31,
    time: NaiveTime::from_hms_opt(23, 59, 0).unwrap(),
})
// Months with fewer than 31 days are skipped (e.g. February, April)
```

| Field | Range | Description |
| ----- | ----- | ----------- |
| `day` | 1-31 | Day of month |
| `time` | `NaiveTime` | Time of day |

## Pattern Selection Guide

| Need | Pattern |
| ---- | ------- |
| Run every N minutes/hours | `SchedulePattern::Interval` |
| Run at specific minute each hour | `SchedulePattern::Hourly` |
| Run at same time every day | `SchedulePattern::Daily` |
| Run on specific weekdays | `SchedulePattern::Weekly` |
| Run monthly on specific date | `SchedulePattern::Monthly` |

## Examples in Context

```rust
use horsies::{
    AppConfig, ScheduleConfig, TaskSchedule,
    SchedulePattern, IntervalSchedule, DailySchedule, WeeklySchedule, Weekday,
};
use chrono::NaiveTime;

let config = AppConfig {
    schedule: Some(ScheduleConfig::new(vec![
        // Heartbeat every 30 seconds
        TaskSchedule::new(
            "heartbeat",
            "send_heartbeat",
            SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(30),
                ..Default::default()
            }),
        ),
        // Daily cleanup at 3 AM UTC
        TaskSchedule::new(
            "daily-cleanup",
            "cleanup_old_data",
            SchedulePattern::Daily(DailySchedule {
                time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            }),
        ),
        // Weekly report on Mondays at 9 AM Eastern
        TaskSchedule::new(
            "weekly-report",
            "generate_weekly_report",
            SchedulePattern::Weekly(WeeklySchedule {
                days: vec![Weekday::Monday],
                time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            }),
        )
        .timezone("America/New_York"),
    ])),
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Why Not Cron?

Horsies uses typed patterns instead of cron strings:

| Cron | Horsies |
| ---- | ------- |
| `*/5 * * * *` | `IntervalSchedule { minutes: Some(5), .. }` |
| `0 3 * * *` | `DailySchedule { time: NaiveTime::from_hms_opt(3, 0, 0).unwrap() }` |
| `0 9 * * 1` | `WeeklySchedule { days: vec![Weekday::Monday], time: NaiveTime::from_hms_opt(9, 0, 0).unwrap() }` |

Benefits:

- Readable
- Type checking at compile time
- Clear validation errors
- IDE autocomplete
- No string parsing bugs
