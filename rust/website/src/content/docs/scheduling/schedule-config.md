---
title: Schedule Config
summary: ScheduleConfig and TaskSchedule configuration models.
related: [scheduler-overview, schedule-patterns]
tags: [scheduling, configuration, TaskSchedule]
---

## ScheduleConfig

Top-level scheduler configuration added to `AppConfig`.

```rust
use horsies::{AppConfig, ScheduleConfig, TaskSchedule};

let config = AppConfig {
    schedule: Some(ScheduleConfig::new(vec![/* ... */]).check_interval_seconds(1)),
    ..AppConfig::for_database_url("postgresql://...")
};
```

### Fields

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `enabled` | `bool` | `true` | Enable/disable scheduler |
| `check_interval_seconds` | `u32` | 1 | Seconds between schedule checks (1-60) |
| `schedules` | `Vec<TaskSchedule>` | `[]` | Schedule definitions |

## TaskSchedule

Defines a single scheduled task.

```rust
use horsies::{TaskSchedule, SchedulePattern, DailySchedule};
use chrono::NaiveTime;

TaskSchedule::new(
    "daily-report",
    "generate_report",
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
.kwargs(serde_json::json!({"format": "pdf"}))
```

### Fields

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `name` | `String` | required | Unique schedule identifier |
| `task_name` | `String` | required | Registered task name |
| `pattern` | `SchedulePattern` | required | When to run |
| `args` | `serde_json::Value` | `Null` | Positional arguments (JSON array) |
| `kwargs` | `serde_json::Value` | `Null` | Keyword arguments (JSON object) |
| `queue_name` | `Option<String>` | `None` | Queue override (CUSTOM mode) |
| `enabled` | `bool` | `true` | Enable/disable this schedule |
| `timezone` | `String` | `"UTC"` | Timezone for schedule evaluation |
| `catch_up_missed` | `bool` | `false` | Execute missed runs on restart |
| `max_catch_up_runs` | `u32` | `100` | Maximum runs to enqueue per scheduler tick when `catch_up_missed=true` (range: 1-10000) |

### Name

Must be unique across all schedules. Used for state tracking:

```rust
TaskSchedule::new(
    "hourly-sync",
    "sync_data",
    SchedulePattern::Interval(IntervalSchedule {
        hours: Some(1),
        ..Default::default()
    }),
);
TaskSchedule::new(
    "daily-cleanup",
    "cleanup_old_data",
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
    }),
);
```

### Task Name

Must match a registered `#[task]`:

```rust
#[task("send_notification")]
async fn send_notification(input: NotifyInput) -> Result<(), TaskError> {
    // ...
    Ok(())
}

TaskSchedule::new(
    "notify-users",
    "send_notification", // Must match
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
```

### Arguments

Pass arguments to the scheduled task as serialized JSON:

```rust
#[task("process_region")]
async fn process_region(input: RegionInput) -> Result<(), TaskError> {
    // ...
    Ok(())
}

TaskSchedule::new(
    "sync-us-east",
    "process_region",
    SchedulePattern::Interval(IntervalSchedule {
        hours: Some(1),
        ..Default::default()
    }),
)
.kwargs(serde_json::json!({
    "region": "us-east",
    "full_sync": true,
}))
```

### Queue Override

In CUSTOM mode, override the task's default queue:

```rust
#[task("background_job", queue = "normal")]
async fn background_job() -> Result<(), TaskError> {
    // ...
    Ok(())
}

TaskSchedule::new(
    "priority-job",
    "background_job",
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
    .queue("critical") // Override to higher priority queue
```

### Timezone

Schedule evaluated in specified timezone:

```rust
// Runs at 9 AM New York time (EST/EDT)
TaskSchedule::new(
    "morning-task",
    "my_task",
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
.timezone("America/New_York")
```

Uses IANA timezone names via `chrono-tz`. Common values:

- `"UTC"` - Coordinated Universal Time
- `"America/New_York"` - US Eastern
- `"America/Los_Angeles"` - US Pacific
- `"Europe/London"` - UK
- `"Asia/Tokyo"` - Japan

### Catch-Up

When `catch_up_missed=true`, missed runs are executed:

```rust
TaskSchedule::new(
    "hourly-sync",
    "sync_data",
    SchedulePattern::Interval(IntervalSchedule {
        hours: Some(1),
        ..Default::default()
    }),
)
.catch_up_missed(true)
```

If scheduler was down 3 hours, 3 tasks are enqueued on restart.

Use for:

- Data synchronization (must process all intervals)
- Compliance reporting (all periods must be covered)

Avoid for:

- Notifications (users do not want 24 emails at once)
- Status updates (only latest matters)

## Disabling Schedules

Disable individual schedules:

```rust
TaskSchedule::new(
    "deprecated-job",
    "old_task",
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
    .enabled(false) // Will not run
```

Disable entire scheduler:

```rust
ScheduleConfig::new(vec![])
    .enabled(false) // No schedules run
    .check_interval_seconds(30)
```

## Configuration Changes

When schedule configuration changes:

1. Scheduler detects via `config_hash`
2. Recalculates `next_run_at` from current time
3. Logs warning about configuration change

This prevents issues when:

- Pattern changes (e.g., hourly to daily)
- Timezone changes
- Time changes within pattern

## Validation

At scheduler startup, validates:

1. **Task exists**: `task_name` must be registered
2. **Queue valid**: `queue_name` must match configured queue (CUSTOM mode)
3. **Timezone valid**: Must be a recognized IANA timezone name
4. **Schedule names unique**: No duplicate `name` values

```rust
// Will fail at startup:
TaskSchedule::new(
    "bad",
    "nonexistent_task", // Not registered
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
```
