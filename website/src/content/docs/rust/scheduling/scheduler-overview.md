---
title: Scheduler Overview
summary: How the scheduler enqueues tasks on a schedule.
tags: [scheduling, scheduler, process]
---

## Architecture

<!-- todo:diagram-needed - Scheduler architecture -->

```text
+------------------------------+
|  Scheduler (tokio task)      |
|  - Checks schedules          |
|  - Calculates next run       |
|  - Enqueues via broker       |
+--------------+---------------+
               | enqueue()
               v
+------------------------------+
|  PostgreSQL                  |
|  - tasks table               |
|  - schedule_state table      |
+--------------+---------------+
               | NOTIFY
               v
+------------------------------+
|  Workers                     |
|  - Execute scheduled tasks   |
+------------------------------+
```

The scheduler:

1. Runs as a separate process (or tokio task) from workers
2. Checks configured schedules at regular intervals
3. Enqueues tasks when schedules are due
4. Tracks state in database to prevent duplicates

## Complete Example

This is the smallest realistic scheduler setup: one registered task, one schedule, one scheduler process.

```rust
use chrono::NaiveTime;
use horsies::{
    task, AppConfig, DailySchedule, Horsies, ScheduleConfig, SchedulePattern,
    TaskError, TaskSchedule,
};

#[task("cleanup_old_data")]
async fn cleanup_old_data() -> Result<(), TaskError> {
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig {
        schedule: Some(ScheduleConfig::new(vec![
            TaskSchedule::new(
                "daily-cleanup",
                "cleanup_old_data",
                SchedulePattern::Daily(DailySchedule {
                    time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
                }),
            ),
        ])),
        ..AppConfig::for_database_url("postgresql://localhost/mydb")
    };

    let mut app = Horsies::new(config)?;
    cleanup_old_data::register(&mut app)?;

    app.run_scheduler().await?;
    Ok(())
}
```

In production, the scheduler usually runs in its own process and workers run in separate processes. The scheduler only enqueues; workers execute the scheduled tasks.

## Configuration

Add a `ScheduleConfig` to your `AppConfig`:

```rust
use horsies::{
    AppConfig, ScheduleConfig, TaskSchedule,
    SchedulePattern, DailySchedule,
};
use chrono::NaiveTime;

let config = AppConfig {
    schedule: Some(ScheduleConfig::new(vec![
        TaskSchedule::new(
            "daily-cleanup",
            "cleanup_old_data",
            SchedulePattern::Daily(DailySchedule {
                time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            }),
        ),
    ])),
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Running the Scheduler

```rust
app.run_scheduler().await?;
```

Run separately from workers. One scheduler per cluster is sufficient in normal deployments. Multiple schedulers can coexist safely because the scheduler loop serializes work with a PostgreSQL advisory lock. The startup path connects to PostgreSQL and ensures the Horsies schema before the scheduler loop begins.

For manual control over the scheduler lifecycle:

```rust
use horsies::spawn_scheduler;
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let handle = spawn_scheduler(broker, schedule_config, app_config, cancel.clone());

// Later, to stop:
cancel.cancel();
handle.await?;
```

If you construct the broker yourself instead of taking it from `app.get_broker().await?`, ensure the schema first:

```rust
broker.ensure_schema_initialized().await?;
```

## What the Scheduler Actually Does

On startup, the scheduler:

1. validates the configured schedules
2. connects to PostgreSQL
3. ensures the Horsies schema exists
4. initializes or refreshes rows in `horsies_schedule_state`

On each tick, it:

1. acquires a PostgreSQL advisory lock
2. checks which schedules are due
3. enqueues deterministic task IDs for those runs
4. updates `horsies_schedule_state`

That advisory lock is why multiple scheduler instances do not double-enqueue the same run.

## Key Concepts

### Schedule Name

Each schedule has a unique name used for state tracking:

```rust
TaskSchedule::new(
    "daily-report", // Must be unique
    "generate_report",
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
```

### Task Name

The `task_name` must match a registered `#[task]`:

```rust
#[task("generate_report")]
async fn generate_report() -> Result<String, TaskError> {
    // ...
    Ok("done".into())
}

TaskSchedule::new(
    "weekly-report",
    "generate_report", // Must match #[task] name
    SchedulePattern::Weekly(WeeklySchedule {
        days: vec![Weekday::Monday],
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
```

### Check Interval

How often the scheduler checks for due schedules:

```rust
ScheduleConfig::new(vec![/* ... */])
    .check_interval_seconds(1) // Check every second
```

Range: 1-60 seconds. Lower values provide better precision but more database queries.

## State Tracking

The `schedule_state` table tracks:

| Field | Purpose |
| ----- | ------- |
| `schedule_name` | Schedule identifier |
| `last_run_at` | When schedule last executed |
| `next_run_at` | When schedule should run next |
| `last_task_id` | ID of most recent enqueued task |
| `run_count` | Total execution count |
| `config_hash` | Detects configuration changes |

This prevents duplicate executions when:

- Scheduler restarts
- Multiple schedulers run (advisory locks serialize)
- Network issues cause delays

## Catch-Up Logic

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
.catch_up_missed(true) // Execute missed runs
```

If the scheduler was down for 3 hours, it will enqueue 3 tasks on restart, up to `max_catch_up_runs` for one scheduler tick.

When `catch_up_missed=false` (default), only the next scheduled run is executed.

## Timezone Support

Each schedule can have its own timezone:

```rust
TaskSchedule::new(
    "morning-report",
    "send_report",
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
.timezone("America/New_York") // 9 AM Eastern
```

Uses IANA timezone names via `chrono-tz`. Default is "UTC".

## Validation

Schedules are validated at startup:

- Task must be registered
- Queue must be valid (CUSTOM mode)
- Timezone must be a valid IANA name

```rust
// This will fail at scheduler start:
TaskSchedule::new(
    "bad",
    "nonexistent_task", // Not registered
    SchedulePattern::Daily(DailySchedule {
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    }),
)
```

## Graceful Shutdown

The scheduler handles SIGTERM/SIGINT (or cancellation token) for clean shutdown. The current schedule check completes, then the scheduler exits.
