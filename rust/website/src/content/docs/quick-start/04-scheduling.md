---
title: Scheduling Tasks
summary: Run tasks on recurring schedules.
related: [03-defining-workflows]
tags: [quickstart, scheduling]
---

For full scheduler documentation, see [Scheduler Overview](/horsies-rust/scheduling/scheduler-overview/).

## Scheduled Tasks

```rust
use horsies::{task, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SyncResult {
    synced_items: u32,
    status: String,
}

#[task("sync_inventory", queue = "low")]
async fn sync_inventory() -> Result<SyncResult, TaskError> {
    Ok(SyncResult {
        synced_items: 150,
        status: "completed".into(),
    })
}

#[derive(Serialize, Deserialize)]
struct ReportResult {
    report_id: String,
    orders_shipped: u32,
}

#[task("generate_shipping_report", queue = "low")]
async fn generate_shipping_report() -> Result<ReportResult, TaskError> {
    Ok(ReportResult {
        report_id: "RPT-2024-001".into(),
        orders_shipped: 42,
    })
}

#[derive(Serialize, Deserialize)]
struct CleanupResult {
    archived_count: u32,
}

#[task("cleanup_completed_orders", queue = "low")]
async fn cleanup_completed_orders() -> Result<CleanupResult, TaskError> {
    Ok(CleanupResult {
        archived_count: 100,
    })
}
```

## Schedule Configuration

```rust
use chrono::NaiveTime;
use horsies::{
    AppConfig, ScheduleConfig, TaskSchedule, SchedulePattern,
    IntervalSchedule, DailySchedule, WeeklySchedule, Weekday,
};

let schedule_config = ScheduleConfig::new(vec![
    TaskSchedule::new(
        "inventory_sync",
        "sync_inventory",
        SchedulePattern::Interval(IntervalSchedule {
            minutes: Some(30),
            ..Default::default()
        }),
    )
    .queue("low"),
    TaskSchedule::new(
        "daily_shipping_report",
        "generate_shipping_report",
        SchedulePattern::Daily(DailySchedule {
            time: NaiveTime::from_hms_opt(2, 0, 0).unwrap(),
        }),
    )
    .queue("low"),
    TaskSchedule::new(
        "weekly_cleanup",
        "cleanup_completed_orders",
        SchedulePattern::Weekly(WeeklySchedule {
            days: vec![Weekday::Sunday],
            time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
        }),
    )
    .queue("low"),
])
.check_interval_seconds(30);

let config = AppConfig {
    schedule: Some(schedule_config),
    ..AppConfig::for_database_url("postgresql://localhost/mydb")
};
```

## Schedule Patterns

| Pattern | Example | Use Case |
|---------|---------|----------|
| `IntervalSchedule` | `minutes: Some(30)` | Frequent syncs |
| `HourlySchedule` | `minute: 0` | Hourly reports |
| `DailySchedule` | `time: 02:00` | Nightly jobs |
| `WeeklySchedule` | `Sunday 03:00` | Weekly cleanup |
| `MonthlySchedule` | `day: 1` | Monthly reports |

## Running the Scheduler

```rust
app.run_scheduler().await?;
```
