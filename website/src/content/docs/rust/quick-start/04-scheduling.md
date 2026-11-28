---
title: Scheduling Tasks
summary: Run the recurring work of the Acme shop with TaskSchedule and SchedulePattern, and start the scheduler.
related: [03-defining-workflows, 05-workflow-patterns, ../scheduling/schedule-config, ../cli]
tags: [rust, quickstart, scheduling, TaskSchedule, SchedulePattern]
---

# Scheduling Tasks

A schedule sends a task at a recurring time. This page adds the recurring work of Acme Clothing: the supplier feed, the sales rollup and the retention audit. For the full scheduler documentation, see [Scheduler Overview](/horsies/scheduling/scheduler-overview/).

## The Scheduled Tasks

The schedule file uses the `chrono` crate for the time of day:

```bash
cargo add chrono
```

In `src/domain.rs`, add:

<!-- rust-check: skip partial -- adds types to the domain file of page 2 -->
```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplierFeed {
    pub supplier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SalesRollup {
    pub window: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionAudit {
    pub older_than_days: i32,
}
```

In `src/tasks/inventory.rs`, add the supplier feed:

<!-- rust-check: skip partial -- adds a task to the inventory module of page 3 -->
```rust
use crate::domain::SupplierFeed;

#[task("sync_supplier_feed", queue = "analytics")]
pub async fn sync_supplier_feed(supplier: String) -> Result<SupplierFeed, TaskError<AcmeCode>> {
    Ok(SupplierFeed { supplier })
}
```

Register it in the `register` function of the same file. The two other tasks go into `src/tasks/analytics.rs`:

<!-- rust-check: skip partial -- uses the domain module of the example package -->
```rust
use horsies::{task, Horsies, HorsiesError, TaskError};

use crate::domain::{AcmeCode, RetentionAudit, SalesRollup};

#[task("sales_rollup", queue = "analytics")]
pub async fn sales_rollup(window: String) -> Result<SalesRollup, TaskError<AcmeCode>> {
    Ok(SalesRollup { window })
}

#[task("retention_audit", queue = "analytics")]
pub async fn retention_audit(older_than_days: i32) -> Result<RetentionAudit, TaskError<AcmeCode>> {
    Ok(RetentionAudit { older_than_days })
}

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _sales_rollup = sales_rollup::register(app)?;
    let _retention_audit = retention_audit::register(app)?;
    Ok(())
}
```

Add `pub mod analytics;` to `src/tasks/mod.rs`, and `tasks::analytics::register` to the list of `discover` in `src/app.rs`.

## The Schedules

In `src/schedules.rs`:

<!-- rust-check: skip partial -- uses the tasks module of the example package -->
```rust
use std::time::Duration;

use chrono::NaiveTime;
use horsies::{
    DailySchedule, IntervalSchedule, SchedulePattern, ScheduleConfig, TaskSchedule, Weekday,
    WeeklySchedule,
};

use crate::tasks::analytics::{retention_audit, sales_rollup};
use crate::tasks::inventory::sync_supplier_feed;

fn at(hour: u32, minute: u32) -> Result<NaiveTime, String> {
    NaiveTime::from_hms_opt(hour, minute, 0).ok_or_else(|| format!("{hour}:{minute} is not a time of day"))
}

pub fn schedules() -> Result<ScheduleConfig, Box<dyn std::error::Error>> {
    let supplier_feed = TaskSchedule::new(
        "supplier-feed-atlas-textiles",
        "sync_supplier_feed",
        SchedulePattern::Interval(IntervalSchedule { every: Duration::from_secs(90) }),
    )
    .input(&sync_supplier_feed::Input { supplier: "atlas-textiles".to_owned() })?;

    let sales_rollup_daily = TaskSchedule::new(
        "sales-rollup-daily",
        "sales_rollup",
        SchedulePattern::Daily(DailySchedule { time: at(3, 0)? }),
    )
    .input(&sales_rollup::Input { window: "daily".to_owned() })?;

    let retention_audit_daily = TaskSchedule::new(
        "retention-audit-daily",
        "retention_audit",
        SchedulePattern::Daily(DailySchedule { time: at(3, 30)? }),
    )
    .input(&retention_audit::Input { older_than_days: 90 })?;

    let weekly_sales_review = TaskSchedule::new(
        "weekly-sales-review",
        "sales_rollup",
        SchedulePattern::Weekly(WeeklySchedule { weekdays: vec![Weekday::Monday], time: at(6, 0)? }),
    )
    .input(&sales_rollup::Input { window: "weekly".to_owned() })?;

    Ok(ScheduleConfig::new(vec![
        supplier_feed,
        sales_rollup_daily,
        retention_audit_daily,
        weekly_sales_review,
    ]))
}
```

- `TaskSchedule::new(name, task_name, pattern)` takes a unique schedule name, the name of a registered task, and a pattern.
- `.input(&value)?` sets the input of each send. `sync_supplier_feed::Input` has one field for each parameter of the task, so a wrong field name or type does not compile.
- A time of day is in the time zone of the schedule. The default is `UTC`. `.timezone("Europe/Berlin")` sets an IANA name.
- A schedule sends to the queue of its task. `.queue(name)` sends to another queue of the app.

## Add the Schedules to the App

In `src/app.rs`, set the `schedule` field of the config:

<!-- rust-check: skip partial -- a fragment of build_app in the example package -->
```rust
let config = AppConfig {
    queue_mode: QueueMode::Custom,
    schedule: Some(schedules()?),
    // ... the other fields of page 1
    ..AppConfig::from_database_url(DATABASE_URL)
};
```

`schedules()` can fail, so `build_app` returns `Result<Horsies, Box<dyn std::error::Error>>`. `src/main.rs` already prints the refusal.

`Horsies::new` refuses a schedule section that breaks a rule of the pattern tables (`HRS-205`). The scheduler start and `check` also refuse:

| Check | Code |
|-------|------|
| `task_name` is a task that this binary registers | `HRS-300` |
| The queue of the schedule is a queue of the app | `HRS-205` |
| `input` reads back with the input type of the task | `HRS-205` |

## Schedule Patterns

| Pattern | Example | Use |
|---------|---------|-----|
| `Interval(IntervalSchedule)` | `every: Duration::from_secs(90)` | Frequent syncs |
| `Hourly(HourlySchedule)` | `minute: 0, second: 0` | Hourly reports |
| `Daily(DailySchedule)` | `time: 03:00` | Nightly jobs |
| `Weekly(WeeklySchedule)` | `weekdays: [Monday], time: 06:00` | Weekly reviews |
| `Monthly(MonthlySchedule)` | `day_of_month: 1, time: 05:00` | Monthly close |
| `Cron(CronSchedule)` | typed `minute`, `hour`, `month` and `day` | What a crontab line holds |

The fields of each pattern, the ranges and the typed cron are on [Schedule Config](/horsies/rust/scheduling/schedule-config/).

## Run the Scheduler

```bash
cargo run --bin getting-started -- scheduler --log-level INFO
```

The scheduler sends the tasks. A worker runs them, so start a worker too:

```bash
cargo run --bin getting-started -- worker --profile default
```

Several schedulers can run against one database. Each send locks the state row of its schedule, so a second scheduler writes nothing for a time that the first one sent.
