---
title: Schedule Config
summary: Define the schedules of a Rust app with ScheduleConfig, TaskSchedule and SchedulePattern, and run the scheduler.
related: [../configuration/app-config, ../cli, ../../scheduling/scheduler-overview]
tags: [rust, scheduling, ScheduleConfig, TaskSchedule, SchedulePattern, CronSchedule]
---

# Schedule Config

`AppConfig.schedule` is an `Option<ScheduleConfig>`: the schedules that the scheduler sends. How the scheduler fires a slot, keeps its state rows, reads the database clock and catches up is on [Scheduler Overview](/horsies/scheduling/scheduler-overview/).

## Define Schedules

```rust
use std::convert::Infallible;

use chrono::NaiveTime;
use horsies::{
    task, AppConfig, DailySchedule, Horsies, HorsiesError, IntervalSchedule, ScheduleConfig,
    SchedulePattern, TaskError, TaskSchedule,
};

#[task("build_report")]
async fn build_report(region: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("report of {region}"))
}

#[task("clean_cache")]
async fn clean_cache() -> Result<u32, TaskError<Infallible>> {
    Ok(0)
}

fn schedules() -> Result<ScheduleConfig, Box<dyn std::error::Error>> {
    let three_am = NaiveTime::from_hms_opt(3, 0, 0).ok_or("03:00:00 is a time of day")?;
    let nightly_report = TaskSchedule::new(
        "nightly-eu-report",
        "build_report",
        SchedulePattern::Daily(DailySchedule { time: three_am }),
    )
    .input(&build_report::Input { region: "eu".to_owned() })?
    .timezone("Europe/Berlin");
    let cache_cleanup = TaskSchedule::new(
        "cache-cleanup",
        "clean_cache",
        SchedulePattern::Interval(IntervalSchedule { minutes: Some(15), ..IntervalSchedule::default() }),
    );
    Ok(ScheduleConfig::new(vec![nightly_report, cache_cleanup]))
}

fn build_app(database_url: &str) -> Result<Horsies, Box<dyn std::error::Error>> {
    let config = AppConfig {
        schedule: Some(schedules()?),
        ..AppConfig::for_database_url(database_url)
    };
    let mut app = Horsies::new(config)?;
    let _report = build_report::register(&mut app)?;
    let _cleanup = clean_cache::register(&mut app)?;
    Ok(app)
}
```

```bash
./myapp scheduler
```

The `scheduler` command, its options and its exit codes are on [CLI Reference](/horsies/rust/cli/#scheduler).

## ScheduleConfig

| Field | Type | Default | Rule |
|-------|------|---------|------|
| `enabled` | `bool` | `true` | The scheduler does not start when it is `false` |
| `schedules` | `Vec<TaskSchedule>` | empty | Each name is unique |
| `check_interval_seconds` | `u32` | 1 | 1 to 60: the time between two checks of the schedules |

`ScheduleConfig::new(schedules)` takes the defaults. `.enabled(bool)` and `.check_interval_seconds(seconds)` set the other fields.

## TaskSchedule

`TaskSchedule::new(name, task_name, pattern)` builds a schedule with the defaults:

| Field | Builder | Type | Default |
|-------|---------|------|---------|
| `name` | `new` | `String` | – |
| `task_name` | `new` | `String`: a registered task | – |
| `pattern` | `new` | `SchedulePattern` | – |
| `input` | `.input(&value)?` | `Option<JsonText>`: the input's JSON text | `None`: the task gets the input `{}` |
| `queue_name` | `.queue(name)` | `Option<String>` | `None`: the queue of the task |
| `enabled` | `.enabled(bool)` | `bool` | `true` |
| `timezone` | `.timezone(name)` | `String`: an IANA name | `UTC` |
| `catch_up_missed` | `.catch_up_missed(bool)` | `bool` | `false` |
| `max_catch_up_runs` | `.max_catch_up_runs(n)` | `u32`: 1 to 10,000 | 100 |

- `input` is the task input in the stored form: one member per task parameter, by name. A task `fn build_report(region: String)` reads `{"region": "eu"}`, which `build_report::Input { region }` writes. See [Defining Tasks](/horsies/rust/tasks/defining-tasks/#define-a-task).
- `.input(&value)` writes `value` with the strict writer of every stored form and returns `Result<TaskSchedule, JsonTextRefused>`. An integer keeps every digit: a `u128` field of `u128::MAX` is stored as `340282366920938463463374607431768211455`. `JsonTextRefused::NotJsonNative` is a value that does not write as JSON (a non-finite float, a map key that is not a text); `JsonTextRefused::NotStrictJson` is a text deeper than 300 levels, with an integer of more than 4,300 digits, or with a repeated key.
- A config text of any serde format gives the `input` member as its value: TOML (`toml::from_str`, `horsies web CONFIG`) and JSON (serde_json). The read writes the value with the same strict writer, so the input text is compact and keeps no spelling of the file. `null` is no input. The read refuses what `.input` refuses: a non-finite float (TOML `nan`, `inf`), a value deeper than 300 levels, a repeated key.
- In a config file, an integer of `input` must fit `i64` or `u64`. TOML holds no wider integer, and serde_json's untyped read gives a wider one as a float. Set an input with a wider integer in code with `.input(&value)`, which keeps every digit.
- A time of day is in the schedule's `timezone`.
- The meaning of `catch_up_missed` and `max_catch_up_runs` is on [Scheduler Overview](/horsies/scheduling/scheduler-overview/#catch-up-logic).

## SchedulePattern

| Variant | Fields | Rule |
|---------|--------|------|
| `Interval(IntervalSchedule)` | `seconds`, `minutes`, `hours`, `days`: `Option<u32>` | At least one unit. `seconds` 1 to 86,400; `minutes` 1 to 1,440; `hours` 1 to 168; `days` 1 to 365. The interval is the sum of the units |
| `Hourly(HourlySchedule)` | `minute`, `second`: `u32` | Each 0 to 59 |
| `Daily(DailySchedule)` | `time: NaiveTime` | A whole second |
| `Weekly(WeeklySchedule)` | `days: Vec<Weekday>`, `time: NaiveTime` | At least one day, no repeat; `time` a whole second |
| `Monthly(MonthlySchedule)` | `day: u32`, `time: NaiveTime` | `day` 1 to 31; `time` a whole second |
| `Cron(CronSchedule)` | `minute`, `hour`, `month`, `day` | See [Typed Cron](#typed-cron) |

`IntervalSchedule::default()` has no unit; set at least one with struct update syntax. `Weekday` is `Monday` to `Sunday`; `Month` is `January` to `December`. Both are enums of the crate.

### Typed Cron

`CronSchedule` holds what a `minute hour day-of-month month day-of-week` crontab line holds, in typed fields. It fires at second 0.

```rust
use horsies::{CronEnumTerm, CronNumericTerm, CronSchedule, DaySelector, SchedulePattern, Weekday};

fn weekdays_at_nine_thirty() -> SchedulePattern {
    SchedulePattern::Cron(CronSchedule {
        minute: vec![CronNumericTerm::Values { values: vec![30] }],
        hour: vec![CronNumericTerm::Values { values: vec![9] }],
        month: vec![CronEnumTerm::Every],
        day: DaySelector::ByWeekday {
            day_of_week: vec![CronEnumTerm::EnumRange {
                start: Weekday::Monday,
                end: Weekday::Friday,
                step: 1,
            }],
        },
    })
}
```

| Field | Type | Domain |
|-------|------|--------|
| `minute` | `Vec<CronNumericTerm>`, at least one term | 0 to 59 |
| `hour` | `Vec<CronNumericTerm>`, at least one term | 0 to 23 |
| `month` | `Vec<CronEnumTerm<Month>>`, at least one term | `January` (1) to `December` (12) |
| `day` | `DaySelector` | Day of month 1 to 31; day of week `Monday` (0) to `Sunday` (6) |

| `CronNumericTerm` | Crontab form |
|-------------------|--------------|
| `Every` | `*` |
| `Step { step }` | `*/n`, from the start of the domain |
| `Values { values }` | `a,b,c` |
| `Range { start, end, step }` | `a-b` or `a-b/n` |

| `CronEnumTerm<T>` | Crontab form |
|-------------------|--------------|
| `Every` | `*` |
| `EnumValues { values }` | `a,b,c` |
| `EnumRange { start, end, step }` | `a-b` or `a-b/n`, by ordinal |
| `EnumStep { step }` | `*/n`, by ordinal |

`DaySelector` makes the choice between the day of the month and the day of the week explicit:

| Variant | Fires on |
|---------|----------|
| `EveryDay` | Every day |
| `ByMonthDay { day_of_month }` | The days of the month |
| `ByWeekday { day_of_week }` | The days of the week |
| `EitherDay { day_of_month, day_of_week }` | A day that matches one of the two |
| `BothDays { day_of_month, day_of_week }` | A day that matches both |

A step is 1 or more and at most the span of its field. A range has `start` at most `end`. A month and day-of-month combination that no date has is refused.

## Checks

`Horsies::new` refuses a schedule section that breaks a rule of the tables above, with `HRS-205`. The scheduler start and `app.check()` check each enabled schedule, in config order, and report the first refusal of each:

| Check | Code |
|-------|------|
| `task_name` is a task registered in this binary | `HRS-300` |
| `queue_name` is a queue of the app | `HRS-205` |
| `input` reads back with the task's input type | `HRS-205` |
| The input size is within `AppConfig.payload.reject_bytes` | `HRS-205` |

The check runs without a database. A worker skips it: a worker never sends a scheduled task.

## Run the Scheduler

`<binary> scheduler` runs the scheduler through `horsies::cli::run(app)`. A binary that owns its `main` calls the methods:

| Method | Does |
|--------|------|
| `app.scheduler()` | `Result<PreparedScheduler, SchedulerRunError>`: the checks and the scheduler manifest, with no I/O |
| `prepared.stop_handle()` | The `SchedulerStop` of the scheduler. `request_stop()` ends the run; a fire in flight runs to its end |
| `prepared.run().await` | `Result<SchedulerStopped, SchedulerRunError>`: connects, installs the schema once, starts the schedule state, then checks the schedules at once and every `check_interval_seconds` until a stop request. A failed check is logged and the next one runs |
| `app.run_scheduler().await` | `scheduler()` and `run()`, stopped by `SIGTERM` or `SIGINT` |

| `SchedulerRunError` | When |
|---------------------|------|
| `NotConfigured` | The config has no schedule section. Nothing started |
| `Disabled` | `schedule.enabled` is `false`. Nothing started |
| `CheckRefused(CheckReport)` | The check refused a task's options or a schedule. Nothing started |
| `ManifestRefused { cause }` | The core refused the scheduler manifest: a value outside its bound, a pattern that breaks its rules, or a repeated schedule name. Nothing started |
| `StartRefused { schedule, reason }` | The start refused `schedule` before any state write: a time zone that the core does not know, a send that the rules refuse, or a pattern with no slot after the database time |
| `StartFailed { message, retryable }` | The connect, the schema install or the client family check failed. No schedule state was written |
| `RuntimeUnavailable(..)` | The process runtime of the core cannot listen for the stop signals. Nothing started |

Several schedulers can run against one database: each fire locks the state row of its schedule, so a second scheduler writes nothing for a slot that the first one fired.

## Things to Avoid

**Do not pass a struct parameter as the `input`.** A task `fn build_report(request: ReportRequest)` reads `{"request": {...}}`: pass `build_report::Input { request }`, not the `ReportRequest`.

**Do not rename a task parameter that a schedule's `input` names.** The parameter name is the stored key. The check refuses the old key, and a slot that the scheduler already sent fails with `WORKER_SERIALIZATION_ERROR`.
