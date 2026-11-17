//! Schedules: the scheduler sends a task on a pattern. A schedule's input is
//! the task's stored input, one member per parameter; the check decodes it
//! with the task's input type before the scheduler starts.
//!
//! ```text
//! cargo run -p horsies-examples --example schedules -- check
//! cargo run -p horsies-examples --example schedules -- scheduler
//! cargo run -p horsies-examples --example schedules -- worker --profile default
//! ```
//!
//! `demo` runs the same check in code and prints the schedules.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;

use horsies::{
    task, AppConfig, Horsies, HorsiesError, HourlySchedule, IntervalSchedule, ScheduleConfig,
    SchedulePattern, TaskError, TaskSchedule,
};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};

#[task("record_heartbeat")]
async fn record_heartbeat(source: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("heartbeat from {source}"))
}

#[task("compact_logs")]
async fn compact_logs(keep_days: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(keep_days)
}

fn schedules() -> ScheduleConfig {
    ScheduleConfig::new(vec![
        TaskSchedule::new(
            "heartbeat-every-10s",
            "record_heartbeat",
            SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(10),
                ..IntervalSchedule::default()
            }),
        )
        .input(serde_json::json!({ "source": "scheduler" })),
        TaskSchedule::new(
            "compact-hourly",
            "compact_logs",
            SchedulePattern::Hourly(HourlySchedule {
                minute: 5,
                second: 0,
            }),
        )
        .input(serde_json::json!({ "keep_days": 7 })),
    ])
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let config = AppConfig {
        schedule: Some(schedules()),
        worker_profiles: BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]),
        ..AppConfig::for_database_url(database_url())
    };
    let mut app = Horsies::new(config)?;
    let _record_heartbeat = record_heartbeat::register(&mut app)?;
    let _compact_logs = compact_logs::register(&mut app)?;
    Ok(app)
}

async fn demo(app: Horsies) -> DemoResult {
    let passed = app.check()?;
    println!("{passed}");
    for schedule in app
        .config()
        .schedule
        .iter()
        .flat_map(|config| &config.schedules)
    {
        println!(
            "{} sends {} with {}",
            schedule.name, schedule.task_name, schedule.input
        );
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
