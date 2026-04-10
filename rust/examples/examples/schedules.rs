//! Schedule example: configure scheduled tasks and run the scheduler.
//!
//! This example is **standalone** — it runs its own worker + scheduler
//! because the scheduler IS the sender (it enqueues tasks on a timer).
//!
//! Run with:
//!   cargo run --example schedules -p horsies-examples

use horsies_examples::common;

use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveTime;
use tokio_util::sync::CancellationToken;

use horsies::{
    mask_database_url, spawn_scheduler, DailySchedule, Horsies, IntervalSchedule, ScheduleConfig,
    SchedulePattern, TaskSchedule, Weekday, WeeklySchedule, Worker, WorkerConfig,
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Database URL and base config from common module ────────────
    let db_url = common::db_url();
    eprintln!("Using database: {}\n", mask_database_url(&db_url));

    // Start from custom mode config and attach schedule
    let mut config = common::custom_mode::app_config(&db_url);

    // ── 2. Build schedule configuration ────────────────────────────────
    let schedule_config = ScheduleConfig::new(vec![
        // Fires every 3 seconds -- short enough to see activity during
        // the 10-second demo window.
        TaskSchedule::new(
            "fast_interval",
            "sync_inventory",
            SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(3),
                ..Default::default()
            }),
        )
        .queue(common::custom_mode::LOW),
        // Daily at 02:00 -- won't fire during the demo, but shows
        // how to configure a daily schedule.
        TaskSchedule::new(
            "daily_report",
            "generate_report",
            SchedulePattern::Daily(DailySchedule {
                time: NaiveTime::from_hms_opt(2, 0, 0).expect("valid time"),
            }),
        )
        .queue(common::custom_mode::LOW),
        // Weekly on Sunday at 03:00 -- configuration demo only.
        TaskSchedule::new(
            "weekly_cleanup",
            "cleanup_data",
            SchedulePattern::Weekly(WeeklySchedule {
                days: vec![Weekday::Sunday],
                time: NaiveTime::from_hms_opt(3, 0, 0).expect("valid time"),
            }),
        )
        .queue(common::custom_mode::LOW),
    ])
    // The scheduler polls for due schedules at this interval.
    .check_interval_seconds(1);

    config.schedule = Some(schedule_config.clone());

    // ── 3. Create Horsies app and register tasks ───────────────────────
    let mut app = Horsies::new(config.clone()).expect("valid AppConfig");
    common::tasks::schedules::register(&mut app)?;

    // ── 4. Validate schedules ──────────────────────────────────────────
    app.validate_schedules()
        .expect("all schedules reference registered tasks");
    eprintln!("Schedule validation passed.\n");

    // ── 5. Print schedule details ──────────────────────────────────────
    eprintln!("Configured schedules:");
    for sched in &schedule_config.schedules {
        let pattern_desc = match &sched.pattern {
            SchedulePattern::Interval(iv) => {
                format!("every {}s", iv.total_seconds())
            }
            SchedulePattern::Hourly(h) => {
                format!("hourly at :{:02}:{:02}", h.minute, h.second)
            }
            SchedulePattern::Daily(d) => {
                format!("daily at {}", d.time)
            }
            SchedulePattern::Weekly(w) => {
                format!("weekly {:?} at {}", w.days, w.time)
            }
            SchedulePattern::Monthly(m) => {
                format!("monthly day {} at {}", m.day, m.time)
            }
        };
        eprintln!(
            "  {:20} -> {:25} | {}",
            sched.name, sched.task_name, pattern_desc,
        );
    }
    eprintln!();

    // ── 6. Connect broker, migrate, and decompose the app ──────────────
    eprintln!("Connecting to PostgreSQL...");
    let (_, task_registry, workflow_registry, broker) = app.into_parts().await?;
    eprintln!("Running migrations...");
    broker.migrate().await?;
    eprintln!("Broker ready.\n");

    // ── 7. Start a worker in the background ────────────────────────────
    let mut worker_config = WorkerConfig::default();

    // Serve all custom queues
    if let Some(ref queues) = config.custom_queues {
        worker_config.queues = queues.iter().map(|q| q.name.clone()).collect();
    }
    worker_config.apply_queue_config(&config);

    let worker = Worker::new(
        broker.clone(),
        Arc::new(task_registry),
        Arc::new(workflow_registry),
        config.clone(),
        worker_config,
    )?;

    let worker_cancel = worker.cancel_token();

    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("Worker error: {}", e);
        }
    });

    eprintln!("Worker started.\n");

    // ── 8. Start the scheduler in the background ───────────────────────
    let scheduler_cancel = CancellationToken::new();

    let scheduler_handle =
        spawn_scheduler(broker, schedule_config, config, scheduler_cancel.clone());

    eprintln!("Scheduler started.  Waiting 12 seconds for scheduled tasks to fire...\n");

    // ── 9. Let the system run for ~12 seconds ─────────────────────────
    tokio::time::sleep(Duration::from_secs(12)).await;

    // ── 10. Shut down ──────────────────────────────────────────────────
    eprintln!("\nShutting down...");
    scheduler_cancel.cancel();
    worker_cancel.cancel();

    // Wait for both to finish (with a timeout).
    let _ = tokio::time::timeout(Duration::from_secs(5), scheduler_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;

    eprintln!("Done. The fast_interval schedule should have fired several times above.");
    eprintln!("The daily_report and weekly_cleanup schedules are configured but did not");
    eprintln!("fire because their next run times are far in the future.");

    Ok(())
}
