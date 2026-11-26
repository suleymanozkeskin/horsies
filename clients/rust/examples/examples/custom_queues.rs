//! Custom queues: each queue has a priority (1 is claimed first) and an
//! optional cap on its running tasks. A task names its queue.
//!
//! ```text
//! cargo run -p horsies-examples --example custom_queues -- worker --profile default
//! cargo run -p horsies-examples --example custom_queues -- demo
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    task, AppConfig, CustomQueueConfig, Horsies, HorsiesError, QueueMode, TaskError, TaskOutcome,
    WaitLimit,
};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};

#[task("send_alert", queue = "critical")]
async fn send_alert(message: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("alert sent: {message}"))
}

#[task("rebuild_index", queue = "bulk")]
async fn rebuild_index(shard: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(shard)
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let config = AppConfig {
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            CustomQueueConfig {
                name: "critical".to_owned(),
                priority: 1,
                max_concurrency: None,
            },
            CustomQueueConfig {
                name: "bulk".to_owned(),
                priority: 50,
                max_concurrency: Some(1),
            },
        ]),
        worker_profiles: BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]),
        ..AppConfig::for_database_url(database_url())
    };
    let mut app = Horsies::new(config)?;
    let _send_alert = send_alert::register(&mut app)?;
    let _rebuild_index = rebuild_index::register(&mut app)?;
    Ok(app)
}

async fn demo(_app: Horsies) -> DemoResult {
    let limit = WaitLimit::Within(Duration::from_secs(60));
    let mut shards = Vec::new();
    for shard in 0..4 {
        shards.push(rebuild_index::send(shard).await?);
    }
    // Sent last, claimed first: `critical` has the higher priority.
    let alert = send_alert::send("disk almost full".to_owned()).await?;

    match alert.get(limit).await? {
        TaskOutcome::Succeeded(text) => println!("{text}"),
        TaskOutcome::Failed(error) => println!("send_alert failed: {error}"),
    }
    for shard in shards {
        match shard.get(limit).await? {
            TaskOutcome::Succeeded(done) => println!("rebuilt shard {done}"),
            TaskOutcome::Failed(error) => println!("rebuild_index failed: {error}"),
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
