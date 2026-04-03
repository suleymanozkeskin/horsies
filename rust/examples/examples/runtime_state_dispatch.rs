//! Dispatch registered tasks from inside a task via generated helpers.
//!
//! This example shows the intended ergonomic path:
//! - register tasks once
//! - call `task_name::send(&rt, args)` or `task_name::schedule(&rt, delay, args)`
//! - use `task_name::handle(&rt)` when reusing a handle repeatedly

use horsies::{task, AppConfig, Horsies, PostgresConfig, QueueMode, TaskError, TaskRuntime};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ExtractTextInput {
    file_id: i32,
    bundesland: String,
}

#[task("extract_attachment_text")]
async fn extract_attachment_text(_input: ExtractTextInput) -> Result<(), TaskError> {
    Ok(())
}

#[task("enqueue_extract_jobs")]
async fn enqueue_extract_jobs(rt: TaskRuntime) -> Result<(), TaskError> {
    extract_attachment_text::send(
        &rt,
        ExtractTextInput {
            file_id: 11,
            bundesland: "berlin".to_owned(),
        },
    )
    .await
    .map_err(|err| TaskError::user("SEND_FAILED", err.message))?;

    extract_attachment_text::schedule(
        &rt,
        std::time::Duration::from_secs(30),
        ExtractTextInput {
            file_id: 12,
            bundesland: "hamburg".to_owned(),
        },
    )
    .await
    .map_err(|err| TaskError::user("SCHEDULE_FAILED", err.message))?;

    let extract = extract_attachment_text::handle(&rt)?;
    for file_id in [13, 14] {
        extract
            .send(ExtractTextInput {
                file_id,
                bundesland: "berlin".to_owned(),
            })
            .await
            .map_err(|err| TaskError::user("SEND_FAILED", err.message.clone()))?;
    }

    Ok(())
}

fn config() -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: PostgresConfig {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://localhost/horsies_example".to_owned()),
            pool_pre_ping: true,
            pool_size: 5,
            max_overflow: 5,
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
        },
        resend_on_transient_err: false,
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: horsies::RecoveryConfig::default(),
        resilience: horsies::WorkerResilienceConfig::default(),
        schedule: None,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(config())?;

    extract_attachment_text::register(&mut app)?;
    let _enqueue = enqueue_extract_jobs::register(&mut app)?;

    println!("registered runtime task dispatch example");
    println!("inside a worker, call task_name::send/schedule/handle(&rt, ...) directly");

    Ok(())
}
