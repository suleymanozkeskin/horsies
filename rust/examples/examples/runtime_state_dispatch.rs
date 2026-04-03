//! Access app-provided typed state from inside a task via `TaskRuntime`.
//!
//! This example shows the next ergonomic layer after `rt.start(spec)`: provide
//! a typed task-handle group once at setup, then retrieve it inside a task
//! without globals.

use horsies::{
    task, AppConfig, Horsies, PostgresConfig, QueueMode, TaskError, TaskFunction, TaskRuntime,
};

struct DispatchTasks {
    extract_attachment_text: TaskFunction<ExtractTextInput, ()>,
}

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
    let tasks = rt.state::<DispatchTasks>()?;

    for (file_id, bundesland) in [(11, "berlin"), (12, "hamburg")] {
        tasks
            .extract_attachment_text
            .send(ExtractTextInput {
                file_id,
                bundesland: bundesland.to_owned(),
            })
            .await
            .map_err(|err| TaskError::user("SEND_FAILED", err.message))?;
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

    let extract = extract_attachment_text::register(&mut app)?;
    let _enqueue = enqueue_extract_jobs::register(&mut app)?;

    app.provide(DispatchTasks {
        extract_attachment_text: extract,
    })?;

    println!("registered task runtime state example");
    println!("inside a worker, call `rt.state::<DispatchTasks>()?` to get typed handles");

    Ok(())
}
