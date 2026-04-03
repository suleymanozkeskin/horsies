//! Dispatch registered tasks from inside a task via generated helpers.
//!
//! This example shows the intended ergonomic path:
//! - register tasks once
//! - call `task_name::send(&rt, args)` or `task_name::schedule(&rt, delay, args)`
//! - use `task_name::handle(&rt)` when reusing a handle repeatedly

use horsies::{task, AppConfig, Horsies, PostgresConfig, QueueMode, TaskError, TaskRuntime};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AddNumbersInput {
    a: i32,
    b: i32,
}

#[task("add_numbers")]
async fn add_numbers(input: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(input.a + input.b)
}

#[task("enqueue_add_numbers")]
async fn enqueue_add_numbers(rt: TaskRuntime) -> Result<(), TaskError> {
    add_numbers::send(&rt, AddNumbersInput { a: 1, b: 2 })
    .await
    .map_err(|err| TaskError::user("SEND_FAILED", err.message))?;

    add_numbers::schedule(&rt, std::time::Duration::from_secs(30), AddNumbersInput { a: 3, b: 4 })
    .await
    .map_err(|err| TaskError::user("SCHEDULE_FAILED", err.message))?;

    let add = add_numbers::handle(&rt)?;
    for (a, b) in [(5, 6), (7, 8)] {
        add
            .send(AddNumbersInput { a, b })
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

    add_numbers::register(&mut app)?;
    let _enqueue = enqueue_add_numbers::register(&mut app)?;

    println!("registered runtime task dispatch example");
    println!("inside a worker, call task_name::send/schedule/handle(&rt, ...) directly");

    Ok(())
}
