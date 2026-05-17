//! Dispatch registered tasks from any call site via generated helpers.
//!
//! This example shows the intended ergonomic path:
//! - register tasks once at startup
//! - call `task_name::send(args)` or `task_name::schedule(delay, args)` from anywhere
//! - use `task_name::handle(&rt)` for explicit runtime-based dispatch

use horsies::{task, AppConfig, Horsies, TaskError, TaskRuntime};
use horsies_examples::common;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AddNumbersInput {
    a: i32,
    b: i32,
}

#[task("add_numbers")]
async fn add_numbers(input: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(input.a + input.b)
}

/// This task dispatches other tasks using the global helpers — no &rt needed.
#[task("enqueue_add_numbers")]
async fn enqueue_add_numbers(_rt: TaskRuntime) -> Result<(), TaskError> {
    // Global send — works from anywhere after register()
    add_numbers::send(AddNumbersInput { a: 1, b: 2 })
        .await
        .map_err(|err| TaskError::new("SEND_FAILED", err.message))?;

    // Global schedule — same, no &rt needed
    add_numbers::schedule(
        std::time::Duration::from_secs(30),
        AddNumbersInput { a: 3, b: 4 },
    )
    .await
    .map_err(|err| TaskError::new("SCHEDULE_FAILED", err.message))?;

    // Explicit path via handle(&rt) — for repeated sends or testing
    // (requires TaskRuntime in signature)
    let add = add_numbers::handle(&_rt)?;
    for (a, b) in [(5, 6), (7, 8)] {
        add.send(AddNumbersInput { a, b })
            .await
            .map_err(|err| TaskError::new("SEND_FAILED", err.message.clone()))?;
    }

    Ok(())
}

fn config() -> AppConfig {
    common::app_config(&common::db_url())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(config())?;

    add_numbers::register(&mut app)?;
    let _enqueue = enqueue_add_numbers::register(&mut app)?;

    println!("registered runtime task dispatch example");
    println!("call task_name::send(args) from anywhere after register()");

    Ok(())
}
