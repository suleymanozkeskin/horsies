//! Retry policy example: send tasks with different retry/backoff strategies.
//!
//! This is a **sender only** — it enqueues tasks and collects results.
//! A worker must be running separately to execute the tasks.
//!
//! Run with:
//!   # Terminal 1: start the worker
//!   cargo run --example worker_default -p horsies-worker
//!
//!   # Terminal 2: run this sender
//!   cargo run --example retries -p horsies-worker

use horsies_examples::common;

use std::time::Duration;

use horsies::{Horsies, RetryPolicy, TaskErrorCode, TaskOptions, TaskResult};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = common::db_url();
    let config = common::default_mode::app_config(&db_url);

    println!("=== Retry Policy Demo (sender) ===\n");
    println!("NOTE: Ensure worker_default is running in another terminal.\n");

    // Register tasks (needed for resolve_enqueue).
    let mut app = Horsies::new(config)?;
    common::tasks::retries::register(&mut app)?;

    let resolved_always_fails = app.resolve_enqueue("always_fails", None, None)?;
    let resolved_custom_code = app.resolve_enqueue("fails_with_custom_code", None, None)?;
    let resolved_succeeds = app.resolve_enqueue("eventually_succeeds", None, None)?;

    let broker = common::connect_broker(&db_url).await;

    // -----------------------------------------------------------------------
    // Demo 1: Fixed backoff retry (always_fails)
    // -----------------------------------------------------------------------
    println!("--- Demo 1: Fixed backoff retry (1s, 2s, 3s) ---\n");
    {
        let opts = TaskOptions {
            task_name: "always_fails".to_string(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User("UNHANDLED_EXCEPTION".to_string())]),
            retry_policy: Some(RetryPolicy::fixed(vec![1, 2, 3], false)?),
            timeout_ms: None,
        };

        let handle = broker
            .send_task::<String>(&resolved_always_fails, None, None, Some(&opts))
            .await?;
        println!(
            "  Sent task {} with fixed retry [1s, 2s, 3s]",
            handle.task_id()
        );
        println!("  Waiting up to 15s for all retries to exhaust...\n");

        let result: TaskResult<String> = handle.get(Some(Duration::from_secs(15))).await;
        print_result("Demo 1 (fixed backoff)", &result);
    }

    // -----------------------------------------------------------------------
    // Demo 2: Custom error code retry (fails_with_custom_code)
    // -----------------------------------------------------------------------
    println!("\n--- Demo 2: Custom error code retry (VALUE_ERROR, 1s, 2s) ---\n");
    {
        let opts = TaskOptions {
            task_name: "fails_with_custom_code".to_string(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User("VALUE_ERROR".to_string())]),
            retry_policy: Some(RetryPolicy::fixed(vec![1, 2], false)?),
            timeout_ms: None,
        };

        let handle = broker
            .send_task::<String>(&resolved_custom_code, None, None, Some(&opts))
            .await?;
        println!(
            "  Sent task {} with retry on VALUE_ERROR [1s, 2s]",
            handle.task_id()
        );
        println!("  Waiting up to 10s for retries to exhaust...\n");

        let result: TaskResult<String> = handle.get(Some(Duration::from_secs(10))).await;
        print_result("Demo 2 (custom error code)", &result);
    }

    // -----------------------------------------------------------------------
    // Demo 3: Exponential backoff retry (always_fails)
    // -----------------------------------------------------------------------
    println!("\n--- Demo 3: Exponential backoff (1s base, 2 retries) ---\n");
    {
        let opts = TaskOptions {
            task_name: "always_fails".to_string(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User("UNHANDLED_EXCEPTION".to_string())]),
            retry_policy: Some(RetryPolicy::exponential(1, 2, false)?),
            timeout_ms: None,
        };

        let handle = broker
            .send_task::<String>(&resolved_always_fails, None, None, Some(&opts))
            .await?;
        println!(
            "  Sent task {} with exponential backoff (base=1s, max_retries=2)",
            handle.task_id()
        );
        println!("  Waiting up to 10s for retries to exhaust...\n");

        let result: TaskResult<String> = handle.get(Some(Duration::from_secs(10))).await;
        print_result("Demo 3 (exponential backoff)", &result);
    }

    // -----------------------------------------------------------------------
    // Demo 4: Task that succeeds (no retries triggered)
    // -----------------------------------------------------------------------
    println!("\n--- Demo 4: Successful task (retries not triggered) ---\n");
    {
        let opts = TaskOptions {
            task_name: "eventually_succeeds".to_string(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User("UNHANDLED_EXCEPTION".to_string())]),
            retry_policy: Some(RetryPolicy::fixed(vec![1, 2, 3], false)?),
            timeout_ms: None,
        };

        let handle = broker
            .send_task::<String>(&resolved_succeeds, None, None, Some(&opts))
            .await?;
        println!(
            "  Sent task {} with retry policy (but it will succeed immediately)",
            handle.task_id()
        );
        println!("  Waiting up to 5s for result...\n");

        let result: TaskResult<String> = handle.get(Some(Duration::from_secs(5))).await;
        print_result("Demo 4 (successful task)", &result);
    }

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------
    println!("\n=== Summary ===\n");
    println!("  Demo 1 (fixed backoff):      always_fails retried 3 times at 1s, 2s, 3s intervals, then failed.");
    println!("  Demo 2 (custom error code):   fails_with_custom_code retried 2 times on VALUE_ERROR, then failed.");
    println!("  Demo 3 (exponential backoff): always_fails retried 2 times with exponential delays, then failed.");
    println!("  Demo 4 (successful task):     eventually_succeeds completed on first attempt, no retries needed.");
    println!("\nAll demos complete.");

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn print_result(label: &str, result: &TaskResult<String>) {
    match result {
        TaskResult::Ok(value) => {
            println!("  {} result: Ok({:?})", label, value);
        }
        TaskResult::Err(err) => {
            println!("  {} result: Err({})", label, err);
        }
    }
}
