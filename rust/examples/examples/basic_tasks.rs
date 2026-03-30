//! Basic task example: send tasks and get results.
//!
//! This is a **sender only** — it enqueues tasks and collects results.
//! A worker must be running separately to execute the tasks.
//!
//! Run with:
//!   # Terminal 1: start the worker
//!   cargo run --example worker_default -p horsies-worker
//!
//!   # Terminal 2: run this sender
//!   cargo run --example basic_tasks -p horsies-worker

use horsies_examples::common;

use std::time::Duration;

use horsies::{Horsies, TaskResult};

use common::tasks::basic::Computed;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = common::db_url();
    let config = common::default_mode::app_config(&db_url);

    println!("=== Basic Tasks Example (sender) ===\n");
    println!("NOTE: Ensure worker_default is running in another terminal.\n");
    println!("Connecting to database...\n");

    // Create Horsies app and register tasks (needed for resolve_enqueue).
    let mut app = Horsies::new(config)?;
    common::tasks::basic::register(&mut app)?;

    // Resolve enqueue for each task BEFORE dropping the app.
    let resolved_compute = app.resolve_enqueue("do_compute", None, None)?;
    let resolved_failing = app.resolve_enqueue("failing_task", None, None)?;
    let resolved_divide = app.resolve_enqueue("divide", None, None)?;
    let resolved_ping = app.resolve_enqueue("ping", None, None)?;

    let broker = common::connect_broker(&db_url).await;

    println!("Broker connected.\n");

    // Send tasks and print results.
    println!("--- Sending tasks ---\n");

    let mut passed = 0u32;
    let mut failed = 0u32;
    let timeout = Some(Duration::from_secs(30));

    // (a) do_compute with {a: 10, b: 20} -> expect Ok(30)
    {
        let kwargs = serde_json::to_string(&serde_json::json!({"a": 10.0, "b": 20.0}))?;
        let handle = broker
            .send_task::<Computed>(&resolved_compute, None, Some(&kwargs), None)
            .await?;
        let result: TaskResult<Computed> = handle.get(timeout).await;
        match result {
            TaskResult::Ok(v) => {
                println!("[PASS] do_compute(10, 20) = {:?} (expected 30)", v.value);
                passed += 1;
            }
            TaskResult::Err(e) => {
                println!("[FAIL] do_compute(10, 20) returned error: {:?}", e);
                failed += 1;
            }
        }
    }

    // (b) do_compute with {a: 999, b: 999} -> expect Err(TOO_LARGE)
    {
        let kwargs = serde_json::to_string(&serde_json::json!({"a": 999.0, "b": 999.0}))?;
        let handle = broker
            .send_task::<Computed>(&resolved_compute, None, Some(&kwargs), None)
            .await?;
        let result: TaskResult<Computed> = handle.get(timeout).await;
        match result {
            TaskResult::Err(e) => {
                println!("[PASS] do_compute(999, 999) errored as expected: {}", e);
                passed += 1;
            }
            TaskResult::Ok(v) => {
                println!(
                    "[FAIL] do_compute(999, 999) should have errored but got: {:?}",
                    v
                );
                failed += 1;
            }
        }
    }

    // (c) failing_task with {should_fail: false} -> expect Ok("succeeded")
    {
        let kwargs = serde_json::to_string(&serde_json::json!({"should_fail": false}))?;
        let handle = broker
            .send_task::<String>(&resolved_failing, None, Some(&kwargs), None)
            .await?;
        let result: TaskResult<String> = handle.get(timeout).await;
        match result {
            TaskResult::Ok(v) => {
                println!("[PASS] failing_task(false) = {:?}", v);
                passed += 1;
            }
            TaskResult::Err(e) => {
                println!("[FAIL] failing_task(false) returned error: {:?}", e);
                failed += 1;
            }
        }
    }

    // (d) failing_task with {should_fail: true} -> expect Err(FORCED_FAILURE)
    {
        let kwargs = serde_json::to_string(&serde_json::json!({"should_fail": true}))?;
        let handle = broker
            .send_task::<String>(&resolved_failing, None, Some(&kwargs), None)
            .await?;
        let result: TaskResult<String> = handle.get(timeout).await;
        match result {
            TaskResult::Err(e) => {
                println!("[PASS] failing_task(true) errored as expected: {}", e);
                passed += 1;
            }
            TaskResult::Ok(v) => {
                println!(
                    "[FAIL] failing_task(true) should have errored but got: {:?}",
                    v
                );
                failed += 1;
            }
        }
    }

    // (e) divide with {a: 100, b: 4} -> expect Ok(25)
    {
        let kwargs = serde_json::to_string(&serde_json::json!({"a": 100.0, "b": 4.0}))?;
        let handle = broker
            .send_task::<f64>(&resolved_divide, None, Some(&kwargs), None)
            .await?;
        let result: TaskResult<f64> = handle.get(timeout).await;
        match result {
            TaskResult::Ok(v) => {
                println!("[PASS] divide(100, 4) = {:?} (expected 25)", v);
                passed += 1;
            }
            TaskResult::Err(e) => {
                println!("[FAIL] divide(100, 4) returned error: {:?}", e);
                failed += 1;
            }
        }
    }

    // (f) divide with {a: 100, b: 0} -> expect Err(DIVISION_ERROR)
    {
        let kwargs = serde_json::to_string(&serde_json::json!({"a": 100.0, "b": 0.0}))?;
        let handle = broker
            .send_task::<f64>(&resolved_divide, None, Some(&kwargs), None)
            .await?;
        let result: TaskResult<f64> = handle.get(timeout).await;
        match result {
            TaskResult::Err(e) => {
                println!("[PASS] divide(100, 0) errored as expected: {}", e);
                passed += 1;
            }
            TaskResult::Ok(v) => {
                println!("[FAIL] divide(100, 0) should have errored but got: {:?}", v);
                failed += 1;
            }
        }
    }

    // (g) ping with no args -> expect Ok("pong")
    {
        let handle = broker
            .send_task::<String>(&resolved_ping, None, None, None)
            .await?;
        let result: TaskResult<String> = handle.get(timeout).await;
        match result {
            TaskResult::Ok(v) => {
                println!("[PASS] ping() = {:?}", v);
                passed += 1;
            }
            TaskResult::Err(e) => {
                println!("[FAIL] ping() returned error: {:?}", e);
                failed += 1;
            }
        }
    }

    // Print summary
    println!("\n=== Summary ===");
    println!("  Passed: {}", passed);
    println!("  Failed: {}", failed);
    println!("  Total:  {}", passed + failed);

    if failed > 0 {
        println!("\nSome tasks did not produce the expected result.");
    } else {
        println!("\nAll tasks completed as expected!");
    }

    Ok(())
}
