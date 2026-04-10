//! Workflow pattern examples: start workflows and collect results.
//!
//! This is a **sender only** — it enqueues workflows and collects results.
//! A worker must be running separately to execute the tasks.
//!
//! Run with:
//!   # Terminal 1: start the worker
//!   cargo run --example worker_default -p horsies-worker
//!
//!   # Terminal 2: run this sender
//!   cargo run --example workflow_patterns -p horsies-worker

use horsies_examples::common;

use std::time::Duration;

use horsies::{get_workflow_result, Horsies, TaskResult};

use common::tasks::workflows::{AggregateResult, TransformResult};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = common::db_url();
    let config = common::default_mode::app_config(&db_url);

    println!("==========================================================");
    println!("  Workflow Pattern Examples (sender)");
    println!("==========================================================\n");
    println!("NOTE: Ensure worker_default is running in another terminal.\n");
    println!("Connecting to database...\n");

    // Create Horsies app and register task functions + workflow specs.
    let mut app = Horsies::new(config)?;
    let workflow_tasks = common::tasks::workflows::register(&mut app)?;
    let workflow_specs =
        common::tasks::workflows::register_workflow_specs(&mut app, &workflow_tasks)?;

    println!("Registered 7 tasks and 4 workflow specs.\n");

    let broker = app.get_broker().await?;
    println!("Broker connected.\n");

    let wf_listener = broker.workflow_done_listener().await?;
    let timeout = Some(Duration::from_secs(30));
    let mut passed = 0u32;
    let mut failed = 0u32;

    // ------------------------------------------------------------------
    // Pattern 1: Linear Chain (fetch_data -> transform_data)
    // ------------------------------------------------------------------
    println!("--- Pattern 1: Linear Chain ---\n");
    {
        let handle = workflow_specs.linear_chain.start().await?;
        println!(
            "  Started workflow: linear_chain (id: {})",
            handle.workflow_id()
        );

        let result: TaskResult<TransformResult> =
            get_workflow_result(broker.pool(), wf_listener, handle.workflow_id(), timeout).await?;

        match result {
            TaskResult::Ok(ref v) => {
                println!("  [PASS] Result: {:?}", v);
                println!(
                    "         processed_count={}, data={:?}",
                    v.processed_count, v.data
                );
                passed += 1;
            }
            TaskResult::Err(ref e) => {
                println!("  [FAIL] Error: {}", e);
                failed += 1;
            }
        }
        println!();
    }

    // ------------------------------------------------------------------
    // Pattern 2: Fan-Out + Fan-In
    // ------------------------------------------------------------------
    println!("--- Pattern 2: Fan-Out + Fan-In ---\n");
    {
        let handle = workflow_specs.fan_in_out.start().await?;
        println!(
            "  Started workflow: fan_in_out (id: {})",
            handle.workflow_id()
        );

        let result: TaskResult<AggregateResult> =
            get_workflow_result(broker.pool(), wf_listener, handle.workflow_id(), timeout).await?;

        match result {
            TaskResult::Ok(ref v) => {
                println!("  [PASS] Result: {:?}", v);
                println!(
                    "         total={} (expected 9: 3 items * 3 chunks)",
                    v.total
                );
                passed += 1;
            }
            TaskResult::Err(ref e) => {
                println!("  [FAIL] Error: {}", e);
                failed += 1;
            }
        }
        println!();
    }

    // ------------------------------------------------------------------
    // Pattern 3: Error Recovery
    // ------------------------------------------------------------------
    println!("--- Pattern 3: Error Recovery ---\n");
    {
        let handle = workflow_specs.error_recovery.start().await?;
        println!(
            "  Started workflow: error_recovery (id: {})",
            handle.workflow_id()
        );

        let result: TaskResult<String> =
            get_workflow_result(broker.pool(), wf_listener, handle.workflow_id(), timeout).await?;

        match result {
            TaskResult::Ok(ref v) => {
                println!("  [PASS] Result: {:?}", v);
                println!("         (upstream failed intentionally, recovery task handled it)");
                passed += 1;
            }
            TaskResult::Err(ref e) => {
                println!("  [FAIL] Error: {}", e);
                failed += 1;
            }
        }
        println!();
    }

    // ------------------------------------------------------------------
    // Summary
    // ------------------------------------------------------------------
    println!("--- Pattern 4: Subworkflow Param Handoff ---\n");
    {
        let handle = workflow_specs.subworkflow_handoff.start().await?;
        println!(
            "  Started workflow: subworkflow_handoff (id: {})",
            handle.workflow_id()
        );

        let result: TaskResult<String> =
            get_workflow_result(broker.pool(), wf_listener, handle.workflow_id(), timeout).await?;

        match result {
            TaskResult::Ok(ref v) => {
                println!("  [PASS] Result: {:?}", v);
                println!("         (child workflow received static label + injected count)");
                passed += 1;
            }
            TaskResult::Err(ref e) => {
                println!("  [FAIL] Error: {}", e);
                failed += 1;
            }
        }
        println!();
    }

    println!("==========================================================");
    println!("  Summary");
    println!("==========================================================");
    println!("  Passed: {}", passed);
    println!("  Failed: {}", failed);
    println!("  Total:  {}", passed + failed);
    println!();
    println!("Patterns demonstrated:");
    println!("  1. Linear Chain     -- fetch_data -> transform_data (sequential pipeline)");
    println!("  2. Fan-Out/Fan-In   -- fetch -> 3x process_chunk -> aggregate");
    println!("  3. Error Recovery   -- failing_fetch -> recovery_task (allow_failed_deps)");
    println!("  4. Subworkflow      -- parent task output + static params into child workflow");
    println!();

    if failed > 0 {
        println!("Some workflows did not produce the expected result.");
    } else {
        println!("All workflows completed as expected!");
    }

    Ok(())
}
