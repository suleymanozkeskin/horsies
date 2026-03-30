//! Custom queue example: send tasks to multiple priority queues.
//!
//! This is a **sender only** — it enqueues tasks and collects results.
//! A worker must be running separately to execute the tasks.
//!
//! Run with:
//!   # Terminal 1: start the worker
//!   cargo run --example worker_custom -p horsies-worker
//!
//!   # Terminal 2: run this sender
//!   cargo run --example custom_queues -p horsies-worker

use horsies_examples::common;

use std::sync::Arc;
use std::time::Duration;

use horsies::{Horsies, ResolvedEnqueue};

use common::custom_mode::{HIGH, LOW, NORMAL};
use common::tasks::custom_queues::Computed;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = common::db_url();
    let config = common::custom_mode::app_config(&db_url);

    println!("=== Custom Queues Example (sender) ===\n");
    println!("NOTE: Ensure worker_custom is running in another terminal.\n");
    println!("{}", config.format_for_logging());

    // Register tasks (needed for resolve_enqueue).
    let mut app = Horsies::new(config)?;
    common::tasks::custom_queues::register(&mut app)?;

    println!("  Registered: high_compute   -> queue \"{}\"", HIGH);
    println!("  Registered: normal_compute -> queue \"{}\"", NORMAL);
    println!("  Registered: low_compute    -> queue \"{}\"", LOW);

    // Resolve enqueue parameters BEFORE dropping the app.
    let resolved_high = app.resolve_enqueue("high_compute", Some(HIGH), None)?;
    let resolved_normal = app.resolve_enqueue("normal_compute", Some(NORMAL), None)?;
    let resolved_low = app.resolve_enqueue("low_compute", Some(LOW), None)?;

    println!(
        "\n  high_compute   -> queue={}, priority={}",
        resolved_high.queue_name, resolved_high.priority
    );
    println!(
        "  normal_compute -> queue={}, priority={}",
        resolved_normal.queue_name, resolved_normal.priority
    );
    println!(
        "  low_compute    -> queue={}, priority={}",
        resolved_low.queue_name, resolved_low.priority
    );

    let broker = common::connect_broker(&db_url).await;

    println!("\n=== Broker connected ===\n");

    // Send tasks to each queue and collect results.
    println!("=== Sending 9 tasks (3 per queue) ===\n");

    let inputs: Vec<(f64, f64)> = vec![(1.0, 2.0), (10.0, 20.0), (100.0, 200.0)];

    async fn send_tasks(
        broker: &Arc<horsies::PostgresBroker>,
        resolved: &ResolvedEnqueue,
        inputs: &[(f64, f64)],
    ) -> Result<Vec<horsies::TaskHandle<Computed>>, Box<dyn std::error::Error>> {
        let mut handles = Vec::new();
        for &(a, b) in inputs {
            let kwargs = serde_json::to_string(&serde_json::json!({"a": a, "b": b}))?;
            let handle = broker
                .send_task::<Computed>(resolved, None, Some(&kwargs), None)
                .await?;
            println!(
                "  Sent task {} to queue \"{}\" with a={}, b={}",
                handle.task_id(),
                resolved.queue_name,
                a,
                b,
            );
            handles.push(handle);
        }
        Ok(handles)
    }

    let high_handles = send_tasks(&broker, &resolved_high, &inputs).await?;
    let normal_handles = send_tasks(&broker, &resolved_normal, &inputs).await?;
    let low_handles = send_tasks(&broker, &resolved_low, &inputs).await?;

    println!("\n=== Waiting for results ===\n");

    let timeout = Some(Duration::from_secs(30));
    let mut high_count = 0u32;
    let mut normal_count = 0u32;
    let mut low_count = 0u32;

    for handle in &high_handles {
        let result = handle.get(timeout).await;
        if result.is_ok() {
            let computed = result.unwrap();
            println!(
                "  [HIGH]   task={} value={:.1} queue={}",
                handle.task_id(),
                computed.value,
                computed.queue,
            );
            high_count += 1;
        } else {
            println!("  [HIGH]   task={} FAILED: {:?}", handle.task_id(), result);
        }
    }

    for handle in &normal_handles {
        let result = handle.get(timeout).await;
        if result.is_ok() {
            let computed = result.unwrap();
            println!(
                "  [NORMAL] task={} value={:.1} queue={}",
                handle.task_id(),
                computed.value,
                computed.queue,
            );
            normal_count += 1;
        } else {
            println!("  [NORMAL] task={} FAILED: {:?}", handle.task_id(), result);
        }
    }

    for handle in &low_handles {
        let result = handle.get(timeout).await;
        if result.is_ok() {
            let computed = result.unwrap();
            println!(
                "  [LOW]    task={} value={:.1} queue={}",
                handle.task_id(),
                computed.value,
                computed.queue,
            );
            low_count += 1;
        } else {
            println!("  [LOW]    task={} FAILED: {:?}", handle.task_id(), result);
        }
    }

    // Print summary.
    println!("\n=== Summary ===\n");
    println!("  High-priority tasks completed:   {}/3", high_count);
    println!("  Normal-priority tasks completed: {}/3", normal_count);
    println!("  Low-priority tasks completed:    {}/3", low_count);
    println!(
        "  Total completed:                 {}/9",
        high_count + normal_count + low_count
    );

    println!("\n=== Done! ===");

    Ok(())
}
