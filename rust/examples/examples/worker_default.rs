//! Long-lived worker for default-mode examples.
//!
//! Registers all tasks used by basic_tasks, retries, and workflow_patterns,
//! then runs the worker until Ctrl+C (SIGINT/SIGTERM).
//!
//! Run with:
//!   cargo run --example worker_default -p horsies-examples

use horsies_examples::common;

use horsies::{Horsies, WorkerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = common::db_url();
    let config = common::default_mode::app_config(&db_url);

    println!("=== Default-Mode Worker ===\n");

    // Register all default-mode tasks.
    let mut app = Horsies::new(config)?;

    common::tasks::basic::register(&mut app)?;
    common::tasks::retries::register(&mut app)?;
    common::tasks::workflows::register(&mut app)?;
    common::tasks::workflows::register_workflow_specs(&mut app)?;

    println!("Registered tasks:");
    println!("  basic:     do_compute, failing_task, divide, ping");
    println!("  retries:   always_fails, fails_with_custom_code, eventually_succeeds");
    println!("  workflows: fetch_data, transform_data, process_chunk, aggregate, failing_fetch, recovery_task");
    println!("  specs:     linear_chain, fan_in_out, error_recovery");
    println!();

    // Connect and migrate.
    let broker = app.get_broker().await?;
    broker.migrate().await.expect("migration failed");

    println!("Worker starting... Press Ctrl+C to stop.\n");

    app.run_worker_with(WorkerConfig::default()).await?;

    Ok(())
}
