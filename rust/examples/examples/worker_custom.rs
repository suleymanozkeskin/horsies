//! Long-lived worker for custom-mode examples.
//!
//! Registers all tasks used by custom_queues, then runs the worker until
//! Ctrl+C (SIGINT/SIGTERM).
//!
//! Run with:
//!   cargo run --example worker_custom -p horsies-examples

use horsies_examples::common;

use horsies::{Horsies, WorkerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = common::db_url();
    let config = common::custom_mode::app_config(&db_url);

    println!("=== Custom-Mode Worker ===\n");
    println!("{}", config.format_for_logging());

    // Register all custom-mode tasks.
    let mut app = Horsies::new(config)?;
    common::tasks::custom_queues::register(&mut app)?;

    println!("Registered tasks:");
    println!("  high_compute   -> queue \"high\"");
    println!("  normal_compute -> queue \"normal\"");
    println!("  low_compute    -> queue \"low\"");
    println!();

    // Connect and migrate.
    let broker = app.get_broker().await?;
    broker.migrate().await.expect("migration failed");

    // Configure worker to consume from all custom queues.
    let mut worker_config = WorkerConfig::default();
    if let Some(ref queues) = app.config().custom_queues {
        worker_config.queues = queues.iter().map(|q| q.name.clone()).collect();
    }

    println!("Worker starting... Press Ctrl+C to stop.\n");

    app.run_worker_with(worker_config).await?;

    Ok(())
}
