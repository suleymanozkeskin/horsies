//! Long-lived worker for the quick-start shipping example.
//!
//! Run with:
//!   cargo run --example quick_start_worker -p horsies-examples

mod models;
mod tasks;

use horsies::{AppConfig, CustomQueueConfig, Horsies, PostgresConfig, QueueMode, WorkerConfig};
use horsies_examples::common;

fn config() -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            CustomQueueConfig {
                name: "urgent".into(),
                priority: 1,
                max_concurrency: 10,
            },
            CustomQueueConfig {
                name: "standard".into(),
                priority: 50,
                max_concurrency: 20,
            },
            CustomQueueConfig {
                name: "low".into(),
                priority: 100,
                max_concurrency: 5,
            },
        ]),
        broker: PostgresConfig {
            database_url: common::db_url(),
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 30,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(config())?;

    tasks::register(&mut app)?;
    app.check()?;

    let broker = app.get_broker().await?;
    broker.migrate().await.expect("migration failed");

    println!("=== Quick-Start Worker ===\n");
    println!("Registered tasks:");
    println!("  validate_order");
    println!("  check_inventory");
    println!("  calculate_shipping_cost");
    println!("  check_address");
    println!("  reserve_inventory");
    println!("  create_shipment");
    println!("  send_notification");
    println!();
    println!("Worker starting... Press Ctrl+C to stop.\n");

    let mut worker_config = WorkerConfig::default();
    if let Some(queues) = &app.config().custom_queues {
        worker_config.queues = queues.iter().map(|q| q.name.clone()).collect();
    }

    app.run_worker_with(worker_config).await?;

    Ok(())
}
