//! Quick-start example: order processing pipeline.
//!
//! Mirrors the Python `examples/quick-start/` shipping example using
//! the unified `horsies::Horsies` app API.
//!
//! This example demonstrates:
//! - One app object for everything
//! - Task registration with `.task().queue().register()`
//! - Typed `TaskFunction` with `.send()` / `.schedule()`
//! - Reusable workflow registration via `WorkflowDefinition`
//! - Workflow start with `.start()` / `.retry_start()`
//! - `app.check()` for offline validation
//!
//! Run:
//!   # Terminal 1: start the quick-start worker
//!   cargo run --example quick_start_worker -p horsies-examples
//!
//!   # Terminal 2: run the sender
//!   cargo run --example quick_start -p horsies-examples

mod models;
mod tasks;
mod workflows;

use std::time::Duration;

use horsies::{AppConfig, CustomQueueConfig, Horsies, QueueMode, TaskResult};
use horsies_examples::common;

use models::*;

fn config() -> AppConfig {
    AppConfig {
        payload: horsies::PayloadPolicy::default(),
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            CustomQueueConfig {
                name: "urgent".into(),
                priority: 1,
                max_concurrency: Some(10),
            },
            CustomQueueConfig {
                name: "standard".into(),
                priority: 50,
                max_concurrency: Some(20),
            },
            CustomQueueConfig {
                name: "low".into(),
                priority: 100,
                max_concurrency: Some(5),
            },
        ]),
        ..common::app_config(&common::db_url())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Create the app ────────────────────────────────────────────
    let mut app = Horsies::new(config())?;

    // ── 2. Register tasks ────────────────────────────────────────────
    let validate_order_task = tasks::register(&mut app)?;

    // ── 3. Register workflow ─────────────────────────────────────────
    let order_workflow = workflows::register(&mut app)?;

    // ── 4. Validate ──────────────────────────────────────────────────
    app.check()?;
    println!("check passed");

    // ── 4b. Run database migrations ──────────────────────────────────
    let broker = app.get_broker().await?;
    broker.migrate().await.expect("migration failed");
    println!("migrations applied");

    // ── 5. Send a standalone task ────────────────────────────────────
    let order = Order {
        order_id: "ORD-001".into(),
        customer_email: "alice@example.com".into(),
        items: vec![
            OrderItem {
                sku: "WIDGET-A".into(),
                quantity: 2,
                price_cents: 1999,
            },
            OrderItem {
                sku: "GADGET-B".into(),
                quantity: 1,
                price_cents: 4999,
            },
        ],
        shipping_address: Address {
            street: "123 Main St".into(),
            city: "Springfield".into(),
            state: "IL".into(),
            postal_code: "62701".into(),
            country: "US".into(),
        },
        shipping_method: ShippingMethod::Express,
    };

    println!("\n--- Sending standalone task ---");
    match validate_order_task.send(order.clone()).await {
        Ok(handle) => {
            let result: TaskResult<ValidatedOrder> =
                handle.get(Some(Duration::from_secs(30))).await;
            match result {
                TaskResult::Ok(validated) => {
                    println!(
                        "order {} validated at {}",
                        validated.order_id, validated.validated_at,
                    );
                }
                TaskResult::Err(err) => {
                    println!("validation failed: {} - {:?}", err, err.error_code);
                }
            }
        }
        Err(send_err) => {
            println!(
                "send failed: {} (retryable={})",
                send_err.code, send_err.retryable
            );
            if send_err.retryable {
                println!("retrying...");
                match validate_order_task.retry_send(&send_err).await {
                    Ok(handle) => {
                        let _result = handle.get(Some(Duration::from_secs(30))).await;
                        println!("retry succeeded");
                    }
                    Err(retry_err) => {
                        println!("retry also failed: {}", retry_err.code);
                    }
                }
            }
        }
    }

    // ── 6. Start the workflow ────────────────────────────────────────
    println!("\n--- Starting workflow ---");
    match order_workflow.start(order.clone()).await {
        Ok(handle) => {
            println!("workflow {} started", handle.workflow_id());

            let result = handle.get(Some(Duration::from_secs(60))).await;
            match result {
                TaskResult::Ok(notification) => {
                    println!(
                        "order {} processed — notification {}",
                        notification.order_id, notification.notification_id,
                    );
                }
                TaskResult::Err(err) => {
                    println!("workflow task error: {:?}", err.error_code);
                }
            }
        }
        Err(start_err) => {
            println!(
                "start failed: {} - {} (retryable={})",
                start_err.code, start_err.message, start_err.retryable
            );
        }
    }

    // ── 7. Schedule a delayed task ───────────────────────────────────
    println!("\n--- Scheduling delayed task ---");
    match validate_order_task
        .schedule(Duration::from_secs(300), order)
        .await
    {
        Ok(handle) => {
            println!("scheduled task {} for 5 minutes from now", handle.task_id());
        }
        Err(err) => {
            println!("schedule failed: {}", err.code);
        }
    }

    println!("\ndone.");
    Ok(())
}
