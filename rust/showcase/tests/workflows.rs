use std::env;
use std::sync::Arc;
use std::time::Duration;

use acme_showcase::app::build_app_with_handles_for_url;
use acme_showcase::domain::{Order, OrderLine, Product, StockLevel};
use acme_showcase::settings::resolve_database_settings;
use acme_showcase::store::{ensure_database, Store};
use acme_showcase::{tuning, workflows};
use chrono::Utc;
use horsies::{
    spawn_scheduler, IntervalSchedule, ScheduleConfig, SchedulePattern, TaskResult, TaskSchedule,
    Worker, WorkerConfig,
};
use serde_json::json;
use uuid::Uuid;

async fn database() -> Option<(String, Store)> {
    env::var("ACME_DATABASE_URL").ok()?;
    let settings = resolve_database_settings().expect("showcase settings");
    ensure_database(&settings).await.expect("showcase database");
    let store = Store::connect(&settings).await.expect("showcase store");
    store.ensure_schema().await.expect("showcase schema");
    Some((settings.sqlx_url().to_owned(), store))
}

async fn seed_order_with_id(
    store: &Store,
    order_id: String,
    prefix: &str,
    stock: i32,
    quantity: i32,
) -> Order {
    let token = Uuid::new_v4().simple().to_string();
    let sku = format!("s3-sku-{prefix}-{}", &token[..12]);
    store
        .load_catalog(
            &[Product {
                sku: sku.clone(),
                name: "S3 test item".into(),
                category: "test".into(),
                price_cents: 1_000,
            }],
            &[StockLevel {
                sku: sku.clone(),
                on_hand: stock,
                reserved: 0,
            }],
        )
        .await
        .expect("catalog");
    let order = Order {
        order_id,
        customer_id: format!("customer-{prefix}"),
        status: "placed".into(),
        total_cents: 1_000 * quantity,
        lines: vec![OrderLine {
            line_no: 1,
            sku,
            size_code: "M".into(),
            quantity,
            unit_price_cents: 1_000,
        }],
        created_at: Utc::now(),
    };
    store.insert_order(&order).await.expect("order");
    order
}

async fn seed_order(store: &Store, prefix: &str, stock: i32, quantity: i32) -> Order {
    let token = Uuid::new_v4().simple().to_string();
    seed_order_with_id(
        store,
        format!("s3-{prefix}-{}", &token[..12]),
        prefix,
        stock,
        quantity,
    )
    .await
}

async fn seed_successful_order(store: &Store, prefix: &str, stock: i32, quantity: i32) -> Order {
    let identity_prefix = Uuid::new_v4().simple().to_string();
    let order_id = (0..100_000)
        .map(|index| format!("s3-{prefix}-{identity_prefix}-{index}"))
        .find(|id| {
            !acme_showcase::simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
                && !acme_showcase::simulate::draw(tuning::INVOICE_HANG_RATE, &[id, "invoice"])
                && !acme_showcase::simulate::draw(tuning::PSP_UNAVAILABLE_RATE, &[id, "psp"])
                && !acme_showcase::simulate::draw(tuning::COURIER_FLAKE_RATE, &[id, "courier"])
        })
        .expect("seeded population contains a successful order");
    seed_order_with_id(store, order_id, prefix, stock, quantity).await
}

async fn run_order(order: Order, url: &str) -> TaskResult<serde_json::Value> {
    let (app, _handles, workflows) = build_app_with_handles_for_url(url).expect("showcase app");
    let handle = workflows
        .order_fulfillment
        .start(order)
        .await
        .expect("start workflow");
    let (config, registry, workflow_registry, broker) = app.into_parts().await.expect("broker");
    let mut worker_config = WorkerConfig {
        queues: vec![
            "payments".into(),
            "fulfillment".into(),
            "notifications".into(),
            "analytics".into(),
        ],
        concurrency: 8,
        ..WorkerConfig::default()
    };
    worker_config.apply_queue_config(&config);
    let worker = Worker::new(
        Arc::clone(&broker),
        Arc::new(registry),
        Arc::new(workflow_registry),
        config,
        worker_config,
    )
    .expect("worker");
    let stop = worker.cancel_token();
    let join = tokio::spawn(async move { worker.run().await });
    let result = handle.get(Some(Duration::from_secs(30))).await;
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("worker stop");
    result
}

#[tokio::test]
#[serial_test::serial]
async fn order_fulfillment_happy_path_completes_and_moves_stock() {
    let Some((url, store)) = database().await else {
        return;
    };
    let order = seed_successful_order(&store, "happy", 4, 1).await;
    let result = run_order(order.clone(), &url).await;
    assert!(
        matches!(result, TaskResult::Ok(_)),
        "happy workflow: {result:?}"
    );
    let status = store
        .get_order(&order.order_id)
        .await
        .expect("order read")
        .expect("order exists")
        .status;
    assert_eq!(status, "captured");
    let shipment = store
        .get_shipment(&order.order_id)
        .await
        .expect("shipment read")
        .expect("shipment exists");
    assert!(shipment.attempts >= 1);
    assert!(shipment.tracking_code.is_some());
}

#[tokio::test]
#[serial_test::serial]
async fn order_fulfillment_courier_retry_records_attempts_before_success() {
    let Some((url, store)) = database().await else {
        return;
    };
    let identity_prefix = Uuid::new_v4().simple().to_string();
    let order_id = (0..100_000)
        .map(|index| format!("s3-courier-{identity_prefix}-{index}"))
        .find(|id| acme_showcase::simulate::draw(tuning::COURIER_FLAKE_RATE, &[id, "courier"]))
        .expect("courier retry identity");
    let order = seed_order_with_id(&store, order_id.clone(), "courier", 4, 1).await;
    let args = json!({"order_id": order.order_id, "courier": "fleetline", "express": false});
    let (app, handles, _workflows) = build_app_with_handles_for_url(&url).expect("showcase app");
    let (config, registry, workflow_registry, broker) = app.into_parts().await.expect("broker");
    let mut worker_config = WorkerConfig {
        queues: vec!["fulfillment".into()],
        concurrency: 2,
        ..WorkerConfig::default()
    };
    worker_config.apply_queue_config(&config);
    let worker = Worker::new(
        Arc::clone(&broker),
        Arc::new(registry),
        Arc::new(workflow_registry),
        config,
        worker_config,
    )
    .expect("worker");
    let stop = worker.cancel_token();
    let join = tokio::spawn(async move { worker.run().await });
    let handle = handles
        .get("book_courier")
        .expect("courier task")
        .send(args)
        .await
        .expect("courier enqueue");
    let result = handle.get(Some(Duration::from_secs(20))).await;
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("worker stop");
    assert!(
        matches!(result, TaskResult::Ok(_)),
        "courier retry: {result:?}"
    );
    let shipment = store
        .get_shipment(&order.order_id)
        .await
        .expect("shipment read")
        .expect("shipment exists");
    assert!(shipment.attempts >= 2, "retry must persist attempts");
}

#[tokio::test]
#[serial_test::serial]
async fn order_fulfillment_insufficient_stock_fails_before_payment() {
    let Some((url, store)) = database().await else {
        return;
    };
    let order = seed_order(&store, "short", 0, 1).await;
    let result = run_order(order.clone(), &url).await;
    let TaskResult::Err(error) = result else {
        panic!("insufficient stock must fail");
    };
    assert_eq!(
        error.error_code.expect("error code").to_string(),
        "INSUFFICIENT_STOCK"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>(
            "SELECT authorization_attempts FROM acme_orders WHERE order_id = $1",
        )
        .bind(&order.order_id)
        .fetch_one(store.pool())
        .await
        .expect("authorization count"),
        0,
        "payment must not start after reservation refusal"
    );
}

#[test]
fn all_showcase_workflows_validate() {
    let app =
        acme_showcase::app::build_app_for_url("postgresql://localhost/acme_demo").expect("app");
    app.check().expect("workflow and scheduler validation");
    assert_eq!(app.workflow_registry().spec_names().count(), 12);
    assert!(workflows::order_fulfillment::check_order().lines.len() >= tuning::MIN_LINES_PER_ORDER);
}

#[tokio::test]
#[serial_test::serial]
async fn compressed_schedule_enqueues_a_due_task() {
    let Some((url, store)) = database().await else {
        return;
    };
    let app = acme_showcase::app::build_app_for_url(&url).expect("app");
    let (config, _registry, _workflow_registry, broker) = app.into_parts().await.expect("broker");
    let schedule_name = format!("s3-compressed-{}", Uuid::new_v4().simple());
    let task_name = format!("s3-schedule-{}", Uuid::new_v4().simple());
    let schedule = TaskSchedule::new(
        &schedule_name,
        "prewarm_search",
        SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(1),
            ..IntervalSchedule::default()
        }),
    )
    .queue("analytics")
    .kwargs(json!({"campaign_id": task_name}));
    let schedule_config = ScheduleConfig::new(vec![schedule]).check_interval_seconds(1);
    let cancel = tokio_util::sync::CancellationToken::new();
    let join = spawn_scheduler(Arc::clone(&broker), schedule_config, config, cancel.clone());
    tokio::time::sleep(Duration::from_secs(3)).await;
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("scheduler stop");
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_tasks WHERE task_name='prewarm_search' AND kwargs::text LIKE $1",
    )
    .bind(format!("%{task_name}%"))
    .fetch_one(store.pool())
    .await
    .expect("scheduled task count");
    assert!(count >= 1, "compressed schedule must enqueue one task");
}
