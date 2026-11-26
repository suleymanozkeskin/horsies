//! The order workflow through a worker of the `acme` binary, the check of
//! every workflow, and a schedule that fires.

mod support;

use std::time::Duration;

use acme_showcase::app::{build_app_for_url, build_app_with_config, config_for_url};
use acme_showcase::domain::{Order, OrderLine, PaymentCapture, Product, StockLevel};
use acme_showcase::store::Store;
use acme_showcase::tasks::shipping::book_courier;
use acme_showcase::workflows::{order_fulfillment, ALL_WORKFLOW_NAMES};
use acme_showcase::{simulate, tuning};
use chrono::Utc;
use horsies::{
    CustomCode, IntervalSchedule, ScheduleConfig, SchedulePattern, SchemaMode, TaskOutcome,
    TaskSchedule, WaitLimit, WorkflowReadError,
};
use serde_json::json;
use serial_test::serial;
use uuid::Uuid;

/// How long the compressed scheduler runs before its stop request.
const SCHEDULER_RUN: Duration = Duration::from_secs(5);

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
            !simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
                && !simulate::draw(tuning::INVOICE_HANG_RATE, &[id, "invoice"])
                && !simulate::draw(tuning::PSP_UNAVAILABLE_RATE, &[id, "psp"])
                && !simulate::draw(tuning::COURIER_FLAKE_RATE, &[id, "courier"])
        })
        .expect("seeded population contains a successful order");
    seed_order_with_id(store, order_id, prefix, stock, quantity).await
}

/// Start the order workflow of `order` and read its outcome while a worker
/// runs.
async fn run_order(
    order: Order,
    url: &str,
) -> Result<TaskOutcome<PaymentCapture, CustomCode>, WorkflowReadError> {
    let showcase =
        build_app_for_url(url, SchemaMode::MigrateAndValidate).expect("the showcase app");
    let _worker = support::WorkerProcess::start(&showcase).await;
    let handle = showcase
        .workflows
        .order_fulfillment
        .start(order)
        .await
        .expect("start the order workflow");
    let limit = WaitLimit::Within(support::RESULT_WAIT);
    handle.get(limit).await
}

#[tokio::test]
#[serial]
async fn order_fulfillment_happy_path_completes_and_moves_stock() {
    let Some((settings, store)) = support::database().await else {
        return;
    };
    let order = seed_successful_order(&store, "happy", 4, 1).await;
    let outcome = run_order(order.clone(), settings.sqlx_url()).await;
    assert!(
        matches!(outcome, Ok(TaskOutcome::Succeeded(_))),
        "happy workflow: {outcome:?}"
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
#[serial]
async fn order_fulfillment_courier_retry_records_attempts_before_success() {
    let Some((settings, store)) = support::database().await else {
        return;
    };
    let identity_prefix = Uuid::new_v4().simple().to_string();
    let order_id = (0..100_000)
        .map(|index| format!("s3-courier-{identity_prefix}-{index}"))
        .find(|id| simulate::draw(tuning::COURIER_FLAKE_RATE, &[id, "courier"]))
        .expect("courier retry identity");
    let order = seed_order_with_id(&store, order_id, "courier", 4, 1).await;
    let showcase = build_app_for_url(settings.sqlx_url(), SchemaMode::MigrateAndValidate)
        .expect("the showcase app");
    let _worker = support::WorkerProcess::start(&showcase).await;
    let handle = showcase
        .tasks
        .book_courier
        .send(book_courier::Input {
            order_id: order.order_id.clone(),
            courier: "fleetline".to_owned(),
            express: false,
        })
        .await
        .expect("send the courier booking");
    let limit = WaitLimit::Within(support::RESULT_WAIT);
    let outcome = handle.get(limit).await;
    assert!(
        matches!(outcome, Ok(TaskOutcome::Succeeded(_))),
        "courier retry: {outcome:?}"
    );
    let shipment = store
        .get_shipment(&order.order_id)
        .await
        .expect("shipment read")
        .expect("shipment exists");
    assert!(
        shipment.attempts >= 2,
        "the retry must persist its attempts"
    );
}

#[tokio::test]
#[serial]
async fn order_fulfillment_insufficient_stock_fails_before_payment() {
    let Some((settings, store)) = support::database().await else {
        return;
    };
    let order = seed_order(&store, "short", 0, 1).await;
    let outcome = run_order(order.clone(), settings.sqlx_url()).await;
    let error = match outcome {
        Ok(TaskOutcome::Failed(error)) => error,
        other => panic!("insufficient stock must fail the workflow: {other:?}"),
    };
    assert_eq!(error.code().as_str(), "INSUFFICIENT_STOCK");
    assert_eq!(
        sqlx::query_scalar::<_, i32>(
            "SELECT authorization_attempts FROM acme_orders WHERE order_id = $1",
        )
        .bind(&order.order_id)
        .fetch_one(store.pool())
        .await
        .expect("authorization count"),
        0,
        "payment must not start after a refused reservation"
    );
}

#[test]
fn all_showcase_workflows_validate() {
    let showcase = build_app_for_url(
        "postgresql://localhost/acme_demo",
        SchemaMode::MigrateAndValidate,
    )
    .expect("the showcase app");
    showcase
        .app
        .check()
        .expect("workflow and scheduler validation");
    assert_eq!(showcase.workflows.names().len(), ALL_WORKFLOW_NAMES.len());
    assert!(order_fulfillment::check_order().lines.len() >= tuning::MIN_LINES_PER_ORDER);
}

#[tokio::test]
#[serial]
async fn compressed_schedule_sends_a_due_task() {
    let Some((settings, store)) = support::database().await else {
        return;
    };
    let marker = format!("s3-schedule-{}", Uuid::new_v4().simple());
    let schedule = TaskSchedule::new(
        format!("s3-compressed-{}", Uuid::new_v4().simple()),
        "prewarm_search",
        SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(1),
            ..IntervalSchedule::default()
        }),
    )
    .input(&json!({ "campaign_id": marker.as_str() }))
    .expect("the schedule input writes as JSON");
    let mut config = config_for_url(settings.sqlx_url());
    config.schedule = Some(ScheduleConfig::new(vec![schedule]).check_interval_seconds(1));
    let showcase =
        build_app_with_config(config, settings.sqlx_url(), SchemaMode::MigrateAndValidate)
            .expect("the showcase app");
    let scheduler = showcase.app.scheduler().expect("the prepared scheduler");
    let stop = scheduler.stop_handle();
    let stop_later = async move {
        tokio::time::sleep(SCHEDULER_RUN).await;
        stop.request_stop();
    };
    let (ran, ()) = tokio::join!(scheduler.run(), stop_later);
    ran.expect("the scheduler runs until its stop request");
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_tasks WHERE task_name = 'prewarm_search' AND input LIKE $1",
    )
    .bind(format!("%{marker}%"))
    .fetch_one(store.pool())
    .await
    .expect("scheduled task count");
    assert!(count >= 1, "the compressed schedule must send one task");
}
