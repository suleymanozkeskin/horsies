//! Registered Acme tasks.

use horsies::{
    async_task_fn, Horsies, HorsiesError, OperationalErrorCode, RetryPolicy, TaskError,
    TaskErrorCode, TaskOptions,
};
use serde_json::Value;

use crate::domain::STORE_UNAVAILABLE;

pub mod analytics;
pub mod inventory;
pub mod notify;
pub mod orders;
pub mod payments;
pub mod promotions;
pub mod returns;
pub mod shipping;

pub const QUEUE_PAYMENTS: &str = "payments";
pub const QUEUE_FULFILLMENT: &str = "fulfillment";
pub const QUEUE_NOTIFICATIONS: &str = "notifications";
pub const QUEUE_ANALYTICS: &str = "analytics";

/// The complete task surface from the pinned showcase source.
pub const ALL_TASK_NAMES: &[&str] = &[
    "sales_rollup",
    "abandoned_cart_sweep",
    "regional_rollup",
    "retention_audit",
    "catalog_import_chunk",
    "flaky_export",
    "reserve_stock",
    "release_stock",
    "replenish_catalog",
    "sync_supplier_feed",
    "update_stock_levels",
    "send_order_email",
    "send_shipping_sms",
    "marketing_blast",
    "winback_blast",
    "validate_order",
    "pick_pack",
    "allocate_warehouse",
    "generate_invoice",
    "authorize_payment",
    "capture_payment",
    "refund_payment",
    "reconcile_payments",
    "apply_promotions",
    "compute_loyalty_points",
    "publish_cdn",
    "publish_origin",
    "prewarm_search",
    "warm_cache_edge",
    "update_price",
    "receive_return",
    "inspect_item",
    "restock_or_writeoff",
    "book_courier",
    "print_label",
    "tracking_seed",
];

async fn generic_task(input: Value) -> Result<Value, TaskError> {
    Ok(input)
}

fn fixed_options() -> TaskOptions {
    fixed_options_with(
        crate::tuning::CRASH_RETRY_INTERVALS_SECONDS,
        vec![OperationalErrorCode::WorkerCrashed.into()],
    )
}

pub(crate) fn fixed_options_with(
    intervals: &[u32],
    auto_retry_for: Vec<TaskErrorCode>,
) -> TaskOptions {
    TaskOptions {
        task_name: String::new(),
        queue_name: None,
        good_until: None,
        auto_retry_for: Some(auto_retry_for),
        retry_policy: Some(
            RetryPolicy::fixed(intervals.to_vec(), false)
                .expect("pinned fixed retry policy is valid"),
        ),
        timeout_ms: None,
    }
}

pub(crate) fn register_json(
    app: &mut Horsies,
    name: &str,
    queue: &str,
    options: TaskOptions,
) -> Result<(), HorsiesError> {
    app.task::<Value, Value>(name, async_task_fn!(generic_task, Value))?
        .queue(queue)
        .task_options(options)
        .finish()?;
    Ok(())
}

pub(crate) fn options_with_timeout(timeout_ms: u32) -> TaskOptions {
    let mut options = fixed_options();
    options.timeout_ms = Some(timeout_ms);
    options
}

pub(crate) fn exponential_options(
    base_seconds: u32,
    max_retries: u32,
    auto_retry_for: Vec<TaskErrorCode>,
) -> TaskOptions {
    let mut options = fixed_options_with(&[base_seconds], auto_retry_for);
    options.retry_policy = Some(
        RetryPolicy::exponential(base_seconds, max_retries, false)
            .expect("pinned exponential retry policy is valid"),
    );
    options
}

pub(crate) fn supplier_options() -> TaskOptions {
    fixed_options_with(
        crate::tuning::SUPPLIER_RETRY_INTERVALS_SECONDS,
        vec![
            "SUPPLIER_TIMEOUT".into(),
            OperationalErrorCode::WorkerCrashed.into(),
        ],
    )
}

pub fn register_all(app: &mut Horsies) -> Result<(), HorsiesError> {
    analytics::register(app)?;
    inventory::register(app)?;
    notify::register(app)?;
    orders::register(app)?;
    payments::register(app)?;
    promotions::register(app)?;
    returns::register(app)?;
    shipping::register(app)?;
    Ok(())
}

/// Convert an Acme store failure at the task boundary.
pub fn store_failure(operation: &str, message: impl Into<String>) -> TaskError {
    let mut error = TaskError::new(
        STORE_UNAVAILABLE,
        format!("{operation} failed: {}", message.into()),
    );
    error.data = Some(serde_json::json!({ "operation": operation }));
    error
}
