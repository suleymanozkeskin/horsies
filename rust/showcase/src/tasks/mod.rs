//! Registered Acme tasks.

use std::collections::HashMap;

use horsies::{
    async_task_fn, Horsies, HorsiesError, OperationalErrorCode, RetryPolicy, TaskError,
    TaskErrorCode, TaskFunction, TaskOptions,
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
pub mod runtime;
pub mod shipping;

pub const QUEUE_PAYMENTS: &str = "payments";
pub const QUEUE_FULFILLMENT: &str = "fulfillment";
pub const QUEUE_NOTIFICATIONS: &str = "notifications";
pub const QUEUE_ANALYTICS: &str = "analytics";

pub type JsonTask = TaskFunction<Value, Value>;
pub type TaskHandles = HashMap<String, JsonTask>;

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
) -> Result<JsonTask, HorsiesError> {
    let registered = match name {
        "validate_order" => async_task_fn!(runtime::validate_order, Value),
        "reserve_stock" => async_task_fn!(runtime::reserve_stock, Value),
        "authorize_payment" => async_task_fn!(runtime::authorize_payment, Value),
        "capture_payment" => async_task_fn!(runtime::capture_payment, Value),
        "pick_pack" => async_task_fn!(runtime::pick_pack, Value),
        "generate_invoice" => async_task_fn!(runtime::generate_invoice, Value),
        "book_courier" => async_task_fn!(runtime::book_courier, Value),
        "print_label" => async_task_fn!(runtime::print_label, Value),
        "tracking_seed" => async_task_fn!(runtime::tracking_seed, Value),
        "send_order_email" => async_task_fn!(runtime::send_order_email, Value),
        "apply_promotions" => async_task_fn!(runtime::apply_promotions, Value),
        "compute_loyalty_points" => async_task_fn!(runtime::compute_loyalty_points, Value),
        _ => async_task_fn!(generic_task, Value),
    };
    Ok(app
        .task::<Value, Value>(name, registered)?
        .queue(queue)
        .task_options(options)
        .finish()?)
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

pub fn register_all(app: &mut Horsies) -> Result<TaskHandles, HorsiesError> {
    let mut handles = TaskHandles::new();
    for handle in analytics::register(app)?
        .into_iter()
        .chain(inventory::register(app)?)
        .chain(notify::register(app)?)
        .chain(orders::register(app)?)
        .chain(payments::register(app)?)
        .chain(promotions::register(app)?)
        .chain(returns::register(app)?)
        .chain(shipping::register(app)?)
    {
        handles.insert(handle.task_name().to_owned(), handle);
    }
    Ok(handles)
}

/// Convert an Acme store failure at the task boundary.
pub fn store_failure(operation: &str, message: impl std::fmt::Display) -> TaskError {
    let mut error = TaskError::new(STORE_UNAVAILABLE, format!("{operation} failed: {message}"));
    error.data = Some(serde_json::json!({ "operation": operation }));
    error
}
