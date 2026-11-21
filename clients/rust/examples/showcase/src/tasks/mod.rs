//! The Acme tasks.
//!
//! Every task returns `TaskError<AcmeCode>`. The tasks that read or write
//! the Acme tables run in isolated children and reach the store through the
//! `TaskStore` that the app provides; the others run on the seats of a
//! shared async child and need a `timeout_ms`. The queue of a task is a
//! literal in its attribute; `QUEUES` lists the same names.

use horsies::{Horsies, HorsiesError, RetryPolicy, TaskError, TaskFunction, TaskRuntime};

use crate::domain::{AcmeCode, LoyaltyPoints, PromotionOutcome};
use crate::store::{Store, TaskStore};
use crate::tuning;

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

/// The complete task surface of the showcase.
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

/// The typed functions that the scenarios and the tests send through. The
/// other tasks are sent by workflows and schedules only.
#[derive(Clone)]
pub struct AcmeTasks {
    pub apply_promotions:
        TaskFunction<promotions::apply_promotions::Input, PromotionOutcome, AcmeCode>,
    pub compute_loyalty_points:
        TaskFunction<promotions::compute_loyalty_points::Input, LoyaltyPoints, AcmeCode>,
    pub flaky_export:
        TaskFunction<analytics::flaky_export::Input, crate::domain::ExportManifest, AcmeCode>,
    pub update_price:
        TaskFunction<promotions::update_price::Input, crate::domain::PriceUpdate, AcmeCode>,
    pub book_courier:
        TaskFunction<shipping::book_courier::Input, crate::domain::CourierBooking, AcmeCode>,
}

/// The retry of a task that a worker crash ended: `WORKER_CRASHED` only.
pub fn crash_retry() -> RetryPolicy {
    fixed_retry(tuning::CRASH_RETRY_INTERVALS_SECONDS)
}

/// The retry of `flaky_export`: wider intervals, so a restarted worker is
/// back before the retry.
pub fn export_retry() -> RetryPolicy {
    fixed_retry(tuning::CHAOS_EXPORT_RETRY_INTERVALS_SECONDS)
}

/// The retry of `sync_supplier_feed` on `SUPPLIER_TIMEOUT` and a crash.
pub fn supplier_retry() -> RetryPolicy {
    fixed_retry(tuning::SUPPLIER_RETRY_INTERVALS_SECONDS)
}

/// The retry of `authorize_payment` on `PSP_UNAVAILABLE` and a crash.
pub fn psp_retry() -> RetryPolicy {
    exponential_retry(tuning::PSP_RETRY_BASE_SECONDS, tuning::PSP_MAX_RETRIES)
}

/// The retry of `book_courier` on `COURIER_UNAVAILABLE` and a crash.
pub fn courier_retry() -> RetryPolicy {
    exponential_retry(
        tuning::COURIER_RETRY_BASE_SECONDS,
        tuning::COURIER_MAX_RETRIES,
    )
}

/// A fixed policy of the tuned `intervals`. The intervals are constants of
/// `tuning`, inside the policy's bounds, so a refusal is a defect.
fn fixed_retry(intervals: &[u32]) -> RetryPolicy {
    RetryPolicy::fixed(intervals.to_vec(), false)
        .unwrap_or_else(|refused| panic!("a tuned fixed retry policy is refused: {refused}"))
}

/// An exponential policy of the tuned base and count; a refusal is a
/// defect, as for [`fixed_retry`].
fn exponential_retry(base_seconds: u32, max_retries: u32) -> RetryPolicy {
    RetryPolicy::exponential(base_seconds, max_retries, false)
        .unwrap_or_else(|refused| panic!("a tuned exponential retry policy is refused: {refused}"))
}

/// Register every Acme task on `app` and return the functions that the
/// scenarios send through.
///
/// Errors: the first registration refusal.
pub fn register_all(app: &mut Horsies) -> Result<AcmeTasks, HorsiesError> {
    let flaky_export = analytics::register(app)?;
    inventory::register(app)?;
    notify::register(app)?;
    orders::register(app)?;
    payments::register(app)?;
    returns::register(app)?;
    let promotions::PromotionTasks {
        apply_promotions,
        compute_loyalty_points,
        update_price,
    } = promotions::register(app)?;
    let book_courier = shipping::register(app)?;
    Ok(AcmeTasks {
        apply_promotions,
        compute_loyalty_points,
        flaky_export,
        update_price,
        book_courier,
    })
}

/// The Acme store of the process that runs the task.
///
/// Errors: `STORE_UNAVAILABLE` when the app provides no `TaskStore`, or the
/// store does not connect.
pub async fn task_store(runtime: &TaskRuntime) -> Result<Store, TaskError<AcmeCode>> {
    let store = runtime
        .state::<TaskStore>()
        .map_err(|missing| store_failure("state", missing))?;
    store
        .get()
        .await
        .map_err(|error| store_failure("connect", error))
}

/// The task error of a failed store `operation`.
pub fn store_failure(operation: &str, message: impl std::fmt::Display) -> TaskError<AcmeCode> {
    TaskError::user(
        AcmeCode::StoreUnavailable,
        format!("{operation} failed: {message}"),
    )
    .with_data(serde_json::json!({ "operation": operation }))
}

/// A count of the store as the `i32` of a payload. A count above `i32::MAX`
/// rows does not occur in the demo tables; it is reported as `i32::MAX`.
pub(crate) fn payload_count(count: i64) -> i32 {
    i32::try_from(count).unwrap_or(i32::MAX)
}

/// A length as the `i32` of a payload, as [`payload_count`].
pub(crate) fn payload_len(length: usize) -> i32 {
    i32::try_from(length).unwrap_or(i32::MAX)
}
