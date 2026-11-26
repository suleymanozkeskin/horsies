//! Stock tasks: reservations, releases, the nightly stocktake, and the
//! supplier feeds that the restock workflow aggregates.

use horsies::{
    task, Horsies, HorsiesError, NodeKey, TaskError, TaskOutcome, TaskRuntime, WorkflowContext,
};

use crate::domain::{
    AcmeCode, StockRelease, StockReservation, StockUpdate, StocktakeSummary, SupplierFeed,
};
use crate::{simulate, tuning};

use super::{payload_len, runtime, task_store};

/// The supplier name of the stock update that the restock workflow writes.
pub const RESTOCK_SOURCE: &str = "restock";

/// Reserve the units of one order line. A refused reservation fails the
/// order and releases its other reservations.
#[task(
    "reserve_stock",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn reserve_stock(
    rt: TaskRuntime,
    order_id: String,
    line_no: i32,
    sku: String,
    quantity: i32,
) -> Result<StockReservation, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::reserve(&store, &order_id, line_no, &sku, quantity).await
}

/// Put units back on the shelf.
#[task(
    "release_stock",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn release_stock(
    rt: TaskRuntime,
    sku: String,
    quantity: i32,
) -> Result<StockRelease, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::release(&store, &sku, quantity).await
}

/// The nightly stocktake: top every SKU up and clear the reservations that
/// no live order holds.
#[task(
    "replenish_catalog",
    queue = "analytics",
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn replenish_catalog(
    rt: TaskRuntime,
    target_units: i32,
) -> Result<StocktakeSummary, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::replenish(&store, target_units).await
}

/// Read the stock feed of one supplier.
#[task(
    "sync_supplier_feed",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::supplier_retry(),
    retry_for = ["SUPPLIER_TIMEOUT", "WORKER_CRASHED"]
)]
pub async fn sync_supplier_feed(supplier: String) -> Result<SupplierFeed, TaskError<AcmeCode>> {
    let changed_count = simulate::integer(0, 5, &[&supplier, "changed"]);
    Ok(SupplierFeed {
        sku_count: payload_len(tuning::RESTOCK_SKUS_PER_SUPPLIER),
        changed_count: i32::try_from(changed_count).unwrap_or(i32::MAX),
        supplier,
    })
}

/// Apply the supplier feeds that the workflow context holds. The restock
/// workflow starts this node when a quorum of feeds completed; a feed that
/// failed or has not ended is skipped.
#[task(
    "update_stock_levels",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"],
    workflow_context
)]
pub async fn update_stock_levels(
    feed_node_ids: Vec<String>,
    workflow_ctx: Option<WorkflowContext>,
) -> Result<StockUpdate, TaskError<AcmeCode>> {
    let context = workflow_ctx.ok_or_else(|| {
        TaskError::custom(
            AcmeCode::NoWorkflowContext,
            "update_stock_levels runs as a node of the restock workflow",
        )
    })?;
    let mut applied = 0;
    let mut skipped = 0;
    for node_id in &feed_node_ids {
        let key = NodeKey::<SupplierFeed>::new(node_id.clone());
        match context.result_for::<SupplierFeed, AcmeCode>(&key) {
            Ok(TaskOutcome::Succeeded(feed)) => applied += feed.changed_count,
            Ok(TaskOutcome::Failed(_)) | Err(_) => skipped += 1,
        }
    }
    let required = payload_len(tuning::RESTOCK_MIN_SUCCESSFUL_FEEDS.min(feed_node_ids.len()));
    let reporting = payload_len(feed_node_ids.len()) - skipped;
    if reporting < required {
        return Err(TaskError::custom(
            AcmeCode::QuorumNotMet,
            format!("{reporting} supplier feeds reported, {required} needed"),
        ));
    }
    Ok(StockUpdate {
        supplier: RESTOCK_SOURCE.to_owned(),
        applied,
        skipped,
    })
}

/// Register the stock tasks on `app`.
///
/// Errors: the first registration refusal.
pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _reserve_stock = reserve_stock::register(app)?;
    let _release_stock = release_stock::register(app)?;
    let _replenish_catalog = replenish_catalog::register(app)?;
    let _sync_supplier_feed = sync_supplier_feed::register(app)?;
    let _update_stock_levels = update_stock_levels::register(app)?;
    Ok(())
}
