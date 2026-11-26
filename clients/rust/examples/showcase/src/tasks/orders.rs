//! Order tasks: validation, pick and pack, warehouse allocation, invoice.

use horsies::{task, Horsies, HorsiesError, TaskError, TaskRuntime};

use crate::domain::{AcmeCode, Invoice, OrderValidation, PickPack, WarehouseAllocation};
use crate::{simulate, tuning};

use super::{runtime, task_store};

/// Check that the order exists and has lines, and mark it validated.
#[task(
    "validate_order",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn validate_order(
    rt: TaskRuntime,
    order_id: String,
) -> Result<OrderValidation, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::validate(&store, &order_id).await
}

/// Consume the reserved units of the order and mark it packed.
#[task(
    "pick_pack",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn pick_pack(rt: TaskRuntime, order_id: String) -> Result<PickPack, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::pick_and_pack(&store, &order_id).await
}

/// Pick the warehouse that a stock transfer draws from.
#[task(
    "allocate_warehouse",
    queue = "fulfillment",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn allocate_warehouse(
    sku: String,
    quantity: i32,
) -> Result<WarehouseAllocation, TaskError<AcmeCode>> {
    let distance_km = simulate::integer(20, 1_400, &[&sku, "distance"]);
    Ok(WarehouseAllocation {
        order_id: format!("transfer:{sku}:{quantity}"),
        warehouse_code: simulate::choice(tuning::WAREHOUSES, &[&sku, "warehouse"]).to_owned(),
        distance_km: i32::try_from(distance_km).unwrap_or(i32::MAX),
    })
}

/// Render the invoice of the order. The deadline is
/// `tuning::INVOICE_TIMEOUT_MS`.
#[task(
    "generate_invoice",
    queue = "fulfillment",
    timeout_ms = 8_000,
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn generate_invoice(
    rt: TaskRuntime,
    order_id: String,
) -> Result<Invoice, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::invoice(&store, &order_id).await
}

/// Register the order tasks on `app`.
///
/// Errors: the first registration refusal.
pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _validate_order = validate_order::register(app)?;
    let _pick_pack = pick_pack::register(app)?;
    let _allocate_warehouse = allocate_warehouse::register(app)?;
    let _generate_invoice = generate_invoice::register(app)?;
    Ok(())
}
