//! Return tasks: receipt, inspection, and the restock or write-off.
//!
//! A damaged item is an error of the inspection, which pauses the
//! `returns_review` workflow; the settle node takes failed dependencies, so
//! a resume writes the item off.

use chrono::Utc;
use horsies::{task, Horsies, HorsiesError, TaskError, TaskOutcome, TaskRuntime};

use crate::domain::{AcmeCode, Inspection, RestockDecision, ReturnCase, ReturnReceipt};
use crate::store::Store;
use crate::{simulate, tuning};

use super::{store_failure, task_store};

/// The condition of a resellable item.
const RESELLABLE: &str = "resellable";
/// The condition of a damaged item.
const DAMAGED: &str = "damaged";
/// The status of a return whose item is back in stock.
const RESTOCKED: &str = "restocked";
/// The status of a return whose item is written off.
const WRITTEN_OFF: &str = "written_off";

/// Book the returned item in. A return that is booked already stays as it
/// is.
#[task(
    "receive_return",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn receive_return(
    rt: TaskRuntime,
    return_id: String,
    order_id: String,
    sku: String,
    quantity: i32,
) -> Result<ReturnReceipt, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    store
        .open_return(&ReturnCase {
            return_id: return_id.clone(),
            order_id: order_id.clone(),
            sku: sku.clone(),
            quantity,
            status: "received".to_owned(),
            condition: None,
            created_at: Utc::now(),
        })
        .await
        .map_err(|error| store_failure("open_return", error))?;
    Ok(ReturnReceipt {
        return_id,
        order_id,
        sku,
        quantity,
    })
}

/// Inspect the returned item. The damage is recorded either way; a damaged
/// item is reported as `DAMAGED_ITEM`, which is not retried.
#[task(
    "inspect_item",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn inspect_item(
    rt: TaskRuntime,
    return_id: String,
) -> Result<Inspection, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    let case = return_case(&store, &return_id).await?;
    let damaged = simulate::draw(tuning::RETURN_DAMAGE_RATE, &[&return_id, "damage"]);
    let condition = match damaged {
        true => DAMAGED,
        false => RESELLABLE,
    };
    let _recorded = store
        .record_inspection(&return_id, &condition.to_owned())
        .await
        .map_err(|error| store_failure("record_inspection", error))?;
    match damaged {
        true => Err(TaskError::user(
            AcmeCode::DamagedItem,
            format!("{} came back damaged and cannot be resold", case.sku),
        )
        .with_data(serde_json::json!({
            "return_id": return_id,
            "sku": case.sku,
            "quantity": case.quantity,
        }))),
        false => Ok(Inspection {
            return_id,
            sku: case.sku,
            condition: RESELLABLE.to_owned(),
            notes: "no visible wear, tags intact".to_owned(),
        }),
    }
}

/// Restock a resellable item, or write a damaged one off. The workflow runs
/// this node when the inspection failed too.
#[task(
    "restock_or_writeoff",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn restock_or_writeoff(
    rt: TaskRuntime,
    return_id: String,
    inspection: TaskOutcome<Inspection, AcmeCode>,
) -> Result<RestockDecision, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    let case = return_case(&store, &return_id).await?;
    let outcome = match inspection {
        TaskOutcome::Succeeded(_) => {
            let _adjusted = store
                .adjust_stock(&case.sku, case.quantity)
                .await
                .map_err(|error| store_failure("adjust_stock", error))?;
            RESTOCKED
        }
        TaskOutcome::Failed(_) => WRITTEN_OFF,
    };
    let _closed = store
        .close_return(&return_id, &outcome.to_owned())
        .await
        .map_err(|error| store_failure("close_return", error))?;
    Ok(RestockDecision {
        return_id,
        sku: case.sku,
        quantity: case.quantity,
        outcome: outcome.to_owned(),
        available_after: None,
    })
}

/// The return `return_id`.
///
/// Errors: `RETURN_NOT_FOUND`, `STORE_UNAVAILABLE`.
async fn return_case(store: &Store, return_id: &str) -> Result<ReturnCase, TaskError<AcmeCode>> {
    store
        .get_return(return_id)
        .await
        .map_err(|error| store_failure("get_return", error))?
        .ok_or_else(|| TaskError::user(AcmeCode::ReturnNotFound, format!("no return {return_id}")))
}

/// Register the return tasks on `app`.
///
/// Errors: the first registration refusal.
pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _receive_return = receive_return::register(app)?;
    let _inspect_item = inspect_item::register(app)?;
    let _restock_or_writeoff = restock_or_writeoff::register(app)?;
    Ok(())
}
