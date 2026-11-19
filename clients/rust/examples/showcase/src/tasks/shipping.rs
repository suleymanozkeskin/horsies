//! Shipping tasks: the courier booking, the label and the tracking code.
//! Each step reads the result of the step before it.

use horsies::{task, Horsies, HorsiesError, TaskError, TaskFunction, TaskOutcome, TaskRuntime};

use crate::domain::{AcmeCode, CourierBooking, ShippingLabel, TrackingSeed};

use super::{runtime, task_store};

/// Book the courier of an order. A drawn flake fails the first attempts
/// with `COURIER_UNAVAILABLE`, which the retry policy retries.
#[task(
    "book_courier",
    queue = "fulfillment",
    retry_policy = crate::tasks::courier_retry(),
    auto_retry_for = ["COURIER_UNAVAILABLE", "WORKER_CRASHED"]
)]
pub async fn book_courier(
    rt: TaskRuntime,
    order_id: String,
    courier: String,
    express: bool,
) -> Result<CourierBooking, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::book(&store, &order_id, &courier, express).await
}

/// Print the label of the booked shipment.
#[task(
    "print_label",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn print_label(
    rt: TaskRuntime,
    order_id: String,
    booking: TaskOutcome<CourierBooking, AcmeCode>,
) -> Result<ShippingLabel, TaskError<AcmeCode>> {
    if let TaskOutcome::Failed(error) = booking {
        return Err(error);
    }
    let store = task_store(&rt).await?;
    runtime::label(&store, &order_id).await
}

/// Seed the tracking code of the labelled shipment and mark the order
/// shipped.
#[task(
    "tracking_seed",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn tracking_seed(
    rt: TaskRuntime,
    order_id: String,
    label: TaskOutcome<ShippingLabel, AcmeCode>,
) -> Result<TrackingSeed, TaskError<AcmeCode>> {
    if let TaskOutcome::Failed(error) = label {
        return Err(error);
    }
    let store = task_store(&rt).await?;
    runtime::track(&store, &order_id).await
}

/// Register the shipping tasks on `app` and return the courier booking's
/// function.
///
/// Errors: the first registration refusal.
pub fn register(
    app: &mut Horsies,
) -> Result<TaskFunction<book_courier::Input, CourierBooking, AcmeCode>, HorsiesError> {
    let book_courier = book_courier::register(app)?;
    let _print_label = print_label::register(app)?;
    let _tracking_seed = tracking_seed::register(app)?;
    Ok(book_courier)
}
