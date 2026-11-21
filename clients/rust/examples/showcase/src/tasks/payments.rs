//! Payment tasks: authorization, capture, refund and reconciliation.
//!
//! `PSP_UNAVAILABLE` is operational: the provider is unreachable, and the
//! retry policy backs off exponentially. `CARD_DECLINED` is domain: no
//! retry, and the draw is a stable hash of the order id, so a rerun of a
//! declined payment declines again.

use horsies::{task, Horsies, HorsiesError, TaskError, TaskOutcome, TaskRuntime};

use crate::domain::{
    AcmeCode, PaymentAuthorization, PaymentCapture, PaymentRefund, ReconciliationReport,
};
use crate::simulate;

use super::{runtime, task_store};

/// Hold the order total on the customer's card.
#[task(
    "authorize_payment",
    queue = "payments",
    retry_policy = crate::tasks::psp_retry(),
    auto_retry_for = ["PSP_UNAVAILABLE", "WORKER_CRASHED"]
)]
pub async fn authorize_payment(
    rt: TaskRuntime,
    order_id: String,
    amount_cents: i32,
) -> Result<PaymentAuthorization, TaskError<AcmeCode>> {
    let store = task_store(&rt).await?;
    runtime::authorize(&store, &order_id, amount_cents).await
}

/// Settle the hold that the authorization node produced.
#[task(
    "capture_payment",
    queue = "payments",
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn capture_payment(
    rt: TaskRuntime,
    order_id: String,
    authorization: TaskOutcome<PaymentAuthorization, AcmeCode>,
) -> Result<PaymentCapture, TaskError<AcmeCode>> {
    let hold = match authorization {
        TaskOutcome::Succeeded(hold) => hold,
        TaskOutcome::Failed(error) => return Err(error),
    };
    let store = task_store(&rt).await?;
    runtime::capture(&store, &order_id, &hold).await
}

/// Refund a disputed order.
#[task(
    "refund_payment",
    queue = "payments",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn refund_payment(
    order_id: String,
    amount_cents: i32,
) -> Result<PaymentRefund, TaskError<AcmeCode>> {
    Ok(PaymentRefund {
        refund_id: format!("rfd_{order_id}"),
        order_id,
        amount_cents,
    })
}

/// Compare the authorizations and captures of one reconciliation window.
#[task(
    "reconcile_payments",
    queue = "payments",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn reconcile_payments(
    window: String,
) -> Result<ReconciliationReport, TaskError<AcmeCode>> {
    let authorizations = simulate::integer(10, 400, &[&window, "authorizations"]);
    let unmatched = simulate::integer(0, 3, &[&window, "unmatched"]);
    Ok(ReconciliationReport {
        authorizations: i32::try_from(authorizations).unwrap_or(i32::MAX),
        captures: i32::try_from(authorizations - unmatched).unwrap_or(i32::MAX),
        unmatched: i32::try_from(unmatched).unwrap_or(i32::MAX),
    })
}

/// Register the payment tasks on `app`.
///
/// Errors: the first registration refusal.
pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _authorize_payment = authorize_payment::register(app)?;
    let _capture_payment = capture_payment::register(app)?;
    let _refund_payment = refund_payment::register(app)?;
    let _reconcile_payments = reconcile_payments::register(app)?;
    Ok(())
}
