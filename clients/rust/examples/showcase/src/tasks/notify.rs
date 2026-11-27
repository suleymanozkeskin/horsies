//! Notification tasks: order emails, shipping messages and marketing sends.

use horsies::{task, Horsies, HorsiesError, TaskError, TaskOutcome};

use crate::domain::{
    AbandonedCartSweep, AcmeCode, EmailReceipt, MarketingBlast, PaymentCapture, ShippingNotice,
};
use crate::{simulate, tuning};

use super::payload_len;

/// The recipient of every demo email and message.
const DEMO_RECIPIENT: &str = "customer@example.invalid";

/// Email the customer the end of the order: a confirmation when the payment
/// was captured, a notice otherwise. The order workflow runs this node even
/// when the capture failed.
#[task(
    "send_order_email",
    queue = "notifications",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn send_order_email(
    order_id: String,
    capture: TaskOutcome<PaymentCapture, AcmeCode>,
) -> Result<EmailReceipt, TaskError<AcmeCode>> {
    let template = match capture {
        TaskOutcome::Succeeded(_) => "order-confirmation",
        TaskOutcome::Failed(_) => "order-not-completed",
    };
    Ok(EmailReceipt {
        order_id,
        template: template.to_owned(),
        recipient: DEMO_RECIPIENT.to_owned(),
    })
}

/// Text the customer the tracking code of a shipment.
#[task(
    "send_shipping_sms",
    queue = "notifications",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn send_shipping_sms(
    order_id: String,
    tracking_code: String,
) -> Result<ShippingNotice, TaskError<AcmeCode>> {
    Ok(ShippingNotice {
        order_id,
        recipient: DEMO_RECIPIENT.to_owned(),
        tracking_code,
    })
}

/// Send one marketing segment.
#[task(
    "marketing_blast",
    queue = "notifications",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn marketing_blast(segment: String) -> Result<MarketingBlast, TaskError<AcmeCode>> {
    let recipients = segment_size(&segment);
    Ok(MarketingBlast {
        segment,
        recipients,
    })
}

/// Send the win-back segment to the carts that the sweep found.
#[task(
    "winback_blast",
    queue = "notifications",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    retry_for = ["WORKER_CRASHED"]
)]
pub async fn winback_blast(
    segment: String,
    sweep: TaskOutcome<AbandonedCartSweep, AcmeCode>,
) -> Result<MarketingBlast, TaskError<AcmeCode>> {
    let swept = match sweep {
        TaskOutcome::Succeeded(sweep) => sweep.swept,
        TaskOutcome::Failed(error) => return Err(error),
    };
    Ok(MarketingBlast {
        recipients: swept.min(segment_size(&segment)),
        segment,
    })
}

/// The recipients of `segment`, at most `MARKETING_SEGMENT_SIZE`.
fn segment_size(segment: &str) -> i32 {
    let largest = i64::from(payload_len(tuning::MARKETING_SEGMENT_SIZE));
    i32::try_from(simulate::integer(1, largest, &[segment, "recipients"])).unwrap_or(i32::MAX)
}

/// Register the notification tasks on `app`.
///
/// Errors: the first registration refusal.
pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _send_order_email = send_order_email::register(app)?;
    let _send_shipping_sms = send_shipping_sms::register(app)?;
    let _marketing_blast = marketing_blast::register(app)?;
    let _winback_blast = winback_blast::register(app)?;
    Ok(())
}
