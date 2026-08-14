use horsies::{Horsies, HorsiesError, OperationalErrorCode};

use super::{exponential_options, fixed_options, register_json, JsonTask, QUEUE_PAYMENTS};

pub const TASK_NAMES: &[&str] = &[
    "authorize_payment",
    "capture_payment",
    "refund_payment",
    "reconcile_payments",
];

pub fn register(app: &mut Horsies) -> Result<Vec<JsonTask>, HorsiesError> {
    let mut handles = Vec::new();
    handles.push(register_json(
        app,
        "authorize_payment",
        QUEUE_PAYMENTS,
        exponential_options(
            crate::tuning::PSP_RETRY_BASE_SECONDS,
            crate::tuning::PSP_MAX_RETRIES,
            vec![
                "PSP_UNAVAILABLE".into(),
                OperationalErrorCode::WorkerCrashed.into(),
            ],
        ),
    )?);
    for name in ["capture_payment", "refund_payment", "reconcile_payments"] {
        handles.push(register_json(app, name, QUEUE_PAYMENTS, fixed_options())?);
    }
    Ok(handles)
}
