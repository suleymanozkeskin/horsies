use horsies::{Horsies, HorsiesError, OperationalErrorCode};

use super::{exponential_options, fixed_options, register_json, JsonTask, QUEUE_FULFILLMENT};

pub const TASK_NAMES: &[&str] = &["book_courier", "print_label", "tracking_seed"];

pub fn register(app: &mut Horsies) -> Result<Vec<JsonTask>, HorsiesError> {
    let mut handles = Vec::new();
    handles.push(register_json(
        app,
        "book_courier",
        QUEUE_FULFILLMENT,
        exponential_options(
            crate::tuning::COURIER_RETRY_BASE_SECONDS,
            crate::tuning::COURIER_MAX_RETRIES,
            vec![
                "COURIER_UNAVAILABLE".into(),
                OperationalErrorCode::WorkerCrashed.into(),
            ],
        ),
    )?);
    handles.push(register_json(
        app,
        "print_label",
        QUEUE_FULFILLMENT,
        fixed_options(),
    )?);
    handles.push(register_json(
        app,
        "tracking_seed",
        QUEUE_FULFILLMENT,
        fixed_options(),
    )?);
    Ok(handles)
}
