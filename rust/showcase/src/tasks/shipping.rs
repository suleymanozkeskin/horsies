use horsies::{Horsies, HorsiesError, OperationalErrorCode};

use super::{exponential_options, fixed_options, register_json, QUEUE_FULFILLMENT};

pub const TASK_NAMES: &[&str] = &["book_courier", "print_label", "tracking_seed"];

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    register_json(
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
    )?;
    register_json(app, "print_label", QUEUE_FULFILLMENT, fixed_options())?;
    register_json(app, "tracking_seed", QUEUE_FULFILLMENT, fixed_options())?;
    Ok(())
}
