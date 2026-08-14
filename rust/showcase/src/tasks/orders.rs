use horsies::{Horsies, HorsiesError};

use super::{fixed_options, options_with_timeout, register_json, JsonTask, QUEUE_FULFILLMENT};

pub const TASK_NAMES: &[&str] = &[
    "validate_order",
    "pick_pack",
    "allocate_warehouse",
    "generate_invoice",
];

pub fn register(app: &mut Horsies) -> Result<Vec<JsonTask>, HorsiesError> {
    let mut handles = Vec::new();
    for name in ["validate_order", "pick_pack", "allocate_warehouse"] {
        handles.push(register_json(
            app,
            name,
            QUEUE_FULFILLMENT,
            fixed_options(),
        )?);
    }
    handles.push(register_json(
        app,
        "generate_invoice",
        QUEUE_FULFILLMENT,
        options_with_timeout(crate::tuning::INVOICE_TIMEOUT_MS),
    )?);
    Ok(handles)
}
