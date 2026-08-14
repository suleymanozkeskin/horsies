use horsies::{Horsies, HorsiesError};

use super::{fixed_options, register_json, JsonTask, QUEUE_FULFILLMENT};

pub const TASK_NAMES: &[&str] = &["receive_return", "inspect_item", "restock_or_writeoff"];

pub fn register(app: &mut Horsies) -> Result<Vec<JsonTask>, HorsiesError> {
    let mut handles = Vec::new();
    for name in TASK_NAMES {
        handles.push(register_json(
            app,
            name,
            QUEUE_FULFILLMENT,
            fixed_options(),
        )?);
    }
    Ok(handles)
}
