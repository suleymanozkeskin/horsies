use horsies::{Horsies, HorsiesError};

use super::{fixed_options, register_json, QUEUE_NOTIFICATIONS};

pub const TASK_NAMES: &[&str] = &[
    "send_order_email",
    "send_shipping_sms",
    "marketing_blast",
    "winback_blast",
];

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    for name in TASK_NAMES {
        register_json(app, name, QUEUE_NOTIFICATIONS, fixed_options())?;
    }
    Ok(())
}
