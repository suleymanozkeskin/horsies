use horsies::{Horsies, HorsiesError};

use super::{fixed_options, register_json, QUEUE_FULFILLMENT};

pub const TASK_NAMES: &[&str] = &[
    "reserve_stock",
    "release_stock",
    "replenish_catalog",
    "sync_supplier_feed",
    "update_stock_levels",
];

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    register_json(app, "reserve_stock", QUEUE_FULFILLMENT, fixed_options())?;
    register_json(app, "release_stock", QUEUE_FULFILLMENT, fixed_options())?;
    // These names are owned by analytics::register. The source modules share
    // the same task registry, so this module only documents their ownership.
    Ok(())
}
