use horsies::{Horsies, HorsiesError};

use horsies::OperationalErrorCode;

use super::{
    fixed_options, fixed_options_with, register_json, supplier_options, JsonTask, QUEUE_ANALYTICS,
};

pub const TASK_NAMES: &[&str] = &[
    "sales_rollup",
    "abandoned_cart_sweep",
    "regional_rollup",
    "retention_audit",
    "catalog_import_chunk",
    "flaky_export",
    "replenish_catalog",
    "sync_supplier_feed",
    "update_stock_levels",
    "prewarm_search",
    "warm_cache_edge",
    "update_price",
];

pub fn register(app: &mut Horsies) -> Result<Vec<JsonTask>, HorsiesError> {
    let mut handles = Vec::new();
    for name in [
        "sales_rollup",
        "abandoned_cart_sweep",
        "regional_rollup",
        "retention_audit",
        "catalog_import_chunk",
        "flaky_export",
        "replenish_catalog",
    ] {
        let options = if name == "flaky_export" {
            fixed_options_with(
                crate::tuning::CHAOS_EXPORT_RETRY_INTERVALS_SECONDS,
                vec![OperationalErrorCode::WorkerCrashed.into()],
            )
        } else {
            fixed_options()
        };
        handles.push(register_json(app, name, QUEUE_ANALYTICS, options)?);
    }
    handles.push(register_json(
        app,
        "sync_supplier_feed",
        QUEUE_ANALYTICS,
        supplier_options(),
    )?);
    handles.push(register_json(
        app,
        "update_stock_levels",
        QUEUE_ANALYTICS,
        fixed_options(),
    )?);
    handles.push(register_json(
        app,
        "prewarm_search",
        QUEUE_ANALYTICS,
        fixed_options(),
    )?);
    handles.push(register_json(
        app,
        "warm_cache_edge",
        super::QUEUE_FULFILLMENT,
        fixed_options(),
    )?);
    handles.push(register_json(
        app,
        "update_price",
        QUEUE_ANALYTICS,
        fixed_options(),
    )?);
    Ok(handles)
}
