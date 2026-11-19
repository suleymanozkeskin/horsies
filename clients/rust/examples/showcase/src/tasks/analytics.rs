//! Reporting tasks that the schedules and the maintenance workflows send:
//! rollups, sweeps, audits, the catalog import and the export drill.

use horsies::{task, Horsies, HorsiesError, TaskError, TaskFunction};

use crate::domain::{
    AbandonedCartSweep, AcmeCode, CatalogChunk, ExportManifest, RegionalRollup, RetentionAudit,
    SalesRollup,
};
use crate::{simulate, tuning};

use super::payload_len;

/// A drawn count in `low..=high` for `parts`, as the `i32` of a payload.
fn drawn_count(low: i64, high: i64, parts: &[&str]) -> i32 {
    i32::try_from(simulate::integer(low, high, parts)).unwrap_or(i32::MAX)
}

/// Roll the sales of one window up.
#[task(
    "sales_rollup",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn sales_rollup(window: String) -> Result<SalesRollup, TaskError<AcmeCode>> {
    let orders_counted = drawn_count(20, 400, &[&window, "orders"]);
    let gross_cents = orders_counted * drawn_count(2_000, 9_000, &[&window, "basket"]);
    Ok(SalesRollup {
        orders_counted,
        gross_cents,
        captured_cents: gross_cents / 10 * 9,
    })
}

/// Count the carts that were left longer than `older_than_minutes`.
#[task(
    "abandoned_cart_sweep",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn abandoned_cart_sweep(
    older_than_minutes: i32,
) -> Result<AbandonedCartSweep, TaskError<AcmeCode>> {
    let window = older_than_minutes.to_string();
    let swept = drawn_count(0, 12, &[&window, "swept"]);
    let oldest_order_id = match swept {
        0 => None,
        _ => Some(format!(
            "ACME-{:05}",
            drawn_count(1, 99_999, &[&window, "oldest"])
        )),
    };
    Ok(AbandonedCartSweep {
        swept,
        oldest_order_id,
    })
}

/// Roll the sales of one region up.
#[task(
    "regional_rollup",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn regional_rollup(region: String) -> Result<RegionalRollup, TaskError<AcmeCode>> {
    let orders_counted = drawn_count(5, 120, &[&region, "orders"]);
    let gross_cents = orders_counted * drawn_count(2_000, 9_000, &[&region, "basket"]);
    Ok(RegionalRollup {
        region,
        orders_counted,
        gross_cents,
    })
}

/// Count the rows that retention can prune after `older_than_days`.
#[task(
    "retention_audit",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn retention_audit(older_than_days: i32) -> Result<RetentionAudit, TaskError<AcmeCode>> {
    let window = older_than_days.to_string();
    let orders_examined = drawn_count(100, 5_000, &[&window, "examined"]);
    Ok(RetentionAudit {
        older_than_days,
        orders_examined,
        rows_prunable: orders_examined / 4,
    })
}

/// Import one chunk of a catalog import.
#[task(
    "catalog_import_chunk",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn catalog_import_chunk(
    import_id: String,
    chunk_index: i32,
) -> Result<CatalogChunk, TaskError<AcmeCode>> {
    let chunk = chunk_index.to_string();
    let checksum = simulate::integer(0, i64::from(u32::MAX), &[&import_id, &chunk, "checksum"]);
    Ok(CatalogChunk {
        chunk_index,
        rows: payload_len(tuning::CATALOG_IMPORT_ROWS_PER_CHUNK),
        checksum: format!("{checksum:08x}"),
    })
}

/// Write one export. `chaos` sends these while an operator restarts the
/// worker: a run that the restart ends is retried as `WORKER_CRASHED`.
#[task(
    "flaky_export",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::export_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn flaky_export(export_id: String) -> Result<ExportManifest, TaskError<AcmeCode>> {
    let rows = drawn_count(1_000, 50_000, &[&export_id, "rows"]);
    Ok(ExportManifest { export_id, rows })
}

/// Register the reporting tasks on `app` and return the export drill's
/// function.
///
/// Errors: the first registration refusal.
pub fn register(
    app: &mut Horsies,
) -> Result<TaskFunction<flaky_export::Input, ExportManifest, AcmeCode>, HorsiesError> {
    let _sales_rollup = sales_rollup::register(app)?;
    let _abandoned_cart_sweep = abandoned_cart_sweep::register(app)?;
    let _regional_rollup = regional_rollup::register(app)?;
    let _retention_audit = retention_audit::register(app)?;
    let _catalog_import_chunk = catalog_import_chunk::register(app)?;
    flaky_export::register(app)
}
