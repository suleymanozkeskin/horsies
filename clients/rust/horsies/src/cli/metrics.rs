//! `<binary> metrics --metrics-listen HOST:PORT`: serve the fleet metrics of the
//! app's database at `GET /metrics` (Python `horsies metrics`).
//!
//! - The fleet state is read at once and then at most once per
//!   `FLEET_COLLECT_INTERVAL`; a scrape only renders the last read, so
//!   scrapes add no load on the database.
//! - The `queue` and `schedule` label values are the queues and the
//!   schedules of the app's config; every other name is `(unknown)`. A
//!   config name that cannot be a label value of its own (empty, the text
//!   `(unknown)`, or past the bound of names) counts in the `(unknown)`
//!   series too, and one WARNING line at the start names it.
//! - The connection runs no schema step and opens no listener
//!   (`SchemaMode::AssumeInitialized`).
//! - A failed connect or a failed statement is
//!   `horsies_exporter_collect_failed{query} 1` in the text, never an end of
//!   the command.

use std::sync::Arc;

use horsies_core::monitoring::fleet::{serve_fleet_metrics, FleetLabels};
use horsies_core::telemetry::declared::{warn_folded_names, Folded};
use horsies_core::telemetry::endpoint::{bind_endpoint, EndpointFailed};
use horsies_core::telemetry::listen::ListenAddress;

use crate::signals::stop_on_signals;
use crate::{Horsies, SchemaMode, ValidationReport};

/// Why `metrics` did not serve, or stopped serving with a failure. Each
/// ends the command with exit status 1.
#[derive(Debug, thiserror::Error)]
pub(super) enum MetricsFailed {
    /// The config does not build a handle.
    #[error("invalid application configuration: {0}")]
    Config(ValidationReport),
    /// The address did not bind.
    #[error("{0}")]
    Endpoint(EndpointFailed),
}

/// Serve the fleet metrics of the database of `app` on `metrics_listen` until
/// `SIGTERM` or `SIGINT`.
///
/// Errors: [`MetricsFailed`].
pub(super) async fn serve(
    app: &Horsies,
    metrics_listen: &ListenAddress,
) -> Result<(), MetricsFailed> {
    let Folded {
        declared: labels,
        folded,
    } = FleetLabels::of_config(app.config());
    warn_folded_names(&"the fleet metrics", &folded);
    let reader = Horsies::with_schema_mode(app.config().clone(), SchemaMode::AssumeInitialized)
        .map_err(MetricsFailed::Config)?;
    let endpoint = bind_endpoint(metrics_listen)
        .await
        .map_err(MetricsFailed::Endpoint)?;
    serve_fleet_metrics(
        endpoint,
        Arc::clone(reader.handle()),
        labels,
        stop_on_signals("fleet metrics server", || {}),
    )
    .await;
    Ok(())
}
