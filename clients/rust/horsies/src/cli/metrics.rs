//! `<binary> metrics --listen HOST:PORT`: serve the fleet metrics of the
//! app's database at `GET /metrics` (Python `horsies metrics`).
//!
//! - The fleet state is read at once and then at most once per
//!   `FLEET_COLLECT_INTERVAL`; a scrape only renders the last read, so
//!   scrapes add no load on the database.
//! - The `queue` and `schedule` label values are the queues and the
//!   schedules of the app's config; every other name is `(unknown)`.
//! - The connection runs no schema step and opens no listener
//!   (`SchemaMode::AssumeInitialized`).
//! - A failed connect or a failed statement is
//!   `horsies_exporter_collect_failed{query} 1` in the text, never an end of
//!   the command.

use std::sync::Arc;

use horsies_core::monitoring::fleet::{serve_fleet_metrics, FleetLabels};
use horsies_core::telemetry::declared::DeclaredNamesRefused;
use horsies_core::telemetry::endpoint::{bind_endpoint, EndpointFailed};
use horsies_core::telemetry::listen::ListenAddress;

use crate::signals::stop_on_signals;
use crate::{Horsies, HorsiesError, SchemaMode};

/// Why `metrics` did not serve, or stopped serving with a failure. Each
/// ends the command with exit status 1.
#[derive(Debug, thiserror::Error)]
pub(super) enum MetricsFailed {
    /// A queue or a schedule name of the config cannot be a label value:
    /// it is empty or it is `(unknown)`.
    #[error("the queue or schedule names of the config are not valid metric labels: {0}")]
    Labels(DeclaredNamesRefused),
    /// The config does not build a handle.
    #[error("invalid application configuration: {0}")]
    Config(HorsiesError),
    /// The address did not bind, or the server failed.
    #[error("{0}")]
    Endpoint(EndpointFailed),
}

/// Serve the fleet metrics of the database of `app` on `listen` until
/// `SIGTERM` or `SIGINT`.
///
/// Errors: [`MetricsFailed`].
pub(super) async fn serve(app: &Horsies, listen: &ListenAddress) -> Result<(), MetricsFailed> {
    let labels = FleetLabels::of_config(app.config()).map_err(MetricsFailed::Labels)?;
    let reader = Horsies::with_schema_mode(app.config().clone(), SchemaMode::AssumeInitialized)
        .map_err(MetricsFailed::Config)?;
    let endpoint = bind_endpoint(listen)
        .await
        .map_err(MetricsFailed::Endpoint)?;
    serve_fleet_metrics(
        endpoint,
        Arc::clone(reader.handle()),
        labels,
        stop_on_signals("fleet metrics server", || {}),
    )
    .await
    .map_err(MetricsFailed::Endpoint)
}
