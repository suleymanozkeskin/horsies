//! The metrics endpoint of a worker, which `--metrics-listen HOST:PORT`
//! turns on: what the worker holds for it, the `telemetry` column of its
//! snapshot, and the start and the stop of its thread.
//!
//! Without the `metrics-endpoint` feature a worker has no endpoint: the
//! only value is [`WorkerEndpoint::NoMetricsEndpoint`], each point of the
//! claim loop is one branch, and the snapshot shows the endpoint `off`.
//!
//! With an address (`WorkerEndpoint::Listen`, `serve`): the run starts one
//! OS thread with its own current-thread runtime. The thread binds the
//! address and serves `GET /metrics` (the registry of the worker),
//! `GET /livez` and `GET /readyz` (`crate::worker::probes`). A bind that
//! fails does not stop the worker: the worker starts and claims, the thread
//! binds again after the slot backoff, and the snapshot shows the endpoint
//! `down` with the address and the cause. The probes answer only once the
//! address is bound. The thread stops after the stop of the worker drained,
//! so the last scrape sees the attempts of the drain.

use std::time::Duration;

#[cfg(feature = "metrics-endpoint")]
use std::sync::Arc;

use crate::worker::executor::handler_child::{handler_child_json, HandlerChildState};

#[cfg(feature = "metrics-endpoint")]
mod serve;

#[cfg(feature = "metrics-endpoint")]
pub(crate) use serve::ListenEndpoint;

/// The metrics endpoint of a worker. Clones share one endpoint.
#[derive(Clone)]
pub(crate) enum WorkerEndpoint {
    /// No `--metrics-listen`: no thread, no probe state.
    NoMetricsEndpoint,
    /// The endpoint of `--metrics-listen`: its address, the registry that
    /// it serves, the probe state and the state of the endpoint.
    #[cfg(feature = "metrics-endpoint")]
    Listen(Arc<ListenEndpoint>),
}

impl WorkerEndpoint {
    /// The claim loop turns again within `wait` plus the probe allowance.
    /// One branch with no endpoint.
    pub(crate) fn claim_turn_within(
        &self,
        #[cfg_attr(not(feature = "metrics-endpoint"), allow(unused_variables))] wait: Duration,
    ) {
        match self {
            Self::NoMetricsEndpoint => {}
            #[cfg(feature = "metrics-endpoint")]
            Self::Listen(endpoint) => endpoint.probes().claim_turn_within(wait),
        }
    }

    /// The claim loop starts: the probes read `facts` from now on. `facts`
    /// is built only for an endpoint.
    #[cfg(feature = "metrics-endpoint")]
    pub(crate) fn claiming(&self, facts: impl FnOnce() -> crate::worker::probes::ServingFacts) {
        match self {
            Self::NoMetricsEndpoint => {}
            Self::Listen(endpoint) => endpoint.probes().claiming(facts()),
        }
    }

    /// The claim loop ended; the stop drains.
    pub(crate) fn stopping(&self) {
        match self {
            Self::NoMetricsEndpoint => {}
            #[cfg(feature = "metrics-endpoint")]
            Self::Listen(endpoint) => endpoint.probes().stopping(),
        }
    }

    /// Start the thread of the endpoint: nothing with no endpoint. Never
    /// fails: a thread that the OS refuses leaves the endpoint down with
    /// its cause, and the worker runs on.
    pub(crate) fn start(&self) -> RunningEndpoint {
        match self {
            Self::NoMetricsEndpoint => RunningEndpoint::NotStarted,
            #[cfg(feature = "metrics-endpoint")]
            Self::Listen(endpoint) => serve::start_endpoint_thread(endpoint),
        }
    }
}

/// The thread of a started endpoint.
pub(crate) enum RunningEndpoint {
    /// No endpoint, or a thread that the OS refused.
    NotStarted,
    #[cfg(feature = "metrics-endpoint")]
    Thread(serve::EndpointThread),
}

impl RunningEndpoint {
    /// Stop the endpoint and wait for its thread, at most the stop bound of
    /// the thread. Never fails; a thread past the bound is left to end with
    /// the process, and the stop logs it.
    pub(crate) async fn stop(self) {
        match self {
            Self::NotStarted => {}
            #[cfg(feature = "metrics-endpoint")]
            Self::Thread(thread) => thread.stop().await,
        }
    }
}

/// The `state` of a telemetry part that is down, in the `telemetry` column:
/// the endpoint member and the handler child member write it, and the fleet
/// read counts it (`horsies.fleet.workers.telemetry_down`).
pub(crate) const TELEMETRY_PART_DOWN: &str = "down";

/// The `telemetry` column of a worker snapshot: the state of the metrics
/// endpoint (`endpoint`) and of the handler child (`handler_child`,
/// [`handler_child_json`]) of the worker. The snapshot writer adds the
/// task children whose RSS was unreadable (`children_rss`,
/// `worker_state::children_rss_json`) to [`telemetry_members`]. Pure.
///
/// The `endpoint` member:
///
/// - `{"endpoint": {"state": "off"}}`: no `--metrics-listen`.
/// - `{"endpoint": {"state": "binding", "host": .., "port": ..}}`: the first
///   bind has not answered.
/// - `{"endpoint": {"state": "serving", "host": .., "port": ..,
///   "local_address": "IP:PORT"}}`.
/// - `{"endpoint": {"state": "down", "host": .., "port": .., "cause": {"kind":
///   "bind_failed" | "thread_refused" | "runtime_refused", "detail": ..},
///   "down_since": RFC 3339}}`; a `bind_failed` cause also has
///   `failed_binds`, the failed binds in a row.
#[cfg(test)]
pub(crate) fn telemetry_json(
    endpoint: &WorkerEndpoint,
    handler_child: &HandlerChildState,
) -> serde_json::Value {
    serde_json::Value::Object(telemetry_members(endpoint, handler_child))
}

/// The members `endpoint` and `handler_child` of `telemetry_json`. Pure.
pub(crate) fn telemetry_members(
    endpoint: &WorkerEndpoint,
    handler_child: &HandlerChildState,
) -> serde_json::Map<String, serde_json::Value> {
    let endpoint = match endpoint {
        WorkerEndpoint::NoMetricsEndpoint => serde_json::json!({ "state": "off" }),
        #[cfg(feature = "metrics-endpoint")]
        WorkerEndpoint::Listen(endpoint) => endpoint.state_json(),
    };
    let mut members = serde_json::Map::new();
    let _absent = members.insert("endpoint".to_owned(), endpoint);
    let _absent = members.insert(
        "handler_child".to_owned(),
        handler_child_json(handler_child),
    );
    members
}
