//! The `metrics` command: the fleet metrics of the database of a
//! `CoreHandle`, served at `GET /metrics` by the core's metrics endpoint.
//!
//! - `parseMetricsListenAddress(text)` (sync, pure): the `HOST:PORT`
//!   address, or the refusal that names the text and the rule.
//! - `prepareFleetMetrics(handle, settings)` (sync, pure): a server that
//!   has not run. It reads through its own lazy handle of the manifest of
//!   `handle`, with the schema mode `ASSUME_INITIALIZED`: no schema step and
//!   no listener, as the Rust client's `metrics` command.
//! - `FleetMetricsServer.run()`: a service run. It logs one WARNING line
//!   that names each queue or schedule name that the labels do not declare,
//!   binds the address, then reads the fleet state at most once per collect
//!   interval and serves the last read until a stop request. It ends
//!   `stopped`, or `failed` when the address does not bind. A failed read
//!   is a failure gauge in the text, never an end of the run. A server runs
//!   once.
//! - `FleetMetricsServer.requestStop()`: idempotent, returns at once. A
//!   request before the run ends the run right after the bind.

use std::sync::{Arc, Mutex, PoisonError};

use horsies_core::handle::CoreConnection as LazyHandle;
use horsies_core::manifest::SchemaMode;
use horsies_core::monitoring::fleet::{serve_fleet_metrics, FleetLabels};
use horsies_core::telemetry::declared::{warn_folded_names, Folded};
use horsies_core::telemetry::endpoint::bind_endpoint;
use horsies_core::telemetry::listen::ListenAddress;
use horsies_core::ClientFamily;
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;
use tokio::sync::watch;

use crate::call::guard::sync_call;
use crate::call::service_run::spawn_service_run;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::facades::web_handler::{fleet_labels, thrown_promise};
use crate::handle::core_handle::CoreHandle;
use crate::process_core::runtime::pure_call_refusal;
use crate::values::boundary::BoundaryRefusal;

/// The subject of the run, in its refusals and records.
const RUN_SUBJECT: &str = "FleetMetricsServer.run";

/// The answer of `parseMetricsListenAddress`.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetricsListenAddressCheck {
    /// The host (a name or an IP address; an IPv6 address without
    /// brackets) and the port, 1 to 65535. The host is resolved at the bind.
    Address { host: String, port: u32 },
    /// `message` names the text and the rule that it breaks.
    Refused { message: String },
}

/// The address of `text`: `HOST:PORT` or `[IPV6]:PORT`, a host of 1 to 253
/// bytes, a port of 1 to 65535. Pure: starts nothing.
#[napi(ts_return_type = "NativeEnvelope<MetricsListenAddressCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn parse_metrics_listen_address(text: String) -> Envelope<MetricsListenAddressCheck> {
    sync_call("parseMetricsListenAddress", || {
        Envelope::Value(match ListenAddress::parse(&text) {
            Ok(address) => MetricsListenAddressCheck::Address {
                host: address.host().to_owned(),
                port: u32::from(address.port()),
            },
            Err(refused) => MetricsListenAddressCheck::Refused {
                message: refused.to_string(),
            },
        })
    })
}

/// The settings of a fleet metrics server.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetMetricsSettings {
    /// The `HOST:PORT` address, which the package checked with
    /// `parseMetricsListenAddress`.
    pub listen: String,
    /// The queue names of the app config: the `queue` label values.
    pub queue_names: Vec<String>,
    /// The schedule names of the app config: the `schedule` label values.
    pub schedule_names: Vec<String>,
}

/// The labels of a server, and the names that they do not declare, until
/// its run takes them.
enum LabelsSlot {
    Ready(Folded<FleetLabels>),
    Taken,
}

/// Whether a stop of the run was asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopRequest {
    NotAsked,
    Asked,
}

/// The fleet metrics server of one handle, not run yet.
#[napi]
pub struct FleetMetricsServer {
    handle: Arc<LazyHandle>,
    listen: ListenAddress,
    labels: Mutex<LabelsSlot>,
    stop: watch::Sender<StopRequest>,
}

/// The server of the fleet metrics of the database of `handle`. A name that
/// cannot be a label value of its own is not declared; the run names it in
/// one WARNING line. Pure: no I/O, and it starts nothing. The fork and stop
/// rules apply, as for every core call; an address that the package should
/// have refused throws `CLIENT_DEFECT`.
#[napi(ts_return_type = "NativeEnvelope<FleetMetricsServer>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn prepare_fleet_metrics(
    handle: &CoreHandle,
    settings: FleetMetricsSettings,
) -> Envelope<FleetMetricsServer> {
    sync_call("prepareFleetMetrics", || {
        if let Some(refusal) = pure_call_refusal() {
            return Envelope::Thrown(refusal.thrown());
        }
        match server_of(handle.connection().lazy_handle(), settings) {
            Ok(server) => Envelope::Value(server),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// The server of `settings`, with its own lazy handle of the manifest and
/// the pool profile of `lazy`. Pure.
///
/// Failure exits: the refusal of an address that is not `HOST:PORT`.
fn server_of(
    lazy: &LazyHandle,
    settings: FleetMetricsSettings,
) -> Result<FleetMetricsServer, BoundaryRefusal> {
    let listen = ListenAddress::parse(&settings.listen)
        .map_err(|refused| BoundaryRefusal::new("listen", &refused.to_string()))?;
    let mut manifest = lazy.manifest().clone();
    manifest.schema = SchemaMode::AssumeInitialized;
    let (stop, _initial) = watch::channel(StopRequest::NotAsked);
    Ok(FleetMetricsServer {
        handle: Arc::new(LazyHandle::new(
            manifest,
            lazy.pool_profile(),
            ClientFamily::TypeScript,
        )),
        listen,
        labels: Mutex::new(LabelsSlot::Ready(fleet_labels(
            settings.queue_names,
            settings.schedule_names,
        ))),
        stop,
    })
}

/// How a run ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetMetricsRunEnd {
    /// The run ended at a stop request.
    Stopped,
    /// The address did not bind. `message` names the address and the cause.
    Failed { message: String },
}

#[napi]
impl FleetMetricsServer {
    /// Bind the address and serve the fleet metrics until a stop request.
    /// A second run throws `CLIENT_DEFECT`.
    #[napi(ts_return_type = "Promise<NativeEnvelope<FleetMetricsRunEnd>>")]
    pub fn run<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let labels = {
            let mut slot = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::replace(&mut *slot, LabelsSlot::Taken)
        };
        let labels = match labels {
            LabelsSlot::Ready(labels) => labels,
            LabelsSlot::Taken => {
                return thrown_promise::<FleetMetricsRunEnd>(
                    env,
                    NativeThrown::ClientDefect {
                        subject: RUN_SUBJECT.to_owned(),
                        rule: "a fleet metrics server runs once".to_owned(),
                    },
                )
            }
        };
        let handle = Arc::clone(&self.handle);
        let listen = self.listen.clone();
        let stopped = self.stop.subscribe();
        spawn_service_run(env, RUN_SUBJECT, move |_core| {
            run_operation(handle, listen, labels, stopped)
        })
    }

    /// Ask the run to stop: the requests in flight end, then the run ends
    /// `stopped`. Idempotent; returns at once.
    #[napi(ts_return_type = "NativeEnvelope<undefined>")]
    pub fn request_stop(&self) -> Envelope<()> {
        sync_call("FleetMetricsServer.requestStop", || {
            let _previous = self.stop.send_replace(StopRequest::Asked);
            Envelope::Value(())
        })
    }
}

/// `run`: log the names that the labels do not declare, bind `listen`, then
/// serve until a stop request.
async fn run_operation(
    handle: Arc<LazyHandle>,
    listen: ListenAddress,
    labels: Folded<FleetLabels>,
    mut stopped: watch::Receiver<StopRequest>,
) -> FleetMetricsRunEnd {
    let Folded {
        declared: labels,
        folded,
    } = labels;
    warn_folded_names(&"the fleet metrics", &folded);
    let endpoint = match bind_endpoint(&listen).await {
        Ok(endpoint) => endpoint,
        Err(failed) => {
            return FleetMetricsRunEnd::Failed {
                message: failed.to_string(),
            }
        }
    };
    let stop = async move {
        // An error means every sender is gone: the server object is gone,
        // and no stop can come any more; the run ends.
        let _asked_or_gone = stopped
            .wait_for(|request| *request == StopRequest::Asked)
            .await
            .is_ok();
    };
    serve_fleet_metrics(endpoint, handle, labels, stop).await;
    FleetMetricsRunEnd::Stopped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facades::web_handler::TEST_MANIFEST;

    #[test]
    fn a_host_and_port_text_is_an_address() {
        assert_eq!(
            parse_metrics_listen_address("127.0.0.1:9100".to_owned()),
            Envelope::Value(MetricsListenAddressCheck::Address {
                host: "127.0.0.1".to_owned(),
                port: 9100,
            })
        );
        assert_eq!(
            parse_metrics_listen_address("[::1]:9100".to_owned()),
            Envelope::Value(MetricsListenAddressCheck::Address {
                host: "::1".to_owned(),
                port: 9100,
            })
        );
    }

    #[test]
    fn a_text_that_breaks_a_rule_is_refused_with_the_rule() {
        for (text, rule) in [
            ("127.0.0.1", "is not HOST:PORT"),
            (":9100", "has no host"),
            ("127.0.0.1:0", "is not a number from 1 to 65535"),
            ("::1:9100", "in brackets"),
        ] {
            match parse_metrics_listen_address(text.to_owned()) {
                Envelope::Value(MetricsListenAddressCheck::Refused { message }) => {
                    assert!(message.contains(rule), "{text}: {message}");
                }
                other => panic!("{text} is refused, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_server_reads_through_its_own_handle_with_no_schema_step() {
        let manifest = horsies_core::manifest::parse_producer_manifest(TEST_MANIFEST)
            .expect("a complete manifest");
        let lazy = LazyHandle::new(
            manifest,
            horsies_core::PoolProfile::Producer,
            ClientFamily::TypeScript,
        );
        let server = server_of(
            &lazy,
            FleetMetricsSettings {
                listen: "127.0.0.1:9100".to_owned(),
                queue_names: vec!["default".to_owned()],
                schedule_names: vec![],
            },
        )
        .unwrap_or_else(|refusal| panic!("an address in bounds, got {refusal:?}"));
        assert_eq!(server.handle.schema_mode(), SchemaMode::AssumeInitialized);
        assert_eq!(server.handle.family(), ClientFamily::TypeScript);
        assert_eq!(server.listen.port(), 9100);
    }

    #[test]
    fn an_address_that_the_package_should_have_refused_is_a_boundary_refusal() {
        let manifest = horsies_core::manifest::parse_producer_manifest(TEST_MANIFEST)
            .expect("a complete manifest");
        let lazy = LazyHandle::new(
            manifest,
            horsies_core::PoolProfile::Producer,
            ClientFamily::TypeScript,
        );
        let refused = server_of(
            &lazy,
            FleetMetricsSettings {
                listen: "no port".to_owned(),
                queue_names: vec![],
                schedule_names: vec![],
            },
        )
        .err()
        .map(|refusal| refusal.subject);
        assert_eq!(refused.as_deref(), Some("listen"));
    }
}
