//! The thread of the metrics endpoint of a worker: its bind with the slot
//! backoff, its router, its state, and its stop.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use chrono::{DateTime, SecondsFormat, Utc};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::RunningEndpoint;
use crate::telemetry::endpoint::{
    bind_endpoint, metrics_response, serve_endpoint, ENDPOINT_REQUEST_TIMEOUT,
};
use crate::telemetry::exposition::render;
use crate::telemetry::listen::ListenAddress;
use crate::telemetry::registry::Registry;
use crate::worker::executor::supervisor::boot::slot_down_delay;
use crate::worker::probes::{ProbeStatus, WorkerProbes};
use crate::worker::thread_dispatch::with_starter_dispatch;

/// The name of the thread of the endpoint.
const ENDPOINT_THREAD_NAME: &str = "horsies-metrics-endpoint";

/// The content type of a probe answer.
const PROBE_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// The longest the stop of a worker waits for its endpoint thread: the
/// server gives its connections [`ENDPOINT_REQUEST_TIMEOUT`] to end, then
/// the runtime of the thread ends.
const ENDPOINT_THREAD_STOP_BOUND: Duration =
    ENDPOINT_REQUEST_TIMEOUT.saturating_add(Duration::from_secs(5));

/// The endpoint of `--metrics-listen`: its address, the registry it serves,
/// the state that the probes read, and the state of the endpoint.
pub(crate) struct ListenEndpoint {
    address: ListenAddress,
    registry: Arc<Registry>,
    probes: WorkerProbes,
    state: EndpointCell,
}

impl ListenEndpoint {
    /// The endpoint of `address` that serves `registry`, before its start.
    pub(crate) fn new(address: ListenAddress, registry: Arc<Registry>) -> Self {
        Self {
            address,
            registry,
            probes: WorkerProbes::starting(),
            state: EndpointCell::new(),
        }
    }

    pub(crate) fn probes(&self) -> &WorkerProbes {
        &self.probes
    }

    /// The state of the endpoint now.
    pub(crate) fn state(&self) -> EndpointState {
        self.state.read()
    }

    /// The endpoint member of the `telemetry` column
    /// ([`super::telemetry_json`]). Pure apart from the read of the state.
    pub(crate) fn state_json(&self) -> serde_json::Value {
        let host = self.address.host();
        let port = self.address.port();
        match self.state() {
            EndpointState::Binding => {
                serde_json::json!({ "state": "binding", "host": host, "port": port })
            }
            EndpointState::Serving { local } => serde_json::json!({
                "state": "serving",
                "host": host,
                "port": port,
                "local_address": local.to_string(),
            }),
            EndpointState::Down(down) => serde_json::json!({
                "state": "down",
                "host": host,
                "port": port,
                "cause": down.cause.json(),
                "down_since": down.since.to_rfc3339_opts(SecondsFormat::Micros, false),
            }),
        }
    }
}

/// The state of the endpoint, as the snapshot of its worker shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EndpointState {
    /// The thread binds the address; the first bind has not answered.
    Binding,
    /// The address is bound at `local`; the endpoint serves.
    Serving { local: SocketAddr },
    /// The endpoint does not serve.
    Down(EndpointDown),
}

/// An endpoint that does not serve: why, and since when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EndpointDown {
    pub(crate) cause: EndpointDownCause,
    /// The first failure of this down state.
    pub(crate) since: DateTime<Utc>,
}

/// Why an endpoint does not serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EndpointDownCause {
    /// The last bind failed (the port is in use, the host does not resolve,
    /// or the process may not bind it); `failed_binds` failed in a row. The
    /// thread binds again after the slot backoff of that count.
    BindFailed {
        detail: String,
        failed_binds: NonZeroU32,
    },
    /// The OS refused the thread of the endpoint. Not retried: the
    /// endpoint stays down until the worker restarts.
    ThreadRefused { detail: String },
    /// The runtime of the endpoint thread did not build. Not retried.
    RuntimeRefused { detail: String },
}

impl EndpointDownCause {
    /// The `cause` member of a down endpoint. Pure.
    fn json(&self) -> serde_json::Value {
        match self {
            Self::BindFailed {
                detail,
                failed_binds,
            } => serde_json::json!({
                "kind": "bind_failed",
                "detail": detail,
                "failed_binds": failed_binds.get(),
            }),
            Self::ThreadRefused { detail } => {
                serde_json::json!({ "kind": "thread_refused", "detail": detail })
            }
            Self::RuntimeRefused { detail } => {
                serde_json::json!({ "kind": "runtime_refused", "detail": detail })
            }
        }
    }
}

/// The state of an endpoint, shared by its thread and the snapshot loop.
struct EndpointCell(Mutex<EndpointState>);

impl EndpointCell {
    fn new() -> Self {
        Self(Mutex::new(EndpointState::Binding))
    }

    fn read(&self) -> EndpointState {
        // A poisoned lock means a panic while the state was written; the
        // state is still a whole value.
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn serve(&self, local: SocketAddr) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = EndpointState::Serving { local };
    }

    /// The endpoint is down for `cause` at `now`. A state that is down
    /// already keeps its `since`.
    fn down(&self, cause: EndpointDownCause, now: DateTime<Utc>) {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let since = match &*state {
            EndpointState::Down(down) => down.since,
            EndpointState::Binding | EndpointState::Serving { .. } => now,
        };
        *state = EndpointState::Down(EndpointDown { cause, since });
    }
}

/// A started endpoint thread.
pub(crate) struct EndpointThread {
    stop: CancellationToken,
    /// Sent by the thread just before it ends; dropped unsent by a panic.
    ended: oneshot::Receiver<()>,
    thread: std::thread::JoinHandle<()>,
}

/// Start the thread of `endpoint`. A thread that the OS refuses leaves the
/// endpoint down with [`EndpointDownCause::ThreadRefused`], with one error
/// line, and nothing runs.
pub(crate) fn start_endpoint_thread(endpoint: &Arc<ListenEndpoint>) -> RunningEndpoint {
    let stop = CancellationToken::new();
    let (ended_sender, ended) = oneshot::channel::<()>();
    let body = {
        let endpoint = Arc::clone(endpoint);
        let stop = stop.clone();
        move || {
            run_endpoint_thread(&endpoint, stop);
            // The receiver is gone when the stop no longer waits.
            let _sent = ended_sender.send(());
        }
    };
    let spawned = std::thread::Builder::new()
        .name(ENDPOINT_THREAD_NAME.to_owned())
        .spawn(with_starter_dispatch(body));
    match spawned {
        Ok(thread) => RunningEndpoint::Thread(EndpointThread {
            stop,
            ended,
            thread,
        }),
        Err(cause) => {
            tracing::error!(
                address = %endpoint.address,
                %cause,
                "the worker metrics endpoint on {} did not start: the OS refused its thread: \
                 {cause}; the worker runs without it",
                endpoint.address,
            );
            endpoint.state.down(
                EndpointDownCause::ThreadRefused {
                    detail: cause.to_string(),
                },
                Utc::now(),
            );
            RunningEndpoint::NotStarted
        }
    }
}

impl EndpointThread {
    /// Stop the server and the bind retry, then wait at most
    /// [`ENDPOINT_THREAD_STOP_BOUND`] for the thread to end.
    pub(crate) async fn stop(self) {
        let EndpointThread {
            stop,
            ended,
            thread,
        } = self;
        stop.cancel();
        match tokio::time::timeout(ENDPOINT_THREAD_STOP_BOUND, ended).await {
            // Sent or dropped: the thread body ended, by its end or by a
            // panic; the join tells which.
            Ok(_ended_or_dropped) => {
                match tokio::task::spawn_blocking(move || thread.join()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_panic)) => {
                        tracing::error!("the worker metrics endpoint thread panicked");
                    }
                    Err(failure) => tracing::error!(
                        %failure,
                        "joining the worker metrics endpoint thread failed: {failure}",
                    ),
                }
            }
            Err(_elapsed) => tracing::warn!(
                bound_s = ENDPOINT_THREAD_STOP_BOUND.as_secs_f64(),
                "the worker metrics endpoint thread did not end within {}s of the stop; the \
                 worker stops without waiting for it",
                ENDPOINT_THREAD_STOP_BOUND.as_secs_f64(),
            ),
        }
    }
}

/// The body of the endpoint thread: its runtime, then the bind and the
/// server until `stop`.
fn run_endpoint_thread(endpoint: &Arc<ListenEndpoint>, stop: CancellationToken) {
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(bind_and_serve(endpoint, stop)),
        Err(cause) => {
            tracing::error!(
                address = %endpoint.address,
                %cause,
                "the worker metrics endpoint on {} did not start: its runtime did not build: \
                 {cause}; the worker runs without it",
                endpoint.address,
            );
            endpoint.state.down(
                EndpointDownCause::RuntimeRefused {
                    detail: cause.to_string(),
                },
                Utc::now(),
            );
        }
    }
}

/// Bind the address of `endpoint`; after a failed bind, wait the slot
/// backoff of the failures in a row and bind again. Once bound, serve until
/// `stop`. A stop during a bind or a wait ends at once.
async fn bind_and_serve(endpoint: &Arc<ListenEndpoint>, stop: CancellationToken) {
    let mut earlier_failures: u32 = 0;
    loop {
        let bound = tokio::select! {
            () = stop.cancelled() => return,
            bound = bind_endpoint(&endpoint.address) => bound,
        };
        match bound {
            Ok(bound) => {
                let local = bound.local_address();
                endpoint.state.serve(local);
                tracing::info!(
                    endpoint = %local,
                    failed_binds = earlier_failures,
                    "serving the worker metrics on http://{local}/metrics, /livez and /readyz",
                );
                serve_endpoint(
                    bound,
                    worker_router(Arc::clone(endpoint)),
                    stop.cancelled_owned(),
                )
                .await;
                return;
            }
            Err(failed) => {
                let failed_binds = NonZeroU32::MIN.saturating_add(earlier_failures);
                earlier_failures = failed_binds.get();
                let wait = slot_down_delay(failed_binds);
                tracing::warn!(
                    address = %endpoint.address,
                    failed_binds = failed_binds.get(),
                    wait_s = wait.as_secs_f64(),
                    "{failed}; the worker runs on, and the endpoint binds again in {}s",
                    wait.as_secs_f64(),
                );
                endpoint.state.down(
                    EndpointDownCause::BindFailed {
                        detail: failed.to_string(),
                        failed_binds,
                    },
                    Utc::now(),
                );
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
            }
        }
    }
}

/// The routes of the worker endpoint: `GET /metrics`, `GET /livez`,
/// `GET /readyz` (`HEAD` too). Another path answers 404, another method 405.
fn worker_router(endpoint: Arc<ListenEndpoint>) -> Router {
    Router::new()
        .route("/metrics", get(serve_metrics))
        .route("/livez", get(serve_livez))
        .route("/readyz", get(serve_readyz))
        .with_state(endpoint)
}

async fn serve_metrics(State(endpoint): State<Arc<ListenEndpoint>>) -> Response {
    metrics_response(render(&endpoint.registry))
}

async fn serve_livez(State(endpoint): State<Arc<ListenEndpoint>>) -> Response {
    let answer = endpoint.probes.live();
    probe_response(answer.status(), answer.text())
}

async fn serve_readyz(State(endpoint): State<Arc<ListenEndpoint>>) -> Response {
    let answer = endpoint.probes.ready();
    probe_response(answer.status(), answer.text())
}

/// A probe answer: 200 for a pass, 503 for a fail, with one line of text.
fn probe_response(status: ProbeStatus, text: String) -> Response {
    let code = match status {
        ProbeStatus::Pass => StatusCode::OK,
        ProbeStatus::Fail => StatusCode::SERVICE_UNAVAILABLE,
    };
    (code, [(CONTENT_TYPE, PROBE_CONTENT_TYPE)], text).into_response()
}

#[cfg(test)]
mod tests;
