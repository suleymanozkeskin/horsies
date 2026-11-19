//! `horsies metrics`: the fleet metrics of the database of a `CoreHandle`,
//! served at `GET /metrics` by the core's metrics endpoint.
//!
//! - `parse_listen_address(text)`: pure; `ListenAddress` or
//!   `ListenAddressRefused`.
//! - `prepare_fleet_metrics(handle, *, listen, queue_names,
//!   schedule_names)`: pure; `FleetMetricsServer` or `FleetLabelsRefused`.
//! - `FleetMetricsServer.run()`: the blocking run form of a service run:
//!   main thread only, the Python signal handlers run between the wait
//!   slices, and there is no total bound. It binds the address, then reads
//!   the fleet state at most once per collect interval and serves the last
//!   read until a stop request: `FleetMetricsStopped`, or
//!   `FleetMetricsFailed` when the address does not bind. A failed read is a
//!   failure gauge in the text, never an end of the run. A server runs
//!   once.
//! - `FleetMetricsServer.request_stop()`: idempotent, any thread, returns at
//!   once. A request before the run ends the run right after the bind.

use std::sync::{Arc, Mutex, PoisonError};

use horsies_core::handle::CoreHandle as LazyHandle;
use horsies_core::monitoring::fleet::{serve_fleet_metrics, FleetLabels};
use horsies_core::telemetry::declared::{
    DeclaredNames, DeclaredNamesRefused, QueueName, ScheduleName,
};
use horsies_core::telemetry::endpoint::{bind_endpoint, EndpointFailed};
use horsies_core::telemetry::listen::ListenAddress as CoreListenAddress;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use tokio::sync::watch;

use crate::bridge::service_run::{refuse_off_main_thread, run_blocking, StopSignal};
use crate::errors::refusal_error;
use crate::handle::{checked_core, CoreHandle};
use crate::process_core::pure_call_refusal;
use crate::worker::StopRequested;

/// The subject of the run, in its refusals and records.
const RUN_SUBJECT: &str = "FleetMetricsServer.run";

/// A `HOST:PORT` address of a metrics endpoint.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct ListenAddress {
    address: CoreListenAddress,
}

#[pymethods]
impl ListenAddress {
    /// The host: a name or an IP address, an IPv6 address without brackets.
    #[getter]
    fn host(&self) -> &str {
        self.address.host()
    }

    /// The port, 1 to 65535.
    #[getter]
    fn port(&self) -> u16 {
        self.address.port()
    }
}

/// A text that is not a `HOST:PORT` address. `message` names the text and
/// the rule.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ListenAddressRefused {
    pub message: String,
}

/// A queue or a schedule name that cannot be a label value: it is empty or
/// it is `(unknown)`, or a kind holds too many names. `message` names it.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct FleetLabelsRefused {
    pub message: String,
}

/// The run ended at a stop request.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct FleetMetricsStopped {}

/// The address did not bind. `message` names the address and the cause.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct FleetMetricsFailed {
    pub message: String,
}

/// What `parse_listen_address` answers.
pub(crate) enum ParsedAddress {
    Address(ListenAddress),
    Refused(ListenAddressRefused),
}

impl<'py> IntoPyObject<'py> for ParsedAddress {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Address(address) => address.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// The address of `text`: `HOST:PORT` or `[IPV6]:PORT`, the port 1 to
/// 65535. Pure: starts no runtime. The host is resolved at the bind.
#[pyfunction]
pub(crate) fn parse_listen_address(text: &str) -> ParsedAddress {
    match CoreListenAddress::parse(text) {
        Ok(address) => ParsedAddress::Address(ListenAddress { address }),
        Err(refused) => ParsedAddress::Refused(ListenAddressRefused {
            message: refused.to_string(),
        }),
    }
}

/// The labels of a server until its run takes them.
enum LabelsSlot {
    Ready(FleetLabels),
    Taken,
}

/// The fleet metrics server of one handle, not run yet.
#[pyclass(module = "horsies._core", name = "FleetMetricsServer", frozen)]
pub struct FleetMetricsServer {
    handle: Arc<LazyHandle>,
    listen: CoreListenAddress,
    labels: Mutex<LabelsSlot>,
    stop: watch::Sender<bool>,
}

/// What `prepare_fleet_metrics` answers.
pub(crate) enum Prepared {
    Server(FleetMetricsServer),
    LabelsRefused(FleetLabelsRefused),
}

impl<'py> IntoPyObject<'py> for Prepared {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Server(server) => server.into_bound_py_any(py),
            Self::LabelsRefused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// The labels of the declared names. Pure.
///
/// Errors: the refusal of `DeclaredNames::new`, as text.
pub(crate) fn fleet_labels(
    queue_names: Vec<String>,
    schedule_names: Vec<String>,
) -> Result<FleetLabels, FleetLabelsRefused> {
    let refused = |error: DeclaredNamesRefused| FleetLabelsRefused {
        message: error.to_string(),
    };
    Ok(FleetLabels {
        queues: DeclaredNames::<QueueName>::new(queue_names).map_err(refused)?,
        schedules: DeclaredNames::<ScheduleName>::new(schedule_names).map_err(refused)?,
    })
}

/// The server of the fleet metrics of the database of `handle`, on `listen`,
/// with the queue and schedule names of the app config as label values:
/// `FleetMetricsServer` or `FleetLabelsRefused`. Pure: no I/O, and it
/// starts no runtime.
///
/// Failure exits: `ForkedAfterCoreStart`, `CoreStopped`.
#[pyfunction]
#[pyo3(signature = (handle, *, listen, queue_names, schedule_names))]
pub(crate) fn prepare_fleet_metrics(
    handle: &Bound<'_, CoreHandle>,
    listen: &Bound<'_, ListenAddress>,
    queue_names: Vec<String>,
    schedule_names: Vec<String>,
) -> PyResult<Prepared> {
    if let Some(refusal) = pure_call_refusal() {
        return Err(refusal_error(refusal));
    }
    match fleet_labels(queue_names, schedule_names) {
        Ok(labels) => {
            let (stop, _initial) = watch::channel(false);
            Ok(Prepared::Server(FleetMetricsServer {
                handle: handle.get().connection().shared_handle(),
                listen: listen.get().address.clone(),
                labels: Mutex::new(LabelsSlot::Ready(labels)),
                stop,
            }))
        }
        Err(refused) => Ok(Prepared::LabelsRefused(refused)),
    }
}

/// How a run ended, as a record.
enum RunOutcome {
    Stopped(FleetMetricsStopped),
    Failed(FleetMetricsFailed),
}

impl<'py> IntoPyObject<'py> for RunOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Stopped(stopped) => stopped.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

#[pymethods]
impl FleetMetricsServer {
    /// Bind the address and serve the fleet metrics until a stop request:
    /// `FleetMetricsStopped` or `FleetMetricsFailed`. Main thread only.
    /// Raises `NotMainThread` off the main thread, `RuntimeError` for a
    /// second run, and after the run ended the exception of a signal
    /// handler.
    fn run(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Checked before the core starts and before the labels are taken,
        // so a refused call changes nothing.
        refuse_off_main_thread(py, RUN_SUBJECT)?;
        let core = checked_core(py)?;
        let labels = {
            let mut slot = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
            match std::mem::replace(&mut *slot, LabelsSlot::Taken) {
                LabelsSlot::Ready(labels) => labels,
                LabelsSlot::Taken => {
                    return Err(PyRuntimeError::new_err(format!(
                        "{RUN_SUBJECT}: this server ran already"
                    )))
                }
            }
        };
        let signal = self.stop.clone();
        run_blocking(
            py,
            core,
            RUN_SUBJECT,
            StopSignal::new(move || {
                let _was = signal.send_replace(true);
            }),
            run_operation(
                Arc::clone(&self.handle),
                self.listen.clone(),
                labels,
                self.stop.subscribe(),
            ),
        )
    }

    /// Ask the run to stop: the requests in flight end, then the run
    /// answers `FleetMetricsStopped`. Idempotent; any thread; returns at
    /// once.
    fn request_stop(&self) -> StopRequested {
        let _was = self.stop.send_replace(true);
        StopRequested {}
    }
}

/// `run`: bind `listen`, then serve until `stopped` turns true.
async fn run_operation(
    handle: Arc<LazyHandle>,
    listen: CoreListenAddress,
    labels: FleetLabels,
    mut stopped: watch::Receiver<bool>,
) -> RunOutcome {
    let endpoint = match bind_endpoint(&listen).await {
        Ok(endpoint) => endpoint,
        Err(failed) => return failed_run(&failed),
    };
    let stop = async move {
        // An error means every sender is gone: the server object is gone,
        // and no stop can come any more; the run ends.
        let _stopped_or_gone = stopped.wait_for(|stop| *stop).await.is_ok();
    };
    serve_fleet_metrics(endpoint, handle, labels, stop).await;
    RunOutcome::Stopped(FleetMetricsStopped {})
}

fn failed_run(failed: &EndpointFailed) -> RunOutcome {
    RunOutcome::Failed(FleetMetricsFailed {
        message: failed.to_string(),
    })
}
