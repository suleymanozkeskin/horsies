//! `prepare_worker` and `PreparedWorker`: the worker of one worker manifest,
//! which the core runs on the main thread (`horsies worker`).
//!
//! - `prepare_worker(manifest_json, *, host)`: pure; parses the manifest and
//!   picks and checks the profile. It starts no runtime. A manifest that the
//!   core refuses raises `ManifestRefused`, a defect of the client; a
//!   refused profile answers `WorkerProfileRefused` with Python's report
//!   lines.
//! - `PreparedWorker.run()`: the blocking run form of a service run: main
//!   thread only, the Python signal handlers run between the wait slices,
//!   and there is no total bound. It builds the worker and runs it until a
//!   stop request or a failure, then answers `WorkerStopped`,
//!   `WorkerBootFailed` or `WorkerFailed`. A prepared worker runs once.
//! - `PreparedWorker.request_stop()`: idempotent, any thread, returns at
//!   once. A request before the run ends the run after its start.
//! - `host.on_started()` runs once per run on the callback thread, after the
//!   start and before the first claim. An exception from it is logged at
//!   ERROR on `horsies.bridge`, and the worker runs on.
//!
//! The workflow builds of the worker (`build_with` of a sub-workflow node)
//! use an empty registry until the build callback of the workflow switch
//! replaces it.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use horsies_core::worker_manifest::parse_worker_manifest;
use horsies_core::{
    run_end_of, BuilderHookName, OnStarted, RunPhase, Worker, WorkerPlan, WorkerPlanRefused,
    WorkerRunEnd, WorkerStop, WorkflowSpecRegistry,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::bridge::callback_thread::python_call;
use crate::bridge::service_run::{refuse_off_main_thread, run_blocking, StopSignal};
use crate::errors::{refusal_error, ManifestRefused};
use crate::handle::checked_core;
use crate::logging::RecordLevel;
use crate::process_core::{pure_call_refusal, Core};

/// The subject of the run, in its refusals and records.
const RUN_SUBJECT: &str = "PreparedWorker.run";

/// The subject of the start hook, in its failure records.
const ON_STARTED_SUBJECT: &str = "WorkerHost.on_started";

/// The name of the build hook of a Python workflow definition, as the error
/// texts of a child start write it.
const PYTHON_BUILD_HOOK: BuilderHookName = BuilderHookName::new("build_with()");

/// The profile of the manifest is refused: the profile set fails its checks,
/// or the start names no profile or an unknown one. `refusals` are the
/// lines of Python's report.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkerProfileRefused {
    pub refusals: Vec<String>,
}

/// The run ended at a stop request.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct WorkerStopped {}

/// The start of the worker failed; it claimed nothing. `cause` is the wire
/// value of the core's boot cause; `message` names the subject and cause.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkerBootFailed {
    pub cause: String,
    pub message: String,
}

/// The worker was ready, then failed; its running tasks drained first.
/// `cause` is the wire value of the core's failure cause.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkerFailed {
    pub cause: String,
    pub message: String,
}

/// The stop request is recorded.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct StopRequested {}

/// The plan of a prepared worker until its run takes it.
enum PlanSlot {
    Ready(Box<WorkerPlan>),
    Taken,
}

/// The worker of one worker manifest, not run yet.
#[pyclass(module = "horsies._core", name = "PreparedWorker", frozen)]
pub struct PreparedWorker {
    plan: Mutex<PlanSlot>,
    host: Arc<Py<PyAny>>,
    stop: WorkerStop,
}

/// What `prepare_worker` answers.
pub(crate) enum Prepared {
    Worker(PreparedWorker),
    ProfileRefused(WorkerProfileRefused),
}

impl<'py> IntoPyObject<'py> for Prepared {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Worker(worker) => worker.into_bound_py_any(py),
            Self::ProfileRefused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// How a run ended, as a record.
enum RunOutcome {
    Stopped(WorkerStopped),
    BootFailed(WorkerBootFailed),
    Failed(WorkerFailed),
}

impl<'py> IntoPyObject<'py> for RunOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Stopped(stopped) => stopped.into_bound_py_any(py),
            Self::BootFailed(failed) => failed.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// Parse a worker manifest and pick its profile: `PreparedWorker` or
/// `WorkerProfileRefused`. Pure: no I/O, and it starts no runtime.
///
/// Failure exits: `ManifestRefused` (a manifest field, or connection
/// settings that the broker config check refuses), `ForkedAfterCoreStart`,
/// `CoreStopped`.
#[pyfunction]
#[pyo3(signature = (manifest_json, *, host))]
pub(crate) fn prepare_worker(manifest_json: &str, host: Bound<'_, PyAny>) -> PyResult<Prepared> {
    if let Some(refusal) = pure_call_refusal() {
        return Err(refusal_error(refusal));
    }
    let manifest = parse_worker_manifest(manifest_json)
        .map_err(|refused| ManifestRefused::new_err((refused.field_path, refused.cause)))?;
    match WorkerPlan::of_manifest(&manifest) {
        Ok(plan) => Ok(Prepared::Worker(PreparedWorker {
            plan: Mutex::new(PlanSlot::Ready(Box::new(plan))),
            host: Arc::new(host.unbind()),
            stop: WorkerStop::new(),
        })),
        Err(WorkerPlanRefused::Profile(refused)) => {
            Ok(Prepared::ProfileRefused(WorkerProfileRefused {
                refusals: refused.errors().iter().map(ToString::to_string).collect(),
            }))
        }
        Err(WorkerPlanRefused::ConnectionSettings { cause }) => {
            Err(ManifestRefused::new_err(("database".to_owned(), cause)))
        }
    }
}

#[pymethods]
impl PreparedWorker {
    /// Build the worker and run it until a stop request or a failure:
    /// `WorkerStopped`, `WorkerBootFailed` or `WorkerFailed`. Main thread
    /// only. Raises `NotMainThread` off the main thread, `RuntimeError` for
    /// a second run, and the exception of a signal handler after the run
    /// ended.
    fn run(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Checked before the core starts and before the plan is taken, so a
        // refused call changes nothing.
        refuse_off_main_thread(py, RUN_SUBJECT)?;
        let core = checked_core(py)?;
        let plan = {
            let mut slot = self.plan.lock().unwrap_or_else(PoisonError::into_inner);
            match std::mem::replace(&mut *slot, PlanSlot::Taken) {
                PlanSlot::Ready(plan) => plan,
                PlanSlot::Taken => {
                    return Err(PyRuntimeError::new_err(format!(
                        "{RUN_SUBJECT}: this worker ran already"
                    )))
                }
            }
        };
        let signal = self.stop.clone();
        run_blocking(
            py,
            core,
            RUN_SUBJECT,
            StopSignal::new(move || signal.request_stop()),
            run_operation(core, *plan, Arc::clone(&self.host), self.stop.clone()),
        )
    }

    /// Ask the run to stop. The claim loop ends, the children drain within
    /// the profile's drain timeout, and the run answers `WorkerStopped`.
    /// Idempotent; any thread; returns at once.
    fn request_stop(&self) -> StopRequested {
        self.stop.request_stop();
        StopRequested {}
    }
}

/// `run`: build the worker of `plan` and run it to its end.
async fn run_operation(
    core: &'static Core,
    plan: WorkerPlan,
    host: Arc<Py<PyAny>>,
    stop: WorkerStop,
) -> RunOutcome {
    let registry = Arc::new(WorkflowSpecRegistry::new(PYTHON_BUILD_HOOK));
    let end = match Worker::from_plan(plan, registry).await {
        Ok(worker) => {
            worker
                .with_stop(&stop)
                .with_on_started(on_started_hook(core, host))
                .run_to_end()
                .await
        }
        Err(error) => {
            core.log().push_bridge_record(
                RecordLevel::Error,
                format!("{RUN_SUBJECT}: the worker did not build: {error}"),
            );
            run_end_of(Err(error), RunPhase::Starting)
        }
    };
    outcome_of(end)
}

/// The record of a run end. Pure.
fn outcome_of(end: WorkerRunEnd) -> RunOutcome {
    match end {
        WorkerRunEnd::Stopped => RunOutcome::Stopped(WorkerStopped {}),
        WorkerRunEnd::BootFailed { cause, message } => RunOutcome::BootFailed(WorkerBootFailed {
            cause: cause.as_str().to_owned(),
            message,
        }),
        WorkerRunEnd::Failed { cause, message } => RunOutcome::Failed(WorkerFailed {
            cause: cause.as_str().to_owned(),
            message,
        }),
    }
}

/// The start hook of the worker: `host.on_started()` on the callback thread.
/// A failure is logged at ERROR and the worker runs on, as a failed heap
/// freeze does in Python.
fn on_started_hook(core: &'static Core, host: Arc<Py<PyAny>>) -> OnStarted {
    OnStarted::Call(Arc::new(
        move || -> Pin<Box<dyn Future<Output = ()> + Send>> {
            let host = Arc::clone(&host);
            Box::pin(async move {
                let call = python_call(move |py: Python<'_>| -> PyResult<()> {
                    host.bind(py).call_method0("on_started").map(drop)
                });
                if let Err(failure) = core
                    .callbacks()
                    .call(ON_STARTED_SUBJECT.to_owned(), call)
                    .await
                {
                    core.log().push_bridge_record(
                        RecordLevel::Error,
                        format!("{failure}; the worker runs on"),
                    );
                }
            })
        },
    ))
}
