//! `CoreScheduler`: the scheduler of one scheduler manifest, on the broker
//! of a `CoreHandle` (Python `Scheduler`, `core/scheduler/service.py`).
//!
//! - `start_async`: connect and install the schema once per handle, as
//!   Python's `start()`, then the core start: the start checks, the client
//!   family check and the state start. A start that completed is not run
//!   again: a later `start_async` answers `SchedulerStarted` at once.
//! - `tick_async`: one tick of a started scheduler: the due read, the
//!   existence heal on its cadence, one fire per due schedule.
//! - `run_async`: the start when none completed, then one tick at once and
//!   one per check interval until a stop request. A started scheduler with
//!   nothing to fire (the master switch off or no schedule) waits for the
//!   stop request, as Python's loop does. A cancel of the future asks for
//!   the stop; the run ends at its next tick boundary.
//! - `request_stop`: ask the run to stop. Pure, idempotent, any thread. A
//!   request before the run ends the run after its start.
//!
//! One call of a scheduler runs at a time: a call while another call of the
//! same scheduler is in flight raises `RuntimeError`, as does a tick before
//! a completed start or after the end of a run.

use std::sync::Arc;

use horsies_core::scheduler_manifest::{parse_scheduler_manifest, SchedulerManifest};
use horsies_core::{
    BrokerError, SchedulerService, SchedulerStartError, SchedulerStarted as StartedStage,
    SchedulerStop, SchedulerTickError,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::bridge::completion_queue::{call_async, call_service_async};
use crate::bridge::service_run::StopSignal;
use crate::bridge::CallKind;
use crate::errors::ManifestRefused;
use crate::handle::{checked_core, BrokerConnection, CoreHandle};

/// Where the scheduler of a `CoreScheduler` is.
enum Stage {
    /// No start completed: none ran, or the last one was refused or failed.
    NotStarted,
    /// The start completed and found nothing to fire: the master switch is
    /// off or no schedule is configured.
    Idle,
    /// The start completed; the scheduler ticks.
    Active(horsies_core::ActiveScheduler),
    /// A run took the scheduler and ended at a stop request.
    RunEnded,
}

/// The scheduler of one scheduler manifest.
#[pyclass(module = "horsies._core", name = "CoreScheduler", frozen)]
pub struct CoreScheduler {
    connection: BrokerConnection,
    manifest: Arc<SchedulerManifest>,
    stop: SchedulerStop,
    stage: Arc<Mutex<Stage>>,
}

impl CoreScheduler {
    /// The stage, held for one call. `subject` names the call in the error.
    ///
    /// Errors: `RuntimeError` while another call of this scheduler is in
    /// flight.
    fn lease(&self, subject: &str) -> PyResult<OwnedMutexGuard<Stage>> {
        Arc::clone(&self.stage)
            .try_lock_owned()
            .map_err(|_in_flight| {
                PyRuntimeError::new_err(format!(
                    "{subject}: another call of this scheduler is in flight"
                ))
            })
    }
}

#[pymethods]
impl CoreScheduler {
    /// Build the scheduler of a scheduler manifest's JSON text, on the
    /// broker of `handle`. Starts nothing and connects to nothing. A refused
    /// manifest raises `ManifestRefused`.
    #[new]
    #[pyo3(signature = (handle, manifest_json))]
    fn new(handle: &Bound<'_, CoreHandle>, manifest_json: &str) -> PyResult<Self> {
        let manifest = parse_scheduler_manifest(manifest_json)
            .map_err(|refused| ManifestRefused::new_err((refused.field_path, refused.cause)))?;
        Ok(Self {
            connection: handle.get().connection(),
            manifest: Arc::new(manifest),
            stop: SchedulerStop::new(),
            stage: Arc::new(Mutex::new(Stage::NotStarted)),
        })
    }

    /// Start the scheduler: `SchedulerStarted`, `SchedulerStartRefused` or
    /// `SchedulerStartFailed`. Writes the state rows of new and changed
    /// schedules. Runs on to its end when the caller leaves.
    fn start_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let stage = self.lease("CoreScheduler.start_async")?;
        call_async(
            py,
            core,
            "CoreScheduler.start_async",
            CallKind::Write,
            start_operation(
                self.connection.clone(),
                Arc::clone(&self.manifest),
                self.stop.clone(),
                stage,
            ),
        )
    }

    /// One tick of the started scheduler: `SchedulerTicked` or
    /// `SchedulerTickFailed`. Writes. Raises `RuntimeError` before a
    /// completed start and after the end of a run.
    fn tick_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let stage = self.lease("CoreScheduler.tick_async")?;
        if let Some(refusal) = tick_refusal(&stage) {
            return Err(PyRuntimeError::new_err(format!(
                "CoreScheduler.tick_async: {refusal}"
            )));
        }
        call_async(
            py,
            core,
            "CoreScheduler.tick_async",
            CallKind::Write,
            tick_operation(stage),
        )
    }

    /// The start when none completed, then the ticks until a stop request:
    /// `SchedulerStopped`, `SchedulerStartRefused` or
    /// `SchedulerStartFailed`. A cancel of the future asks for the stop.
    fn run_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let stage = self.lease("CoreScheduler.run_async")?;
        let stop = self.stop.clone();
        call_service_async(
            py,
            core,
            "CoreScheduler.run_async",
            StopSignal::new(move || stop.request_stop()),
            run_operation(
                self.connection.clone(),
                Arc::clone(&self.manifest),
                self.stop.clone(),
                stage,
            ),
        )
    }

    /// Ask the run to stop at its next tick boundary. A fire in flight runs
    /// to its end. Pure; idempotent.
    fn request_stop(&self) {
        self.stop.request_stop();
    }
}

/// The start completed. A start that completed before answers this at
/// once.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct SchedulerStarted {}

/// The start refused a schedule before any state write: a zone that the
/// core does not know, a send that the command rules refuse, or a pattern
/// with no slot after the database time.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SchedulerStartRefused {
    pub schedule: String,
    /// The refusal, with the schedule and the cause.
    pub reason: String,
}

/// The start failed before the state start: the connect, the schema
/// install, or the client family check. No schedule state was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SchedulerStartFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The tick ran. The failure of one due schedule is logged and does not
/// fail the tick.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct SchedulerTicked {}

/// The tick fired nothing: the core pools could not open, or the due read
/// failed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SchedulerTickFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The run ended at a stop request.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct SchedulerStopped {}

/// Why a start did not complete.
#[derive(Debug)]
enum StartNotCompleted {
    Refused(SchedulerStartRefused),
    Failed(SchedulerStartFailed),
}

/// The outcome of `start_async`.
#[derive(Debug)]
enum StartOutcome {
    Started(SchedulerStarted),
    NotCompleted(StartNotCompleted),
}

/// The outcome of `tick_async`.
#[derive(Debug)]
enum TickOutcome {
    Ticked(SchedulerTicked),
    Failed(SchedulerTickFailed),
}

/// The outcome of `run_async`.
#[derive(Debug)]
enum RunOutcome {
    Stopped(SchedulerStopped),
    NotCompleted(StartNotCompleted),
}

impl<'py> IntoPyObject<'py> for StartNotCompleted {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Refused(refused) => refused.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

impl<'py> IntoPyObject<'py> for StartOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Started(started) => started.into_bound_py_any(py),
            Self::NotCompleted(not_completed) => not_completed.into_pyobject(py),
        }
    }
}

impl<'py> IntoPyObject<'py> for TickOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Ticked(ticked) => ticked.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

impl<'py> IntoPyObject<'py> for RunOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Stopped(stopped) => stopped.into_bound_py_any(py),
            Self::NotCompleted(not_completed) => not_completed.into_pyobject(py),
        }
    }
}

/// Why a tick cannot run on `stage`, or `None` when it can. Pure.
fn tick_refusal(stage: &Stage) -> Option<&'static str> {
    match stage {
        Stage::NotStarted => Some("no start of this scheduler completed; call start_async first"),
        Stage::RunEnded => Some("the run of this scheduler ended"),
        Stage::Idle | Stage::Active(_) => None,
    }
}

/// The stage of a completed core start. Pure.
fn started_stage(started: StartedStage) -> Stage {
    match started {
        StartedStage::Idle => Stage::Idle,
        StartedStage::Active(active) => Stage::Active(active),
    }
}

/// The failure of a start step that met `error`. Pure.
fn start_failed(error: &BrokerError) -> StartNotCompleted {
    StartNotCompleted::Failed(SchedulerStartFailed {
        message: error.to_string(),
        retryable: error.is_retryable(),
    })
}

/// The record of a refused manifest start. Pure.
fn start_not_completed(error: &SchedulerStartError) -> StartNotCompleted {
    match error {
        SchedulerStartError::UnknownTimezone { schedule, .. }
        | SchedulerStartError::CommandRefused { schedule, .. }
        | SchedulerStartError::NoNextSlot { schedule, .. } => {
            StartNotCompleted::Refused(SchedulerStartRefused {
                schedule: schedule.clone(),
                reason: error.to_string(),
            })
        }
        SchedulerStartError::ClientFamily(cause) => {
            StartNotCompleted::Failed(SchedulerStartFailed {
                message: error.to_string(),
                retryable: cause.is_retryable(),
            })
        }
    }
}

/// The failure record of a tick. Pure.
fn tick_failed(error: &SchedulerTickError) -> SchedulerTickFailed {
    let cause = match error {
        SchedulerTickError::PoolsUnavailable(cause) | SchedulerTickError::DueRead(cause) => cause,
    };
    SchedulerTickFailed {
        message: error.to_string(),
        retryable: cause.is_retryable(),
    }
}

/// Connect with the schema installed once per handle, then run the core
/// start of the manifest.
async fn start_scheduler(
    connection: &BrokerConnection,
    manifest: &SchedulerManifest,
    stop: &SchedulerStop,
) -> Result<StartedStage, StartNotCompleted> {
    let broker = connection
        .initialized_broker()
        .await
        .map_err(|error| start_failed(&error))?;
    SchedulerService::from_manifest(broker, manifest.clone(), stop)
        .start()
        .await
        .map_err(|error| start_not_completed(&error))
}

/// `start_async`: the core start when no start completed. Writes; runs on
/// to its end when the caller leaves. A refused or failed start leaves the
/// scheduler not started, so a later start runs again.
async fn start_operation(
    connection: BrokerConnection,
    manifest: Arc<SchedulerManifest>,
    stop: SchedulerStop,
    mut stage: OwnedMutexGuard<Stage>,
) -> StartOutcome {
    match &*stage {
        Stage::Idle | Stage::Active(_) | Stage::RunEnded => {
            return StartOutcome::Started(SchedulerStarted {})
        }
        Stage::NotStarted => {}
    }
    match start_scheduler(&connection, &manifest, &stop).await {
        Ok(started) => {
            *stage = started_stage(started);
            StartOutcome::Started(SchedulerStarted {})
        }
        Err(not_completed) => StartOutcome::NotCompleted(not_completed),
    }
}

/// `tick_async`: one tick of an active scheduler; an idle one has nothing
/// to fire. Writes; runs on to its end when the caller leaves.
///
/// Panics: on a stage that `tick_refusal` refuses; the call checks it
/// under the same lease.
async fn tick_operation(mut stage: OwnedMutexGuard<Stage>) -> TickOutcome {
    match &mut *stage {
        Stage::Active(active) => match active.tick().await {
            Ok(()) => TickOutcome::Ticked(SchedulerTicked {}),
            Err(error) => TickOutcome::Failed(tick_failed(&error)),
        },
        Stage::Idle => TickOutcome::Ticked(SchedulerTicked {}),
        Stage::NotStarted | Stage::RunEnded => {
            panic!("tick_async checks the stage before the tick")
        }
    }
}

/// `run_async`: the start when none completed, then the ticks of an active
/// scheduler until a stop request, or the wait for the stop request of an
/// idle one. After a run, the scheduler is `RunEnded`; a run of a scheduler
/// whose run ended ends at once.
async fn run_operation(
    connection: BrokerConnection,
    manifest: Arc<SchedulerManifest>,
    stop: SchedulerStop,
    mut stage: OwnedMutexGuard<Stage>,
) -> RunOutcome {
    let started = match std::mem::replace(&mut *stage, Stage::RunEnded) {
        Stage::NotStarted => match start_scheduler(&connection, &manifest, &stop).await {
            Ok(started) => started_stage(started),
            Err(not_completed) => {
                *stage = Stage::NotStarted;
                return RunOutcome::NotCompleted(not_completed);
            }
        },
        other => other,
    };
    match started {
        Stage::Active(active) => active.run().await,
        Stage::Idle => stop.requested().await,
        Stage::RunEnded => {}
        Stage::NotStarted => panic!("a run starts a scheduler that is not started"),
    }
    RunOutcome::Stopped(SchedulerStopped {})
}

#[cfg(test)]
mod tests;
