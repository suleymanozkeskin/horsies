//! The frames from the worker parent as Python classes: what
//! `decode_parent_frame` and `FrameReader.feed` give a child or the preload
//! process. Python reads them and builds none: a test builds a frame from
//! its wire bytes.
//!
//! The init frame keeps the database URL and the connection settings in
//! Rust: only `CoreChildHandle.from_init` reads them, so the URL never
//! becomes a Python value of the child.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use horsies_core::events::RunTrace;
use horsies_core::frame_protocol::{
    CancelTarget, ChildKind, ChildLogLevel, ChildPool, ChildProgram, EventsFrame, InitFrame,
    IsolatedWork, MalformedFrame, ParentFrame, ParentFrameRefusal, ProtocolMismatch,
    ReceivedProtocol, RunFrame, SharedRunFrame, SharedWork, StoredOptionsRefusal, PROTOCOL_VERSION,
};
use horsies_core::OrphanSelfHeal;
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

/// What a Python child imports before it reports ready.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct PythonProgram {
    app_locator: String,
    imports: Vec<String>,
    sys_path_roots: Vec<String>,
    parent_sys_path: Vec<String>,
    parent_argv: Vec<String>,
}

/// The connection pool of a child: `min_size` connections kept open, at
/// most `max_size`; `check` pings each pooled connection at checkout.
#[pyclass(module = "horsies._core", name = "ChildPool", frozen, get_all)]
pub struct ChildPoolRecord {
    min_size: u64,
    max_size: u64,
    check: bool,
}

/// An isolated child: one task at a time.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct IsolatedChildKind {
    pool: Py<ChildPoolRecord>,
    task_send_pool: u64,
}

/// A shared async child: up to `seats` async tasks on one event loop.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct SharedAsyncChildKind {
    seats: u64,
    pool: Py<ChildPoolRecord>,
    executor_threads: u64,
    task_send_pool: u64,
}

/// A shared thread child: up to `seats` blocking tasks, each on a thread.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct SharedThreadChildKind {
    seats: u64,
    pool: Py<ChildPoolRecord>,
    task_send_pool: u64,
}

/// A handler child: it runs the event handlers of the app and no task. A
/// handler may send tasks, through a pool of `task_send_pool` connections.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct HandlerChildKind {
    task_send_pool: u64,
}

/// The first frame to a child or to the preload process.
#[pyclass(module = "horsies._core", name = "InitFrame", frozen)]
pub struct InitFrameRecord {
    /// `PythonProgram`, or `'RUST'`.
    #[pyo3(get)]
    program: Py<PyAny>,
    /// `'DEBUG'`, `'INFO'`, `'WARNING'`, `'ERROR'` or `'CRITICAL'`.
    #[pyo3(get)]
    log_level: &'static str,
    #[pyo3(get)]
    process_name: String,
    /// `IsolatedChildKind`, `SharedAsyncChildKind`, `SharedThreadChildKind`
    /// or `HandlerChildKind`.
    #[pyo3(get)]
    child_kind: Py<PyAny>,
    /// The decoded frame: the database URL and the connection settings stay
    /// here.
    frame: InitFrame,
}

impl InitFrameRecord {
    /// The decoded init frame.
    pub fn frame(&self) -> &InitFrame {
        &self.frame
    }
}

/// Stored options of the row that the worker cannot use: the child sets the
/// row RUNNING and fails the task with `WORKER_SERIALIZATION_ERROR` before it
/// resolves the task.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct StoredOptionsRefused {
    /// `'OPTIONS_UNREADABLE'`, `'TIMEOUT_VALUE_INVALID'` or
    /// `'CANCEL_GRACE_INVALID'`.
    kind: &'static str,
    reason: String,
}

/// The trace carrier that the send stored on the task row: the keys and the
/// values that the propagator of the sender wrote. The child extracts it
/// with the app's propagator.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct StoredTraceCarrier {
    carrier: BTreeMap<String, String>,
}

/// Run one task in an isolated child. The parent holds the deadline.
#[pyclass(module = "horsies._core", name = "RunFrame", frozen, get_all)]
pub struct RunFrameRecord {
    task_id: Uuid,
    task_name: String,
    /// The queue of the task row.
    queue_name: String,
    /// The attempt that the claim runs, from 1: the retry count plus 1.
    attempt: u32,
    worker_id: String,
    /// The claim generation; each write of the attempt is fenced on it.
    claimed_at: DateTime<Utc>,
    /// Whether the stop before start of a workflow task cancels the task
    /// when no node links it live (the app's
    /// `terminate_orphaned_workflow_tasks`).
    orphan_self_heal: bool,
    /// `'RUN'`, or `StoredOptionsRefused`.
    work: Py<PyAny>,
    /// `StoredTraceCarrier`, or `'NO_CARRIER'`.
    trace: Py<PyAny>,
    /// The JSON text of the task input.
    input: Py<PyString>,
}

/// The work of an async seat: cancel the task `timeout_ms` after its start,
/// then give it `cancel_grace_ms` to stop.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct AsyncTaskWork {
    timeout_ms: u64,
    cancel_grace_ms: u64,
}

/// The work of a thread seat: report the deadline `timeout_ms` after the
/// child takes the run.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct ThreadTaskWork {
    timeout_ms: u64,
}

/// Run one task on a free seat of a shared child.
#[pyclass(module = "horsies._core", name = "SharedRunFrame", frozen, get_all)]
pub struct SharedRunFrameRecord {
    dispatch: u64,
    task_id: Uuid,
    task_name: String,
    /// As `RunFrame.queue_name`.
    queue_name: String,
    /// As `RunFrame.attempt`.
    attempt: u32,
    worker_id: String,
    claimed_at: DateTime<Utc>,
    /// As `RunFrame.orphan_self_heal`.
    orphan_self_heal: bool,
    /// `AsyncTaskWork`, `ThreadTaskWork` or `StoredOptionsRefused`.
    work: Py<PyAny>,
    /// `StoredTraceCarrier`, or `'NO_CARRIER'`.
    trace: Py<PyAny>,
    input: Py<PyString>,
}

/// The run of an isolated child that a `CancelRunFrame` names, by its task.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct IsolatedCancelTarget {
    task_id: Uuid,
}

/// The run on a seat of a shared child that a `CancelRunFrame` names, by its
/// dispatch number and its task.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct SharedCancelTarget {
    dispatch: u64,
    task_id: Uuid,
}

/// An operator cancelled the task of a run: the cancel statement moved its
/// row to history and recorded the attempt as `CANCELLED`. The child gives
/// the cancel to the task code and reports the end of the run as cancelled.
/// A cancel of a run that the child does not run now is ignored: the end of
/// the run and the cancel can cross.
#[pyclass(module = "horsies._core", name = "CancelRunFrame", frozen, get_all)]
pub struct CancelRunFrameRecord {
    /// `IsolatedCancelTarget` or `SharedCancelTarget`.
    target: Py<PyAny>,
}

/// Lifecycle events for a handler child, in the order that the core emitted
/// them. `events` holds the JSON text of each event; the generated decoder
/// reads the value that `json.loads` gives for it.
#[pyclass(module = "horsies._core", name = "EventsFrame", frozen, get_all)]
pub struct EventsFrameRecord {
    events: Vec<String>,
}

/// Exit normally. Sent only to a child with no task.
#[pyclass(module = "horsies._core", name = "StopFrame", frozen)]
pub struct StopFrameRecord {}

/// A parent frame that the child refuses: the child exits with the
/// bad-frame code.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct FrameRefused {
    reason: String,
}

impl FrameRefused {
    pub fn malformed(malformed: &MalformedFrame) -> Self {
        Self {
            reason: malformed.to_string(),
        }
    }
}

/// An init frame of another protocol version: the child logs both numbers
/// and exits with the protocol-mismatch code.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct ParentProtocolMismatch {
    /// The version the parent named, or `'NOT_NAMED'` (a version-1 parent).
    received: Py<PyAny>,
    /// The version this build speaks.
    expected: u32,
}

/// The Python value of a decoded parent frame.
pub fn parent_frame_object(py: Python<'_>, frame: ParentFrame) -> PyResult<Py<PyAny>> {
    match frame {
        ParentFrame::Init(init) => init_object(py, init),
        ParentFrame::Run(run) => run_object(py, run),
        ParentFrame::SharedRun(run) => shared_run_object(py, run),
        ParentFrame::CancelRun(target) => cancel_run_object(py, target),
        ParentFrame::Events(events) => events_object(py, &events),
        ParentFrame::Stop => StopFrameRecord {}.into_py_any(py),
    }
}

/// The Python value of a refused parent frame.
pub fn parent_refusal_object(py: Python<'_>, refusal: &ParentFrameRefusal) -> PyResult<Py<PyAny>> {
    match refusal {
        ParentFrameRefusal::Malformed(malformed) => {
            FrameRefused::malformed(malformed).into_py_any(py)
        }
        ParentFrameRefusal::ProtocolMismatch(mismatch) => mismatch_object(py, mismatch),
    }
}

fn mismatch_object(py: Python<'_>, mismatch: &ProtocolMismatch) -> PyResult<Py<PyAny>> {
    let received = match mismatch.received {
        ReceivedProtocol::NotNamed => "NOT_NAMED".into_py_any(py)?,
        ReceivedProtocol::Named(version) => version.into_py_any(py)?,
    };
    ParentProtocolMismatch {
        received,
        expected: PROTOCOL_VERSION,
    }
    .into_py_any(py)
}

fn log_level_text(level: ChildLogLevel) -> &'static str {
    match level {
        ChildLogLevel::Debug => "DEBUG",
        ChildLogLevel::Info => "INFO",
        ChildLogLevel::Warning => "WARNING",
        ChildLogLevel::Error => "ERROR",
        ChildLogLevel::Critical => "CRITICAL",
    }
}

fn program_object(py: Python<'_>, program: &ChildProgram) -> PyResult<Py<PyAny>> {
    match program {
        ChildProgram::Python(python) => PythonProgram {
            app_locator: python.app_locator.clone(),
            imports: python.imports.clone(),
            sys_path_roots: python.sys_path_roots.clone(),
            parent_sys_path: python.parent_sys_path.clone(),
            parent_argv: python.parent_argv.clone(),
        }
        .into_py_any(py),
        ChildProgram::Rust => "RUST".into_py_any(py),
    }
}

fn pool_object(py: Python<'_>, pool: ChildPool) -> PyResult<Py<ChildPoolRecord>> {
    Py::new(
        py,
        ChildPoolRecord {
            min_size: pool.min_size.get(),
            max_size: pool.max_size.get(),
            check: pool.check,
        },
    )
}

fn child_kind_object(py: Python<'_>, kind: ChildKind) -> PyResult<Py<PyAny>> {
    match kind {
        ChildKind::Isolated(child) => IsolatedChildKind {
            pool: pool_object(py, child.pool)?,
            task_send_pool: child.task_send_pool.get(),
        }
        .into_py_any(py),
        ChildKind::SharedAsync(child) => SharedAsyncChildKind {
            seats: child.seats.get(),
            pool: pool_object(py, child.pool)?,
            executor_threads: child.executor_threads.get(),
            task_send_pool: child.task_send_pool.get(),
        }
        .into_py_any(py),
        ChildKind::SharedThread(child) => SharedThreadChildKind {
            seats: child.seats.get(),
            pool: pool_object(py, child.pool)?,
            task_send_pool: child.task_send_pool.get(),
        }
        .into_py_any(py),
        ChildKind::Handler(child) => HandlerChildKind {
            task_send_pool: child.task_send_pool.get(),
        }
        .into_py_any(py),
    }
}

fn init_object(py: Python<'_>, init: InitFrame) -> PyResult<Py<PyAny>> {
    InitFrameRecord {
        program: program_object(py, &init.program)?,
        log_level: log_level_text(init.log_level),
        process_name: init.process_name.clone(),
        child_kind: child_kind_object(py, init.child_kind)?,
        frame: init,
    }
    .into_py_any(py)
}

fn refusal_object(py: Python<'_>, refusal: StoredOptionsRefusal) -> PyResult<Py<PyAny>> {
    let (kind, reason) = match refusal {
        StoredOptionsRefusal::OptionsUnreadable(reason) => ("OPTIONS_UNREADABLE", reason),
        StoredOptionsRefusal::TimeoutValueInvalid(reason) => ("TIMEOUT_VALUE_INVALID", reason),
        StoredOptionsRefusal::CancelGraceInvalid(reason) => ("CANCEL_GRACE_INVALID", reason),
    };
    StoredOptionsRefused {
        kind,
        reason: reason.into_string(),
    }
    .into_py_any(py)
}

fn isolated_work_object(py: Python<'_>, work: IsolatedWork) -> PyResult<Py<PyAny>> {
    match work {
        IsolatedWork::Run => "RUN".into_py_any(py),
        IsolatedWork::Refusal(refusal) => refusal_object(py, refusal),
    }
}

fn shared_work_object(py: Python<'_>, work: SharedWork) -> PyResult<Py<PyAny>> {
    match work {
        SharedWork::AsyncTask {
            timeout_ms,
            cancel_grace_ms,
        } => AsyncTaskWork {
            timeout_ms: timeout_ms.get(),
            cancel_grace_ms: cancel_grace_ms.get(),
        }
        .into_py_any(py),
        SharedWork::ThreadTask { timeout_ms } => ThreadTaskWork {
            timeout_ms: timeout_ms.get(),
        }
        .into_py_any(py),
        SharedWork::Refusal(refusal) => refusal_object(py, refusal),
    }
}

/// The trace of a run frame. The frame decoder reads only a carrier text
/// that is one JSON object of texts, so its keys and values always read.
fn trace_object(py: Python<'_>, trace: RunTrace) -> PyResult<Py<PyAny>> {
    match trace {
        RunTrace::Carrier { text } => {
            let carrier: BTreeMap<String, String> = serde_json::from_str(text.as_str())
                .unwrap_or_else(|error| {
                    unreachable!(
                        "the frame decoder reads only a carrier text that is one JSON object \
                         of texts: {error}"
                    )
                });
            StoredTraceCarrier { carrier }.into_py_any(py)
        }
        RunTrace::NoCarrier => "NO_CARRIER".into_py_any(py),
    }
}

fn cancel_run_object(py: Python<'_>, target: CancelTarget) -> PyResult<Py<PyAny>> {
    let target = match target {
        CancelTarget::Isolated { task_id } => IsolatedCancelTarget { task_id }.into_py_any(py)?,
        CancelTarget::Shared(run) => SharedCancelTarget {
            dispatch: run.dispatch.get(),
            task_id: run.task_id,
        }
        .into_py_any(py)?,
    };
    CancelRunFrameRecord { target }.into_py_any(py)
}

fn events_object(py: Python<'_>, events: &EventsFrame) -> PyResult<Py<PyAny>> {
    EventsFrameRecord {
        events: events.events.events().map(str::to_owned).collect(),
    }
    .into_py_any(py)
}

/// The `orphan_self_heal` argument of the stop before start: `True` for on.
/// Pure.
fn self_heal_flag(self_heal: OrphanSelfHeal) -> bool {
    match self_heal {
        OrphanSelfHeal::On => true,
        OrphanSelfHeal::Off => false,
    }
}

fn run_object(py: Python<'_>, run: RunFrame) -> PyResult<Py<PyAny>> {
    let input = PyString::new(py, run.input.as_str()).unbind();
    RunFrameRecord {
        task_id: run.task_id,
        task_name: run.task_name,
        queue_name: run.queue_name.into_string(),
        attempt: run.attempt.get(),
        worker_id: run.worker_id,
        claimed_at: run.claimed_at,
        orphan_self_heal: self_heal_flag(run.orphan_self_heal),
        work: isolated_work_object(py, run.work)?,
        trace: trace_object(py, run.trace)?,
        input,
    }
    .into_py_any(py)
}

fn shared_run_object(py: Python<'_>, run: SharedRunFrame) -> PyResult<Py<PyAny>> {
    let input = PyString::new(py, run.input.as_str()).unbind();
    SharedRunFrameRecord {
        dispatch: run.dispatch.get(),
        task_id: run.task_id,
        task_name: run.task_name,
        queue_name: run.queue_name.into_string(),
        attempt: run.attempt.get(),
        worker_id: run.worker_id,
        claimed_at: run.claimed_at,
        orphan_self_heal: self_heal_flag(run.orphan_self_heal),
        work: shared_work_object(py, run.work)?,
        trace: trace_object(py, run.trace)?,
        input,
    }
    .into_py_any(py)
}

/// Every class of this module, for the module registration.
pub fn add_parent_frame_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PythonProgram>()?;
    module.add_class::<ChildPoolRecord>()?;
    module.add_class::<IsolatedChildKind>()?;
    module.add_class::<SharedAsyncChildKind>()?;
    module.add_class::<SharedThreadChildKind>()?;
    module.add_class::<HandlerChildKind>()?;
    module.add_class::<InitFrameRecord>()?;
    module.add_class::<StoredOptionsRefused>()?;
    module.add_class::<RunFrameRecord>()?;
    module.add_class::<AsyncTaskWork>()?;
    module.add_class::<ThreadTaskWork>()?;
    module.add_class::<SharedRunFrameRecord>()?;
    module.add_class::<IsolatedCancelTarget>()?;
    module.add_class::<SharedCancelTarget>()?;
    module.add_class::<CancelRunFrameRecord>()?;
    module.add_class::<StoredTraceCarrier>()?;
    module.add_class::<EventsFrameRecord>()?;
    module.add_class::<StopFrameRecord>()?;
    module.add_class::<FrameRefused>()?;
    module.add_class::<ParentProtocolMismatch>()?;
    Ok(())
}
