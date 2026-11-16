//! `horsies._core`: the extension module through which Python calls
//! horsies-core.
//!
//! Rules of the module:
//! - Importing it starts nothing: no runtime, no thread, no connection.
//! - The first core operation of a process starts one tokio runtime and one
//!   log forwarder thread (`process_core`).
//! - A blocking call runs the operation on the calling thread with the GIL
//!   released and checks signals every 100 ms; an async call returns an
//!   asyncio future that the loop's completion queue sets (`bridge`). No
//!   runtime thread takes the GIL.
//! - A panic in an operation raises `PanicException` (a `BaseException`); a
//!   panic in a core background task stops the core in this process.
//! - A process forked without `exec` after the core started refuses every
//!   core call with `ForkedAfterCoreStart`.
//! - Core log events reach Python `logging` through a bounded channel and one
//!   forwarder thread (`logging`).
//! - A `CoreHandle` is built once from a typed manifest (`handle`).

mod bridge;
mod errors;
mod handle;
mod logging;
mod operations;
mod process_core;
mod records;

use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::process_core::CoreStatus;

/// The allocator of musl targets (see `Cargo.toml`).
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The state of the core in this process: `not_started`, `running`,
/// `stopped` or `inherited`. Starts nothing.
#[pyfunction]
fn core_state() -> &'static str {
    match process_core::core_status() {
        CoreStatus::NotStarted => "not_started",
        CoreStatus::Running => "running",
        CoreStatus::Stopped => "stopped",
        CoreStatus::Inherited => "inherited",
    }
}

/// The refusal of a task id mint: the cause from the core's UUIDv7 source.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
struct IdMintRefused {
    cause: String,
}

/// A minted task id, or the refusal.
enum MintOutcome {
    Minted(Uuid),
    Refused(IdMintRefused),
}

impl<'py> IntoPyObject<'py> for MintOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Minted(task_id) => task_id.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// Mint a new task id (UUIDv7): `uuid.UUID` or `IdMintRefused`. Pure: no
/// I/O, and it starts no runtime; the fork and stop rules still apply.
#[pyfunction]
fn mint_task_id() -> PyResult<MintOutcome> {
    if let Some(refusal) = process_core::pure_call_refusal() {
        return Err(errors::refusal_error(refusal));
    }
    Ok(match horsies_core::mint_task_id() {
        Ok(task_id) => MintOutcome::Minted(task_id),
        Err(error) => MintOutcome::Refused(IdMintRefused {
            cause: error.to_string(),
        }),
    })
}

/// Read the effective level of every `horsies` logger again, after a change
/// of the logging configuration. Does nothing before the core started; the
/// start reads the levels.
#[pyfunction]
fn refresh_log_levels(py: Python<'_>) -> PyResult<()> {
    match process_core::running_core() {
        Some(core) => core.log().refresh_levels(py),
        None => Ok(()),
    }
}

#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<handle::CoreHandle>()?;
    module.add_class::<records::BrokerFailure>()?;
    module.add_class::<records::DatabasePingRecord>()?;
    module.add_class::<IdMintRefused>()?;
    module.add_class::<operations::arguments::StartNow>()?;
    module.add_class::<operations::arguments::StartAfterDelay>()?;
    module.add_class::<operations::arguments::StartAt>()?;
    module.add_class::<operations::arguments::RetentionDefault>()?;
    module.add_class::<operations::arguments::RetentionForever>()?;
    module.add_class::<operations::arguments::RetentionPeriod>()?;
    module.add_class::<operations::enqueue::EnqueueRequest>()?;
    module.add_class::<operations::enqueue::Enqueued>()?;
    module.add_class::<operations::enqueue::SendSuppressed>()?;
    module.add_class::<operations::enqueue::SendValidationFailed>()?;
    module.add_class::<operations::enqueue::SendEnqueueFailed>()?;
    module.add_class::<operations::enqueue::SendPayloadMismatch>()?;
    module.add_class::<operations::enqueue::SendPayloadTooLarge>()?;
    module.add_class::<operations::enqueue::SendKeyConflict>()?;
    module.add_class::<operations::schema::SchemaInstalled>()?;
    module.add_class::<operations::schema::SchemaInstallNotRun>()?;
    module.add_class::<operations::schema::SchemaStatusRecord>()?;
    module.add_class::<operations::task_info::TaskInfoRecord>()?;
    module.add_class::<operations::task_info::TaskAttemptRecord>()?;
    module.add_class::<operations::task_info::TaskInfoNotFound>()?;
    module.add_function(wrap_pyfunction!(core_state, module)?)?;
    module.add_function(wrap_pyfunction!(mint_task_id, module)?)?;
    module.add_function(wrap_pyfunction!(refresh_log_levels, module)?)?;
    errors::add_errors(module)?;
    Ok(())
}
