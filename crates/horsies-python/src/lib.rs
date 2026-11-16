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
mod process_core;
mod records;

use pyo3::prelude::*;

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
    module.add_function(wrap_pyfunction!(core_state, module)?)?;
    module.add_function(wrap_pyfunction!(refresh_log_levels, module)?)?;
    errors::add_errors(module)?;
    Ok(())
}
