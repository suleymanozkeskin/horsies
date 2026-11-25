//! The panic rule of the process: what a panic in a core background task
//! does, and what a defect of the package does.
//!
//! - `STOP_CORE`, the default, the rule of a process that the core does not
//!   own (a producer inside a web server): the core stops in this process,
//!   and every later call and every call in flight ends with `CoreStopped`.
//! - `ABORT_PROCESS`, the rule of a worker process: the panic goes to stderr
//!   and to the log sink, the log drains for a bounded time, and the process
//!   aborts; its supervisor restarts it.
//!
//! The entry point sets the rule once, before the first core call. The start
//! of the core fixes it.

use napi_derive::napi;

/// What a panic in a core background task does to this process.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanicRule {
    /// Stop the core in this process; every later call throws `CoreStopped`.
    StopCore,
    /// Write the panic to stderr and to the log sink, drain the log for a
    /// bounded time, and abort the process.
    AbortProcess,
}
