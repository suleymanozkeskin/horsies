//! Child mode of a worker binary: the Rust child host.
//!
//! A Rust worker starts its own binary again with [`CHILD_MODE_ARGUMENT`],
//! the socket descriptor and the worker pid. The binary's entry checks
//! [`child_mode`] first; in child mode it calls [`run_child_host`] with its
//! task registry and exits with the code it returns. Code that `main` runs
//! before that call runs in each child too, as a Python child imports the
//! app again.
//!
//! The host speaks the same frames as the Python child and gives the same
//! guarantees: the start write and the finalizing marker through the core,
//! a result envelope only after the body stopped, the deadline reports of
//! each seat kind, the cancel signal of a shared async run at its deadline
//! ([`run_cancel_signal`]), the reports of work a body left
//! ([`spawn_in_run`]), and the protocol's exit codes.

mod args;
mod host;
mod link;
mod panic;
mod producer;
mod role;
mod rss;
mod run;
mod run_work;
mod seats;

#[cfg(test)]
pub(crate) mod in_process;
#[cfg(test)]
mod start_node_tests;
#[cfg(test)]
mod start_tests;
#[cfg(test)]
mod tests;

pub use args::{child_mode, ChildArgs, ChildArgsError, ChildMode, CHILD_MODE_ARGUMENT};
pub use host::{
    child_command, run_child_host, ChildApp, ChildHookFailure, ChildHostExit, ChildLogSetup,
    ChildStartFuture, ChildStartHook, CHILD_START_HOOK_BOUND,
};
pub use producer::{child_producer, ChildProducer};
pub use role::{process_role, thread_role, wait_inside_task_message, ProcessRole, ThreadRole};
pub use run_work::{
    run_cancel_signal, spawn_in_run, RunCancelSignal, RunSignalRefused, RunSpawnRefused,
    MAX_RUN_WORK,
};
