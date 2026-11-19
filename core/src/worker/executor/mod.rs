//! The executor of the worker process: every task runs in a child process.
//!
//! User code never runs in the worker process. A Python task runs in a
//! Python child program, a Rust task in the worker binary started again in
//! child mode. One supervisor serves both: the same frames, the same
//! deadlines and kills, the same verdicts and the same exit codes.
//!
//! - [`protocol`]: the parent-child frame codec. The worker parent and every
//!   child use this one implementation.
//! - [`supervisor`]: the spawner thread, child channels, exits and the
//!   restart rules.
//! - [`slots`]: routes, class budgets, isolated slots, and the hand-off of
//!   each attempt's end to the outcome writers.
//! - [`child_host`]: child mode of a Rust worker binary, which runs its
//!   registered tasks of the three classes.
//! - [`handler_child`]: the one child that runs the event handlers of the
//!   app; not a slot.
//! - [`placement`]: the CPUs of the worker process and of its children.

pub mod child_host;
pub mod handler_child;
pub mod placement;
pub mod protocol;
pub mod slots;
pub mod supervisor;
