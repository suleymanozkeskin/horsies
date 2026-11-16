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
//! - [`placement`]: the CPUs of the worker process and of its children.

// The worker loop still runs task bodies in its own process; it switches to
// this executor in a later change. Until then only the tests call these
// items, so the dead-code lint would name every one of them.
#![allow(dead_code)]

pub mod child_host;
pub mod placement;
pub mod protocol;
pub mod slots;
pub mod supervisor;
