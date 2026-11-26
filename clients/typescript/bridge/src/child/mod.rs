//! The child host entry of task and handler children: child mode, the task
//! body forms, the start hooks, the event loop watch, the body facts, the exit
//! rule, and the slots that the thread seats and the handler child fill.
//!
//! - `host`: `childArguments` and `serveChild`, the child app of a plan on
//!   the core's child host.
//! - `body`: the core's task function forms for JavaScript bodies, the run
//!   table, the cancel watch, the settle calls, and the host-held work of
//!   `runtime.spawn`.
//! - `body_facts`: the facts of each run for the body observer.
//! - `start_hooks`: the providers and the `onChildStart` hooks as the core's
//!   start hooks.
//! - `loop_watch`: the marks of the JavaScript main thread's loop.
//! - `exit`: the host thread, the exit of the process, the backstop.
//! - `handler_slot`, `seat_slot`: the slots that the handler child area and
//!   the thread seat area fill before the serve.

pub mod body;
pub mod body_facts;
pub mod exit;
pub mod handler_slot;
pub mod host;
pub mod loop_watch;
pub mod seat_slot;
pub mod start_hooks;
