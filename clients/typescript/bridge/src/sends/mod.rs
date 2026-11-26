//! Task sends and task reads on a `CoreHandle`: the send, the result wait,
//! and the task info read.
//!
//! - `checks`: the pure checks of the app config and of the declared error
//!   codes, which `Horsies.create` calls.
//! - `task_options`: the pure check of the options of a task, and its stored
//!   options text.
//! - `send`: the rules of each send of an accepted config, and the enqueue.
//! - `task_reads`: the result read and wait, the task info read, the role of
//!   this process.
//! - `producer_handle`: the handle that a send or a task read goes through:
//!   the child host's producer handle in a task child, else the given one.
//! - `task_error`: the core's task error of a body, built from its parts.
//! - `child_producer_probe`: the test exports of the producer handle of a
//!   task child (only with the cargo feature `test-probes`).

pub mod checks;
#[cfg(feature = "test-probes")]
pub mod child_producer_probe;
pub mod early_end;
pub mod producer_handle;
pub mod refusals;
pub mod send;
pub mod task_error;
pub mod task_options;
pub mod task_reads;
