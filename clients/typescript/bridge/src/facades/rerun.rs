//! Rerun of a retained terminal task: in a transaction of the app's handle,
//! or in a transaction that the caller owns.
//!
//! - `rerunTask` ([`app`]): one operation on the core runtime. It runs the
//!   source read and, for an eligible source, the write in one transaction
//!   of the handle's broker, and commits it (`PostgresBroker::rerun_task`).
//! - `rerunReadStatement`, `prepareRerunWrite` and the class
//!   `PreparedRerunWrite` ([`steps`]): the core's three pure rerun steps as
//!   sync functions. The package runs each statement with the caller's
//!   driver, so the rerun commits or rolls back with the caller's own
//!   writes. They do no I/O and start no core.
//!
//! Every outcome but `enqueued` writes nothing. An expected failure is a
//! value ([`records`]); only a value that the package should have refused
//! first is thrown (`CLIENT_DEFECT`).
//!
//! The new task stores the trace carrier that the package passes: the
//! carrier of the caller's active context. A rerun opens no span.

pub mod app;
pub mod arguments;
pub mod caller_values;
pub mod records;
pub mod steps;
