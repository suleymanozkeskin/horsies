//! Sending a task: the typed send with its options (the delay among them),
//! the read-back of the input before any statement, the send retry, the
//! typed functions of each app, and the closed catalog of send errors.

mod catalog;
mod function;
mod options;

pub use catalog::TaskFunctionNotFound;
pub use function::{TaskFunction, TaskFunctionSendOptions};
pub use options::{SendRetention, TaskSendOptions};

pub(crate) use catalog::TaskFunctions;
#[cfg(test)]
pub(crate) use function::send_options_text;

/// A whole number of seconds: the delay of a send
/// (`TaskSendOptions::delay`).
pub use horsies_core::manifest::WholeSeconds;
/// A retention period of whole days, for `SendRetention::Period`.
pub use horsies_core::RetentionDays;
/// The send catalog of the core: one variant per send code, each with the
/// facts of its state. `retry_send` accepts the two that are sent again
/// (`resend_target`): `AsyncContext` and `EnqueueFailed`. It refuses
/// `IdempotencyKeyConflict`: a repeat of the same send does not complete it.
pub use horsies_core::{TaskSendError, TaskSendErrorCode, TaskSendPayload};
