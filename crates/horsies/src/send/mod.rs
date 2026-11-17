//! Sending a task: the typed send and delayed send, the send options, the
//! read-back of the input before any statement, the send retry, and the
//! closed catalog of send errors.

mod catalog;
mod function;
mod options;

pub use catalog::TaskFunctionNotFound;
pub use function::{TaskFunction, TaskFunctionSendOptions};
pub use options::{SendRetention, TaskSendOptions};

pub(crate) use catalog::TaskFunctions;

/// A whole number of seconds: the delay of `schedule`.
pub use horsies_core::manifest::WholeSeconds;
/// A retention period of whole days, for `SendRetention::Period`.
pub use horsies_core::RetentionDays;
/// The send catalog of the core: one variant per send code, each with the
/// facts of its state. `retry_send` and `retry_schedule` accept the three
/// that carry the payload (`resend_target`): `AsyncContext`, `EnqueueFailed`
/// and `IdempotencyKeyConflict`.
pub use horsies_core::{TaskSendError, TaskSendErrorCode, TaskSendPayload};
