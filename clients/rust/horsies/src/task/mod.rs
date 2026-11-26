//! Task definition: the `#[horsies::task]` and `#[horsies::blocking_task]`
//! attributes, the typed signature contract, `TaskError<E>` with the user's
//! error code enum, the body adapters that the core's child host calls,
//! `TaskRuntime`, and `StoredDateTime` for a date-time in Python's stored
//! text.

pub(crate) mod body;
mod error;
mod macros;
mod runtime;
mod stored_datetime;
mod user_code;

pub use error::{TaskError, TaskErrorCode, UnknownCustomCode};
pub use runtime::TaskRuntime;
pub use stored_datetime::StoredDateTime;
pub use user_code::UserErrorCode;

pub(crate) use user_code::{is_free_user_code, is_retry_code, stored_retry_code};

/// `#[derive(horsies::UserErrorCode)]`: one code per unit variant, its name
/// in UPPER_SNAKE_CASE; a built-in or reserved name is a compile error.
pub use horsies_macros::UserErrorCode;

/// Any user code as checked text: the code type of a read that takes the
/// codes of several tasks (`WorkflowHandle::get`, a dependency result of
/// another code type). `UserCode::parse` refuses a built-in or reserved
/// name with `UserCodeRefused`.
pub use horsies_core::{UserCode, UserCodeRefused};

/// The fencing token of a run (`TaskRuntime::fencing_token`), and why a read
/// gave none.
pub use horsies_core::{FencingToken, FencingTokenRefused};

/// The cancel state of a run (`TaskRuntime::cancel_state`), and why a cancel
/// was requested.
pub use horsies_core::{CancelCause, RunCancelState};

// The library's code catalogs, with Python's names and wire values.
pub use horsies_core::core::task::error::ExceptionRecord;
pub use horsies_core::{
    BuiltInTaskCode, ContractCode, OperationalErrorCode, OutcomeCode, RetrievalCode,
    SubWorkflowFailure,
};

// Registration by hand: `Horsies::register` takes a `RegisteredTask` that
// `async_task_fn!` or `blocking_task_fn!` builds, with its options.
pub use horsies_core::{
    BackoffStrategy, CancelGraceMs, CancelGraceOutOfRange, Execution, RegisteredTask, RetryPolicy,
    RetryPolicyError, TaskOptions,
};
