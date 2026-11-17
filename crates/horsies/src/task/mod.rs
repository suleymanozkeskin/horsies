//! Task definition: the `#[horsies::task]` and `#[horsies::blocking_task]`
//! attributes, the typed signature contract, `TaskError<E>` with the user's
//! error code enum, the body adapters that the core's child host calls, and
//! `TaskRuntime`.

mod error;
mod user_code;

pub use error::{TaskError, TaskErrorCode, UnknownUserCode};
pub use user_code::UserErrorCode;

pub(crate) use user_code::{is_free_user_code, user_code_refusals};

/// `#[derive(horsies::UserErrorCode)]`: one code per unit variant, its name
/// in UPPER_SNAKE_CASE; a built-in or reserved name is a compile error.
pub use horsies_macros::UserErrorCode;

// The library's code catalogs, with Python's names and wire values.
pub use horsies_core::core::task::error::ExceptionRecord;
pub use horsies_core::{
    BuiltInTaskCode, ContractCode, OperationalErrorCode, OutcomeCode, RetrievalCode,
    SubWorkflowFailure,
};
