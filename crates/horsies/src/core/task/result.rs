use serde::{Deserialize, Serialize};

use super::error::{BuiltInTaskCode, OperationalErrorCode, TaskError, TaskErrorCode};

/// Outcome of a task or workflow result retrieval.
///
/// Two-state discriminated union matching Python's `TaskResult[T, TaskError]`:
/// - `Ok(T)`: task completed successfully with a value
/// - `Err(TaskError)`: task failed, or result could not be retrieved
///
/// Retrieval failures (timeout, not-found, broker errors) are folded into
/// `Err(TaskError)` with the appropriate `BuiltInTaskCode` — one result type,
/// one error taxonomy, one matching shape.
///
/// Serializes with adjacently tagged encoding:
/// `{"__type": "ok", "value": ...}`, `{"__type": "err", "value": {...}}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "__type", content = "value", rename_all = "snake_case")]
pub enum TaskResult<T> {
    Ok(T),
    Err(TaskError),
}

impl<T> TaskResult<T> {
    /// Returns `true` if the result is `Ok`.
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok(_))
    }

    /// Returns `true` if the result is `Err`.
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Err(_))
    }

    /// Returns `true` if this is a transient error that may resolve on retry.
    ///
    /// Transient errors are retrieval-class codes (`RetrievalCode`) and
    /// `OperationalErrorCode::BrokerError`. All other errors are terminal.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Ok(_) => false,
            Self::Err(err) => err.is_transient(),
        }
    }

    /// Returns `true` if this is a terminal result (should be cached).
    ///
    /// `Ok` is always terminal. `Err` is terminal unless it's a transient
    /// retrieval/broker error.
    pub fn is_terminal(&self) -> bool {
        !self.is_transient()
    }

    /// Unwraps the `Ok` value, panicking with a message if not `Ok`.
    pub fn unwrap(self) -> T {
        match self {
            Self::Ok(value) => value,
            Self::Err(err) => panic!("called unwrap() on TaskResult::Err: {}", err),
        }
    }

    /// Unwraps the `Err` value, panicking if not `Err`.
    pub fn unwrap_err(self) -> TaskError {
        match self {
            Self::Err(err) => err,
            Self::Ok(_) => panic!("called unwrap_err() on TaskResult::Ok"),
        }
    }

    /// Converts to `Option<T>`, discarding error information.
    pub fn ok(self) -> Option<T> {
        match self {
            Self::Ok(value) => Some(value),
            _ => None,
        }
    }

    /// Maps the `Ok` value using the provided function.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> TaskResult<U> {
        match self {
            Self::Ok(value) => TaskResult::Ok(f(value)),
            Self::Err(err) => TaskResult::Err(err),
        }
    }

    /// Flat-maps the `Ok` value using the provided function.
    pub fn and_then<U, F: FnOnce(T) -> TaskResult<U>>(self, f: F) -> TaskResult<U> {
        match self {
            Self::Ok(value) => f(value),
            Self::Err(err) => TaskResult::Err(err),
        }
    }
}

impl TaskError {
    /// Returns `true` if this error is transient (may resolve on retry).
    ///
    /// Transient errors: `RetrievalCode::*` and `OperationalErrorCode::BrokerError`.
    pub fn is_transient(&self) -> bool {
        matches!(
            &self.error_code,
            Some(TaskErrorCode::BuiltIn(BuiltInTaskCode::Retrieval(_)))
                | Some(TaskErrorCode::BuiltIn(BuiltInTaskCode::Operational(
                    OperationalErrorCode::BrokerError,
                )))
        )
    }
}

impl<T: std::fmt::Display> std::fmt::Display for TaskResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok(value) => write!(f, "Ok({})", value),
            Self::Err(err) => write!(f, "Err({})", err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::task::error::{OperationalErrorCode, RetrievalCode, TaskErrorCode};

    #[test]
    fn ok_predicates() {
        let result: TaskResult<i32> = TaskResult::Ok(42);
        assert!(result.is_ok());
        assert!(!result.is_err());
    }

    #[test]
    fn err_predicates() {
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "boom"));
        assert!(!result.is_ok());
        assert!(result.is_err());
    }

    #[test]
    fn unwrap_ok() {
        let result: TaskResult<i32> = TaskResult::Ok(42);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    #[should_panic(expected = "called unwrap() on TaskResult::Err")]
    fn unwrap_err_panics() {
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "boom"));
        result.unwrap();
    }

    #[test]
    fn unwrap_err_ok() {
        let err = TaskError::builtin(OperationalErrorCode::TaskError, "boom");
        let result: TaskResult<i32> = TaskResult::Err(err);
        let extracted = result.unwrap_err();
        assert_eq!(
            extracted.error_code,
            Some(TaskErrorCode::from(OperationalErrorCode::TaskError)),
        );
    }

    #[test]
    fn ok_extracts_value() {
        let result: TaskResult<i32> = TaskResult::Ok(42);
        assert_eq!(result.ok(), Some(42));
    }

    #[test]
    fn ok_returns_none_for_err() {
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "boom"));
        assert_eq!(result.ok(), None);
    }

    #[test]
    fn map_transforms_ok() {
        let result: TaskResult<i32> = TaskResult::Ok(21);
        let mapped = result.map(|v| v * 2);
        assert_eq!(mapped.unwrap(), 42);
    }

    #[test]
    fn map_preserves_err() {
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "boom"));
        let mapped = result.map(|v| v * 2);
        assert!(mapped.is_err());
    }

    #[test]
    fn and_then_chains_ok() {
        let result: TaskResult<i32> = TaskResult::Ok(21);
        let chained = result.and_then(|v| TaskResult::Ok(v * 2));
        assert_eq!(chained.unwrap(), 42);
    }

    #[test]
    fn and_then_short_circuits_err() {
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "boom"));
        let chained = result.and_then(|v| TaskResult::Ok(v * 2));
        assert!(chained.is_err());
    }

    #[test]
    fn serde_round_trip_ok() {
        let result: TaskResult<i32> = TaskResult::Ok(42);
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<i32> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.unwrap(), 42);
    }

    #[test]
    fn serde_round_trip_err() {
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "boom"));
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<i32> = serde_json::from_str(&json).unwrap();
        let err = back.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OperationalErrorCode::TaskError)),
        );
        assert_eq!(err.message.as_deref(), Some("boom"));
    }

    #[test]
    fn serde_ok_contains_type_tag() {
        let result: TaskResult<i32> = TaskResult::Ok(42);
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"__type\""));
        assert!(json.contains("\"ok\""));
        assert!(json.contains("\"value\""));
    }

    #[test]
    fn serde_ok_with_struct_value() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Output {
            sum: i32,
        }

        let result = TaskResult::Ok(Output { sum: 42 });
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<Output> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.unwrap(), Output { sum: 42 });
    }

    #[test]
    fn serde_ok_with_vec_u8_value() {
        let result = TaskResult::Ok(vec![1u8, 2, 3]);
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<Vec<u8>> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.unwrap(), vec![1u8, 2, 3]);
    }

    #[test]
    fn display_ok_variant() {
        let r: TaskResult<i32> = TaskResult::Ok(42);
        let s = format!("{}", r);
        assert!(s.contains("Ok"), "display should contain 'Ok', got: {}", s);
    }

    #[test]
    fn display_err_variant() {
        let r: TaskResult<i32> = TaskResult::Err(TaskError::new(
            "BAD_INPUT".to_owned(),
            "invalid data".to_owned(),
        ));
        let s = format!("{}", r);
        assert!(
            s.contains("Err"),
            "display should contain 'Err', got: {}",
            s
        );
    }

    // -- Transient/terminal tests --

    #[test]
    fn ok_is_terminal() {
        let r: TaskResult<i32> = TaskResult::Ok(42);
        assert!(r.is_terminal());
        assert!(!r.is_transient());
    }

    #[test]
    fn task_exception_is_terminal() {
        let r: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "boom"));
        assert!(r.is_terminal());
        assert!(!r.is_transient());
    }

    #[test]
    fn wait_timeout_is_transient() {
        let r: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(RetrievalCode::WaitTimeout, "timed out"));
        assert!(r.is_transient());
        assert!(!r.is_terminal());
    }

    #[test]
    fn broker_error_is_transient() {
        let r: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::BrokerError,
            "connection refused",
        ));
        assert!(r.is_transient());
        assert!(!r.is_terminal());
    }

    #[test]
    fn task_not_found_is_transient() {
        let r: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(RetrievalCode::TaskNotFound, "not found"));
        assert!(r.is_transient());
        assert!(!r.is_terminal());
    }

    #[test]
    fn user_code_is_terminal() {
        let r: TaskResult<i32> = TaskResult::Err(TaskError::new(
            "MY_ERROR".to_owned(),
            "custom fail".to_owned(),
        ));
        assert!(r.is_terminal());
        assert!(!r.is_transient());
    }

    #[test]
    fn broker_error_serde_round_trip() {
        let result: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::BrokerError,
            "database connection pool exhausted",
        ));
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<i32> = serde_json::from_str(&json).unwrap();
        assert!(back.is_err());
        assert!(back.is_transient());
    }
}
