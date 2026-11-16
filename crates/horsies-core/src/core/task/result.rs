use serde::de::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
/// Stored as Python's result envelope (`encode_task_result`, `core/codec/typed.py:798-859`):
/// `{"__h_task_result__": true, "ok": <value>, "err": null}` or
/// `{"__h_task_result__": true, "ok": null, "err": {...}}`. A read refuses any other shape
/// (`validate_task_result_envelope`, `core/codec/typed.py:1133-1180`): not an object, the marker missing or not
/// `true`, a missing or an extra key, or both slots set. A non-null `err` slot is an error; otherwise the `ok`
/// slot is the value, `null` included.
#[derive(Debug, Clone)]
pub enum TaskResult<T> {
    Ok(T),
    Err(TaskError),
}

/// The marker key of a stored task result (Python `_TASK_RESULT_ENVELOPE_KEY`).
pub const TASK_RESULT_ENVELOPE_KEY: &str = "__h_task_result__";

const OK_SLOT: &str = "ok";
const ERR_SLOT: &str = "err";

impl<T: Serialize> Serialize for TaskResult<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut envelope = serializer.serialize_map(Some(3))?;
        envelope.serialize_entry(TASK_RESULT_ENVELOPE_KEY, &true)?;
        match self {
            Self::Ok(value) => {
                envelope.serialize_entry(OK_SLOT, value)?;
                envelope.serialize_entry(ERR_SLOT, &serde_json::Value::Null)?;
            }
            Self::Err(error) => {
                envelope.serialize_entry(OK_SLOT, &serde_json::Value::Null)?;
                envelope.serialize_entry(ERR_SLOT, error)?;
            }
        }
        envelope.end()
    }
}

/// The envelope is taken as raw text and read by the core's strict reader (`crate::core::codec::from_json_str`):
/// the depth bound is the core's 300 levels, not the 128 of the caller's deserializer, so a value of 200 levels
/// reads back inside its envelopes (Python `core/codec/json_value.py:54-75`).
impl<'de, T: Deserialize<'de>> Deserialize<'de> for TaskResult<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw_text = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        let raw: serde_json::Value =
            crate::core::codec::from_json_str(raw_text.get()).map_err(D::Error::custom)?;
        let serde_json::Value::Object(mut envelope) = raw else {
            return Err(D::Error::custom(
                "TaskResult envelope must be a JSON object",
            ));
        };
        match envelope.get(TASK_RESULT_ENVELOPE_KEY) {
            Some(serde_json::Value::Bool(true)) => {}
            _ => {
                return Err(D::Error::custom(format!(
                    "TaskResult envelope missing {TASK_RESULT_ENVELOPE_KEY:?} marker (or marker is not true)"
                )))
            }
        }
        if let Some(extra) = envelope
            .keys()
            .find(|key| !matches!(key.as_str(), TASK_RESULT_ENVELOPE_KEY | OK_SLOT | ERR_SLOT))
        {
            return Err(D::Error::custom(format!(
                "unexpected key {extra:?} in TaskResult envelope"
            )));
        }
        let (Some(ok_slot), Some(err_slot)) = (envelope.remove(OK_SLOT), envelope.remove(ERR_SLOT))
        else {
            return Err(D::Error::custom(
                "TaskResult envelope missing a required key (ok, err)",
            ));
        };
        match (ok_slot, err_slot) {
            (ok_slot, serde_json::Value::Null) => T::deserialize(ok_slot)
                .map(Self::Ok)
                .map_err(D::Error::custom),
            (serde_json::Value::Null, err_slot) => TaskError::from_stored_value(err_slot)
                .map(Self::Err)
                .map_err(D::Error::custom),
            (_, _) => Err(D::Error::custom(
                "TaskResult envelope has both ok and err populated; exactly one must be set",
            )),
        }
    }
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
            TaskErrorCode::BuiltIn(BuiltInTaskCode::Retrieval(_))
                | TaskErrorCode::BuiltIn(BuiltInTaskCode::Operational(
                    OperationalErrorCode::BrokerError,
                ))
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
            TaskErrorCode::from(OperationalErrorCode::TaskError),
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
            TaskErrorCode::from(OperationalErrorCode::TaskError),
        );
        assert_eq!(err.message.as_deref(), Some("boom"));
    }

    #[test]
    fn stored_text_is_the_python_envelope() {
        let ok: TaskResult<serde_json::Value> = TaskResult::Ok(serde_json::json!({"total": 3}));
        assert_eq!(
            serde_json::to_string(&ok).unwrap(),
            r#"{"__h_task_result__":true,"ok":{"total":3},"err":null}"#
        );
        let unit: TaskResult<()> = TaskResult::Ok(());
        assert_eq!(
            serde_json::to_string(&unit).unwrap(),
            r#"{"__h_task_result__":true,"ok":null,"err":null}"#
        );
        let err: TaskResult<i32> =
            TaskResult::Err(TaskError::new("RATE_LIMITED", "slow down").unwrap());
        assert_eq!(
            serde_json::to_string(&err).unwrap(),
            r#"{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":"RATE_LIMITED","data":null,"message":"slow down"}}"#
        );
    }

    #[test]
    fn python_envelopes_read_back() {
        let ok: TaskResult<i32> =
            serde_json::from_str(r#"{"__h_task_result__":true,"ok":7,"err":null}"#).unwrap();
        assert!(matches!(ok, TaskResult::Ok(7)));
        let null_ok: TaskResult<Option<i32>> =
            serde_json::from_str(r#"{"__h_task_result__":true,"ok":null,"err":null}"#).unwrap();
        assert!(matches!(null_ok, TaskResult::Ok(None)));
        let err: TaskResult<i32> = serde_json::from_str(
            r#"{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":{"__builtin_task_code__":"TASK_TIMEOUT"},"data":null,"message":"late"}}"#,
        )
        .unwrap();
        assert_eq!(err.unwrap_err().error_code.to_string(), "TASK_TIMEOUT");
    }

    /// An err slot is read by the error contract: a missing code, an unknown key or a plain built-in name is
    /// refused (Python `decode_task_error`).
    #[test]
    fn an_err_slot_follows_the_error_contract() {
        for text in [
            r#"{"__h_task_result__":true,"ok":null,"err":{"message":"m"}}"#,
            r#"{"__h_task_result__":true,"ok":null,"err":{"error_code":"X","extra":1}}"#,
            r#"{"__h_task_result__":true,"ok":null,"err":{"error_code":"TASK_TIMEOUT"}}"#,
            r#"{"__h_task_result__":true,"ok":null,"err":{"error_code":"X","data":{"__h_x":1}}}"#,
        ] {
            assert!(
                serde_json::from_str::<TaskResult<serde_json::Value>>(text).is_err(),
                "{text} must be refused"
            );
        }
    }

    /// The core's strict reader reads the envelope: a repeated key is refused, and a value deeper than
    /// serde_json's own limit of 128 reads back inside a caller's plain `serde_json::from_str`.
    #[test]
    fn the_envelope_is_read_by_the_strict_reader() {
        assert!(serde_json::from_str::<TaskResult<i32>>(
            r#"{"__h_task_result__":true,"ok":1,"ok":2,"err":null}"#
        )
        .is_err());
        let mut value = serde_json::json!([1]);
        for _ in 1..crate::core::codec::MAX_VALUE_JSON_DEPTH {
            value = serde_json::Value::Array(vec![value]);
        }
        let text = serde_json::to_string(&TaskResult::Ok(value.clone())).unwrap();
        let back: TaskResult<serde_json::Value> = serde_json::from_str(&text).unwrap();
        assert_eq!(back.unwrap(), value);
    }

    #[test]
    fn malformed_envelopes_are_refused() {
        for text in [
            r#"{"__type":"ok","value":1}"#,
            r#"[1]"#,
            r#"{"__h_task_result__":false,"ok":1,"err":null}"#,
            r#"{"ok":1,"err":null}"#,
            r#"{"__h_task_result__":true,"ok":1}"#,
            r#"{"__h_task_result__":true,"ok":1,"err":null,"extra":1}"#,
            r#"{"__h_task_result__":true,"ok":1,"err":{"error_code":"X","message":"m"}}"#,
        ] {
            assert!(
                serde_json::from_str::<TaskResult<serde_json::Value>>(text).is_err(),
                "{text} must be refused"
            );
        }
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
        let r: TaskResult<i32> = TaskResult::Err(
            TaskError::new("BAD_INPUT".to_owned(), "invalid data".to_owned()).unwrap(),
        );
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
        let r: TaskResult<i32> = TaskResult::Err(
            TaskError::new("MY_ERROR".to_owned(), "custom fail".to_owned()).unwrap(),
        );
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
