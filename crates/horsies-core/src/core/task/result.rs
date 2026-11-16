use std::collections::BTreeMap;

use serde::de::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use super::error::{
    BuiltInTaskCode, OperationalErrorCode, StoredErrorRefused, TaskError, TaskErrorCode,
};
use crate::core::codec::{JsonReadError, StrictJsonError};

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

/// A task result envelope read from its text. The `ok` slot keeps the JSON
/// text it was written with, `null` included; the `err` slot is read as a
/// [`TaskError`].
#[derive(Debug)]
pub enum TaskResultText<'a> {
    Ok(&'a RawValue),
    Err(TaskError),
}

/// Why a text does not read as a task result envelope. The rules are those
/// of the reader of [`TaskResult`] (Python `validate_task_result_envelope`,
/// `core/codec/typed.py:1133-1180`).
#[derive(Debug, thiserror::Error)]
pub enum TaskResultTextRefused {
    /// The text breaks the strict read rule: syntax, depth, a repeated key,
    /// a non-finite number.
    #[error("TaskResult envelope is not strict JSON: {0}")]
    NotStrictJson(#[from] StrictJsonError),
    #[error("TaskResult envelope must be a JSON object")]
    NotAnObject,
    /// Strict JSON that serde_json does not decode as an object of raw
    /// values: the two readers disagree on the text.
    #[error("TaskResult envelope does not decode: {0}")]
    Undecodable(serde_json::Error),
    #[error("TaskResult envelope missing \"__h_task_result__\" marker (or marker is not true)")]
    MarkerMissing,
    #[error("unexpected key {key:?} in TaskResult envelope")]
    ExtraKey { key: String },
    #[error("TaskResult envelope missing a required key (ok, err)")]
    MissingSlot,
    #[error("TaskResult envelope has both ok and err populated; exactly one must be set")]
    BothSlotsSet,
    /// The `err` slot does not decode as a JSON value: the two readers
    /// disagree on the text.
    #[error("the err slot of the TaskResult envelope does not decode: {0}")]
    ErrorSlotUndecodable(JsonReadError),
    #[error("the err slot of the TaskResult envelope is not a task error: {0}")]
    ErrorRefused(#[from] StoredErrorRefused),
}

/// Read a task result envelope from its text, with the refusal rules of the
/// reader of [`TaskResult`]: strict JSON, an object, the marker `true`, the
/// keys `ok` and `err` and no other, not both slots set. A non-null `err` is
/// the error; otherwise `ok` is the value, as its text. Pure.
pub fn read_task_result_text(text: &str) -> Result<TaskResultText<'_>, TaskResultTextRefused> {
    crate::core::codec::check_json_text(text)?;
    if !text.trim_start().starts_with('{') {
        return Err(TaskResultTextRefused::NotAnObject);
    }
    let mut slots = envelope_slots(text).map_err(TaskResultTextRefused::Undecodable)?;
    match slots.remove(TASK_RESULT_ENVELOPE_KEY).map(RawValue::get) {
        Some("true") => {}
        Some(_) | None => return Err(TaskResultTextRefused::MarkerMissing),
    }
    if let Some(key) = slots
        .keys()
        .find(|key| !matches!(key.as_str(), OK_SLOT | ERR_SLOT))
    {
        return Err(TaskResultTextRefused::ExtraKey { key: key.clone() });
    }
    let (Some(ok_slot), Some(err_slot)) = (slots.remove(OK_SLOT), slots.remove(ERR_SLOT)) else {
        return Err(TaskResultTextRefused::MissingSlot);
    };
    match (ok_slot.get(), err_slot.get()) {
        (_, "null") => Ok(TaskResultText::Ok(ok_slot)),
        ("null", err_text) => {
            let stored: serde_json::Value = crate::core::codec::from_json_str(err_text)
                .map_err(TaskResultTextRefused::ErrorSlotUndecodable)?;
            Ok(TaskResultText::Err(TaskError::from_stored_value(stored)?))
        }
        (_, _) => Err(TaskResultTextRefused::BothSlotsSet),
    }
}

/// The keys of an envelope object and the raw text of each value. The
/// strict reader has bounded the depth, so serde_json's own recursion limit
/// is off, as in `crate::core::codec::from_json_str`.
fn envelope_slots(text: &str) -> Result<BTreeMap<String, &RawValue>, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    deserializer.disable_recursion_limit();
    let slots = BTreeMap::<String, &RawValue>::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(slots)
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
        let result: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::TaskException,
            "boom",
        ));
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
        let result: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::TaskException,
            "boom",
        ));
        result.unwrap();
    }

    #[test]
    fn unwrap_err_ok() {
        let err = TaskError::builtin(OperationalErrorCode::TaskException, "boom");
        let result: TaskResult<i32> = TaskResult::Err(err);
        let extracted = result.unwrap_err();
        assert_eq!(
            extracted.error_code,
            TaskErrorCode::from(OperationalErrorCode::TaskException),
        );
    }

    #[test]
    fn ok_extracts_value() {
        let result: TaskResult<i32> = TaskResult::Ok(42);
        assert_eq!(result.ok(), Some(42));
    }

    #[test]
    fn ok_returns_none_for_err() {
        let result: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::TaskException,
            "boom",
        ));
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
        let result: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::TaskException,
            "boom",
        ));
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
        let result: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::TaskException,
            "boom",
        ));
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
        let result: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::TaskException,
            "boom",
        ));
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<i32> = serde_json::from_str(&json).unwrap();
        let err = back.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OperationalErrorCode::TaskException),
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

    // The envelope texts below are Python 0.7.0 `dumps_json(encode_task_result(..))`.

    /// The `ok` slot keeps the text it was written with; a `null` value is a
    /// value.
    #[test]
    fn the_text_reader_keeps_the_text_of_the_value() {
        match read_task_result_text(r#"{"__h_task_result__":true,"ok":{"b":1,"a":[2]},"err":null}"#)
        {
            Ok(TaskResultText::Ok(value)) => assert_eq!(value.get(), r#"{"b":1,"a":[2]}"#),
            other => panic!("expected a value, got {other:?}"),
        }
        match read_task_result_text(r#"{"__h_task_result__":true,"ok":null,"err":null}"#) {
            Ok(TaskResultText::Ok(value)) => assert_eq!(value.get(), "null"),
            other => panic!("expected the null value, got {other:?}"),
        }
    }

    #[test]
    fn the_text_reader_reads_the_error_slot_as_a_task_error() {
        let read = read_task_result_text(
            r#"{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":{"__builtin_task_code__":"TASK_TIMEOUT"},"data":null,"message":"late"}}"#,
        );
        match read {
            Ok(TaskResultText::Err(error)) => {
                assert_eq!(
                    error.error_code,
                    TaskErrorCode::BuiltIn(BuiltInTaskCode::Outcome(
                        crate::core::task::error::OutcomeCode::TaskTimeout
                    ))
                );
                assert_eq!(error.message.as_deref(), Some("late"));
            }
            other => panic!("expected a task error, got {other:?}"),
        }
    }

    /// One text per refusal of the text reader. `Undecodable` and
    /// `ErrorSlotUndecodable` have no text: strict JSON always decodes.
    #[test]
    fn the_text_reader_refuses_what_the_value_reader_refuses() {
        let refusal = |text: &str| match read_task_result_text(text) {
            Ok(read) => panic!("{text} must be refused, read {read:?}"),
            Err(refused) => refused,
        };
        assert!(matches!(
            refusal(r#"{"__h_task_result__":true,"ok":1,"ok":2,"err":null}"#),
            TaskResultTextRefused::NotStrictJson(_)
        ));
        assert!(matches!(
            refusal(r#"{"__h_task_result__":true,"ok":NaN,"err":null}"#),
            TaskResultTextRefused::NotStrictJson(_)
        ));
        assert!(matches!(refusal("[1]"), TaskResultTextRefused::NotAnObject));
        for text in [
            r#"{"__type":"ok","value":1}"#,
            r#"{"__h_task_result__":false,"ok":1,"err":null}"#,
            r#"{"ok":1,"err":null}"#,
        ] {
            assert!(
                matches!(refusal(text), TaskResultTextRefused::MarkerMissing),
                "{text}"
            );
        }
        assert!(matches!(
            refusal(r#"{"__h_task_result__":true,"ok":1,"err":null,"extra":1}"#),
            TaskResultTextRefused::ExtraKey { ref key } if key == "extra"
        ));
        assert!(matches!(
            refusal(r#"{"__h_task_result__":true,"ok":1}"#),
            TaskResultTextRefused::MissingSlot
        ));
        assert!(matches!(
            refusal(r#"{"__h_task_result__":true,"ok":1,"err":{"error_code":"X","message":"m"}}"#),
            TaskResultTextRefused::BothSlotsSet
        ));
        assert!(matches!(
            refusal(r#"{"__h_task_result__":true,"ok":null,"err":{"message":"m"}}"#),
            TaskResultTextRefused::ErrorRefused(_)
        ));
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
        let r: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::TaskException,
            "boom",
        ));
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
