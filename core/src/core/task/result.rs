use std::collections::BTreeMap;

use serde::de::{DeserializeOwned, Error as _};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use super::error::{
    BuiltInTaskCode, OperationalCode, StoredErrorRefused, TaskError, TaskErrorCode,
};
use crate::core::codec::user_value::{check_user_value_text, MAX_VALUE_JSON_DEPTH};
use crate::core::codec::{JsonText, StrictJsonError};

/// The stored result of a task: its value or its error.
///
/// Two-state discriminated union, the stored form of what a body returns
/// (Python `Result[T, TaskError]`) and of the outcome that a reader gets
/// (Python and Rust `TaskOutcome`):
/// - `Ok(T)`: task completed successfully with a value
/// - `Err(TaskError)`: task failed, with its own error or a library error
///
/// A failed read (timeout, not-found, broker errors) is not a stored result:
/// each client gives it as a read error, apart from the outcome.
///
/// Stored as Python's result envelope (`encode_task_result`, `core/codec/typed.py:798-859`):
/// `{"__h_task_result__": true, "ok": <value>, "err": null}` or
/// `{"__h_task_result__": true, "ok": null, "err": {...}}`. A read refuses any other shape
/// (`validate_task_result_envelope`, `core/codec/typed.py:1133-1180`): not an object, the marker missing or not
/// `true`, a missing or an extra key, or both slots set. A non-null `err` slot is an error; otherwise the `ok`
/// slot is the value, `null` included, under the rules of a user value (no engine key, at most 200 levels).
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

/// The envelope is taken as raw text and read by [`read_task_result_text`]: the depth bound is the core's 300
/// levels, not the 128 of the caller's deserializer, so a value of 200 levels reads back inside its envelopes
/// (Python `core/codec/json_value.py:54-75`). The `ok` slot is checked under the rules of a user value and `T`
/// decodes from the slot's own text, so an integer outside the `i64` and `u64` range reads as `u128` or `i128`.
///
/// A deserializer that buffers the value (`#[serde(flatten)]`, an untagged or an internally tagged enum) gives
/// no raw text, and the read fails.
impl<'de, T: DeserializeOwned> Deserialize<'de> for TaskResult<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw_text = Box::<RawValue>::deserialize(deserializer)?;
        match read_task_result_text(raw_text.get()).map_err(D::Error::custom)? {
            TaskResultText::Err(error) => Ok(Self::Err(error)),
            TaskResultText::Ok(ok_slot) => {
                // Python `decode_value` on the ok slot: the rules of a user value
                // (`core/codec/typed.py:590-629`).
                let value = JsonText::of_checked_part(ok_slot);
                check_user_value_text(&value, MAX_VALUE_JSON_DEPTH).map_err(D::Error::custom)?;
                value.decode::<T>().map(Self::Ok).map_err(D::Error::custom)
            }
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
        ("null", err_text) => Ok(TaskResultText::Err(TaskError::from_stored_text(err_text)?)),
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
    /// `OperationalCode::DatabaseError`. All other errors are terminal.
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
    /// Transient errors: `RetrievalCode::*` and `OperationalCode::DatabaseError`.
    pub fn is_transient(&self) -> bool {
        matches!(
            &self.error_code,
            TaskErrorCode::BuiltIn(BuiltInTaskCode::Retrieval(_))
                | TaskErrorCode::BuiltIn(BuiltInTaskCode::Operational(
                    OperationalCode::DatabaseError,
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
    use crate::core::task::error::{OperationalCode, RetrievalCode, TaskErrorCode};

    #[test]
    fn ok_predicates() {
        let result: TaskResult<i32> = TaskResult::Ok(42);
        assert!(result.is_ok());
        assert!(!result.is_err());
    }

    #[test]
    fn err_predicates() {
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "boom"));
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
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "boom"));
        result.unwrap();
    }

    #[test]
    fn unwrap_err_ok() {
        let err = TaskError::builtin(OperationalCode::TaskException, "boom");
        let result: TaskResult<i32> = TaskResult::Err(err);
        let extracted = result.unwrap_err();
        assert_eq!(
            extracted.error_code,
            TaskErrorCode::from(OperationalCode::TaskException),
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
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "boom"));
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
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "boom"));
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
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "boom"));
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
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "boom"));
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<i32> = serde_json::from_str(&json).unwrap();
        let err = back.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OperationalCode::TaskException),
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

    /// Python `decode_value` on the ok slot: an engine key at any depth, or a value deeper than 200 levels,
    /// does not read (`clients/python/tests/unit/test_typed_codec.py:474-610`).
    #[test]
    fn an_ok_slot_follows_the_user_value_rules() {
        for text in [
            r#"{"__h_task_result__":true,"ok":{"__h_task_result__":true},"err":null}"#,
            r#"{"__h_task_result__":true,"ok":[{"__builtin_task_code__":"X"}],"err":null}"#,
            r#"{"__h_task_result__":true,"ok":{"a":{"__horsies_x":1}},"err":null}"#,
        ] {
            assert!(
                serde_json::from_str::<TaskResult<serde_json::Value>>(text).is_err(),
                "{text} must be refused"
            );
        }
        let depth = crate::core::codec::MAX_VALUE_JSON_DEPTH + 1;
        let deep = format!(
            r#"{{"__h_task_result__":true,"ok":{}{},"err":null}}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        );
        assert!(serde_json::from_str::<TaskResult<serde_json::Value>>(&deep).is_err());
    }

    /// The value of the ok slot and the `data` of the err slot decode from their own text: an integer outside
    /// the `i64` and `u64` range reads as `u128` or `i128`, and a read that only checks the envelope
    /// (`IgnoredAny`) takes it.
    #[test]
    fn a_value_outside_i64_and_u64_reads_from_its_text() {
        let wide: TaskResult<u128> = serde_json::from_str(
            r#"{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null}"#,
        )
        .unwrap();
        assert!(matches!(wide, TaskResult::Ok(u128::MAX)));
        let below: TaskResult<i128> = serde_json::from_str(
            r#"{"__h_task_result__":true,"ok":-9223372036854775809,"err":null}"#,
        )
        .unwrap();
        assert_eq!(below.unwrap(), i128::from(i64::MIN) - 1);
        let narrow = serde_json::from_str::<TaskResult<u64>>(
            r#"{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null}"#,
        );
        assert!(narrow.is_err());
        let err_text = r#"{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":"X","data":[340282366920938463463374607431768211455,-9223372036854775809],"message":null}}"#;
        let error = serde_json::from_str::<TaskResult<serde::de::IgnoredAny>>(err_text)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.data.as_ref().map(JsonText::get),
            Some("[340282366920938463463374607431768211455,-9223372036854775809]")
        );
        assert_eq!(
            serde_json::to_string(&TaskResult::<()>::Err(error)).unwrap(),
            err_text
        );
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

    /// One text per refusal of the text reader. `Undecodable` has no text:
    /// strict JSON always decodes.
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

    /// Python `test_task_error_stored_decode.py::TestRecoveryReaders::test_finalize_records_a_refused_result_as_undecodable`
    /// (its `_NOT_A_TASK_ERROR` cases): an envelope whose err slot is not a
    /// task error is refused, and the text of the refusal, which the outcome
    /// writer stores as the diagnosis of `WORKER_SERIALIZATION_ERROR`
    /// (`worker/executor/slots/report_tests.rs`), names the cause.
    #[test]
    fn an_err_slot_that_is_not_a_task_error_is_refused_with_its_cause() {
        for (err_slot, cause) in [
            (r#"{"nope":1}"#, "nope"),
            (
                r#"{"error_code":"MY_CODE","message":"m","data":null,"exception":null,"nope":1}"#,
                "nope",
            ),
            ("{}", "error_code"),
            (r#"{"message":"m"}"#, "error_code"),
            (
                r#"{"error_code":null,"message":"m","data":null,"exception":null}"#,
                "error_code",
            ),
        ] {
            let text = format!(r#"{{"__h_task_result__":true,"ok":null,"err":{err_slot}}}"#);
            match read_task_result_text(&text) {
                Err(refused @ TaskResultTextRefused::ErrorRefused(_)) => {
                    assert!(refused.to_string().contains(cause), "{text}: {refused}");
                }
                other => panic!("{text} must be refused for its err slot, got {other:?}"),
            }
        }
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
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "boom"));
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
            OperationalCode::DatabaseError,
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
            OperationalCode::DatabaseError,
            "database connection pool exhausted",
        ));
        let json = serde_json::to_string(&result).unwrap();
        let back: TaskResult<i32> = serde_json::from_str(&json).unwrap();
        assert!(back.is_err());
        assert!(back.is_transient());
    }
}
