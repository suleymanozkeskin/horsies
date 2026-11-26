use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::core::task::result::TaskResult;

mod any_format_text;
mod json_text;
mod object_members;
pub(crate) mod python_loads;
mod strict;
mod strict_read;
pub mod user_value;
pub(crate) use any_format_text::optional_json_text_of_any_format;
pub use json_text::{JsonText, JsonTextRefused};
pub use object_members::object_members;
pub use strict::{to_json_bytes_strict, to_json_value_strict};
pub use strict_read::{
    check_json_bytes, check_json_bytes_with, check_json_text, check_json_text_with, JsonConstant,
    StrictJsonError, SyntaxReason, TextRule, MAX_INT_DIGITS, MAX_JSON_DEPTH,
};
pub use user_value::{
    check_user_value, check_user_value_text, UserValueRefused, MAX_VALUE_JSON_DEPTH,
    STRICT_JSON_ERROR_CAUSE,
};

/// Why a JSON text did not read as a value.
#[derive(Debug, thiserror::Error)]
pub enum JsonReadError {
    /// The text breaks the strict read rule of its kind.
    #[error(transparent)]
    Refused(#[from] StrictJsonError),
    /// The text is strict JSON and does not decode as the declared type.
    #[error(transparent)]
    Decode(#[from] serde_json::Error),
}

/// Serialize a value to a JSON byte vector. Strict: a non-finite float at
/// any depth is an error, never `null` (Python `dumps_json`).
pub fn to_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    to_json_bytes_strict(value)
}

/// Read a stored JSON payload: the strict read rule with [`MAX_JSON_DEPTH`]
/// levels, then the decode.
pub fn from_json_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, JsonReadError> {
    from_json_bytes_with(bytes, TextRule::Stored)
}

/// Read a JSON payload under `rule` ([`TextRule::UserValue`] for a value that
/// user code built: [`MAX_VALUE_JSON_DEPTH`] levels and no engine key), then
/// decode it.
pub fn from_json_bytes_with<T: DeserializeOwned>(
    bytes: &[u8],
    rule: TextRule,
) -> Result<T, JsonReadError> {
    check_json_bytes_with(bytes, rule)?;
    Ok(decode_checked(serde_json::Deserializer::from_slice(bytes))?)
}

/// Serialize a value to a JSON string. Strict, as [`to_json_bytes`].
pub fn to_json_string<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let bytes = to_json_bytes_strict(value)?;
    // serde_json writes UTF-8 only; a failure here would be a serde_json defect.
    String::from_utf8(bytes).map_err(|error| {
        <serde_json::Error as serde::ser::Error>::custom(format!(
            "serialized JSON is not UTF-8: {error}"
        ))
    })
}

/// Read a stored JSON text: the strict read rule with [`MAX_JSON_DEPTH`]
/// levels, then the decode.
pub fn from_json_str<T: DeserializeOwned>(s: &str) -> Result<T, JsonReadError> {
    check_json_text(s)?;
    Ok(decode_checked(serde_json::Deserializer::from_str(s))?)
}

/// Decode a text that the strict reader has checked. Its depth is bounded, so
/// serde_json's own recursion limit (128 levels) is turned off: a value of
/// [`MAX_VALUE_JSON_DEPTH`] levels inside its envelopes reads back (Python
/// `core/codec/json_value.py:54-75`).
fn decode_checked<'de, R, T>(
    mut deserializer: serde_json::Deserializer<R>,
) -> Result<T, serde_json::Error>
where
    R: serde_json::de::Read<'de>,
    T: Deserialize<'de>,
{
    deserializer.disable_recursion_limit();
    let value = T::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

/// Serialize a `TaskResult<T>` to JSON bytes. Strict, as [`to_json_bytes`].
pub fn encode_task_result<T: Serialize>(
    result: &TaskResult<T>,
) -> Result<Vec<u8>, serde_json::Error> {
    to_json_bytes_strict(result)
}

/// Read a `TaskResult<T>` from stored JSON bytes.
pub fn decode_task_result<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<TaskResult<T>, JsonReadError> {
    from_json_bytes(bytes)
}

/// Serialize a `TaskResult<T>` to a JSON string. Strict, as [`to_json_bytes`].
pub fn task_result_to_json_string<T: Serialize>(
    result: &TaskResult<T>,
) -> Result<String, serde_json::Error> {
    to_json_string(result)
}

/// Read a `TaskResult<T>` from a stored JSON string.
pub fn task_result_from_json_str<T: DeserializeOwned>(
    s: &str,
) -> Result<TaskResult<T>, JsonReadError> {
    from_json_str(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        name: String,
        value: i32,
    }

    #[test]
    fn round_trip_bytes() {
        let sample = Sample {
            name: "test".to_owned(),
            value: 42,
        };
        let bytes = to_json_bytes(&sample).unwrap();
        let back: Sample = from_json_bytes(&bytes).unwrap();
        assert_eq!(back, sample);
    }

    #[test]
    fn round_trip_string() {
        let sample = Sample {
            name: "test".to_owned(),
            value: 42,
        };
        let json = to_json_string(&sample).unwrap();
        let back: Sample = from_json_str(&json).unwrap();
        assert_eq!(back, sample);
    }

    #[test]
    fn encode_decode_task_result_ok() {
        let result: TaskResult<Sample> = TaskResult::Ok(Sample {
            name: "ok".to_owned(),
            value: 1,
        });
        let bytes = encode_task_result(&result).unwrap();
        let back: TaskResult<Sample> = decode_task_result(&bytes).unwrap();
        assert!(back.is_ok());
        assert_eq!(
            back.unwrap(),
            Sample {
                name: "ok".to_owned(),
                value: 1
            }
        );
    }

    #[test]
    fn encode_decode_task_result_err() {
        use crate::core::task::error::{OperationalCode, TaskError};
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalCode::TaskException, "fail"));
        let bytes = encode_task_result(&result).unwrap();
        let back: TaskResult<i32> = decode_task_result(&bytes).unwrap();
        assert!(back.is_err());
    }

    /// An object keeps its members in insertion order, as a Python dict does:
    /// a value built in code and a value read from text are written back in
    /// that order, at every depth.
    #[test]
    fn an_object_keeps_its_member_order() {
        let built = serde_json::json!({"zeta": 1, "alpha": {"delta": [], "beta": null}});
        let text = r#"{"zeta":1,"alpha":{"delta":[],"beta":null}}"#;
        assert_eq!(to_json_string(&built).unwrap(), text);
        let read: serde_json::Value = from_json_str(text).unwrap();
        assert_eq!(to_json_string(&read).unwrap(), text);
    }

    /// `shift_remove` keeps the order of the members that stay, as Python's
    /// `dict.pop` does.
    #[test]
    fn a_member_removal_keeps_the_order_of_the_rest() {
        let mut object = serde_json::Map::new();
        for (key, value) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
            let _previous = object.insert(key.to_owned(), serde_json::Value::from(value));
        }
        let _removed = object.shift_remove("b");
        assert_eq!(to_json_string(&object).unwrap(), r#"{"a":1,"c":3,"d":4}"#);
    }

    #[test]
    fn task_result_json_string_round_trip() {
        let result: TaskResult<i32> = TaskResult::Ok(99);
        let json = task_result_to_json_string(&result).unwrap();
        let back: TaskResult<i32> = task_result_from_json_str(&json).unwrap();
        assert_eq!(back.unwrap(), 99);
    }

    /// Python `dumps_json` (`allow_nan=False`): every encoder of the codec
    /// refuses a non-finite float instead of writing `null`.
    #[test]
    fn every_encoder_refuses_a_non_finite_float() {
        #[derive(Serialize)]
        struct Reading {
            value: f64,
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let reading = Reading { value };
            assert!(to_json_bytes(&reading).is_err());
            assert!(to_json_string(&reading).is_err());
            let result: TaskResult<Reading> = TaskResult::Ok(Reading { value });
            assert!(encode_task_result(&result).is_err());
            assert!(task_result_to_json_string(&result).is_err());
        }
        assert_eq!(
            to_json_string(&Reading { value: 1.5 }).unwrap(),
            "{\"value\":1.5}"
        );
    }

    /// Python keeps room for the envelopes: a value of `MAX_VALUE_JSON_DEPTH`
    /// levels reads back inside its result envelope, past serde_json's own
    /// limit of 128 (`clients/python/tests/unit/test_typed_codec.py:1197-1206`).
    #[test]
    fn a_value_of_200_levels_reads_back_in_its_envelope() {
        let mut value = serde_json::json!([1]);
        for _ in 1..MAX_VALUE_JSON_DEPTH {
            value = serde_json::Value::Array(vec![value]);
        }
        let text = task_result_to_json_string(&TaskResult::Ok(value.clone())).unwrap();
        let back: TaskResult<serde_json::Value> = task_result_from_json_str(&text).unwrap();
        assert_eq!(back.unwrap(), value);
    }

    #[test]
    fn a_user_value_text_is_bounded_and_holds_no_engine_key() {
        let deep = format!(
            "{}{}",
            "[".repeat(MAX_VALUE_JSON_DEPTH + 1),
            "]".repeat(MAX_VALUE_JSON_DEPTH + 1)
        );
        assert!(matches!(
            from_json_bytes_with::<serde_json::Value>(deep.as_bytes(), TextRule::UserValue),
            Err(JsonReadError::Refused(StrictJsonError::TooDeep {
                limit: MAX_VALUE_JSON_DEPTH
            }))
        ));
        assert!(matches!(
            from_json_bytes_with::<serde_json::Value>(
                br#"{"a":{"__h_task_result__":true}}"#,
                TextRule::UserValue
            ),
            Err(JsonReadError::Refused(StrictJsonError::EngineKey { .. }))
        ));
        assert!(from_json_bytes::<serde_json::Value>(deep.as_bytes()).is_ok());
    }

    #[test]
    fn task_result_err_round_trip() {
        use crate::core::task::error::{RetrievalCode, TaskError};
        let result: TaskResult<String> =
            TaskResult::Err(TaskError::builtin(RetrievalCode::TaskNotFound, "not found"));
        let json = task_result_to_json_string(&result).unwrap();
        let back: TaskResult<String> = task_result_from_json_str(&json).unwrap();
        assert!(back.is_err());
    }
}
