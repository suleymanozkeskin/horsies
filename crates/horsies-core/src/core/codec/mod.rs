use serde::{de::DeserializeOwned, Serialize};

use crate::core::task::result::TaskResult;

mod strict;
mod strict_read;
pub use strict::{to_json_bytes_strict, to_json_value_strict};
pub use strict_read::{
    check_json_bytes, check_json_text, StrictJsonError, SyntaxReason, MAX_INT_DIGITS,
    MAX_JSON_DEPTH,
};

/// Serialize a value to a JSON byte vector. Strict: a non-finite float at
/// any depth is an error, never `null` (Python `dumps_json`).
pub fn to_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    to_json_bytes_strict(value)
}

/// Deserialize a value from a JSON byte slice.
pub fn from_json_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(bytes)
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

/// Deserialize a value from a JSON string.
pub fn from_json_str<T: DeserializeOwned>(s: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(s)
}

/// Serialize a `TaskResult<T>` to JSON bytes. Strict, as [`to_json_bytes`].
pub fn encode_task_result<T: Serialize>(
    result: &TaskResult<T>,
) -> Result<Vec<u8>, serde_json::Error> {
    to_json_bytes_strict(result)
}

/// Deserialize a `TaskResult<T>` from JSON bytes.
pub fn decode_task_result<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<TaskResult<T>, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Serialize a `TaskResult<T>` to a JSON string. Strict, as [`to_json_bytes`].
pub fn task_result_to_json_string<T: Serialize>(
    result: &TaskResult<T>,
) -> Result<String, serde_json::Error> {
    to_json_string(result)
}

/// Deserialize a `TaskResult<T>` from a JSON string.
pub fn task_result_from_json_str<T: DeserializeOwned>(
    s: &str,
) -> Result<TaskResult<T>, serde_json::Error> {
    serde_json::from_str(s)
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
        use crate::core::task::error::{OperationalErrorCode, TaskError};
        let result: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OperationalErrorCode::TaskError, "fail"));
        let bytes = encode_task_result(&result).unwrap();
        let back: TaskResult<i32> = decode_task_result(&bytes).unwrap();
        assert!(back.is_err());
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
