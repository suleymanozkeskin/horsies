use serde::{de::DeserializeOwned, Serialize};

use crate::core::task::result::TaskResult;

mod strict;
pub use strict::{to_json_bytes_strict, to_json_value_strict};

/// Serialize a value to a JSON byte vector.
pub fn to_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(value)
}

/// Deserialize a value from a JSON byte slice.
pub fn from_json_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Serialize a value to a JSON string.
pub fn to_json_string<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}

/// Deserialize a value from a JSON string.
pub fn from_json_str<T: DeserializeOwned>(s: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(s)
}

/// Serialize a `TaskResult<T>` to JSON bytes.
pub fn encode_task_result<T: Serialize>(
    result: &TaskResult<T>,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(result)
}

/// Deserialize a `TaskResult<T>` from JSON bytes.
pub fn decode_task_result<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<TaskResult<T>, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Serialize a `TaskResult<T>` to a JSON string.
pub fn task_result_to_json_string<T: Serialize>(
    result: &TaskResult<T>,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(result)
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
