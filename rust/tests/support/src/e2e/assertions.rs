/// Assertion helpers for e2e tests.
///
/// Mirrors Python's `tests/e2e/helpers/assertions.py`.
use horsies::{TaskError, TaskResult};

/// Assert a TaskResult is Ok and optionally check the value.
pub fn assert_ok(result: &TaskResult<serde_json::Value>, expected: Option<&serde_json::Value>) {
    assert!(result.is_ok(), "expected Ok, got: {:?}", result);
    if let Some(expected_val) = expected {
        match result {
            TaskResult::Ok(val) => assert_eq!(val, expected_val),
            _ => unreachable!(),
        }
    }
}

/// Assert a TaskResult is Err and optionally check the error code string.
pub fn assert_err(result: &TaskResult<serde_json::Value>, expected_code: Option<&str>) {
    assert!(result.is_err(), "expected Err, got: {:?}", result);
    if let Some(code) = expected_code {
        match result {
            TaskResult::Err(err) => {
                let actual = err.error_code.as_ref().map(|c| c.to_string());
                assert_eq!(
                    actual.as_deref(),
                    Some(code),
                    "error code mismatch: expected {}, got {:?}",
                    code,
                    actual,
                );
            }
            _ => unreachable!(),
        }
    }
}

/// Assert a TaskResult is Err with a specific TaskError.
pub fn assert_task_error(result: &TaskResult<serde_json::Value>, check: impl FnOnce(&TaskError)) {
    match result {
        TaskResult::Err(err) => check(err),
        _ => panic!("expected Err, got: {:?}", result),
    }
}
