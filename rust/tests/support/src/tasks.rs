/// Sample task result factories for tests.
///
/// Mirrors Python's integration conftest task factories.
/// Since Rust tasks are compiled, these produce `TaskResult` values
/// directly rather than registering task functions.
use horsies::{TaskError, TaskResult};

/// Produce an Ok result with the value doubled.
pub fn simple_task_result(value: i32) -> TaskResult<serde_json::Value> {
    TaskResult::Ok(serde_json::json!(value * 2))
}

/// Produce an Err result with a deliberate failure.
pub fn failing_task_result() -> TaskResult<serde_json::Value> {
    TaskResult::Err(TaskError::user("DELIBERATE_FAIL", "Test failure"))
}

/// Produce an Ok result with the value unchanged.
pub fn identity_task_result(value: serde_json::Value) -> TaskResult<serde_json::Value> {
    TaskResult::Ok(value)
}

/// Produce an Ok result with the value incremented by 10.
pub fn args_receiver_result(upstream_value: i32) -> TaskResult<serde_json::Value> {
    TaskResult::Ok(serde_json::json!(upstream_value + 10))
}

/// Produce an Ok recovery result (returns 999 when upstream failed).
pub fn recovery_task_result(
    upstream_ok: bool,
    upstream_value: i32,
) -> TaskResult<serde_json::Value> {
    if upstream_ok {
        TaskResult::Ok(serde_json::json!(upstream_value))
    } else {
        TaskResult::Ok(serde_json::json!(999))
    }
}

/// Produce an Ok result with the sum of two values.
pub fn multi_args_receiver_result(a: i32, b: i32) -> TaskResult<serde_json::Value> {
    TaskResult::Ok(serde_json::json!(a + b))
}

/// Serialize a TaskResult to JSON string (for DB insertion).
pub fn result_json(result: &TaskResult<serde_json::Value>) -> String {
    serde_json::to_string(result).expect("failed to serialize TaskResult")
}
