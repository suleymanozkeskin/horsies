//! Retry tasks (Python `clients/python/tests/e2e/tasks/retry.py`).

use horsies::{task, RetryPolicy, TaskError, UserErrorCode};

use crate::markers;

/// The codes of the retry tasks' own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
pub enum RetryCode {
    /// Retried by every policy of this module.
    Transient,
    /// Retried by `e2e_retry_error_code`.
    RetryableError,
    /// A marker operation failed in the child.
    MarkerFailed,
}

fn fixed(intervals: Vec<u32>) -> RetryPolicy {
    RetryPolicy::fixed(intervals, false).expect("a fixed policy of 1 s intervals is valid")
}

/// Python `e2e_retry_exhausted`: always `TRANSIENT`; three retries.
#[task(
    "e2e_retry_exhausted",
    retry_policy = fixed(vec![1, 1, 1]),
    auto_retry_for = ["TRANSIENT"]
)]
pub async fn retry_exhausted() -> Result<String, TaskError<RetryCode>> {
    Err(TaskError::user(RetryCode::Transient, "always fails"))
}

/// Python `e2e_retry_success`: `TRANSIENT` on the first two attempts of
/// `token`, then `succeeded_on_attempt_3`. The attempts are counted with
/// marker files, which every child of the worker sees.
#[task(
    "e2e_retry_success",
    retry_policy = fixed(vec![1, 1, 1]),
    auto_retry_for = ["TRANSIENT"]
)]
pub async fn retry_success(token: String) -> Result<String, TaskError<RetryCode>> {
    let attempt = markers::next_attempt(&token)
        .map_err(|cause| TaskError::user(RetryCode::MarkerFailed, cause.to_string()))?;
    match attempt < 3 {
        true => Err(TaskError::user(
            RetryCode::Transient,
            format!("attempt {attempt}"),
        )),
        false => Ok(format!("succeeded_on_attempt_{attempt}")),
    }
}

/// Python `e2e_exponential`: always `TRANSIENT`; three retries 1 s, 2 s and
/// 4 s apart.
#[task(
    "e2e_exponential",
    retry_policy = RetryPolicy::exponential(1, 3, false).expect("a valid exponential policy"),
    auto_retry_for = ["TRANSIENT"]
)]
pub async fn exponential() -> Result<String, TaskError<RetryCode>> {
    Err(TaskError::user(RetryCode::Transient, "fail"))
}

/// Python `e2e_retry_error_code`: always `RETRYABLE_ERROR`; one retry.
#[task(
    "e2e_retry_error_code",
    retry_policy = fixed(vec![1]),
    auto_retry_for = ["RETRYABLE_ERROR"]
)]
pub async fn retry_error_code() -> Result<String, TaskError<RetryCode>> {
    Err(TaskError::user(
        RetryCode::RetryableError,
        "Retryable error",
    ))
}

/// The old Rust `e2e_no_retry_code`: a retry policy that names
/// `TRANSIENT`, and an error with another code; no retry.
#[task(
    "e2e_retry_other_code",
    retry_policy = fixed(vec![1, 1]),
    auto_retry_for = ["TRANSIENT"]
)]
pub async fn retry_other_code() -> Result<String, TaskError<RetryCode>> {
    Err(TaskError::user(
        RetryCode::RetryableError,
        "not in auto_retry_for",
    ))
}

/// Python `e2e_retry_exhausted` with the 60 s intervals of
/// `test_retry_blocked_by_good_until_expiry`: a retry would start after the
/// deadline of a send with a near `good_until`.
#[task(
    "e2e_retry_long_interval",
    retry_policy = fixed(vec![60, 60, 60]),
    auto_retry_for = ["TRANSIENT"]
)]
pub async fn retry_long_interval() -> Result<String, TaskError<RetryCode>> {
    Err(TaskError::user(RetryCode::Transient, "always fails"))
}
