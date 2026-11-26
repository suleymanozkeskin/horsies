//! Task fundamentals (Python `clients/python/tests/e2e/tasks/basic.py`). Each task is
//! `ISOLATED`, the default class, on the default queue.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::time::Duration;

use horsies::{
    blocking_task, task, OperationalCode, ReadError, TaskError, TaskRuntime, UserErrorCode,
    WaitLimit,
};
use serde::{Deserialize, Serialize};

use crate::markers;

/// The codes of the basic tasks' own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
pub enum BasicCode {
    /// `e2e_error`.
    DeliberateError,
    /// `e2e_no_retry`.
    Permanent,
    /// `e2e_idempotent` ran twice for one token.
    DoubleExecution,
    /// A marker operation failed in the child.
    MarkerFailed,
    /// A send from a task body failed.
    SendFailed,
}

/// The longest result wait that `e2e_wait_inside_task` tries.
const INSIDE_WAIT: Duration = Duration::from_secs(1);

/// Python `e2e_healthcheck`: the readiness task of a worker with an isolated
/// child.
#[task("e2e_healthcheck")]
pub async fn healthcheck() -> Result<String, TaskError<Infallible>> {
    Ok("ready".to_owned())
}

/// Python `e2e_simple`.
#[task("e2e_simple")]
pub async fn simple(x: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(x * 2)
}

/// Python `e2e_kwargs`. A parameter that may be left out is an `Option`;
/// the body applies Python's default.
#[task("e2e_kwargs")]
pub async fn kwargs(
    required: i64,
    optional: Option<String>,
    multiplier: Option<i64>,
) -> Result<String, TaskError<Infallible>> {
    let product = required * multiplier.unwrap_or(1);
    let optional = optional.unwrap_or_else(|| "default".to_owned());
    Ok(format!("{product}_{optional}"))
}

/// Python `e2e_error`: a returned error with a code, a message and data.
#[task("e2e_error")]
pub async fn error() -> Result<i64, TaskError<BasicCode>> {
    Err(
        TaskError::user(BasicCode::DeliberateError, "This is intentional")
            .with_data(serde_json::json!({ "key": "value" })),
    )
}

/// Python `e2e_no_retry`: a code that no retry policy names.
#[task("e2e_no_retry")]
pub async fn no_retry() -> Result<String, TaskError<BasicCode>> {
    Err(TaskError::user(BasicCode::Permanent, "not retryable"))
}

/// Python `e2e_error_code`: a task body that returns a library code.
#[task("e2e_error_code")]
pub async fn error_code() -> Result<String, TaskError<Infallible>> {
    Err(TaskError::library(OperationalCode::TaskException, "boom"))
}

/// The output of `e2e_complex_result` (Python `OutputModel`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplexOutput {
    pub value: i64,
    pub nested: BTreeMap<String, Vec<i64>>,
}

/// Python `e2e_complex_result`.
#[task("e2e_complex_result")]
pub async fn complex_result() -> Result<ComplexOutput, TaskError<Infallible>> {
    Ok(ComplexOutput {
        value: 42,
        nested: BTreeMap::from([
            ("a".to_owned(), vec![1, 2, 3]),
            ("b".to_owned(), vec![4, 5]),
        ]),
    })
}

/// Python `e2e_slow`: sleeps `duration_ms` on its thread.
#[blocking_task("e2e_slow")]
pub fn slow(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(Duration::from_millis(duration_ms));
    Ok(format!("slept_{duration_ms}"))
}

/// Python `e2e_timeout_sleeper`: a blocking body past its 2 s timeout; the
/// parent kills its child at the deadline.
#[blocking_task("e2e_timeout_sleeper", timeout_ms = 2_000)]
pub fn timeout_sleeper(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(Duration::from_millis(duration_ms));
    Ok(format!("slept_{duration_ms}"))
}

/// Python `e2e_async_timeout_sleeper`: an async body past its 2 s timeout.
#[task("e2e_async_timeout_sleeper", timeout_ms = 2_000)]
pub async fn async_timeout_sleeper(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    tokio::time::sleep(Duration::from_millis(duration_ms)).await;
    Ok(format!("slept_{duration_ms}"))
}

/// The old Rust `e2e_timeout_retry_sleeper`: 1 s timeout, one retry on
/// `TASK_TIMEOUT`.
#[task(
    "e2e_timeout_retry_sleeper",
    timeout_ms = 1_000,
    retry_policy = horsies::RetryPolicy::fixed(vec![1], false).expect("a valid policy"),
    auto_retry_for = ["TASK_TIMEOUT"]
)]
pub async fn timeout_retry_sleeper(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    tokio::time::sleep(Duration::from_millis(duration_ms)).await;
    Ok(format!("slept_{duration_ms}"))
}

/// Python `e2e_pid`: the process id of the child that runs it, after
/// `sleep_ms`.
#[task("e2e_pid")]
pub async fn pid(sleep_ms: u64) -> Result<u32, TaskError<Infallible>> {
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    Ok(std::process::id())
}

/// Python `e2e_exception`: the body panics; the library records the panic.
#[task("e2e_panic")]
pub async fn panics() -> Result<i64, TaskError<Infallible>> {
    panic!("Deliberate exception");
}

/// Python `e2e_pool_breaker`: the body ends its own child process after the
/// row is RUNNING. Crash recovery records `WORKER_CRASHED`.
#[blocking_task("e2e_abort")]
pub fn abort() -> Result<String, TaskError<Infallible>> {
    std::process::abort();
}

/// Python `e2e_idempotent`: runs once per token; a second run of a token
/// fails with `DOUBLE_EXECUTION`.
#[task("e2e_idempotent")]
pub async fn idempotent(token: String) -> Result<String, TaskError<BasicCode>> {
    match markers::create_once(&format!("idempotent-{token}"), "executed") {
        Ok(true) => Ok(format!("executed:{token}")),
        Ok(false) => Err(TaskError::user(
            BasicCode::DoubleExecution,
            format!("Token {token} already executed"),
        )),
        Err(cause) => Err(TaskError::user(BasicCode::MarkerFailed, cause.to_string())),
    }
}

/// Python `e2e_wait_inside_task`: send a task through the typed function of
/// the runtime, then try to wait for it. The output is whether the wait was
/// refused with `WAIT_INSIDE_TASK`.
#[task("e2e_wait_inside_task")]
pub async fn wait_inside_task(rt: TaskRuntime) -> Result<bool, TaskError<BasicCode>> {
    let function = simple::handle(&rt)
        .map_err(|missing| TaskError::user(BasicCode::SendFailed, missing.to_string()))?;
    let handle = function
        .send(simple::Input { x: 1 })
        .await
        .map_err(|error| TaskError::user(BasicCode::SendFailed, error.to_string()))?;
    Ok(matches!(
        handle.get(WaitLimit::Within(INSIDE_WAIT)).await,
        Err(ReadError::WaitInsideTask { .. })
    ))
}

/// Python `e2e_async_sends`: a task body that sends `e2e_simple`, through
/// the typed function of its runtime. The output is the sent task id.
#[task("e2e_async_sends")]
pub async fn async_sends(rt: TaskRuntime, x: i64) -> Result<String, TaskError<BasicCode>> {
    let function = simple::handle(&rt)
        .map_err(|missing| TaskError::user(BasicCode::SendFailed, missing.to_string()))?;
    let handle = function
        .send(simple::Input { x })
        .await
        .map_err(|error| TaskError::user(BasicCode::SendFailed, error.to_string()))?;
    Ok(handle.task_id().to_string())
}

/// A body that blocks its thread for `duration_ms` and reports when it ran:
/// `"<pid>:<start ms>:<end ms>"` on the wall clock.
#[blocking_task("e2e_stamp")]
pub fn stamp(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    let start = markers::unix_millis();
    std::thread::sleep(Duration::from_millis(duration_ms));
    Ok(format!(
        "{}:{start}:{}",
        std::process::id(),
        markers::unix_millis()
    ))
}

/// Python `e2e_softcap_slow_idempotent`: runs once per token, then sleeps
/// `duration_ms`; a second run of a token fails with `DOUBLE_EXECUTION`.
#[task("e2e_slow_idempotent")]
pub async fn slow_idempotent(
    token: String,
    duration_ms: u64,
) -> Result<String, TaskError<BasicCode>> {
    match markers::create_once(&format!("idempotent-{token}"), "executed") {
        Ok(true) => {
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
            Ok(format!("executed:{token}"))
        }
        Ok(false) => Err(TaskError::user(
            BasicCode::DoubleExecution,
            format!("Token {token} already executed"),
        )),
        Err(cause) => Err(TaskError::user(BasicCode::MarkerFailed, cause.to_string())),
    }
}
