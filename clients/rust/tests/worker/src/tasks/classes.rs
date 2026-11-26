//! Tasks of the shared execution classes and of the runner defects of the
//! old in-process worker (Python `clients/python/tests/e2e/tasks/shared_async_tasks.py`,
//! `shared_thread_tasks.py`, `async_tasks.py`).

use std::convert::Infallible;
use std::time::Duration;

use horsies::{blocking_task, task, RetryPolicy, TaskError, TaskRuntime, UserErrorCode};

use crate::markers;

/// The codes of these tasks' own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
pub enum ClassCode {
    /// The body saw the cancel signal at its deadline and returned.
    Interrupted,
    /// A marker operation failed in the child.
    MarkerFailed,
}

/// The deadline of the tasks that run past it: short, to keep the tests
/// short (Python `STOP_TIMEOUT_MS`).
pub const STOP_TIMEOUT_MS: u64 = 2_000;

/// How long a body that ignores its deadline sleeps.
pub const PAST_THE_DEADLINE: Duration = Duration::from_secs(3_600);

/// How long `e2e_shared_blocks_once` blocks its runtime: far past the block
/// limit of a shared async child.
pub const BLOCK_DURATION: Duration = Duration::from_secs(30);

/// The interval of the lines of `e2e_thread_overrun`.
pub const OVERRUN_TICK: Duration = Duration::from_millis(100);

/// The ticks of one run of `e2e_thread_overrun`: 6 s, far past its 1 s
/// deadline.
pub const OVERRUN_TICKS: u32 = 60;

fn retry_on(after_seconds: u32) -> RetryPolicy {
    RetryPolicy::fixed(vec![after_seconds], false).expect("a 1 s fixed policy is valid")
}

/// `"<pid>:<start ms>:<end ms>"` of a body that ran from `start` to now.
fn stamp_since(start: u128) -> String {
    format!("{}:{start}:{}", std::process::id(), markers::unix_millis())
}

/// The readiness task of a worker whose only children are shared async.
#[task("e2e_shared_healthcheck", execution = shared_async, timeout_ms = 30_000)]
pub async fn shared_healthcheck() -> Result<String, TaskError<Infallible>> {
    Ok("ready".to_owned())
}

/// The readiness task of a worker whose only children are shared thread.
#[blocking_task("e2e_thread_healthcheck", execution = shared_thread, timeout_ms = 30_000)]
pub fn thread_healthcheck() -> Result<String, TaskError<Infallible>> {
    Ok("ready".to_owned())
}

/// Python `e2e_shared_sleep`: sleeps `milliseconds` on the runtime of its
/// child; reports `"<pid>:<start ms>:<end ms>"`.
#[task("e2e_shared_sleep", execution = shared_async, timeout_ms = 30_000)]
pub async fn shared_sleep(milliseconds: u64) -> Result<String, TaskError<Infallible>> {
    let start = markers::unix_millis();
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
    Ok(stamp_since(start))
}

/// Python `e2e_shared_hang`: waits far past its deadline and ignores the
/// cancel signal; the child drops it after the 500 ms grace
/// (`cancel_end` `DROPPED_AFTER_GRACE`).
#[task(
    "e2e_shared_hang",
    execution = shared_async,
    timeout_ms = 2_000,
    cancel_grace_ms = 500
)]
pub async fn shared_hang() -> Result<String, TaskError<Infallible>> {
    tokio::time::sleep(PAST_THE_DEADLINE).await;
    Ok("never".to_owned())
}

/// Waits far past its deadline, returns at the cancel signal within its
/// grace (`cancel_end` `RETURNED_AFTER_SIGNAL`).
#[task(
    "e2e_shared_cooperative",
    execution = shared_async,
    timeout_ms = 2_000,
    cancel_grace_ms = 2_000
)]
pub async fn shared_cooperative(rt: TaskRuntime) -> Result<String, TaskError<ClassCode>> {
    tokio::select! {
        () = tokio::time::sleep(PAST_THE_DEADLINE) => Ok("never".to_owned()),
        () = rt.cancelled() => Err(TaskError::user(ClassCode::Interrupted, "cancelled at the deadline")),
    }
}

/// Python `e2e_shared_blocks_once`: the first attempt of `token` blocks the
/// runtime of its shared child past the block limit; the retry returns its
/// process id.
#[task(
    "e2e_shared_blocks_once",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = retry_on(1),
    retry_for = ["TASK_BLOCKED_LOOP"]
)]
pub async fn shared_blocks_once(token: String) -> Result<u32, TaskError<ClassCode>> {
    let first = markers::create_once(&format!("{token}-blocked"), &std::process::id().to_string())
        .map_err(|cause| TaskError::user(ClassCode::MarkerFailed, cause.to_string()))?;
    if first {
        // Holds the runtime thread: no await point until it returns.
        std::thread::sleep(BLOCK_DURATION);
    }
    Ok(std::process::id())
}

/// Python `e2e_shared_stuck_once`: the first attempt of `token` ignores its
/// deadline and every cancel; the retry returns its process id.
#[task(
    "e2e_shared_stuck_once",
    execution = shared_async,
    timeout_ms = 2_000,
    cancel_grace_ms = 100,
    retry_policy = retry_on(1),
    retry_for = ["TASK_TIMEOUT"]
)]
pub async fn shared_stuck_once(token: String) -> Result<u32, TaskError<ClassCode>> {
    let first = markers::create_once(&format!("{token}-stuck"), &std::process::id().to_string())
        .map_err(|cause| TaskError::user(ClassCode::MarkerFailed, cause.to_string()))?;
    if first {
        tokio::time::sleep(PAST_THE_DEADLINE).await;
    }
    Ok(std::process::id())
}

/// Python `e2e_thread_sleep`: sleeps `milliseconds` on a seat thread;
/// reports `"<pid>:<start ms>:<end ms>"`.
#[blocking_task("e2e_thread_sleep", execution = shared_thread, timeout_ms = 30_000)]
pub fn thread_sleep(milliseconds: u64) -> Result<String, TaskError<Infallible>> {
    let start = markers::unix_millis();
    std::thread::sleep(Duration::from_millis(milliseconds));
    Ok(stamp_since(start))
}

/// Python `e2e_thread_hang`: a seat thread that runs far past its 2 s
/// deadline; a thread cannot be cancelled, so the worker closes its child.
#[blocking_task("e2e_thread_hang", execution = shared_thread, timeout_ms = 2_000)]
pub fn thread_hang() -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(PAST_THE_DEADLINE);
    Ok("never".to_owned())
}

/// A seat thread that runs 6 s against a 1 s deadline, with three retries
/// on `TASK_TIMEOUT`. Each run appends `start <attempt> <ms>`, then
/// `tick <attempt> <ms>` every 100 ms, to the marker `<token>-runs`: a test
/// reads whether a run went on after the next one started (#560).
#[blocking_task(
    "e2e_thread_overrun",
    execution = shared_thread,
    timeout_ms = 1_000,
    retry_policy = RetryPolicy::fixed(vec![1, 1, 1], false).expect("a valid policy"),
    retry_for = ["TASK_TIMEOUT"]
)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn thread_overrun(token: String) -> Result<String, TaskError<ClassCode>> {
    let marker_failed =
        |cause: markers::MarkerError| TaskError::user(ClassCode::MarkerFailed, cause.to_string());
    let attempt = markers::next_attempt(&token).map_err(marker_failed)?;
    let runs = format!("{token}-runs");
    markers::append_line(
        &runs,
        &format!("start {attempt} {}", markers::unix_millis()),
    )
    .map_err(marker_failed)?;
    for _tick in 0..OVERRUN_TICKS {
        std::thread::sleep(OVERRUN_TICK);
        markers::append_line(&runs, &format!("tick {attempt} {}", markers::unix_millis()))
            .map_err(marker_failed)?;
    }
    Ok(format!("ran {attempt}"))
}

/// An isolated blocking body that runs `seconds`: longer than the drain
/// timeout of every test profile (#561).
#[blocking_task("e2e_outlives_drain")]
pub fn outlives_drain(seconds: u64) -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(Duration::from_secs(seconds));
    Ok("ended".to_owned())
}
