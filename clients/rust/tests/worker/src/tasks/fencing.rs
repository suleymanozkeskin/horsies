//! Tasks that return the fencing token of their run: one per execution
//! class, one that reads it twice and in work that it started, and one that
//! a retry runs again. Each task is on the default queue.

use horsies::{blocking_task, task, CustomErrorCode, RetryPolicy, TaskError, TaskRuntime};
use serde::{Deserialize, Serialize};

use crate::markers;

/// The codes of the fencing tasks' own errors.
#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
pub enum FenceCode {
    /// The body read no fencing token.
    NoToken,
    /// The work that the body started did not give its token.
    WorkFailed,
    /// Retried by `e2e_fence_retry`.
    Transient,
    /// A marker operation failed in the child.
    MarkerFailed,
}

/// A fencing token that a body read: the task id as text, and the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fence {
    pub task_id: String,
    pub value: u32,
}

/// The output of `e2e_fence_retry`: the token of the run that succeeded,
/// and the number of the run that the marker files counted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceRun {
    pub fence: Fence,
    pub counted_run: u32,
}

/// The fencing token of the current run, as a [`Fence`].
fn fence_of(rt: &TaskRuntime) -> Result<Fence, TaskError<FenceCode>> {
    let token = rt
        .fencing_token()
        .map_err(|refused| TaskError::custom(FenceCode::NoToken, refused.to_string()))?;
    Ok(Fence {
        task_id: token.task_id().to_string(),
        value: token.value().get(),
    })
}

fn one_retry() -> RetryPolicy {
    RetryPolicy::fixed(vec![1], false).expect("a fixed policy of one 1 s interval is valid")
}

/// The token of an isolated async run.
#[task("e2e_fence_isolated_async")]
pub async fn fence_isolated_async(rt: TaskRuntime) -> Result<Fence, TaskError<FenceCode>> {
    fence_of(&rt)
}

/// The token of an isolated blocking run.
#[blocking_task("e2e_fence_isolated_blocking")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn fence_isolated_blocking(rt: TaskRuntime) -> Result<Fence, TaskError<FenceCode>> {
    fence_of(&rt)
}

/// The token of a shared async run.
#[task("e2e_fence_shared_async", execution = shared_async, timeout_ms = 30_000)]
pub async fn fence_shared_async(rt: TaskRuntime) -> Result<Fence, TaskError<FenceCode>> {
    fence_of(&rt)
}

/// The token of a shared thread run.
#[blocking_task("e2e_fence_shared_thread", execution = shared_thread, timeout_ms = 30_000)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn fence_shared_thread(rt: TaskRuntime) -> Result<Fence, TaskError<FenceCode>> {
    fence_of(&rt)
}

/// Two reads of the token in the body, then a read in work that the body
/// started with `TaskRuntime::spawn`, in that order.
#[task("e2e_fence_reads_twice")]
pub async fn fence_reads_twice(rt: TaskRuntime) -> Result<Vec<Fence>, TaskError<FenceCode>> {
    let first = fence_of(&rt)?;
    let second = fence_of(&rt)?;
    let work_runtime = rt.clone();
    let work = rt
        .spawn("read_fence", async move { fence_of(&work_runtime) })
        .map_err(|refused| TaskError::custom(FenceCode::WorkFailed, refused.to_string()))?;
    let from_work = work
        .await
        .map_err(|joined| TaskError::custom(FenceCode::WorkFailed, joined.to_string()))??;
    Ok(vec![first, second, from_work])
}

/// The first run for `marker` fails with `TRANSIENT`; its retry returns
/// its token and the number of the run, which the marker files count.
#[task(
    "e2e_fence_retry",
    retry_policy = one_retry(),
    retry_for = ["TRANSIENT"]
)]
pub async fn fence_retry(
    rt: TaskRuntime,
    marker: String,
) -> Result<FenceRun, TaskError<FenceCode>> {
    let fence = fence_of(&rt)?;
    let counted_run = markers::next_attempt(&marker)
        .map_err(|cause| TaskError::custom(FenceCode::MarkerFailed, cause.to_string()))?;
    match counted_run {
        1 => Err(TaskError::custom(
            FenceCode::Transient,
            format!("run 1 read the fence value {}", fence.value),
        )),
        _ => Ok(FenceRun { fence, counted_run }),
    }
}
