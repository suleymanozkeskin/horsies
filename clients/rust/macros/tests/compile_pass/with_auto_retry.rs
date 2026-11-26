// A retry policy with built-in codes and codes of the task's own code type.
use horsies::{task, CustomErrorCode, RetryPolicy, TaskError};

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum FetchCode {
    RateLimited,
    UpstreamDown,
}

#[task(
    "retryable",
    retry_policy = RetryPolicy::fixed(vec![1, 5], true).expect("a valid policy"),
    auto_retry_for = ["RATE_LIMITED", "TASK_TIMEOUT", "WORKER_CRASHED"],
    timeout_ms = 5_000
)]
async fn retryable(_: ()) -> Result<String, TaskError<FetchCode>> {
    Err(TaskError::custom(FetchCode::UpstreamDown, "upstream down"))
}

fn main() {
    let _ = retryable::register as fn(&mut horsies::Horsies) -> _;
}
