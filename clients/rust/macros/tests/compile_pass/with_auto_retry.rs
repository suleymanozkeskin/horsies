// A retry policy with built-in codes and codes of the task's own code type.
use horsies::{task, RetryPolicy, TaskError, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
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
    Err(TaskError::user(FetchCode::UpstreamDown, "upstream down"))
}

fn main() {
    let _ = retryable::register as fn(&mut horsies::Horsies) -> _;
}
