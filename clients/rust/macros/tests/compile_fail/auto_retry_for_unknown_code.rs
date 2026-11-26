// RATE_LIMITED is neither a built-in code nor a code of FetchCode.
use horsies::{task, CustomErrorCode, RetryPolicy, TaskError};

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum FetchCode {
    UpstreamDown,
}

#[task(
    "bad",
    retry_policy = RetryPolicy::fixed(vec![1], true).expect("a valid policy"),
    auto_retry_for = ["UPSTREAM_DOWN", "RATE_LIMITED"]
)]
async fn bad(_: ()) -> Result<(), TaskError<FetchCode>> {
    Ok(())
}

fn main() {}
