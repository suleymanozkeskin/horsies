// RATE_LIMITED is neither a built-in code nor a code of FetchCode.
use horsies::{task, RetryPolicy, TaskError, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum FetchCode {
    UpstreamDown,
}

#[task(
    "bad",
    retry_policy = RetryPolicy::fixed(vec![1], true).expect("a valid policy"),
    retry_for = ["UPSTREAM_DOWN", "RATE_LIMITED"]
)]
async fn bad(_: ()) -> Result<(), TaskError<FetchCode>> {
    Ok(())
}

fn main() {}
