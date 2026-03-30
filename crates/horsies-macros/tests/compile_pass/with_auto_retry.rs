use horsies::task;
use horsies::TaskError;

#[task("retryable", auto_retry_for = ["RATE_LIMITED", "TIMEOUT"])]
async fn retryable(_: ()) -> Result<String, TaskError> {
    Ok("ok".into())
}

fn main() {
    let _ = retryable::register as fn(&mut horsies::Horsies) -> _;
}
