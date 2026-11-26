// Without a policy a task does not retry, so the codes would be dropped.
use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("bad", retry_for = ["TASK_TIMEOUT"], timeout_ms = 5_000)]
async fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
