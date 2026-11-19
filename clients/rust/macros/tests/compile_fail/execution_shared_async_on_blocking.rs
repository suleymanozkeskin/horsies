// A shared async seat runs an async fn, not a blocking fn.
use std::convert::Infallible;

use horsies::{blocking_task, TaskError};

#[blocking_task("bad", execution = shared_async, timeout_ms = 5_000)]
fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
