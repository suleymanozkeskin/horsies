// A shared thread seat runs a blocking fn, not an async fn.
use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("bad", execution = shared_thread, timeout_ms = 5_000)]
async fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
