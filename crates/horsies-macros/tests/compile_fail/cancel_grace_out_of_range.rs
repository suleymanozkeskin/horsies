use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("bad", execution = shared_async, timeout_ms = 5_000, cancel_grace_ms = 10_001)]
async fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
