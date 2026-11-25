// A shared thread task takes no cancel_grace_ms: a thread cannot be stopped.
use std::convert::Infallible;

use horsies::{blocking_task, TaskError};

#[blocking_task("bad", execution = shared_thread, timeout_ms = 5_000, cancel_grace_ms = 500)]
fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
