// cancel_grace_ms belongs to a shared async task only.
use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("bad", timeout_ms = 5_000, cancel_grace_ms = 500)]
async fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
