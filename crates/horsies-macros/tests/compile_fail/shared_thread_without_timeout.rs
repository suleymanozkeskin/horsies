use std::convert::Infallible;

use horsies::{blocking_task, TaskError};

#[blocking_task("bad", execution = shared_thread)]
fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
