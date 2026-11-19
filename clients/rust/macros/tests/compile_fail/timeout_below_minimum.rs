use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("bad", timeout_ms = 999)]
async fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
