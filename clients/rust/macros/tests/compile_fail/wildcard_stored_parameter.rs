// A parameter whose value the input stores needs a name: `_` is allowed
// only for a single `()` parameter.
use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("bad")]
async fn bad(_: u32) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
