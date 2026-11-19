// The error type names the code type of the task's own errors.
use horsies::{task, TaskError};

#[task("bad")]
async fn bad(_: ()) -> Result<(), TaskError> {
    Ok(())
}

fn main() {}
