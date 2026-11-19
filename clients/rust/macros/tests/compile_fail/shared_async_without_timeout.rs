use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("bad", execution = shared_async)]
async fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
