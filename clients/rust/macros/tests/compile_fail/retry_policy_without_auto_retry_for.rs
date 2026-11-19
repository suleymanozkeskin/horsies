use std::convert::Infallible;

use horsies::{task, RetryPolicy, TaskError};

#[task("bad", retry_policy = RetryPolicy::fixed(vec![1], true).expect("a valid policy"))]
async fn bad(_: ()) -> Result<(), TaskError<Infallible>> {
    Ok(())
}

fn main() {}
