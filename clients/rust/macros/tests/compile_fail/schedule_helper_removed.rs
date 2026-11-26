// The delay is a send option: the generated module has no `schedule`.
use horsies::{task, TaskError, WholeSeconds};

#[task("sum")]
async fn sum(a: i32, b: i32) -> Result<i32, TaskError<std::convert::Infallible>> {
    Ok(a + b)
}

fn main() {
    let _future = sum::schedule(WholeSeconds::from_secs(30), 1, 2);
}
