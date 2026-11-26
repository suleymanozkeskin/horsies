// A send names the app that it goes through: no send reads a process-wide
// app.
use horsies::{task, TaskError};

#[task("sum")]
async fn sum(a: i32, b: i32) -> Result<i32, TaskError<std::convert::Infallible>> {
    Ok(a + b)
}

fn main() {
    let _future = sum::send(1, 2);
}
