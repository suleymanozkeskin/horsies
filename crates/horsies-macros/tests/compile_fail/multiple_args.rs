use horsies::task;
use horsies::TaskError;

#[task("bad")]
async fn bad(a: i32, b: i32) -> Result<i32, TaskError> {
    Ok(a + b)
}

fn main() {}
