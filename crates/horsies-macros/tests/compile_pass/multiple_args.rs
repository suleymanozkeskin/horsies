use horsies::task;
use horsies::TaskError;

#[task("sum_two")]
async fn sum_two(a: i32, b: i32) -> Result<i32, TaskError> {
    Ok(a + b)
}

fn main() {
    let _ = sum_two::params::a();
    let _ = sum_two::params::b();
}
