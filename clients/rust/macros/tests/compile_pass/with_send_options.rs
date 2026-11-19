use horsies::task;
use horsies::TaskError;

#[task("sum_with_options")]
async fn sum_with_options(a: i32, b: i32) -> Result<i32, TaskError<std::convert::Infallible>> {
    Ok(a + b)
}

#[task("no_arg_with_options")]
async fn no_arg_with_options() -> Result<i32, TaskError<std::convert::Infallible>> {
    Ok(1)
}

fn main() {
    let _ = sum_with_options::with_options;
    let _ = no_arg_with_options::with_options;
}
