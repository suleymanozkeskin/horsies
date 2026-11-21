use horsies::task;
use horsies::TaskError;

#[task("first")]
async fn first(_: ()) -> Result<String, TaskError<std::convert::Infallible>> {
    Ok("a".into())
}

#[task("second")]
async fn second(_: ()) -> Result<String, TaskError<std::convert::Infallible>> {
    Ok("b".into())
}

fn main() {
    let _a = first::register as fn(&mut horsies::Horsies) -> _;
    let _b = second::register as fn(&mut horsies::Horsies) -> _;
}
