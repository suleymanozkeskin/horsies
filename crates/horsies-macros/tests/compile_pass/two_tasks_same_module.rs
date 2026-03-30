use horsies::task;
use horsies::TaskError;

#[task("first")]
async fn first(_: ()) -> Result<String, TaskError> {
    Ok("a".into())
}

#[task("second")]
async fn second(_: ()) -> Result<String, TaskError> {
    Ok("b".into())
}

fn main() {
    let _a = first::register as fn(&mut horsies::Horsies) -> _;
    let _b = second::register as fn(&mut horsies::Horsies) -> _;
}
