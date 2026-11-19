use horsies::task;
use horsies::TaskError;

#[task("minimal")]
async fn minimal(_: ()) -> Result<String, TaskError<std::convert::Infallible>> {
    Ok("ok".into())
}

fn main() {
    let _ = minimal::register as fn(&mut horsies::Horsies) -> _;
}
