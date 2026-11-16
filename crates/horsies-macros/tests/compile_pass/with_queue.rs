use horsies::task;
use horsies::TaskError;

#[task("queued", queue = "critical")]
async fn queued(_: ()) -> Result<String, TaskError> {
    Ok("ok".into())
}

fn main() {
    let _ = queued::register as fn(&mut horsies::Horsies) -> _;
}
