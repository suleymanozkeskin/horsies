use horsies::task;

// Using horsies::TaskError should be accepted.
#[task("core_qualified")]
async fn core_qualified(_: ()) -> Result<String, horsies::TaskError> {
    Ok("ok".into())
}

fn main() {
    let _ = core_qualified::register as fn(&mut horsies::Horsies) -> _;
}
