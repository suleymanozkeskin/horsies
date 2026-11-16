use horsies::task;

// Using fully qualified horsies::TaskError should be accepted.
#[task("qualified")]
async fn qualified(_: ()) -> Result<String, horsies::TaskError> {
    Ok("ok".into())
}

fn main() {
    let _ = qualified::register as fn(&mut horsies::Horsies) -> _;
}
