use horsies::task;

// Using fully qualified horsies::TaskError<std::convert::Infallible> should be accepted.
#[task("qualified")]
async fn qualified(_: ()) -> Result<String, horsies::TaskError<std::convert::Infallible>> {
    Ok("ok".into())
}

fn main() {
    let _ = qualified::register as fn(&mut horsies::Horsies) -> _;
}
