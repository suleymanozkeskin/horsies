use horsies::task;
use horsies::TaskError;

#[task("")]
async fn bad(_: ()) -> Result<String, TaskError<std::convert::Infallible>> {
    Ok("nope".into())
}

fn main() {}
