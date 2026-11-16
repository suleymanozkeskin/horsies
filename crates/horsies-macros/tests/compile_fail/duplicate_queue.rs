use horsies::task;
use horsies::TaskError;

#[task("bad", queue = "a", queue = "b")]
async fn bad(_: ()) -> Result<String, TaskError> {
    Ok("nope".into())
}

fn main() {}
