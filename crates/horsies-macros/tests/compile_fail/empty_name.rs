use horsies::task;
use horsies::TaskError;

#[task("")]
async fn bad(_: ()) -> Result<String, TaskError> {
    Ok("nope".into())
}

fn main() {}
