use horsies::blocking_task;
use horsies::TaskError;

#[blocking_task("bad")]
async fn bad(_: ()) -> Result<String, TaskError> {
    Ok("nope".into())
}

fn main() {}
