use horsies::task;
use horsies::TaskError;

#[task("bad")]
fn bad(_: ()) -> Result<String, TaskError> {
    Ok("nope".into())
}

fn main() {}
