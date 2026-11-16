use horsies::task;
use horsies::TaskError;

#[task("bad", timeout = 5000)]
async fn bad(_: ()) -> Result<String, TaskError> {
    Ok("nope".into())
}

fn main() {}
