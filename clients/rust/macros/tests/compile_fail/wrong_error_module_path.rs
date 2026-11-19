use horsies::task;

mod other {
    pub struct TaskError;
}

#[task("bad")]
async fn bad(_: ()) -> Result<String, other::TaskError> {
    Ok("nope".into())
}

fn main() {}
