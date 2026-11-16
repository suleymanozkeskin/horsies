use horsies::task;

#[derive(Debug)]
struct MyError;

#[task("bad")]
async fn bad(_: ()) -> Result<String, MyError> {
    Ok("nope".into())
}

fn main() {}
