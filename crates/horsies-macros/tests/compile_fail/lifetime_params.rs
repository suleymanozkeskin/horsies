use horsies::task;
use horsies::TaskError;

#[task("bad")]
async fn bad<'a>(input: &'a str) -> Result<String, TaskError> {
    Ok(input.to_owned())
}

fn main() {}
