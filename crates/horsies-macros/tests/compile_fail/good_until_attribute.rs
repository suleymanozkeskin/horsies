use horsies::task;
use horsies::TaskError;

#[task("bad", good_until = 1)]
async fn bad(_: ()) -> Result<(), TaskError> {
    Ok(())
}

fn main() {}
