use horsies::task;
use horsies::TaskError;

#[task("bad")]
async fn bad<T: Default>(_: ()) -> Result<T, TaskError<std::convert::Infallible>> {
    Ok(T::default())
}

fn main() {}
