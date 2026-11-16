use horsies::task;
use horsies::TaskError;

#[task("ping")]
async fn ping(_: ()) -> Result<String, TaskError> {
    Ok("pong".into())
}

fn main() {
    let _ = ping::register as fn(&mut horsies::Horsies) -> _;
}
