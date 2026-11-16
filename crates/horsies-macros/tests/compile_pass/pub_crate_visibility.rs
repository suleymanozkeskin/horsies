use horsies::task;
use horsies::TaskError;

#[task("crate_vis")]
pub(crate) async fn crate_vis(_: ()) -> Result<String, TaskError> {
    Ok("ok".into())
}

fn main() {
    let _ = crate_vis::register as fn(&mut horsies::Horsies) -> _;
}
