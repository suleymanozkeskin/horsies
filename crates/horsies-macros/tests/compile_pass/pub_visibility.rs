use horsies::task;
use horsies::TaskError;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct PubArgs {
    pub value: i32,
}

#[task("pub_task")]
pub async fn pub_task(args: PubArgs) -> Result<i32, TaskError> {
    Ok(args.value)
}

fn main() {
    let _ = pub_task::register as fn(&mut horsies::Horsies) -> _;
}
