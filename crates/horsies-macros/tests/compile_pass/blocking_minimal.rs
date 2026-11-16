use horsies::blocking_task;
use horsies::TaskError;

#[blocking_task("cpu_work")]
fn cpu_work(_: ()) -> Result<i32, TaskError> {
    Ok(42)
}

fn main() {
    let _ = cpu_work::register as fn(&mut horsies::Horsies) -> _;
}
