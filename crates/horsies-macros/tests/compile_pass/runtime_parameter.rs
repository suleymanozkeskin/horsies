// A first parameter of type TaskRuntime is not input: the body gets the
// runtime of the app.
use std::convert::Infallible;

use horsies::{blocking_task, task, TaskError, TaskRuntime};

struct Settings {
    factor: u32,
}

#[task("scaled", execution = shared_async, timeout_ms = 5_000)]
async fn scaled(runtime: TaskRuntime, count: u32) -> Result<u32, TaskError<Infallible>> {
    let factor = runtime.state::<Settings>().map_or(1, |settings| settings.factor);
    if runtime.is_cancelled() {
        return Ok(0);
    }
    Ok(count * factor)
}

#[blocking_task("runtime_only")]
fn runtime_only(runtime: TaskRuntime) -> Result<bool, TaskError<Infallible>> {
    Ok(runtime.is_cancelled())
}

fn main() {
    let _ = scaled::register as fn(&mut horsies::Horsies) -> _;
    let _ = runtime_only::register as fn(&mut horsies::Horsies) -> _;
}
