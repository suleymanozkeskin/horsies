// A first parameter of type TaskRuntime is not input: the body gets the
// runtime of the app, with the cancel state of its run.
use std::convert::Infallible;

use horsies::{blocking_task, task, CancelCause, RunCancelState, TaskError, TaskRuntime};

struct Settings {
    factor: u32,
}

#[task("scaled", execution = shared_async, timeout_ms = 5_000)]
async fn scaled(runtime: TaskRuntime, count: u32) -> Result<u32, TaskError<Infallible>> {
    let factor = runtime.state::<Settings>().map_or(1, |settings| settings.factor);
    if runtime.is_cancelled() {
        return Ok(0);
    }
    match runtime.cancel_state() {
        RunCancelState::Requested {
            cause: CancelCause::Operator,
        } => Ok(0),
        RunCancelState::Requested {
            cause: CancelCause::Deadline,
        }
        | RunCancelState::NotRequested
        | RunCancelState::NotInARun => Ok(count * factor),
    }
}

#[blocking_task("runtime_only")]
fn runtime_only(runtime: TaskRuntime) -> Result<bool, TaskError<Infallible>> {
    Ok(runtime.is_cancelled())
}

#[blocking_task("stops_at_a_cancel", cancel_grace_ms = 1_000)]
fn stops_at_a_cancel(runtime: TaskRuntime, rows: u32) -> Result<u32, TaskError<Infallible>> {
    let mut done = 0;
    for _ in 0..rows {
        match runtime.cancel_state() {
            RunCancelState::Requested { .. } => return Ok(done),
            RunCancelState::NotRequested | RunCancelState::NotInARun => done += 1,
        }
    }
    Ok(done)
}

fn main() {
    let _ = scaled::register as fn(&mut horsies::Horsies) -> _;
    let _ = runtime_only::register as fn(&mut horsies::Horsies) -> _;
    let _ = stops_at_a_cancel::register as fn(&mut horsies::Horsies) -> _;
}
