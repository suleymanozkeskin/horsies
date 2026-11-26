//! The event loop watch of a JavaScript child: the marks of the JavaScript
//! main thread's loop, which the core's watch thread reads (`HostLoop`).
//!
//! - The package's timer calls `loopTurned()` every `HOST_LOOP_TURN_TICK`
//!   (100 ms).
//! - The body adapter calls `loopHold(taskId)` before the synchronous part of
//!   each body call and of each spawned work's start, and `loopRelease()`
//!   after it. On Node the culprit method of the package also marks the run
//!   at each continuation after an `await`.
//! - A shared async child watches the loop: when it has not turned for
//!   `LOOP_BLOCK_LIMIT` (5 s), the child sends one `loop_blocked` frame and
//!   the parent kills it. An isolated child and a handler child watch no
//!   loop; their marks are written and not read.
//! - Each mark is a short write, safe from the JavaScript thread. A mark
//!   before the serve is a defect of the package: the serve makes the loop.

use std::sync::{Arc, OnceLock};

use horsies_core::HostLoop;
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::values::boundary::uuid_of_text;

/// The loop of this process, once the serve made it.
static HOST_LOOP: OnceLock<Arc<HostLoop>> = OnceLock::new();

/// The loop is made already: a second serve in one process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoopMadeAlready;

/// Make the loop of this process, for the serve. A second make is refused.
pub fn make_host_loop() -> Result<Arc<HostLoop>, LoopMadeAlready> {
    let made = Arc::new(HostLoop::new());
    HOST_LOOP
        .set(Arc::clone(&made))
        .map(|()| made)
        .map_err(|_made_before| LoopMadeAlready)
}

/// Run `mark` on the loop of this process; before the serve it is
/// `CLIENT_DEFECT`.
fn with_loop(subject: &'static str, mark: impl FnOnce(&HostLoop)) -> Envelope<()> {
    sync_call(subject, || match HOST_LOOP.get() {
        Some(host_loop) => {
            mark(host_loop);
            Envelope::Value(())
        }
        None => Envelope::Thrown(NativeThrown::ClientDefect {
            subject: subject.to_owned(),
            rule: "the loop marks start after the serve of the child".to_owned(),
        }),
    })
}

/// The loop of the JavaScript main thread turned now.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
pub fn loop_turned() -> Envelope<()> {
    with_loop("loop_turned", HostLoop::turned)
}

/// The loop runs code of the run of task `taskId` now. A task id that is not
/// a UUID text is `CLIENT_DEFECT`: the package gives the ids of its runs.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn loop_hold(task_id: String) -> Envelope<()> {
    match uuid_of_text("taskId", &task_id) {
        Ok(task_id) => with_loop("loop_hold", |host_loop| host_loop.hold(task_id)),
        Err(refusal) => Envelope::Thrown(refusal.into()),
    }
}

/// The loop runs code of no run now.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
pub fn loop_release() -> Envelope<()> {
    with_loop("loop_release", HostLoop::release)
}
