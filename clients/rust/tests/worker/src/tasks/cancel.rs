//! Tasks that an operator cancels while they run (`cancel_task` with
//! `include_running`): one per execution class and body kind that waits for
//! the cancel, one that ignores it and is killed after its grace, and one
//! that ignores it with no grace and runs to its end. Each writes the
//! marker `<token>-started` with the pid of its child when it starts; the
//! tasks that wait write `<token>-saw` with the cancel state they read and
//! the wall clock in milliseconds, as `<state>|<ms>`; the tasks that ignore
//! the cancel write `<token>-ended` with the pid of their child when they
//! end.

use std::time::Duration;

use horsies::{blocking_task, task, CustomErrorCode, RunCancelState, TaskError, TaskRuntime};

use crate::markers;

/// The codes of these tasks' own errors.
#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
pub enum CancelCode {
    /// A marker operation failed in the child.
    MarkerFailed,
}

/// How long a body waits for its cancel, or runs when it ignores it and is
/// killed: far past every wait of the tests.
pub const CANCEL_WAIT: Duration = Duration::from_secs(120);

/// The interval at which a blocking body reads its cancel state.
pub const STATE_POLL: Duration = Duration::from_millis(20);

/// The reads of a blocking body: [`CANCEL_WAIT`] over [`STATE_POLL`].
const STATE_POLLS: u32 = 6_000;

/// How long `e2e_cancel_ignored_no_grace` runs after its start, whatever
/// the cancel.
pub const IGNORED_RUN: Duration = Duration::from_secs(3);

/// The grace of `e2e_cancel_ignored_with_grace`: the worker kills its child
/// this long after the cancel reached it.
pub const KILL_GRACE_MS: u64 = 500;

/// The separator of the state and the time in the marker `<token>-saw`.
pub const SAW_SEPARATOR: char = '|';

fn marker_failed(cause: &markers::MarkerError) -> TaskError<CancelCode> {
    TaskError::custom(CancelCode::MarkerFailed, cause.to_string())
}

/// Write the marker `<token>-<suffix>` with the pid of this child.
fn note_pid(token: &str, suffix: &str) -> Result<(), TaskError<CancelCode>> {
    markers::create_once(
        &format!("{token}-{suffix}"),
        &std::process::id().to_string(),
    )
    .map(|_created| ())
    .map_err(|cause| marker_failed(&cause))
}

/// Write the marker `<token>-saw` with `state` and the wall clock.
fn note_state(token: &str, state: RunCancelState) -> Result<(), TaskError<CancelCode>> {
    markers::create_once(
        &format!("{token}-saw"),
        &format!("{state:?}{SAW_SEPARATOR}{}", markers::unix_millis()),
    )
    .map(|_created| ())
    .map_err(|cause| marker_failed(&cause))
}

/// Read the cancel state of the run until a cancel is requested, at most
/// [`STATE_POLLS`] times; the last state read.
fn wait_for_cancel_state(rt: &TaskRuntime) -> RunCancelState {
    for _ in 0..STATE_POLLS {
        match rt.cancel_state() {
            requested @ RunCancelState::Requested { .. } => return requested,
            RunCancelState::NotRequested | RunCancelState::NotInARun => {
                std::thread::sleep(STATE_POLL);
            }
        }
    }
    rt.cancel_state()
}

/// The async body of the tasks that wait for the cancel signal.
async fn wait_for_cancel(rt: &TaskRuntime, token: &str) -> Result<String, TaskError<CancelCode>> {
    note_pid(token, "started")?;
    tokio::select! {
        () = rt.cancelled() => note_state(token, rt.cancel_state())?,
        () = tokio::time::sleep(CANCEL_WAIT) => {}
    }
    Ok("returned after the cancel".to_owned())
}

/// The blocking body of the tasks that read their cancel state.
fn read_cancel_state(rt: &TaskRuntime, token: &str) -> Result<String, TaskError<CancelCode>> {
    note_pid(token, "started")?;
    note_state(token, wait_for_cancel_state(rt))?;
    Ok("returned after the cancel".to_owned())
}

/// An isolated async body that waits for its cancel signal.
#[task("e2e_cancel_isolated_async")]
pub async fn cancel_isolated_async(
    rt: TaskRuntime,
    token: String,
) -> Result<String, TaskError<CancelCode>> {
    wait_for_cancel(&rt, &token).await
}

/// An isolated blocking body that reads its cancel state.
#[blocking_task("e2e_cancel_isolated_blocking")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn cancel_isolated_blocking(
    rt: TaskRuntime,
    token: String,
) -> Result<String, TaskError<CancelCode>> {
    read_cancel_state(&rt, &token)
}

/// A shared async body that waits for its cancel signal.
#[task(
    "e2e_cancel_shared_async",
    execution = shared_async,
    timeout_ms = 300_000,
    cancel_grace_ms = 2_000
)]
pub async fn cancel_shared_async(
    rt: TaskRuntime,
    token: String,
) -> Result<String, TaskError<CancelCode>> {
    wait_for_cancel(&rt, &token).await
}

/// A shared thread body that reads its cancel state.
#[blocking_task("e2e_cancel_shared_thread", execution = shared_thread, timeout_ms = 300_000)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn cancel_shared_thread(
    rt: TaskRuntime,
    token: String,
) -> Result<String, TaskError<CancelCode>> {
    read_cancel_state(&rt, &token)
}

/// An isolated blocking body that ignores the cancel; the worker kills its
/// child [`KILL_GRACE_MS`] after the cancel reached it, so it never writes
/// `<token>-ended`.
#[blocking_task("e2e_cancel_ignored_with_grace", cancel_grace_ms = 500)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn cancel_ignored_with_grace(token: String) -> Result<String, TaskError<CancelCode>> {
    note_pid(&token, "started")?;
    std::thread::sleep(CANCEL_WAIT);
    note_pid(&token, "ended")?;
    Ok("ran to its end".to_owned())
}

/// An isolated blocking body with no grace that ignores the cancel: the
/// worker kills nothing, so it runs [`IGNORED_RUN`] and writes
/// `<token>-ended`.
#[blocking_task("e2e_cancel_ignored_no_grace")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn cancel_ignored_no_grace(token: String) -> Result<String, TaskError<CancelCode>> {
    note_pid(&token, "started")?;
    std::thread::sleep(IGNORED_RUN);
    note_pid(&token, "ended")?;
    Ok("ran to its end".to_owned())
}
