//! The operator cancel of a RUNNING task on a live worker
//! (`horsies::monitoring::cancel_task` with `include_running`), per
//! execution class and body kind:
//!
//! - the task code sees the cancel within 2 s on a local database: an async
//!   body through `TaskRuntime::cancelled()`, a blocking body through
//!   `TaskRuntime::cancel_state()`, both with the cause `Operator`;
//! - the history row is `CANCELLED` with one attempt, whose outcome is
//!   `CANCELLED` with the code `TASK_CANCELLED`;
//! - an `ISOLATED` task with `cancel_grace_ms` that ignores the cancel is
//!   killed with its child after the grace; one with no grace is not killed
//!   and runs to its end in the same child.
//!
//! ```text
//! HORSIES_TEST_DATABASE_URL=postgresql://... cargo test -p horsies-test-worker --test cancel_running
//! ```

use std::time::{Duration, Instant};

use horsies::{TaskHandle, TaskInfoParts, TaskStatus, UserErrorCode};
use horsies_test_worker::harness::{succeeded, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::markers;
use horsies_test_worker::tasks::{basic, cancel};
use serde::de::DeserializeOwned;
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for a task to start on a running worker.
const START_WAIT: Duration = Duration::from_secs(30);

/// The bound from the cancel to the moment the task code read it.
const SEEN_BOUND_MS: u128 = 2_000;

/// The longest wait for a marker that a task writes after the cancel.
const MARKER_WAIT: Duration = Duration::from_secs(15);

/// The interval of a marker wait.
const MARKER_POLL: Duration = Duration::from_millis(20);

/// The longest wait for the outcome of a task that a running worker runs.
const RESULT_WAIT: Duration = Duration::from_secs(30);

/// The state that a body reads after an operator cancel, as its marker
/// holds it.
const OPERATOR_STATE: &str = "Requested { cause: Operator }";

/// Every part of the task info.
const ALL_PARTS: TaskInfoParts = TaskInfoParts {
    result: true,
    failed_reason: true,
    attempts: true,
};

/// A token of one test task: the prefix of its markers.
fn new_token() -> String {
    format!("cancel-{}", Uuid::new_v4().simple())
}

/// The text of marker `name` once it exists and is written, at most
/// `limit`; `None` when the limit passed first.
async fn marker_within(suite: &Suite, name: &str, limit: Duration) -> Option<String> {
    let deadline = Instant::now() + limit;
    loop {
        match suite.markers.read(name) {
            Some(text) if !text.is_empty() => return Some(text),
            Some(_) | None => {}
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(MARKER_POLL).await;
    }
}

/// The pid of the child that started the task of `token`.
async fn started_pid(suite: &Suite, token: &str) -> String {
    marker_within(suite, &format!("{token}-started"), START_WAIT)
        .await
        .unwrap_or_else(|| panic!("the task of {token} did not start"))
}

/// Cancel the running task `task_id`; the wall clock in milliseconds right
/// before the cancel.
async fn cancel_running(suite: &Suite, task_id: Uuid) -> u128 {
    let cancel_at = markers::unix_millis();
    let _cancelled = horsies::monitoring::cancel_task(&suite.app, task_id, true)
        .await
        .unwrap_or_else(|error| panic!("cancel the running task {task_id}: {error}"));
    cancel_at
}

/// The task of `token` read the operator cancel within [`SEEN_BOUND_MS`]
/// of `cancel_at`.
async fn assert_seen_in_time(suite: &Suite, token: &str, cancel_at: u128) {
    let saw = marker_within(suite, &format!("{token}-saw"), MARKER_WAIT)
        .await
        .unwrap_or_else(|| panic!("the task of {token} did not see the cancel"));
    let Some((state, at)) = saw.split_once(cancel::SAW_SEPARATOR) else {
        panic!("the saw marker is `<state>|<ms>`, not {saw:?}");
    };
    assert_eq!(state, OPERATOR_STATE);
    let at: u128 = at.parse().expect("a time in ms");
    assert!(
        at.saturating_sub(cancel_at) <= SEEN_BOUND_MS,
        "the code saw the cancel {} ms after it",
        at.saturating_sub(cancel_at)
    );
}

/// The task is `CANCELLED` in history, with one attempt: `CANCELLED`, code
/// `TASK_CANCELLED`, no retry.
async fn assert_cancelled_with_one_attempt<T, E>(handle: &TaskHandle<T, E>)
where
    T: DeserializeOwned + Clone + std::fmt::Debug,
    E: UserErrorCode,
{
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!(info.status, TaskStatus::Cancelled);
    let attempts = info.attempts.expect("attempts were asked for");
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    let attempt = &attempts[0];
    assert_eq!(attempt.attempt, 1);
    assert_eq!(attempt.outcome.to_string(), "CANCELLED");
    assert_eq!(attempt.error_code.as_deref(), Some("TASK_CANCELLED"));
    assert!(!attempt.will_retry);
}

/// The pid of the isolated child that runs the next task.
async fn isolated_pid(suite: &Suite) -> String {
    let handle = suite
        .function(basic::pid::handle)
        .send(basic::pid::Input { sleep_ms: 0 })
        .await
        .expect("send e2e_pid");
    succeeded(&handle, RESULT_WAIT).await.to_string()
}

/// An isolated async body sees the operator cancel through
/// `TaskRuntime::cancelled()`.
#[tokio::test]
#[serial]
async fn an_isolated_async_body_sees_the_cancel() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let token = new_token();
    let handle = suite
        .function(cancel::cancel_isolated_async::handle)
        .send(cancel::cancel_isolated_async::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_cancel_isolated_async");
    let _pid = started_pid(&suite, &token).await;

    let cancel_at = cancel_running(&suite, handle.task_id()).await;

    assert_seen_in_time(&suite, &token, cancel_at).await;
    assert_cancelled_with_one_attempt(&handle).await;
}

/// An isolated blocking body sees the operator cancel through
/// `TaskRuntime::cancel_state()` on its thread.
#[tokio::test]
#[serial]
async fn an_isolated_blocking_body_sees_the_cancel() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let token = new_token();
    let handle = suite
        .function(cancel::cancel_isolated_blocking::handle)
        .send(cancel::cancel_isolated_blocking::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_cancel_isolated_blocking");
    let _pid = started_pid(&suite, &token).await;

    let cancel_at = cancel_running(&suite, handle.task_id()).await;

    assert_seen_in_time(&suite, &token, cancel_at).await;
    assert_cancelled_with_one_attempt(&handle).await;
}

/// A shared async body sees the operator cancel through
/// `TaskRuntime::cancelled()`.
#[tokio::test]
#[serial]
async fn a_shared_async_body_sees_the_cancel() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::SharedOnly1x4).await;
    let token = new_token();
    let handle = suite
        .function(cancel::cancel_shared_async::handle)
        .send(cancel::cancel_shared_async::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_cancel_shared_async");
    let _pid = started_pid(&suite, &token).await;

    let cancel_at = cancel_running(&suite, handle.task_id()).await;

    assert_seen_in_time(&suite, &token, cancel_at).await;
    assert_cancelled_with_one_attempt(&handle).await;
}

/// A shared thread body sees the operator cancel through
/// `TaskRuntime::cancel_state()` on its seat thread.
#[tokio::test]
#[serial]
async fn a_shared_thread_body_sees_the_cancel() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::ThreadOnly1x4).await;
    let token = new_token();
    let handle = suite
        .function(cancel::cancel_shared_thread::handle)
        .send(cancel::cancel_shared_thread::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_cancel_shared_thread");
    let _pid = started_pid(&suite, &token).await;

    let cancel_at = cancel_running(&suite, handle.task_id()).await;

    assert_seen_in_time(&suite, &token, cancel_at).await;
    assert_cancelled_with_one_attempt(&handle).await;
}

/// An isolated task with `cancel_grace_ms` that ignores the cancel is
/// killed with its child after the grace: it never ends, and the next task
/// runs in a new child.
#[tokio::test]
#[serial]
async fn an_isolated_task_that_ignores_the_cancel_is_killed_after_its_grace() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let token = new_token();
    let handle = suite
        .function(cancel::cancel_ignored_with_grace::handle)
        .send(cancel::cancel_ignored_with_grace::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_cancel_ignored_with_grace");
    let cancelled_pid = started_pid(&suite, &token).await;

    let _cancel_at = cancel_running(&suite, handle.task_id()).await;

    let next_pid = isolated_pid(&suite).await;
    assert_ne!(next_pid, cancelled_pid, "the cancelled child was killed");
    assert_eq!(suite.markers.read(&format!("{token}-ended")), None);
    assert_cancelled_with_one_attempt(&handle).await;
}

/// An isolated task with no grace that ignores the cancel is not killed: it
/// runs to its end, and its child runs the next task.
#[tokio::test]
#[serial]
async fn an_isolated_task_with_no_grace_is_not_killed() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let token = new_token();
    let handle = suite
        .function(cancel::cancel_ignored_no_grace::handle)
        .send(cancel::cancel_ignored_no_grace::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_cancel_ignored_no_grace");
    let cancelled_pid = started_pid(&suite, &token).await;

    let _cancel_at = cancel_running(&suite, handle.task_id()).await;

    let ended_pid = marker_within(
        &suite,
        &format!("{token}-ended"),
        cancel::IGNORED_RUN + MARKER_WAIT,
    )
    .await
    .unwrap_or_else(|| panic!("the task of {token} ran to its end"));
    assert_eq!(ended_pid, cancelled_pid);
    assert_eq!(isolated_pid(&suite).await, cancelled_pid);
    assert_cancelled_with_one_attempt(&handle).await;
}
