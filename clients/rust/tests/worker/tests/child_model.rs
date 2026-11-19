//! The child model of a Rust worker: each execution class runs its bodies
//! in children of the worker binary, and a timeout, a blocked runtime, a
//! crash or a drain ends one child, never the worker (Python
//! `clients/python/tests/e2e/test_shared_async_tasks.py`, `test_shared_thread_tasks.py`,
//! `test_task_timeout.py`).
//!
//! Four tests show that the child model closes the defects of the old
//! in-process runner: #559 (a body that blocks the runtime stopped the whole
//! worker), #560 (a timed-out blocking task overlapped its retry), #561 (the
//! drain left rows RUNNING and held the exit), #562 (a crash in task code
//! ended every task of the worker).

use std::time::{Duration, Instant};

use horsies::{TaskHandle, TaskInfoParts, TaskOutcome, TaskStatus, UserErrorCode};
use horsies_test_support::process::StopEnd;
use horsies_test_support::rows;
use horsies_test_worker::harness::{failed_code, outcome, succeeded, Suite};
use horsies_test_worker::instance::{Instance, Profile, TEST_DRAIN_TIMEOUT};
use horsies_test_worker::tasks::{basic, classes};
use serde::de::DeserializeOwned;
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for the outcome of a task that a running worker runs.
const RESULT_WAIT: Duration = Duration::from_secs(30);

/// Every part of the task info.
const ALL_PARTS: TaskInfoParts = TaskInfoParts {
    result: true,
    failed_reason: true,
    attempts: true,
};

/// The process id of the isolated child of a split profile.
async fn isolated_pid(suite: &Suite) -> u32 {
    let handle = suite
        .function(basic::pid::handle)
        .send(basic::pid::Input { sleep_ms: 0 })
        .await
        .expect("send e2e_pid");
    succeeded(&handle, RESULT_WAIT).await
}

/// The `"<pid>:<start ms>:<end ms>"` of a stamping body.
fn stamp_parts(stamp: &str) -> (String, u128, u128) {
    let parts: Vec<&str> = stamp.split(':').collect();
    let [pid, start, end] = parts.as_slice() else {
        panic!("a stamp is pid:start:end, not {stamp}");
    };
    (
        (*pid).to_owned(),
        start.parse().expect("a start in ms"),
        end.parse().expect("an end in ms"),
    )
}

/// The `cancel_end` of the `TASK_TIMEOUT` error of a task. Panics when the
/// task did not fail with `TASK_TIMEOUT`.
async fn cancel_end<T, E>(handle: &TaskHandle<T, E>) -> Option<String>
where
    T: DeserializeOwned + Clone + std::fmt::Debug,
    E: UserErrorCode,
{
    match outcome(handle, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().text(), "TASK_TIMEOUT");
            error
                .data()
                .map(|data| {
                    data.decode::<serde_json::Value>()
                        .expect("the data decodes as a JSON value")
                })
                .and_then(|data| data.get("cancel_end").cloned())
                .and_then(|end| end.as_str().map(str::to_owned))
        }
        TaskOutcome::Succeeded(value) => panic!("the task succeeded with {value:?}"),
    }
}

/// Python: `test_shared_async_tasks.py::test_shared_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished`.
/// The hang ignores the cancel signal: the child drops it after the grace,
/// and its neighbours on the same runtime finish.
#[tokio::test]
#[serial]
async fn shared_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::SplitShared1x4).await;
    let isolated = isolated_pid(&suite).await.to_string();
    let hang = suite
        .function(classes::shared_hang::handle)
        .send(())
        .await
        .expect("send e2e_shared_hang");
    let sleep = suite.function(classes::shared_sleep::handle);
    let mut sleepers = Vec::new();
    for _sleeper in 0..3 {
        sleepers.push(
            sleep
                .send(classes::shared_sleep::Input {
                    milliseconds: 1_500,
                })
                .await
                .expect("send e2e_shared_sleep"),
        );
    }
    let mut stamps = Vec::new();
    for sleeper in &sleepers {
        stamps.push(stamp_parts(&succeeded(sleeper, RESULT_WAIT).await));
    }
    match outcome(&hang, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().text(), "TASK_TIMEOUT");
            let data: serde_json::Value = error
                .data()
                .expect("TASK_TIMEOUT stores data")
                .decode()
                .expect("the data decodes as a JSON value");
            assert_eq!(data["cancel_end"], serde_json::json!("DROPPED_AFTER_GRACE"));
            assert_eq!(data["timeout_ms"], serde_json::json!(2_000));
        }
        TaskOutcome::Succeeded(value) => panic!("the hang succeeded with {value}"),
    }
    let pids: std::collections::BTreeSet<&str> =
        stamps.iter().map(|(pid, _, _)| pid.as_str()).collect();
    assert_eq!(pids.len(), 1, "the sleepers share one shared child");
    assert!(!pids.contains(isolated.as_str()), "not the isolated child");
    let latest_start = stamps.iter().map(|(_, start, _)| *start).max();
    let earliest_end = stamps.iter().map(|(_, _, end)| *end).min();
    assert!(latest_start < earliest_end, "the sleepers ran at once");
}

/// Decision 2 of the Rust client phase (`cancel_grace_ms`): a body that
/// returns at the cancel signal within its grace ends the run; the attempt
/// records `cancel_end` `RETURNED_AFTER_SIGNAL`. Python: the same
/// `cancel_end` key on a shared async timeout of
/// `test_shared_async_tasks.py`.
#[tokio::test]
#[serial]
async fn a_shared_timeout_that_returns_at_the_signal_records_returned_after_signal() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::SharedOnly1x4).await;
    let handle = suite
        .function(classes::shared_cooperative::handle)
        .send(())
        .await
        .expect("send e2e_shared_cooperative");
    assert_eq!(
        cancel_end(&handle).await.as_deref(),
        Some("RETURNED_AFTER_SIGNAL")
    );
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 0));
}

/// Python: `test_shared_async_tasks.py::test_a_stuck_task_waits_for_its_neighbour_then_retries_isolated`.
/// A Rust run that ignores its deadline is dropped after its grace, so it
/// does not wait for its neighbour; its retry completes.
#[tokio::test]
#[serial]
async fn a_stuck_task_is_dropped_after_its_grace_then_retries() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::SplitShared1x4).await;
    let neighbour = suite
        .function(classes::shared_sleep::handle)
        .send(classes::shared_sleep::Input {
            milliseconds: 8_000,
        })
        .await
        .expect("send the neighbour");
    let stuck = suite
        .function(classes::shared_stuck_once::handle)
        .send(classes::shared_stuck_once::Input {
            token: Uuid::new_v4().simple().to_string(),
        })
        .await
        .expect("send the stuck task");
    let _retry_pid = succeeded(&stuck, RESULT_WAIT).await;
    let _neighbour = succeeded(&neighbour, RESULT_WAIT).await;
    let info = stuck.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Completed, 1));
    let attempts = info.attempts.expect("attempts were asked for");
    let first = attempts
        .iter()
        .find(|attempt| attempt.attempt == 1)
        .expect("the first attempt");
    assert_eq!(first.error_code.as_deref(), Some("TASK_TIMEOUT"));
    assert!(first.will_retry);
    let ran = first.finished_at - first.started_at;
    assert!(
        ran < chrono::Duration::seconds(6),
        "the stuck attempt ended {ran} after its start: past its deadline and grace"
    );
}

/// #559. Python: `test_shared_async_tasks.py::test_a_blocked_loop_kills_the_child_and_names_the_culprit`.
/// The culprit blocks the runtime of its shared child for 30 s. The worker
/// keeps claiming and running other tasks meanwhile; the culprit fails with
/// `TASK_BLOCKED_LOOP` and its retry completes; the neighbour on the same
/// runtime fails with `NEIGHBOUR_BLOCKED_LOOP`.
#[tokio::test]
#[serial]
async fn a_blocked_runtime_kills_its_child_and_the_worker_runs_on() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::SplitShared1x4).await;
    let client = suite.client().await;
    let neighbour = suite
        .function(classes::shared_sleep::handle)
        .send(classes::shared_sleep::Input {
            milliseconds: 20_000,
        })
        .await
        .expect("send the neighbour");
    rows::wait_for_task_status(&client, neighbour.task_id(), "RUNNING", RESULT_WAIT).await;
    let culprit = suite
        .function(classes::shared_blocks_once::handle)
        .send(classes::shared_blocks_once::Input {
            token: Uuid::new_v4().simple().to_string(),
        })
        .await
        .expect("send the culprit");
    rows::wait_for_task_status(&client, culprit.task_id(), "RUNNING", RESULT_WAIT).await;
    let sent = Instant::now();
    let during = suite
        .function(basic::simple::handle)
        .send(basic::simple::Input { x: 3 })
        .await
        .expect("send during the block");
    assert_eq!(succeeded(&during, RESULT_WAIT).await, 6);
    assert!(
        sent.elapsed() < classes::BLOCK_DURATION / 2,
        "a task sent during the block ended {:?} later",
        sent.elapsed()
    );
    let _retry_pid = succeeded(&culprit, Duration::from_secs(60)).await;
    let culprit_info = culprit.info(ALL_PARTS).await.expect("read the culprit");
    assert_eq!(
        (culprit_info.status, culprit_info.retry_count),
        (TaskStatus::Completed, 1)
    );
    let first = culprit_info
        .attempts
        .expect("attempts were asked for")
        .into_iter()
        .find(|attempt| attempt.attempt == 1)
        .expect("the first attempt");
    assert_eq!(first.error_code.as_deref(), Some("TASK_BLOCKED_LOOP"));
    assert_eq!(
        failed_code(&neighbour, RESULT_WAIT).await,
        "NEIGHBOUR_BLOCKED_LOOP"
    );
    let neighbour_info = neighbour.info(ALL_PARTS).await.expect("read the neighbour");
    assert_eq!(neighbour_info.retry_count, 0);
}

/// Python: `test_shared_thread_tasks.py::test_thread_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished`.
#[tokio::test]
#[serial]
async fn thread_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::SplitThread1x4).await;
    let isolated = isolated_pid(&suite).await.to_string();
    let hang = suite
        .function(classes::thread_hang::handle)
        .send(())
        .await
        .expect("send e2e_thread_hang");
    let sleep = suite.function(classes::thread_sleep::handle);
    let mut sleepers = Vec::new();
    for _sleeper in 0..3 {
        sleepers.push(
            sleep
                .send(classes::thread_sleep::Input {
                    milliseconds: 1_500,
                })
                .await
                .expect("send e2e_thread_sleep"),
        );
    }
    let mut stamps = Vec::new();
    for sleeper in &sleepers {
        stamps.push(stamp_parts(&succeeded(sleeper, RESULT_WAIT).await));
    }
    match outcome(&hang, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().text(), "TASK_TIMEOUT");
            let data: serde_json::Value = error
                .data()
                .expect("TASK_TIMEOUT stores data")
                .decode()
                .expect("the data decodes as a JSON value");
            assert!(
                data.get("cancel_end").is_none(),
                "only a shared async timeout stores cancel_end: {data}"
            );
        }
        TaskOutcome::Succeeded(value) => panic!("the hang succeeded with {value}"),
    }
    assert!(
        stamps.iter().all(|(pid, _, _)| *pid != isolated),
        "the sleepers ran in the shared thread child"
    );
    let latest_start = stamps.iter().map(|(_, start, _)| *start).max();
    let earliest_end = stamps.iter().map(|(_, _, end)| *end).min();
    assert!(latest_start < earliest_end, "the sleepers ran at once");
}

/// #560. A seat thread past its deadline cannot be cancelled: the worker
/// closes its child and kills it, so no run of the task goes on after the
/// next attempt starts. The runs write their lines to one marker.
#[tokio::test]
#[serial]
async fn a_timed_out_thread_does_not_overlap_its_retry() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::SplitThread1x4).await;
    let token = Uuid::new_v4().simple().to_string();
    let handle = suite
        .function(classes::thread_overrun::handle)
        .send(classes::thread_overrun::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_thread_overrun");
    assert_eq!(
        failed_code(&handle, Duration::from_secs(90)).await,
        "TASK_TIMEOUT"
    );
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 3));
    let lines = suite
        .markers
        .read(&format!("{token}-runs"))
        .expect("the runs wrote their lines");
    let mut starts = std::collections::BTreeMap::new();
    let mut last_line = std::collections::BTreeMap::new();
    for line in lines.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        let [kind, attempt, at] = fields.as_slice() else {
            panic!("a run line is `<kind> <attempt> <ms>`, not {line:?}");
        };
        let attempt: u32 = attempt.parse().expect("an attempt number");
        let at: u128 = at.parse().expect("a time in ms");
        if *kind == "start" {
            let _first = starts.insert(attempt, at);
        }
        let latest = last_line.entry(attempt).or_insert(at);
        *latest = (*latest).max(at);
    }
    assert_eq!(starts.len(), 4, "four runs started: {lines}");
    for (attempt, start) in &starts {
        if let Some(previous_end) = last_line.get(&(attempt - 1)) {
            assert!(
                previous_end < start,
                "run {} wrote a line at {previous_end} after run {attempt} started at {start}",
                attempt - 1
            );
        }
    }
}

/// #561. A stop while a task runs past the drain timeout: the worker kills
/// that child at the drain deadline, the task takes the crash path, and
/// the worker exits on its own with status 0. Python:
/// `test_task_timeout.py::test_timeout_fails_task_and_worker_survives`, at
/// the drain.
#[tokio::test]
#[serial]
async fn a_drain_past_its_timeout_ends_the_task_and_the_worker() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let mut worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = suite
        .function(classes::outlives_drain::handle)
        .send(classes::outlives_drain::Input { seconds: 60 })
        .await
        .expect("send e2e_outlives_drain");
    rows::wait_for_task_status(&client, handle.task_id(), "RUNNING", RESULT_WAIT).await;
    let stopped = Instant::now();
    let end = worker.stop().await;
    let took = stopped.elapsed();
    match end {
        StopEnd::Exited(status) => assert!(
            status.success(),
            "the worker exit after a stop: {status}\n{}",
            worker.report()
        ),
        other => panic!(
            "the worker did not exit after the stop: {other:?}\n{}",
            worker.report()
        ),
    }
    assert!(
        took < TEST_DRAIN_TIMEOUT + Duration::from_secs(20),
        "the exit came {took:?} after the stop"
    );
    rows::wait_for_task_status(&client, handle.task_id(), "FAILED", RESULT_WAIT).await;
    let row = rows::read_task(&client, handle.task_id())
        .await
        .expect("the task row");
    assert_eq!(row.error_code.as_deref(), Some("WORKER_CRASHED"));
}

/// #562. Python: `test_task_timeout.py::test_pool_breaker_storm_does_not_kill_worker`.
/// A body that aborts its child ends that child only: its task fails with
/// `WORKER_CRASHED` without waiting for the stale threshold, the other
/// child's task completes, and the worker runs on.
#[tokio::test]
#[serial]
async fn a_crash_in_task_code_ends_one_child_not_the_worker() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let mut worker = suite.worker(Profile::Isolated2).await;
    let healthy = suite
        .function(basic::slow::handle)
        .send(basic::slow::Input { duration_ms: 3_000 })
        .await
        .expect("send the healthy task");
    let sent = Instant::now();
    let crashing = suite
        .function(basic::abort::handle)
        .send(())
        .await
        .expect("send e2e_abort");
    assert_eq!(failed_code(&crashing, RESULT_WAIT).await, "WORKER_CRASHED");
    assert!(
        sent.elapsed() < Duration::from_secs(25),
        "the crash was recorded {:?} after the send",
        sent.elapsed()
    );
    assert_eq!(succeeded(&healthy, RESULT_WAIT).await, "slept_3000");
    assert!(
        worker.exit_status().is_none(),
        "the worker ended with the child\n{}",
        worker.report()
    );
    let after = suite
        .function(basic::simple::handle)
        .send(basic::simple::Input { x: 4 })
        .await
        .expect("send after the crash");
    assert_eq!(succeeded(&after, RESULT_WAIT).await, 8);
}

/// Python: `test_child_recycling.py`. Each isolated child of
/// `recycle-after-2-tasks` is replaced after two tasks: six tasks, one at a
/// time, run in at least three processes.
#[tokio::test]
#[serial]
async fn an_isolated_child_is_replaced_after_its_task_limit() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::RecycleAfter2).await;
    let pid = suite.function(basic::pid::handle);
    let mut pids = std::collections::BTreeSet::new();
    for _task in 0..6 {
        let handle = pid
            .send(basic::pid::Input { sleep_ms: 0 })
            .await
            .expect("send e2e_pid");
        let _new = pids.insert(succeeded(&handle, RESULT_WAIT).await);
    }
    assert!(
        pids.len() >= 3,
        "six tasks ran in {} children: {pids:?}",
        pids.len()
    );
}
