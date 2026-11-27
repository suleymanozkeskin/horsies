//! Layer 1: task fundamentals on the child model (Python
//! `clients/python/tests/e2e/test_layer1_tasks.py`).
//!
//! Each test has its own database, builds the app of its instance in this
//! process, and starts the test binary as its worker; every task body runs
//! in a child of that worker.
//!
//! ```text
//! HORSIES_TEST_DATABASE_URL=postgresql://... cargo test -p horsies-test-worker --test layer1_tasks
//! ```

use std::convert::Infallible;
use std::time::Duration;

use chrono::Utc;
use horsies::{
    task, Horsies, ReadError, TaskError, TaskInfoParts, TaskOutcome, TaskSendError,
    TaskSendOptions, TaskStatus,
};
use horsies_test_support::{poll, rows};
use horsies_test_worker::harness::{failed_code, outcome, succeeded, within, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::tasks::{basic, custom, fencing, retry};
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for the outcome of a task that a running worker runs.
const RESULT_WAIT: Duration = Duration::from_secs(15);

/// Every part of the task info.
const ALL_PARTS: TaskInfoParts = TaskInfoParts {
    result: true,
    failed_reason: true,
    attempts: true,
};

/// A task that only this test process registers: no worker knows its name.
#[task("nonexistent_task_xyz")]
async fn nonexistent_task() -> Result<String, TaskError<Infallible>> {
    Ok("never".to_owned())
}

/// `e2e_simple` with an input type that the worker's `e2e_simple` does not
/// decode.
#[task("e2e_simple")]
async fn simple_as_text(x: String) -> Result<i64, TaskError<Infallible>> {
    Ok(i64::try_from(x.len()).unwrap_or(0))
}

/// Python: `test_layer1_tasks.py::test_simple_task_lifecycle`.
#[tokio::test]
#[serial]
async fn test_simple_task_lifecycle() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let simple = suite.function(basic::simple::function);
    let handle = simple
        .send(basic::simple::Input { x: 5 })
        .await
        .expect("send e2e_simple");
    assert_eq!(succeeded(&handle, RESULT_WAIT).await, 10);
}

/// Python: `test_layer1_tasks.py::test_status_transitions`.
#[tokio::test]
#[serial]
async fn test_status_transitions() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 5 })
        .await
        .expect("send e2e_simple");
    let _value = succeeded(&handle, RESULT_WAIT).await;
    let info = handle
        .info(TaskInfoParts::default())
        .await
        .expect("read the info");
    assert_eq!(info.status, TaskStatus::Completed);
    assert!(info.claimed_at.is_some(), "claimed_at is set");
    assert!(info.started_at.is_some(), "started_at is set");
    assert!(info.completed_at.is_some(), "completed_at is set");
}

/// Python: `test_layer1_tasks.py::test_kwargs_with_defaults`. A Rust input
/// has every field; a field that may be left out is an `Option`.
#[tokio::test]
#[serial]
async fn test_kwargs_with_defaults() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let kwargs = suite.function(basic::kwargs::function);
    let defaults = kwargs
        .send(basic::kwargs::Input {
            required: 5,
            optional: None,
            multiplier: None,
        })
        .await
        .expect("send with defaults");
    let given = kwargs
        .send(basic::kwargs::Input {
            required: 5,
            optional: Some("custom".to_owned()),
            multiplier: Some(2),
        })
        .await
        .expect("send with values");
    assert_eq!(succeeded(&defaults, RESULT_WAIT).await, "5_default");
    assert_eq!(succeeded(&given, RESULT_WAIT).await, "10_custom");
}

/// Python: `test_layer1_tasks.py::test_explicit_error_result`.
#[tokio::test]
#[serial]
async fn test_explicit_error_result() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::error::function)
        .send(())
        .await
        .expect("send e2e_error");
    match outcome(&handle, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().as_str(), "DELIBERATE_ERROR");
            assert_eq!(error.message(), Some("This is intentional"));
            assert_eq!(
                error.data().map(|data| data
                    .decode::<serde_json::Value>()
                    .expect("the data decodes")),
                Some(serde_json::json!({ "key": "value" }))
            );
        }
        TaskOutcome::Succeeded(value) => panic!("e2e_error succeeded with {value}"),
    }
}

/// Python: `test_layer1_tasks.py::test_no_retry_for_non_matching`. The
/// policy names `TRANSIENT`; the task fails with another code.
#[tokio::test]
#[serial]
async fn test_no_retry_for_non_matching() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::retry_other_code::function)
        .send(())
        .await
        .expect("send");
    assert_eq!(failed_code(&handle, RESULT_WAIT).await, "RETRYABLE_ERROR");
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 0));
}

/// Python: `test_task_timeout.py::test_timeout_fails_task_and_worker_survives`.
/// A blocking and an async isolated body past their deadline: the parent
/// kills each child at the deadline, records `TASK_TIMEOUT`, and runs the
/// next task in a new child.
#[tokio::test]
#[serial]
async fn test_task_timeout_fails_with_task_timeout() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let blocking = suite
        .function(basic::timeout_sleeper::function)
        .send(basic::timeout_sleeper::Input {
            duration_ms: 30_000,
        })
        .await
        .expect("send e2e_timeout_sleeper");
    assert_eq!(failed_code(&blocking, RESULT_WAIT).await, "TASK_TIMEOUT");
    let async_body = suite
        .function(basic::async_timeout_sleeper::function)
        .send(basic::async_timeout_sleeper::Input {
            duration_ms: 30_000,
        })
        .await
        .expect("send e2e_async_timeout_sleeper");
    assert_eq!(failed_code(&async_body, RESULT_WAIT).await, "TASK_TIMEOUT");
    match outcome(&async_body, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            let data: serde_json::Value = error
                .data()
                .expect("TASK_TIMEOUT stores data")
                .decode()
                .expect("the data decodes as a JSON value");
            assert_eq!(data["timeout_ms"], serde_json::json!(2_000));
            assert!(
                data.get("cancel_end").is_none(),
                "only a shared async timeout stores cancel_end: {data}"
            );
        }
        TaskOutcome::Succeeded(value) => panic!("the sleeper succeeded with {value}"),
    }
    let after = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 1 })
        .await
        .expect("send after the timeouts");
    assert_eq!(succeeded(&after, RESULT_WAIT).await, 2);
}

/// Old Rust `test_task_timeout_retry_opt_in`; Python:
/// `test_task_timeout.py::test_timeout_fails_task_and_worker_survives` with
/// `TASK_TIMEOUT` in `retry_for`.
#[tokio::test]
#[serial]
async fn test_task_timeout_retry_opt_in() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::timeout_retry_sleeper::function)
        .send(basic::timeout_retry_sleeper::Input {
            duration_ms: 30_000,
        })
        .await
        .expect("send");
    assert_eq!(failed_code(&handle, RESULT_WAIT).await, "TASK_TIMEOUT");
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 1));
    let attempts = info.attempts.expect("attempts were asked for");
    let codes: Vec<Option<String>> = attempts.iter().map(|a| a.error_code.clone()).collect();
    assert_eq!(
        codes,
        vec![
            Some("TASK_TIMEOUT".to_owned()),
            Some("TASK_TIMEOUT".to_owned())
        ]
    );
}

/// Old Rust `test_slow_task_completes`; Python: `test_simple_task_lifecycle`
/// with a body that sleeps 1.5 s.
#[tokio::test]
#[serial]
async fn test_slow_task_completes() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::slow::function)
        .send(basic::slow::Input { duration_ms: 1_500 })
        .await
        .expect("send e2e_slow");
    assert_eq!(succeeded(&handle, RESULT_WAIT).await, "slept_1500");
}

/// Python: `test_layer1_tasks.py::test_multiple_tasks_concurrent`.
#[tokio::test]
#[serial]
async fn test_multiple_tasks_concurrent() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let slow = suite.function(basic::slow::function);
    let mut handles = Vec::new();
    for _task in 0..4 {
        handles.push(
            slow.send(basic::slow::Input { duration_ms: 800 })
                .await
                .expect("send e2e_slow"),
        );
    }
    let client = suite.client().await;
    let most_running = poll::max_during(
        Duration::from_secs(3),
        Duration::from_millis(50),
        async || {
            rows::count(
                &client,
                "SELECT count(*) FROM horsies_tasks WHERE status = 'RUNNING'",
                &[],
            )
            .await
        },
    )
    .await;
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    assert!(
        most_running >= 2,
        "at most {most_running} tasks ran at once"
    );
}

/// Old Rust `test_multiple_tasks_concurrent_db_proof`; Python:
/// `test_multiple_tasks_concurrent`. Four children run four bodies at once:
/// every body started before any ended.
#[tokio::test]
#[serial]
async fn test_multiple_tasks_concurrent_db_proof() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated4).await;
    let stamp = suite.function(basic::stamp::function);
    let mut handles = Vec::new();
    for _task in 0..4 {
        handles.push(
            stamp
                .send(basic::stamp::Input { duration_ms: 1_500 })
                .await
                .expect("send e2e_stamp"),
        );
    }
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut pids = Vec::new();
    for handle in &handles {
        let stamp = succeeded(handle, RESULT_WAIT).await;
        let parts: Vec<&str> = stamp.split(':').collect();
        let [pid, start, end] = parts.as_slice() else {
            panic!("a stamp is pid:start:end, not {stamp}");
        };
        pids.push((*pid).to_owned());
        starts.push(start.parse::<u128>().expect("a start in ms"));
        ends.push(end.parse::<u128>().expect("an end in ms"));
    }
    pids.sort();
    pids.dedup();
    assert_eq!(pids.len(), 4, "each body ran in its own child");
    assert!(
        starts.iter().max() < ends.iter().min(),
        "every body started before the first ended"
    );
}

/// Old Rust `test_healthcheck`; Python: the ready check of every test.
#[tokio::test]
#[serial]
async fn test_healthcheck() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::healthcheck::function)
        .send(())
        .await
        .expect("send e2e_healthcheck");
    assert_eq!(succeeded(&handle, RESULT_WAIT).await, "ready");
}

/// Python: `test_layer1_tasks.py::test_retry_exhausted`.
#[tokio::test]
#[serial]
async fn test_retry_exhausted() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::retry_exhausted::function)
        .send(())
        .await
        .expect("send");
    assert_eq!(failed_code(&handle, RESULT_WAIT).await, "TRANSIENT");
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 3));
}

/// Python: `test_layer1_tasks.py::test_retry_eventual_success`.
#[tokio::test]
#[serial]
async fn test_retry_eventual_success() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::retry_success::function)
        .send(retry::retry_success::Input {
            token: Uuid::new_v4().simple().to_string(),
        })
        .await
        .expect("send");
    assert_eq!(
        succeeded(&handle, RESULT_WAIT).await,
        "succeeded_on_attempt_3"
    );
}

/// Python: `test_layer1_tasks.py::test_good_until_already_past`.
#[tokio::test]
#[serial]
async fn test_good_until_already_past() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let past = Utc::now() - chrono::Duration::seconds(10);
    let handle = suite
        .function(basic::simple::function)
        .send_with_options(
            TaskSendOptions::new().good_until(past),
            basic::simple::Input { x: 5 },
        )
        .await
        .expect("send");
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    poll::never_during(
        Duration::from_secs(2),
        "the expired task runs",
        async || {
            let status = rows::task_status(&client, handle.task_id()).await;
            !matches!(status.as_deref(), Some("PENDING" | "EXPIRED"))
        },
    )
    .await;
}

/// Old Rust `test_good_until_future_completes`; Python:
/// `test_layer1_tasks.py::test_task_completes_before_expiry`.
#[tokio::test]
#[serial]
async fn test_good_until_future_completes() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::simple::function)
        .send_with_options(
            TaskSendOptions::new().good_until(Utc::now() + chrono::Duration::seconds(60)),
            basic::simple::Input { x: 5 },
        )
        .await
        .expect("send");
    assert_eq!(succeeded(&handle, RESULT_WAIT).await, 10);
}

/// Python: `test_layer1_tasks.py::test_retry_blocked_by_good_until_expiry`.
/// The first retry would start 60 s after the failure, past the deadline:
/// no retry, the task fails.
#[tokio::test]
#[serial]
async fn test_retry_blocked_by_good_until_expiry() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::retry_long_interval::function)
        .send_with_options(
            TaskSendOptions::new().good_until(Utc::now() + chrono::Duration::seconds(10)),
            (),
        )
        .await
        .expect("send");
    assert_eq!(failed_code(&handle, RESULT_WAIT).await, "TRANSIENT");
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 0));
}

/// Python: `test_layer1_tasks.py::test_retry_succeeds_within_good_until`.
#[tokio::test]
#[serial]
async fn test_retry_succeeds_within_good_until() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::retry_success::function)
        .send_with_options(
            TaskSendOptions::new().good_until(Utc::now() + chrono::Duration::seconds(60)),
            retry::retry_success::Input {
                token: Uuid::new_v4().simple().to_string(),
            },
        )
        .await
        .expect("send");
    assert_eq!(
        succeeded(&handle, RESULT_WAIT).await,
        "succeeded_on_attempt_3"
    );
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Completed, 2));
}

/// Python: `test_layer1_tasks.py::test_worker_resolution_error`.
#[tokio::test]
#[serial]
async fn test_worker_resolution_error() {
    let mut suite = Suite::start(BIN, Instance::Default).await;
    let unknown = nonexistent_task::register(&mut suite.app).expect("register in this process");
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = unknown.send(()).await.expect("send");
    assert_eq!(
        failed_code(&handle, RESULT_WAIT).await,
        "WORKER_RESOLUTION_ERROR"
    );
}

/// Python: `test_layer1_tasks.py::test_argument_deserialization_error`. A
/// second app stores `{"x": "not_a_number"}` for `e2e_simple`, whose input
/// field is an integer in the worker.
#[tokio::test]
#[serial]
async fn test_argument_deserialization_error() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let mut other = Horsies::new(suite.config()).expect("a second app on the database");
    let as_text = simple_as_text::register(&mut other).expect("register the text input");
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = as_text
        .send(simple_as_text::Input {
            x: "not_a_number".to_owned(),
        })
        .await
        .expect("send");
    assert_eq!(
        failed_code(&handle, RESULT_WAIT).await,
        "WORKER_SERIALIZATION_ERROR"
    );
}

/// Python: `test_layer1_tasks.py::test_task_cancelled`.
#[tokio::test]
#[serial]
async fn test_task_cancelled() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 5 })
        .await
        .expect("send");
    let _cancelled = horsies::monitoring::cancel_task(&suite.app, handle.task_id(), false)
        .await
        .expect("cancel the pending task");
    match outcome(&handle, Duration::from_secs(1)).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().as_str(), "TASK_CANCELLED");
            let data: serde_json::Value = error
                .data()
                .expect("TASK_CANCELLED stores data")
                .decode()
                .expect("the data decodes as a JSON value");
            assert_eq!(
                data["task_id"],
                serde_json::json!(handle.task_id().to_string())
            );
        }
        TaskOutcome::Succeeded(value) => panic!("the cancelled task succeeded with {value}"),
    }
}

/// Old Rust `test_task_cancelled_get_result`; Python:
/// `test_layer1_tasks.py::test_task_cancelled`, read by id.
#[tokio::test]
#[serial]
async fn test_task_cancelled_get_result() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 5 })
        .await
        .expect("send");
    let _cancelled = horsies::monitoring::cancel_task(&suite.app, handle.task_id(), false)
        .await
        .expect("cancel the pending task");
    let read = suite
        .app
        .get_result::<i64, Infallible>(handle.task_id(), within(Duration::from_secs(1)))
        .await
        .expect("read the result by id");
    match read {
        TaskOutcome::Failed(error) => assert_eq!(error.code().as_str(), "TASK_CANCELLED"),
        TaskOutcome::Succeeded(value) => panic!("the cancelled task succeeded with {value}"),
    }
}

/// Old Rust `test_task_info_after_completion`; Python:
/// `test_layer1_tasks.py::test_task_handle_info`.
#[tokio::test]
#[serial]
async fn test_task_info_after_completion() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 21 })
        .await
        .expect("send");
    let _value = succeeded(&handle, RESULT_WAIT).await;
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!(info.task_id, handle.task_id());
    assert_eq!(info.task_name, "e2e_simple");
    assert_eq!(info.queue, "default");
    assert_eq!(info.status, TaskStatus::Completed);
    let result = info.result.expect("the result was asked for");
    assert_eq!(
        result.ok().map(|value| value.get().to_owned()),
        Some("42".to_owned())
    );
    assert_eq!(info.attempts.map(|attempts| attempts.len()), Some(1));
}

/// Python: `test_layer1_tasks.py::test_exponential_backoff_state`.
#[tokio::test]
#[serial]
async fn test_exponential_backoff_state() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::exponential::function)
        .send(())
        .await
        .expect("send");
    let info = poll::poll_until(Duration::from_secs(5), "a retry is scheduled", async || {
        let info = handle.info(TaskInfoParts::default()).await.ok()?;
        (info.retry_count >= 1 && info.next_retry_at.is_some()).then_some(info)
    })
    .await;
    assert_eq!(info.max_retries, 3);
}

/// Python: `test_layer1_tasks.py::test_exponential_backoff_intervals`. The
/// second retry waits longer than the first; `sent_at` never changes.
#[tokio::test]
#[serial]
async fn test_exponential_backoff_intervals() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let handle = suite
        .function(retry::exponential::function)
        .send(())
        .await
        .expect("send");
    let first = handle
        .info(TaskInfoParts::default())
        .await
        .expect("read the info");
    let mut enqueued = vec![first.due_at];
    let _worker = suite.worker(Profile::Isolated1).await;
    poll::poll_until(Duration::from_secs(15), "the second retry", async || {
        let info = handle.info(TaskInfoParts::default()).await.ok()?;
        let count = usize::try_from(info.retry_count).ok()?;
        if count == enqueued.len() {
            enqueued.push(info.due_at);
        }
        assert_eq!(info.sent_at, first.sent_at, "sent_at changed on a retry");
        (enqueued.len() > 2).then_some(())
    })
    .await;
    let first_interval = enqueued[1] - enqueued[0];
    let second_interval = enqueued[2] - enqueued[1];
    assert!(
        second_interval > first_interval,
        "exponential intervals: {first_interval} then {second_interval}"
    );
}

/// Python: `test_layer1_tasks.py::test_auto_retry_by_error_code`.
#[tokio::test]
#[serial]
async fn test_auto_retry_by_error_code() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::retry_error_code::function)
        .send(())
        .await
        .expect("send");
    assert_eq!(failed_code(&handle, RESULT_WAIT).await, "RETRYABLE_ERROR");
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 1));
}

/// Python: `test_layer1_tasks.py::test_task_expires_before_claim`.
#[tokio::test]
#[serial]
async fn test_task_expires_before_claim() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let expiry = Utc::now() + chrono::Duration::milliseconds(500);
    let handle = suite
        .function(basic::simple::function)
        .send_with_options(
            TaskSendOptions::new().good_until(expiry),
            basic::simple::Input { x: 5 },
        )
        .await
        .expect("send");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    poll::never_during(
        Duration::from_secs(2),
        "the expired task runs",
        async || {
            let status = rows::task_status(&client, handle.task_id()).await;
            !matches!(status.as_deref(), Some("PENDING" | "EXPIRED"))
        },
    )
    .await;
}

/// Old Rust `test_good_until_expires_while_claimed_before_start`; Python:
/// `test_task_expires_before_claim`, with the deadline passing while the
/// task is CLAIMED by a prefetch and its child is busy.
#[tokio::test]
#[serial]
async fn test_good_until_expires_while_claimed_before_start() {
    let suite = Suite::start(BIN, Instance::SoftCap).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let blocker = suite
        .function(basic::slow::function)
        .send(basic::slow::Input { duration_ms: 4_000 })
        .await
        .expect("send the blocker");
    let client = suite.client().await;
    rows::wait_for_task_status(&client, blocker.task_id(), "RUNNING", RESULT_WAIT).await;
    let expiring = suite
        .function(basic::simple::function)
        .send_with_options(
            TaskSendOptions::new().good_until(Utc::now() + chrono::Duration::seconds(1)),
            basic::simple::Input { x: 5 },
        )
        .await
        .expect("send the expiring task");
    rows::wait_for_task_status(&client, expiring.task_id(), "EXPIRED", RESULT_WAIT).await;
    assert_eq!(succeeded(&blocker, RESULT_WAIT).await, "slept_4000");
}

/// Python: `test_layer1_tasks.py::test_priority_ordering`.
#[tokio::test]
#[serial]
async fn test_priority_ordering() {
    let suite = Suite::start(BIN, Instance::CustomQueues).await;
    let low = suite
        .function(custom::low::function)
        .send(())
        .await
        .expect("send e2e_low");
    let high = suite
        .function(custom::high::function)
        .send(())
        .await
        .expect("send e2e_high");
    let _worker = suite.worker(Profile::Isolated1).await;
    let _low = succeeded(&low, RESULT_WAIT).await;
    let _high = succeeded(&high, RESULT_WAIT).await;
    let client = suite.client().await;
    let low_row = rows::read_task(&client, low.task_id())
        .await
        .expect("low row");
    let high_row = rows::read_task(&client, high.task_id())
        .await
        .expect("high row");
    assert!(
        high_row.started_at < low_row.started_at,
        "high started at {:?}, low at {:?}",
        high_row.started_at,
        low_row.started_at
    );
}

/// Python: `test_layer1_tasks.py::test_skip_locked_prevents_double_claim`.
/// Two workers, 20 tokens: each body runs once.
#[tokio::test]
#[serial]
async fn test_skip_locked_prevents_double_claim() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _first = suite.worker(Profile::Isolated2).await;
    let _second = suite.worker(Profile::Isolated2).await;
    let idempotent = suite.function(basic::idempotent::function);
    let mut handles = Vec::new();
    for index in 0..20 {
        handles.push(
            idempotent
                .send(basic::idempotent::Input {
                    token: format!("task_{index}"),
                })
                .await
                .expect("send"),
        );
    }
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    assert_eq!(suite.markers.count("idempotent-task_"), 20);
}

/// Python: `test_layer1_tasks.py::test_max_claim_batch`.
#[tokio::test]
#[serial]
async fn test_max_claim_batch() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::ClaimBatch1).await;
    let slow = suite.function(basic::slow::function);
    let mut handles = Vec::new();
    for _task in 0..6 {
        handles.push(
            slow.send(basic::slow::Input { duration_ms: 300 })
                .await
                .expect("send"),
        );
    }
    let client = suite.client().await;
    let most_claimed = poll::max_during(
        Duration::from_secs(4),
        Duration::from_millis(50),
        async || {
            rows::count(
                &client,
                "SELECT count(*) FROM horsies_tasks WHERE status = 'CLAIMED'",
                &[],
            )
            .await
        },
    )
    .await;
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    assert!(
        most_claimed <= 1,
        "{most_claimed} tasks were CLAIMED at once"
    );
}

/// Python: `test_layer1_tasks.py::test_task_not_found`.
#[tokio::test]
#[serial]
async fn test_task_not_found() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let read = suite
        .app
        .get_result::<i64, Infallible>(Uuid::nil(), within(Duration::from_secs(1)))
        .await;
    assert!(
        matches!(read, Err(ReadError::TaskNotFound { .. })),
        "{read:?}"
    );
}

/// Python: `test_layer1_tasks.py::test_wait_timeout`. No worker runs.
#[tokio::test]
#[serial]
async fn test_wait_timeout() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 5 })
        .await
        .expect("send");
    let read = handle.get(within(Duration::from_millis(500))).await;
    assert!(
        matches!(read, Err(ReadError::WaitTimeout { .. })),
        "{read:?}"
    );
}

/// Python: `test_layer1_tasks.py::test_library_error_code_in_user_task`.
#[tokio::test]
#[serial]
async fn test_library_error_code_in_user_task() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::error_code::function)
        .send(())
        .await
        .expect("send");
    match outcome(&handle, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().as_str(), "TASK_EXCEPTION");
            assert_eq!(error.message(), Some("boom"));
        }
        TaskOutcome::Succeeded(value) => panic!("e2e_error_code succeeded with {value}"),
    }
}

/// Python: `test_layer1_tasks.py::test_exception_becomes_error`. A panic of
/// the body is recorded with its exception record; the child survives it.
#[tokio::test]
#[serial]
async fn test_exception_becomes_error() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::panics::function)
        .send(())
        .await
        .expect("send");
    match outcome(&handle, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().as_str(), "UNHANDLED_EXCEPTION");
            assert!(error.exception().is_some(), "a panic stores its record");
            let data: serde_json::Value = error
                .data()
                .expect("a panic stores data")
                .decode()
                .expect("the data decodes as a JSON value");
            assert_eq!(data["exception_type"], serde_json::json!("panic"));
        }
        TaskOutcome::Succeeded(value) => panic!("e2e_panic succeeded with {value}"),
    }
}

/// Python: `test_layer1_tasks.py::test_complex_result_serialization`.
#[tokio::test]
#[serial]
async fn test_complex_result_serialization() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::complex_result::function)
        .send(())
        .await
        .expect("send");
    let value = succeeded(&handle, RESULT_WAIT).await;
    assert_eq!(value.value, 42);
    assert_eq!(value.nested["a"], vec![1, 2, 3]);
    assert_eq!(value.nested["b"], vec![4, 5]);
}

/// Python: `test_layer1_tasks.py::test_custom_mode_queue_routing`.
#[tokio::test]
#[serial]
async fn test_custom_mode_queue_routing() {
    let suite = Suite::start(BIN, Instance::CustomQueues).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let high = suite
        .function(custom::high::function)
        .send(())
        .await
        .expect("send high");
    let normal = suite
        .function(custom::normal::function)
        .send(())
        .await
        .expect("send normal");
    let low = suite
        .function(custom::low::function)
        .send(())
        .await
        .expect("send low");
    assert_eq!(succeeded(&high, RESULT_WAIT).await, "high");
    assert_eq!(succeeded(&normal, RESULT_WAIT).await, "normal");
    assert_eq!(succeeded(&low, RESULT_WAIT).await, "low");
}

/// Python: `test_layer1_tasks.py::test_custom_mode_queue_names_stored`.
#[tokio::test]
#[serial]
async fn test_custom_mode_queue_names_stored() {
    let suite = Suite::start(BIN, Instance::CustomQueues).await;
    let high = suite
        .function(custom::high::function)
        .send(())
        .await
        .expect("send high");
    let normal = suite
        .function(custom::normal::function)
        .send(())
        .await
        .expect("send normal");
    let low = suite
        .function(custom::low::function)
        .send(())
        .await
        .expect("send low");
    let client = suite.client().await;
    for (handle, queue, priority) in [
        (&high, "high", 1),
        (&normal, "normal", 50),
        (&low, "low", 100),
    ] {
        let row = rows::read_task(&client, handle.task_id())
            .await
            .expect("a live row");
        assert_eq!((row.queue_name.as_str(), row.priority), (queue, priority));
    }
}

/// Old Rust `test_attempt_recorded_on_success`; Python:
/// `test_layer1_tasks.py::test_task_handle_info` with its attempts.
#[tokio::test]
#[serial]
async fn test_attempt_recorded_on_success() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 1 })
        .await
        .expect("send");
    let _value = succeeded(&handle, RESULT_WAIT).await;
    let attempts = handle
        .info(ALL_PARTS)
        .await
        .expect("read the info")
        .attempts
        .expect("attempts were asked for");
    let [attempt] = attempts.as_slice() else {
        panic!("one attempt, not {attempts:?}");
    };
    assert_eq!(attempt.attempt, 1);
    assert_eq!(attempt.outcome.to_string(), "COMPLETED");
    assert!(!attempt.will_retry);
    assert_eq!(attempt.error_code, None);
}

/// Old Rust `test_attempt_recorded_on_failure`; Python:
/// `test_layer1_tasks.py::test_explicit_error_result` with its attempts.
#[tokio::test]
#[serial]
async fn test_attempt_recorded_on_failure() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::error::function)
        .send(())
        .await
        .expect("send");
    let _code = failed_code(&handle, RESULT_WAIT).await;
    let attempts = handle
        .info(ALL_PARTS)
        .await
        .expect("read the info")
        .attempts
        .expect("attempts were asked for");
    let [attempt] = attempts.as_slice() else {
        panic!("one attempt, not {attempts:?}");
    };
    assert_eq!(attempt.outcome.to_string(), "FAILED");
    assert!(!attempt.will_retry);
    assert_eq!(attempt.error_code.as_deref(), Some("DELIBERATE_ERROR"));
}

/// Old Rust `test_attempt_recorded_on_retry_exhausted`; Python:
/// `test_layer1_tasks.py::test_retry_exhausted` with its attempts.
#[tokio::test]
#[serial]
async fn test_attempt_recorded_on_retry_exhausted() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(retry::retry_exhausted::function)
        .send(())
        .await
        .expect("send");
    let _code = failed_code(&handle, RESULT_WAIT).await;
    let attempts = handle
        .info(ALL_PARTS)
        .await
        .expect("read the info")
        .attempts
        .expect("attempts were asked for");
    // Newest first: the last attempt does not retry, the three before do.
    let retries: Vec<(i32, bool)> = attempts.iter().map(|a| (a.attempt, a.will_retry)).collect();
    assert_eq!(retries, vec![(4, false), (3, true), (2, true), (1, true)]);
    assert!(attempts
        .iter()
        .all(|a| a.error_code.as_deref() == Some("TRANSIENT")));
}

/// Old Rust `test_enqueue_sha_idempotent_same_sha`; Python:
/// `clients/python/tests/unit/test_task_decorator.py` idempotency key sends. A second send
/// of the same command with the same key answers with the first task.
#[tokio::test]
#[serial]
async fn test_idempotency_key_same_input_returns_the_first_task() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let simple = suite.function(basic::simple::function);
    let options = TaskSendOptions::new().idempotency_key("order-7");
    let first = simple
        .send_with_options(options.clone(), basic::simple::Input { x: 7 })
        .await
        .expect("first send");
    let second = simple
        .send_with_options(options, basic::simple::Input { x: 7 })
        .await
        .expect("second send");
    assert_eq!(first.task_id(), second.task_id());
    let _worker = suite.worker(Profile::Isolated1).await;
    assert_eq!(succeeded(&first, RESULT_WAIT).await, 14);
    let client = suite.client().await;
    assert_eq!(
        rows::count_tasks(&client, "e2e_simple", "COMPLETED").await,
        1
    );
}

/// Old Rust `test_enqueue_sha_payload_mismatch`; Python: the
/// `IDEMPOTENCY_KEY_CONFLICT` of a key that another command holds.
#[tokio::test]
#[serial]
async fn test_idempotency_key_other_input_is_a_conflict() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let simple = suite.function(basic::simple::function);
    let options = TaskSendOptions::new().idempotency_key("order-8");
    let _first = simple
        .send_with_options(options.clone(), basic::simple::Input { x: 8 })
        .await
        .expect("first send");
    let second = simple
        .send_with_options(options, basic::simple::Input { x: 9 })
        .await;
    assert!(
        matches!(second, Err(TaskSendError::IdempotencyKeyConflict { .. })),
        "{second:?}"
    );
}

/// Python: `test_layer1_tasks.py::test_a_task_cannot_wait_for_another_task`.
#[tokio::test]
#[serial]
async fn test_a_task_cannot_wait_for_another_task() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let handle = suite
        .function(basic::wait_inside_task::function)
        .send(())
        .await
        .expect("send");
    assert!(
        succeeded(&handle, RESULT_WAIT).await,
        "the wait inside the task was not refused with WAIT_INSIDE_TASK"
    );
}

/// Python: `test_async_tasks.py::test_an_async_task_sends_with_send_async`.
#[tokio::test]
#[serial]
async fn test_an_async_task_sends() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let handle = suite
        .function(basic::async_sends::function)
        .send(basic::async_sends::Input { x: 4 })
        .await
        .expect("send");
    let sent: Uuid = succeeded(&handle, RESULT_WAIT)
        .await
        .parse()
        .expect("the sent task id");
    let read = suite
        .app
        .get_result::<i64, Infallible>(sent, within(RESULT_WAIT))
        .await
        .expect("read the task that the task sent");
    assert_eq!(read, TaskOutcome::Succeeded(8));
}

/// Python: `test_layer1_tasks.py::test_send_suppressed`.
#[tokio::test]
#[serial]
async fn test_send_suppressed() {
    let suite = Suite::start(BIN, Instance::Default).await;
    suite.app.suppress_sends(true);
    let refused = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 1 })
        .await;
    assert!(
        matches!(refused, Err(TaskSendError::Suppressed { .. })),
        "{refused:?}"
    );
    let client = suite.client().await;
    assert_eq!(
        rows::read_tasks_named(&client, "e2e_simple").await,
        Vec::new()
    );
}

/// The fencing token of an isolated run: the task id of the run and the
/// value 1 on the first attempt, for an async body and for a blocking body.
#[tokio::test]
#[serial]
async fn test_fencing_token_of_isolated_runs() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let async_run = suite
        .function(fencing::fence_isolated_async::function)
        .send(())
        .await
        .expect("send e2e_fence_isolated_async");
    let blocking_run = suite
        .function(fencing::fence_isolated_blocking::function)
        .send(())
        .await
        .expect("send e2e_fence_isolated_blocking");
    assert_eq!(
        succeeded(&async_run, RESULT_WAIT).await,
        fencing::Fence {
            task_id: async_run.task_id().to_string(),
            value: 1,
        }
    );
    assert_eq!(
        succeeded(&blocking_run, RESULT_WAIT).await,
        fencing::Fence {
            task_id: blocking_run.task_id().to_string(),
            value: 1,
        }
    );
}

/// Two reads in one attempt, and a read in work that the body started with
/// `TaskRuntime::spawn`, give the same token.
#[tokio::test]
#[serial]
async fn test_fencing_token_is_the_same_within_one_attempt() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(fencing::fence_reads_twice::function)
        .send(())
        .await
        .expect("send e2e_fence_reads_twice");
    let expected = fencing::Fence {
        task_id: handle.task_id().to_string(),
        value: 1,
    };
    assert_eq!(
        succeeded(&handle, RESULT_WAIT).await,
        vec![expected.clone(), expected.clone(), expected]
    );
}

/// The retry of a task runs with a greater token: the second run, which
/// the marker files count, reads the value 2 and the same task id.
#[tokio::test]
#[serial]
async fn test_fencing_token_rises_on_retry() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(fencing::fence_retry::function)
        .send(fencing::fence_retry::Input {
            marker: Uuid::new_v4().simple().to_string(),
        })
        .await
        .expect("send e2e_fence_retry");
    let run = succeeded(&handle, RESULT_WAIT).await;
    assert_eq!(run.counted_run, 2);
    assert_eq!(
        run.fence,
        fencing::Fence {
            task_id: handle.task_id().to_string(),
            value: 2,
        }
    );
    let info = handle.info(ALL_PARTS).await.expect("read the info");
    assert_eq!((info.status, info.retry_count), (TaskStatus::Completed, 1));
}
