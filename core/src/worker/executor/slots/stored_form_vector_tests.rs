//! The error data vectors (`tests/fixtures/stored_forms/error_data.json`)
//! through the slot writes: `TASK_TIMEOUT` with each `cancel_end`, and
//! `WORKER_CRASHED` of a killed child.

use chrono::{TimeZone, Utc};
use uuid::Uuid;

use super::outcome::{killed_child_write, timeout_write};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::core::task::error::OperationalErrorCode;
use crate::stored_form_vectors::error_data::{vector, ErrorVector};
use crate::worker::executor::protocol::TaskTimeoutMs;
use crate::worker::retry::read_stored_options;

/// A claimed shared async row with the vector's task id.
fn claimed(vector: &ErrorVector) -> ClaimedTask {
    let task_id = vector.inputs["task_id"]
        .as_str()
        .unwrap_or_else(|| panic!("{}: a task id", vector.name));
    ClaimedTask {
        task_id: Uuid::parse_str(task_id).expect("a UUID"),
        task_name: "app.tasks.work".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: Utc.with_ymd_and_hms(2026, 8, 12, 9, 10, 0).unwrap(),
        execution: ExecutionClass::SharedAsync,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(None),
    }
}

#[test]
fn the_timeout_write_stores_python_s_text_for_each_cancel_end() {
    for (name, cancel) in [
        (
            "task_timeout_returned_after_signal",
            TimeoutCancel::Ended(CancelEnd::ReturnedAfterSignal),
        ),
        (
            "task_timeout_dropped_after_grace",
            TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace),
        ),
        (
            "task_timeout_killed_with_child",
            TimeoutCancel::Ended(CancelEnd::KilledWithChild),
        ),
        ("task_timeout_no_cancel_signal", TimeoutCancel::NoSignal),
    ] {
        let vector = vector(name);
        assert_eq!(vector.inputs["timeout_ms"], 1000, "{name}");
        let write = timeout_write(
            &claimed(&vector),
            "worker-1",
            TaskTimeoutMs::new(1000).unwrap(),
            RetryPlacement::SameClass,
            cancel,
        );
        assert_eq!(write.result_json, vector.text, "{name}");
    }
}

#[test]
fn the_killed_child_write_stores_python_s_text() {
    let vector = vector("worker_crashed_killed_child");
    let worker_id = vector.inputs["worker_id"]
        .as_str()
        .expect("a worker id")
        .to_owned();
    let message = vector.error["message"]
        .as_str()
        .expect("a message")
        .to_owned();
    let write = killed_child_write(
        &claimed(&vector),
        &worker_id,
        OperationalErrorCode::WorkerCrashed.into(),
        message,
        RetryPlacement::SameClass,
    );
    assert_eq!(write.result_json, vector.text);
}
