use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::task::execution::ExecutionClass;
use crate::core::types::status::TaskAttemptOutcome;
use crate::core::types::TaskStatus;

/// The stored result of a task record, in one of three states.
///
/// - `NotRequested`: the read did not ask for the result.
/// - `NotStored`: the read asked, and the task stored no result. A live task
///   has none: the result of a terminal task is in its history row.
/// - `Stored(value)`: the read asked, and the task stored a result.
///
/// `R` is the form of the stored result: the envelope text for a raw read, a
/// task result for a typed read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultSlot<R> {
    NotRequested,
    NotStored,
    Stored(R),
}

impl<R> ResultSlot<R> {
    /// The same slot with its stored value read by `read`. The other two
    /// states pass unchanged and call nothing. Pure.
    ///
    /// Errors: the error of `read`.
    pub fn try_map<S, X>(self, read: impl FnOnce(R) -> Result<S, X>) -> Result<ResultSlot<S>, X> {
        match self {
            Self::NotRequested => Ok(ResultSlot::NotRequested),
            Self::NotStored => Ok(ResultSlot::NotStored),
            Self::Stored(value) => read(value).map(ResultSlot::Stored),
        }
    }
}

/// Metadata for a broker-backed task.
///
/// `R` is the form of the stored result. The broker's read gives the result
/// envelope as stored, checked as strict JSON and as an object
/// ([`crate::broker::StoredEnvelopeText`]), with no typed decode (Python
/// `PostgresBroker.get_task_info_async`). A client reads it with the task's
/// types through [`TaskInfo::try_map_result`] (Python
/// `Horsies.get_task_info_async`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo<R> {
    pub task_id: Uuid,
    pub task_name: String,
    pub status: TaskStatus,
    /// The queue of the task.
    pub queue: String,
    pub priority: i32,
    pub retry_count: u32,
    pub max_retries: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
    /// When the task became due: a worker may claim it from this time.
    pub due_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_process_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_reason: Option<String>,
    /// The stored result: not requested, not stored, or stored.
    pub result: ResultSlot<R>,

    /// Execution attempt history (opt-in via `include_attempts=true`).
    /// `None` when not requested, empty `Vec` when no attempts recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<Vec<TaskAttemptInfo>>,

    /// How a worker runs the task: the stored class of a live task, or
    /// [`ExecutionClass::NotKeptInHistory`] for a task read from history.
    pub execution: ExecutionClass,
}

impl<R> TaskInfo<R> {
    /// The same info with its result read by `read`; the other fields do not
    /// change. Without a stored result, `read` is not called. Pure.
    ///
    /// Errors: the error of `read`.
    pub fn try_map_result<S, X>(
        self,
        read: impl FnOnce(R) -> Result<S, X>,
    ) -> Result<TaskInfo<S>, X> {
        let result = self.result.try_map(read)?;
        Ok(TaskInfo {
            task_id: self.task_id,
            task_name: self.task_name,
            status: self.status,
            queue: self.queue,
            priority: self.priority,
            retry_count: self.retry_count,
            max_retries: self.max_retries,
            next_retry_at: self.next_retry_at,
            sent_at: self.sent_at,
            due_at: self.due_at,
            claimed_at: self.claimed_at,
            started_at: self.started_at,
            completed_at: self.completed_at,
            failed_at: self.failed_at,
            worker_hostname: self.worker_hostname,
            worker_pid: self.worker_pid,
            worker_process_name: self.worker_process_name,
            error_code: self.error_code,
            failed_reason: self.failed_reason,
            result,
            attempts: self.attempts,
            execution: self.execution,
        })
    }
}

/// Metadata for a single task execution attempt.
///
/// Mirrors Python's `TaskAttemptInfo` dataclass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAttemptInfo {
    pub task_id: Uuid,
    pub attempt: i32,
    pub outcome: TaskAttemptOutcome,
    pub will_retry: bool,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_pid: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_process_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::task::execution::Execution;
    use crate::core::task::result::TaskResult;

    /// A task info with its result read as a task result.
    type ReadInfo = TaskInfo<TaskResult<serde_json::Value>>;

    /// Helper to build a minimal TaskInfo for testing.
    fn make_task_info<R>(result: ResultSlot<R>) -> TaskInfo<R> {
        TaskInfo {
            task_id: Uuid::nil(),
            task_name: "my_task".to_owned(),
            status: TaskStatus::Completed,
            queue: "default".to_owned(),
            priority: 0,
            retry_count: 0,
            max_retries: 3,
            next_retry_at: None,
            sent_at: None,
            due_at: Utc::now(),
            claimed_at: None,
            started_at: None,
            completed_at: None,
            failed_at: None,
            worker_hostname: None,
            worker_pid: None,
            worker_process_name: None,
            error_code: None,
            failed_reason: None,
            result,
            attempts: None,
            execution: ExecutionClass::Live(Execution::Isolated),
        }
    }

    #[test]
    fn serialize_a_result_that_was_not_requested() {
        let info: ReadInfo = make_task_info(ResultSlot::NotRequested);
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["result"], "not_requested");
    }

    #[test]
    fn serialize_a_result_that_is_not_stored() {
        let info: ReadInfo = make_task_info(ResultSlot::NotStored);
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["result"], "not_stored");
    }

    #[test]
    fn serialize_a_stored_result() {
        let info = make_task_info(ResultSlot::Stored(TaskResult::Ok(
            serde_json::json!({"answer": 42}),
        )));
        let json = serde_json::to_value(&info).unwrap();
        // TaskResult serializes as Python's envelope.
        let stored = &json["result"]["stored"];
        assert_eq!(stored["__h_task_result__"], true);
        assert_eq!(stored["ok"]["answer"], 42);
    }

    #[test]
    fn deserialize_without_result_field_is_refused() {
        // The state is never guessed: a task info without it does not decode.
        let json = serde_json::json!({
            "task_id": "00000000-0000-0000-0000-000000000002",
            "task_name": "another_task",
            "status": "PENDING",
            "queue": "default",
            "priority": 5,
            "retry_count": 1,
            "max_retries": 3,
            "due_at": "2025-01-01T00:00:00Z",
            "execution": "SHARED_ASYNC"
        });
        assert!(serde_json::from_value::<ReadInfo>(json).is_err());
    }

    #[test]
    fn deserialize_each_result_state() {
        let base = |result: serde_json::Value| {
            serde_json::json!({
                "task_id": "00000000-0000-0000-0000-000000000003",
                "task_name": "result_task",
                "status": "COMPLETED",
                "queue": "high",
                "priority": 10,
                "retry_count": 0,
                "max_retries": 1,
                "due_at": "2025-01-01T00:00:00Z",
                "result": result,
                "execution": "NOT_KEPT_IN_HISTORY"
            })
        };
        let not_requested: ReadInfo =
            serde_json::from_value(base(serde_json::json!("not_requested"))).unwrap();
        assert!(matches!(not_requested.result, ResultSlot::NotRequested));
        let not_stored: ReadInfo =
            serde_json::from_value(base(serde_json::json!("not_stored"))).unwrap();
        assert!(matches!(not_stored.result, ResultSlot::NotStored));
        let stored: ReadInfo = serde_json::from_value(base(serde_json::json!({
            "stored": {"__h_task_result__": true, "ok": "hello world", "err": null}
        })))
        .unwrap();
        match stored.result {
            ResultSlot::Stored(result) => assert!(result.is_ok()),
            other => panic!("expected a stored result, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_preserves_each_result_state() {
        let stored = make_task_info(ResultSlot::Stored(TaskResult::Ok(
            serde_json::json!({"nested": [1, 2, 3]}),
        )));
        let back: ReadInfo =
            serde_json::from_str(&serde_json::to_string(&stored).unwrap()).unwrap();
        match back.result {
            ResultSlot::Stored(result) => assert!(result.is_ok()),
            other => panic!("expected a stored result, got {other:?}"),
        }
        assert_eq!(back.task_id, stored.task_id);
        assert_eq!(back.task_name, stored.task_name);
        for slot in [ResultSlot::NotRequested, ResultSlot::NotStored] {
            let info: ReadInfo = make_task_info(slot.clone());
            let back: ReadInfo =
                serde_json::from_str(&serde_json::to_string(&info).unwrap()).unwrap();
            assert!(matches!(
                (&slot, &back.result),
                (ResultSlot::NotRequested, ResultSlot::NotRequested)
                    | (ResultSlot::NotStored, ResultSlot::NotStored)
            ));
        }
    }

    #[test]
    fn round_trip_preserves_execution_class() {
        for execution in [
            ExecutionClass::Live(Execution::SharedThread),
            ExecutionClass::NotKeptInHistory,
        ] {
            let mut info: ReadInfo = make_task_info(ResultSlot::NotRequested);
            info.execution = execution;
            let json = serde_json::to_value(&info).unwrap();
            assert_eq!(json["execution"], execution.as_str());
            let back: ReadInfo = serde_json::from_value(json).unwrap();
            assert_eq!(back.execution, execution);
        }
    }

    #[test]
    fn deserialize_without_execution_is_refused() {
        // The class is never guessed: a task info without it does not decode.
        let json = serde_json::json!({
            "task_id": "00000000-0000-0000-0000-000000000004",
            "task_name": "no_class",
            "status": "PENDING",
            "queue": "default",
            "priority": 5,
            "retry_count": 0,
            "max_retries": 0,
            "due_at": "2025-01-01T00:00:00Z"
        });
        assert!(serde_json::from_value::<ReadInfo>(json).is_err());
    }

    #[test]
    fn try_map_result_reads_the_stored_result_and_keeps_the_other_fields() {
        let mut info = make_task_info(ResultSlot::Stored("7".to_owned()));
        info.execution = ExecutionClass::NotKeptInHistory;
        let read = info
            .try_map_result(|text| text.parse::<u8>())
            .expect("the result reads");
        assert_eq!(read.result, ResultSlot::Stored(7));
        assert_eq!(read.task_name, "my_task");
        assert_eq!(read.max_retries, 3);
        assert_eq!(read.execution, ExecutionClass::NotKeptInHistory);
    }

    #[test]
    fn try_map_result_keeps_a_result_that_was_not_requested_or_not_stored() {
        for slot in [ResultSlot::NotRequested, ResultSlot::NotStored] {
            let info = make_task_info::<String>(slot.clone());
            let read = info
                .try_map_result(|_text| -> Result<u8, std::num::ParseIntError> {
                    panic!("a task info without a stored result has nothing to read")
                })
                .expect("a slot without a stored result reads");
            match (slot, read.result) {
                (ResultSlot::NotRequested, ResultSlot::NotRequested)
                | (ResultSlot::NotStored, ResultSlot::NotStored) => {}
                (_, other) => panic!("the state changed to {other:?}"),
            }
        }
    }

    #[test]
    fn try_map_result_gives_the_error_of_a_result_that_does_not_read() {
        let info = make_task_info(ResultSlot::Stored("seven".to_owned()));
        assert!(info.try_map_result(|text| text.parse::<u8>()).is_err());
    }
}
