use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::task::execution::ExecutionClass;
use crate::core::types::status::TaskAttemptOutcome;
use crate::core::types::TaskStatus;

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
    /// The stored result, when the read asked for it and the task has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<R>,

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
    /// change. With no result, `read` is not called. Pure.
    ///
    /// Errors: the error of `read`.
    pub fn try_map_result<S, X>(
        self,
        read: impl FnOnce(R) -> Result<S, X>,
    ) -> Result<TaskInfo<S>, X> {
        let result = self.result.map(read).transpose()?;
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
    fn make_task_info<R>(result: Option<R>) -> TaskInfo<R> {
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
    fn serialize_with_result_none_omits_field() {
        let info: ReadInfo = make_task_info(None);
        let json = serde_json::to_value(&info).unwrap();
        // The "result" key should be absent when None due to skip_serializing_if
        assert!(
            json.get("result").is_none(),
            "expected 'result' field to be omitted when None, got: {}",
            json
        );
    }

    #[test]
    fn serialize_with_result_some_includes_field() {
        let info = make_task_info(Some(TaskResult::Ok(serde_json::json!({"answer": 42}))));
        let json = serde_json::to_value(&info).unwrap();
        let result_field = json
            .get("result")
            .expect("expected 'result' field to be present");
        // TaskResult serializes as Python's envelope.
        assert_eq!(result_field["__h_task_result__"], true);
        assert_eq!(result_field["ok"]["answer"], 42);
    }

    #[test]
    fn deserialize_without_result_field() {
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
        let info: ReadInfo = serde_json::from_value(json).unwrap();
        assert_eq!(
            info.task_id,
            Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
        );
        assert!(info.result.is_none());
        assert_eq!(info.execution, ExecutionClass::Live(Execution::SharedAsync));
    }

    #[test]
    fn deserialize_with_result_field() {
        let json = serde_json::json!({
            "task_id": "00000000-0000-0000-0000-000000000003",
            "task_name": "result_task",
            "status": "COMPLETED",
            "queue": "high",
            "priority": 10,
            "retry_count": 0,
            "max_retries": 1,
            "due_at": "2025-01-01T00:00:00Z",
            "result": {"__h_task_result__": true, "ok": "hello world", "err": null},
            "execution": "NOT_KEPT_IN_HISTORY"
        });
        let info: ReadInfo = serde_json::from_value(json).unwrap();
        assert!(info.result.is_some());
        assert!(info.result.unwrap().is_ok());
    }

    #[test]
    fn round_trip_preserves_result() {
        let info = make_task_info(Some(TaskResult::Ok(
            serde_json::json!({"nested": [1, 2, 3]}),
        )));
        let json_string = serde_json::to_string(&info).unwrap();
        let back: ReadInfo = serde_json::from_str(&json_string).unwrap();
        assert!(back.result.is_some());
        assert!(back.result.unwrap().is_ok());
        assert_eq!(back.task_id, info.task_id);
        assert_eq!(back.task_name, info.task_name);
    }

    #[test]
    fn round_trip_preserves_execution_class() {
        for execution in [
            ExecutionClass::Live(Execution::SharedThread),
            ExecutionClass::NotKeptInHistory,
        ] {
            let mut info: ReadInfo = make_task_info(None);
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
    fn round_trip_preserves_none_result() {
        let info: ReadInfo = make_task_info(None);
        let json_string = serde_json::to_string(&info).unwrap();
        let back: ReadInfo = serde_json::from_str(&json_string).unwrap();
        assert!(back.result.is_none());
    }

    #[test]
    fn try_map_result_reads_the_result_and_keeps_the_other_fields() {
        let mut info = make_task_info(Some("7".to_owned()));
        info.execution = ExecutionClass::NotKeptInHistory;
        let read = info
            .try_map_result(|text| text.parse::<u8>())
            .expect("the result reads");
        assert_eq!(read.result, Some(7));
        assert_eq!(read.task_name, "my_task");
        assert_eq!(read.max_retries, 3);
        assert_eq!(read.execution, ExecutionClass::NotKeptInHistory);
    }

    #[test]
    fn try_map_result_with_no_result_calls_nothing() {
        let info = make_task_info::<String>(None);
        let read = info
            .try_map_result(|_text| -> Result<u8, std::num::ParseIntError> {
                panic!("a task info with no result has nothing to read")
            })
            .expect("no result reads");
        assert_eq!(read.result, None);
    }

    #[test]
    fn try_map_result_gives_the_error_of_a_result_that_does_not_read() {
        let info = make_task_info(Some("seven".to_owned()));
        assert!(info.try_map_result(|text| text.parse::<u8>()).is_err());
    }
}
