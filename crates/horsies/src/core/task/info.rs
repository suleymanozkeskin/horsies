use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::task::result::TaskResult;
use crate::core::types::status::TaskAttemptOutcome;
use crate::core::types::TaskStatus;

/// Metadata for a broker-backed task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub task_id: Uuid,
    pub task_name: String,
    pub status: TaskStatus,
    pub queue_name: String,
    pub priority: i32,
    pub retry_count: u32,
    pub max_retries: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
    pub enqueued_at: DateTime<Utc>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<TaskResult<serde_json::Value>>,

    /// Execution attempt history (opt-in via `include_attempts=true`).
    /// `None` when not requested, empty `Vec` when no attempts recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<Vec<TaskAttemptInfo>>,
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

    /// Helper to build a minimal TaskInfo for testing.
    fn make_task_info(result: Option<TaskResult<serde_json::Value>>) -> TaskInfo {
        TaskInfo {
            task_id: Uuid::nil(),
            task_name: "my_task".to_owned(),
            status: TaskStatus::Completed,
            queue_name: "default".to_owned(),
            priority: 0,
            retry_count: 0,
            max_retries: 3,
            next_retry_at: None,
            sent_at: None,
            enqueued_at: Utc::now(),
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
        }
    }

    #[test]
    fn serialize_with_result_none_omits_field() {
        let info = make_task_info(None);
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
        // TaskResult serializes with __type tag
        assert!(result_field.get("__type").is_some());
    }

    #[test]
    fn deserialize_without_result_field() {
        let json = serde_json::json!({
            "task_id": "00000000-0000-0000-0000-000000000002",
            "task_name": "another_task",
            "status": "PENDING",
            "queue_name": "default",
            "priority": 5,
            "retry_count": 1,
            "max_retries": 3,
            "enqueued_at": "2025-01-01T00:00:00Z"
        });
        let info: TaskInfo = serde_json::from_value(json).unwrap();
        assert_eq!(
            info.task_id,
            Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
        );
        assert!(info.result.is_none());
    }

    #[test]
    fn deserialize_with_result_field() {
        let json = serde_json::json!({
            "task_id": "00000000-0000-0000-0000-000000000003",
            "task_name": "result_task",
            "status": "COMPLETED",
            "queue_name": "high",
            "priority": 10,
            "retry_count": 0,
            "max_retries": 1,
            "enqueued_at": "2025-01-01T00:00:00Z",
            "result": {"__type": "ok", "value": "hello world"}
        });
        let info: TaskInfo = serde_json::from_value(json).unwrap();
        assert!(info.result.is_some());
        assert!(info.result.unwrap().is_ok());
    }

    #[test]
    fn round_trip_preserves_result() {
        let info = make_task_info(Some(TaskResult::Ok(
            serde_json::json!({"nested": [1, 2, 3]}),
        )));
        let json_string = serde_json::to_string(&info).unwrap();
        let back: TaskInfo = serde_json::from_str(&json_string).unwrap();
        assert!(back.result.is_some());
        assert!(back.result.unwrap().is_ok());
        assert_eq!(back.task_id, info.task_id);
        assert_eq!(back.task_name, info.task_name);
    }

    #[test]
    fn round_trip_preserves_none_result() {
        let info = make_task_info(None);
        let json_string = serde_json::to_string(&info).unwrap();
        let back: TaskInfo = serde_json::from_str(&json_string).unwrap();
        assert!(back.result.is_none());
    }
}
