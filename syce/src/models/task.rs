use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// A distinct value with its count, used by the filter sidebar.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DistinctFilterRow {
    pub kind: String,
    pub value: String,
    pub count: i64,
}

/// A named value with count for display in the sidebar.
#[derive(Debug, Clone)]
pub struct FilterValue {
    pub value: String,
    pub count: i64,
}

/// Full task details for the detail view
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TaskDetail {
    pub id: String,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub status: String,
    pub args: Option<String>,
    pub kwargs: Option<String>,
    pub result: Option<String>,
    pub failed_reason: Option<String>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
    pub claimed_by_worker_id: Option<String>,
    pub error_code: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
    pub enqueued_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Attempt history, populated separately (not from the task row query).
    #[sqlx(skip)]
    pub attempts: Vec<TaskAttemptRow>,
}

impl TaskDetail {
    /// Serialize task to pretty JSON for clipboard
    pub fn to_clipboard_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| format!("{:?}", self))
    }
}

/// Lightweight task row for the drill-down task list view (Layer 2).
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TaskListRow {
    pub id: String,
    pub task_name: String,
    pub queue_name: String,
    pub status: String,
    pub error_code: Option<String>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub enqueued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
}

/// A single task execution attempt from horsies_task_attempts.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TaskAttemptRow {
    pub task_id: String,
    pub attempt: i32,
    pub outcome: String,
    pub will_retry: bool,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub failed_reason: Option<String>,
    pub worker_id: Option<String>,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
}
