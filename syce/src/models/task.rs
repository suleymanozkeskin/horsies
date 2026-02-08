use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

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
    pub claimed_by_worker_id: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TaskDetail {
    /// Serialize task to pretty JSON for clipboard
    pub fn to_clipboard_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| format!("{:?}", self))
    }
}
