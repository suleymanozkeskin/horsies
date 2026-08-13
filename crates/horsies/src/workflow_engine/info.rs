use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::core::task::result::TaskResult;
use crate::core::WorkflowTaskStatus;

/// Metadata for a single task within a workflow.
///
/// Returned by `get_workflow_tasks()` for inspection and monitoring.
/// Field names match Python's `WorkflowTaskInfo`.
#[derive(Debug, Clone)]
pub struct WorkflowTaskInfo {
    /// Stable node identifier (auto-assigned or explicit).
    pub node_id: Option<String>,

    /// Zero-based index in the workflow's task list.
    pub index: i32,

    /// Registered task function name.
    pub name: String,

    /// Current status of this workflow task.
    pub status: WorkflowTaskStatus,

    /// Parsed task result (present when completed or failed).
    pub result: Option<TaskResult<serde_json::Value>>,

    /// When the task started executing.
    pub started_at: Option<DateTime<Utc>>,

    /// When the task reached a terminal state.
    pub completed_at: Option<DateTime<Utc>>,

    /// ID of the child workflow (if this task is a sub-workflow node).
    pub sub_workflow_id: Option<Uuid>,

    /// Summary of the child workflow result (if this task is a sub-workflow node).
    pub sub_workflow_summary: Option<String>,
}
