use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::core::{WorkflowStatus, WorkflowTaskStatus};

use crate::broker::error::BrokerError;

/// Row from the `horsies_workflows` table.
#[derive(Debug, Clone, FromRow)]
pub struct WorkflowRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub on_error: String,
    pub output_task_index: Option<i32>,
    pub success_policy: Option<serde_json::Value>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub definition_key: Option<String>,
    pub parent_workflow_id: Option<String>,
    pub parent_task_index: Option<i32>,
    pub depth: i32,
    pub root_workflow_id: Option<String>,
    pub sent_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// Row from the `horsies_workflow_tasks` table.
#[derive(Debug, Clone, FromRow)]
pub struct WorkflowTaskRow {
    pub id: String,
    pub workflow_id: String,
    pub task_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub task_args: Option<String>,
    pub task_kwargs: Option<String>,
    pub queue_name: String,
    pub priority: i32,
    pub dependencies: Vec<i32>,
    pub args_from: Option<serde_json::Value>,
    pub workflow_ctx_from: Option<Vec<String>>,
    pub allow_failed_deps: bool,
    pub join_type: String,
    pub min_success: Option<i32>,
    pub task_options: Option<String>,
    pub status: String,
    pub task_id: Option<String>,
    pub is_subworkflow: bool,
    pub sub_workflow_id: Option<String>,
    pub sub_workflow_name: Option<String>,
    pub sub_definition_key: Option<String>,
    pub sub_workflow_summary: Option<String>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Parse a workflow status string from the database into the typed enum.
pub fn parse_workflow_status(s: &str) -> Result<WorkflowStatus, BrokerError> {
    match s {
        "PENDING" => Ok(WorkflowStatus::Pending),
        "RUNNING" => Ok(WorkflowStatus::Running),
        "COMPLETED" => Ok(WorkflowStatus::Completed),
        "FAILED" => Ok(WorkflowStatus::Failed),
        "PAUSED" => Ok(WorkflowStatus::Paused),
        "CANCELLED" => Ok(WorkflowStatus::Cancelled),
        _ => Err(BrokerError::InvalidStatus(s.to_owned())),
    }
}

/// Parse a workflow task status string from the database into the typed enum.
pub fn parse_workflow_task_status(s: &str) -> Result<WorkflowTaskStatus, BrokerError> {
    match s {
        "PENDING" => Ok(WorkflowTaskStatus::Pending),
        "READY" => Ok(WorkflowTaskStatus::Ready),
        "ENQUEUED" => Ok(WorkflowTaskStatus::Enqueued),
        "RUNNING" => Ok(WorkflowTaskStatus::Running),
        "COMPLETED" => Ok(WorkflowTaskStatus::Completed),
        "FAILED" => Ok(WorkflowTaskStatus::Failed),
        "SKIPPED" => Ok(WorkflowTaskStatus::Skipped),
        _ => Err(BrokerError::InvalidStatus(s.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_workflow_status_valid() {
        assert_eq!(
            parse_workflow_status("RUNNING").unwrap(),
            WorkflowStatus::Running
        );
        assert_eq!(
            parse_workflow_status("COMPLETED").unwrap(),
            WorkflowStatus::Completed
        );
        assert_eq!(
            parse_workflow_status("PAUSED").unwrap(),
            WorkflowStatus::Paused
        );
    }

    #[test]
    fn parse_workflow_status_invalid() {
        assert!(parse_workflow_status("INVALID").is_err());
    }

    #[test]
    fn parse_workflow_task_status_valid() {
        assert_eq!(
            parse_workflow_task_status("PENDING").unwrap(),
            WorkflowTaskStatus::Pending,
        );
        assert_eq!(
            parse_workflow_task_status("ENQUEUED").unwrap(),
            WorkflowTaskStatus::Enqueued,
        );
        assert_eq!(
            parse_workflow_task_status("SKIPPED").unwrap(),
            WorkflowTaskStatus::Skipped,
        );
    }

    #[test]
    fn parse_workflow_task_status_invalid() {
        assert!(parse_workflow_task_status("BOGUS").is_err());
    }
}
