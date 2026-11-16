use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Summary of a completed sub-workflow, stored as the parent workflow_task result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubWorkflowSummary {
    /// Terminal status of the child workflow ("COMPLETED" or "FAILED").
    pub status: String,

    /// Whether the child workflow succeeded.
    pub is_success: bool,

    /// Name/identifier of the satisfied `SuccessCase` (if a `SuccessPolicy` was used).
    ///
    /// `None` when no success policy was defined, or when the workflow failed
    /// (no case was satisfied). Matches Python's `SubWorkflowSummary.success_case`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success_case: Option<String>,

    /// The child workflow's final output (if output_index was set).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,

    /// Total number of tasks in the child workflow.
    pub total_tasks: i32,

    /// Number of completed tasks.
    pub completed_tasks: i32,

    /// Number of failed tasks.
    pub failed_tasks: i32,

    /// Number of skipped tasks.
    pub skipped_tasks: i32,

    /// Error summary if the child workflow failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_summary: Option<String>,

    /// Child workflow ID for tracing.
    pub child_workflow_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_round_trip() {
        let summary = SubWorkflowSummary {
            status: "COMPLETED".to_owned(),
            is_success: true,
            success_case: Some("primary_path".to_owned()),
            output: Some(serde_json::json!({"result": 42})),
            total_tasks: 3,
            completed_tasks: 3,
            failed_tasks: 0,
            skipped_tasks: 0,
            error_summary: None,
            child_workflow_id: Uuid::new_v4(),
        };

        let json = serde_json::to_string(&summary).unwrap();
        let back: SubWorkflowSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.status, "COMPLETED");
        assert!(back.is_success);
        assert_eq!(back.success_case.as_deref(), Some("primary_path"));
        assert_eq!(back.total_tasks, 3);
        assert_eq!(back.child_workflow_id, summary.child_workflow_id);
    }

    #[test]
    fn failed_summary() {
        let summary = SubWorkflowSummary {
            status: "FAILED".to_owned(),
            is_success: false,
            success_case: None,
            output: None,
            total_tasks: 5,
            completed_tasks: 3,
            failed_tasks: 1,
            skipped_tasks: 1,
            error_summary: Some("task 'process' failed: timeout".to_owned()),
            child_workflow_id: Uuid::new_v4(),
        };

        let json = serde_json::to_string(&summary).unwrap();
        let back: SubWorkflowSummary = serde_json::from_str(&json).unwrap();
        assert!(!back.is_success);
        assert!(back.success_case.is_none());
        assert!(back.error_summary.is_some());
    }

    #[test]
    fn success_case_none_when_no_policy() {
        let summary = SubWorkflowSummary {
            status: "COMPLETED".to_owned(),
            is_success: true,
            success_case: None,
            output: None,
            total_tasks: 1,
            completed_tasks: 1,
            failed_tasks: 0,
            skipped_tasks: 0,
            error_summary: None,
            child_workflow_id: Uuid::nil(),
        };

        let json = serde_json::to_string(&summary).unwrap();
        // success_case should be absent due to skip_serializing_if
        assert!(!json.contains("success_case"));
        let back: SubWorkflowSummary = serde_json::from_str(&json).unwrap();
        assert!(back.success_case.is_none());
    }

    #[test]
    fn deserialize_legacy_without_success_case() {
        // Ensure backward compat: JSON without success_case field still parses.
        let json = r#"{
            "status": "COMPLETED",
            "is_success": true,
            "total_tasks": 2,
            "completed_tasks": 2,
            "failed_tasks": 0,
            "skipped_tasks": 0,
            "child_workflow_id": "00000000-0000-0000-0000-000000000000"
        }"#;
        let summary: SubWorkflowSummary = serde_json::from_str(json).unwrap();
        assert!(summary.is_success);
        assert!(summary.success_case.is_none());
    }
}
