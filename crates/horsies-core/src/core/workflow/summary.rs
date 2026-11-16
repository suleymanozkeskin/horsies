use serde::{Deserialize, Serialize};

/// Summary of a terminal sub-workflow, stored in `horsies_workflow_tasks.sub_workflow_summary` of its parent
/// node.
///
/// The stored JSON has exactly Python's seven keys, with `null` kept for an absent output or error summary
/// (`SubWorkflowSummary.to_json`, `core/models/workflow/context.py:72-88`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubWorkflowSummary {
    /// Terminal status of the child workflow (`COMPLETED`, `FAILED`, `CANCELLED` or `EXPIRED`).
    pub status: String,

    /// The child's output value: set only when the child is COMPLETED and its stored result is an ok value.
    pub output: Option<serde_json::Value>,

    /// Total number of tasks in the child workflow.
    pub total_tasks: i32,

    /// Number of completed tasks.
    pub completed_tasks: i32,

    /// Number of failed tasks.
    pub failed_tasks: i32,

    /// Number of skipped tasks.
    pub skipped_tasks: i32,

    /// The message of the child workflow's stored error, when the child has one.
    pub error_summary: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_json_has_python_keys_with_nulls_kept() {
        let summary = SubWorkflowSummary {
            status: "FAILED".to_owned(),
            output: None,
            total_tasks: 5,
            completed_tasks: 3,
            failed_tasks: 1,
            skipped_tasks: 1,
            error_summary: None,
        };

        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "status": "FAILED",
                "output": null,
                "total_tasks": 5,
                "completed_tasks": 3,
                "failed_tasks": 1,
                "skipped_tasks": 1,
                "error_summary": null,
            })
        );
        let back: SubWorkflowSummary = serde_json::from_value(json).unwrap();
        assert_eq!(back, summary);
    }

    #[test]
    fn serde_round_trip_with_output_and_error_summary() {
        let summary = SubWorkflowSummary {
            status: "COMPLETED".to_owned(),
            output: Some(serde_json::json!({"result": 42})),
            total_tasks: 3,
            completed_tasks: 3,
            failed_tasks: 0,
            skipped_tasks: 0,
            error_summary: Some("task 'process' failed: timeout".to_owned()),
        };

        let json = serde_json::to_string(&summary).unwrap();
        let back: SubWorkflowSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back, summary);
    }
}
