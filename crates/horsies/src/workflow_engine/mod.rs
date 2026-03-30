#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod args_merge;
pub mod bound_handle;
pub mod bound_spec;
pub mod engine;
pub mod error;
pub mod info;
pub mod lifecycle;
pub mod query;
pub mod recovery;
pub mod start;

pub use bound_handle::WorkflowHandle;
pub use bound_spec::{BoundWorkflowSpec, WorkflowSpecExt};
pub use engine::{on_subworkflow_complete, on_workflow_task_complete};
pub use error::WorkflowError;
pub use info::WorkflowTaskInfo;
pub use lifecycle::{cancel_workflow, pause_workflow, resume_workflow};
pub use query::{
    get_workflow_result, get_workflow_result_for, get_workflow_results, get_workflow_status,
    get_workflow_tasks,
};
pub use recovery::{recover_stuck_workflows, RecoveryReport};
pub use start::{
    retry_start, start_child_workflow, start_child_workflow_in_tx, start_workflow,
    start_workflow_with_retry,
};

/// Parse `good_until` from a serialized task_options JSON string.
///
/// In the Python spec, `good_until` is stored inside the `task_options` JSON
/// (not as a separate column on `horsies_workflow_tasks`).
pub(crate) fn parse_good_until_from_options(
    task_options: Option<&str>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    task_options
        .and_then(|opts| serde_json::from_str::<serde_json::Value>(opts).ok())
        .and_then(|v| v.get("good_until")?.as_str().map(String::from))
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

/// Merge `good_until` from an `AnyNode` into its `task_options_json`.
///
/// Mirrors Python's `lifecycle.py` which merges `TaskNode.good_until` into
/// `task_options` at workflow start time. The merged JSON is what gets stored
/// in `horsies_workflow_tasks.task_options` and later read back by
/// `parse_good_until_from_options` when enqueuing.
///
/// Returns the (possibly modified) task_options_json string.
pub(crate) fn merge_good_until_into_options(
    good_until: Option<chrono::DateTime<chrono::Utc>>,
    task_options_json: Option<&str>,
) -> Option<String> {
    let Some(deadline) = good_until else {
        return task_options_json.map(|s| s.to_owned());
    };

    let mut obj: serde_json::Map<String, serde_json::Value> = match task_options_json {
        Some(json) => serde_json::from_str(json).unwrap_or_default(),
        None => serde_json::Map::new(),
    };

    obj.insert(
        "good_until".to_owned(),
        serde_json::Value::String(deadline.to_rfc3339()),
    );

    Some(serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_else(|_| "{}".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};

    // -- parse_good_until_from_options --

    #[test]
    fn parse_good_until_none_options() {
        assert!(parse_good_until_from_options(None).is_none());
    }

    #[test]
    fn parse_good_until_empty_json() {
        assert!(parse_good_until_from_options(Some("{}")).is_none());
    }

    #[test]
    fn parse_good_until_invalid_json() {
        assert!(parse_good_until_from_options(Some("not{json")).is_none());
    }

    #[test]
    fn parse_good_until_no_good_until_key() {
        assert!(parse_good_until_from_options(Some(r#"{"retries": 3}"#)).is_none());
    }

    #[test]
    fn parse_good_until_valid_rfc3339() {
        let opts = r#"{"good_until": "2025-06-15T12:00:00+00:00"}"#;
        let result = parse_good_until_from_options(Some(opts));
        assert!(result.is_some());
        let dt = result.unwrap();
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 6);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn parse_good_until_invalid_date_string() {
        let opts = r#"{"good_until": "not-a-date"}"#;
        assert!(parse_good_until_from_options(Some(opts)).is_none());
    }

    #[test]
    fn parse_good_until_non_string_value() {
        let opts = r#"{"good_until": 12345}"#;
        assert!(parse_good_until_from_options(Some(opts)).is_none());
    }

    // -- merge_good_until_into_options --

    #[test]
    fn merge_no_deadline_no_options() {
        assert!(merge_good_until_into_options(None, None).is_none());
    }

    #[test]
    fn merge_no_deadline_preserves_existing_options() {
        let opts = r#"{"retries": 3}"#;
        let result = merge_good_until_into_options(None, Some(opts));
        assert_eq!(result.as_deref(), Some(opts));
    }

    #[test]
    fn merge_deadline_no_existing_options() {
        let deadline = Utc.with_ymd_and_hms(2025, 6, 15, 12, 0, 0).unwrap();
        let result = merge_good_until_into_options(Some(deadline), None);
        assert!(result.is_some());
        let json: serde_json::Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert!(json.get("good_until").is_some());
    }

    #[test]
    fn merge_deadline_with_existing_options() {
        let deadline = Utc.with_ymd_and_hms(2025, 6, 15, 12, 0, 0).unwrap();
        let opts = r#"{"retries": 3}"#;
        let result = merge_good_until_into_options(Some(deadline), Some(opts));
        let json: serde_json::Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(json.get("retries").unwrap().as_i64(), Some(3));
        assert!(json.get("good_until").is_some());
    }

    #[test]
    fn merge_deadline_round_trips_through_parse() {
        let deadline = Utc.with_ymd_and_hms(2025, 6, 15, 12, 0, 0).unwrap();
        let merged = merge_good_until_into_options(Some(deadline), None).unwrap();
        let parsed = parse_good_until_from_options(Some(&merged));
        assert_eq!(parsed, Some(deadline));
    }
}
