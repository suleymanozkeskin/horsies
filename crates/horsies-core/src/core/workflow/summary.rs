use serde::{Deserialize, Serialize};

use crate::core::workflow::status::WorkflowStatus;

/// Summary of a terminal sub-workflow, stored in `horsies_workflow_tasks.sub_workflow_summary` of its parent
/// node.
///
/// The stored JSON has exactly Python's seven keys, with `null` kept for an absent output or error summary
/// (`SubWorkflowSummary.to_json`, `core/models/workflow/context.py:72-88`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubWorkflowSummary {
    /// Terminal status of the child workflow (`COMPLETED`, `FAILED`, `CANCELLED` or `EXPIRED`). A stored
    /// status outside [`WorkflowStatus`] does not decode.
    pub status: WorkflowStatus,

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

/// Why a stored summary is refused (Python `SubWorkflowSummary.from_json`, `core/models/workflow/context.py:91-157`).
/// The texts are Python's `ValueError` messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SummaryRefused {
    /// The value is not a JSON object.
    #[error("SubWorkflowSummary payload must be a string-keyed dict, got {python_type}")]
    NotAnObject { python_type: &'static str },
    /// `status` is missing or is not a string.
    #[error("SubWorkflowSummary status must be a status string, got {python_type}")]
    StatusNotText { python_type: &'static str },
    /// `status` names no workflow status.
    #[error("SubWorkflowSummary has unrecognized status {status:?}")]
    UnknownStatus { status: String },
    /// `error_summary` is neither a string nor `null`.
    #[error("SubWorkflowSummary error_summary must be a string or null, got {python_type}")]
    ErrorSummaryNotText { python_type: &'static str },
    /// A count is missing, not an integer, negative, or above the `i32` range of the stored count.
    #[error("SubWorkflowSummary {field} must be a non-negative integer, got {found}")]
    BadCount { field: &'static str, found: String },
}

impl SubWorkflowSummary {
    /// Reads a stored summary with Python's rules (`SubWorkflowSummary.from_json`): an object; `status` a string
    /// that names a [`WorkflowStatus`]; the four counts non-negative integers; `error_summary` a string or `null`
    /// (missing reads as `null`); `output` any value (missing reads as `None`). Other keys are ignored, as Python
    /// ignores them. Pure.
    pub fn from_stored_value(value: &serde_json::Value) -> Result<Self, SummaryRefused> {
        let serde_json::Value::Object(fields) = value else {
            return Err(SummaryRefused::NotAnObject {
                python_type: python_type_name(Some(value)),
            });
        };
        let status = match fields.get("status") {
            Some(serde_json::Value::String(text)) => WorkflowStatus::try_from(text.as_str())
                .map_err(|status| SummaryRefused::UnknownStatus { status })?,
            other => {
                return Err(SummaryRefused::StatusNotText {
                    python_type: python_type_name(other),
                })
            }
        };
        let error_summary = match fields.get("error_summary") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(text)) => Some(text.clone()),
            Some(other) => {
                return Err(SummaryRefused::ErrorSummaryNotText {
                    python_type: python_type_name(Some(other)),
                })
            }
        };
        Ok(Self {
            status,
            output: match fields.get("output") {
                None | Some(serde_json::Value::Null) => None,
                Some(output) => Some(output.clone()),
            },
            total_tasks: stored_count(fields, "total_tasks")?,
            completed_tasks: stored_count(fields, "completed_tasks")?,
            failed_tasks: stored_count(fields, "failed_tasks")?,
            skipped_tasks: stored_count(fields, "skipped_tasks")?,
            error_summary,
        })
    }
}

/// One count of a stored summary: a JSON integer from 0 to `i32::MAX`. Python refuses a missing, boolean,
/// non-integer or negative count; the core also refuses one above the `i32` range of its count fields.
fn stored_count(
    fields: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<i32, SummaryRefused> {
    let refused = |found: String| SummaryRefused::BadCount { field, found };
    match fields.get(field) {
        Some(serde_json::Value::Number(number)) => number
            .as_u64()
            .and_then(|count| i32::try_from(count).ok())
            .ok_or_else(|| refused(number.to_string())),
        Some(other) => Err(refused(other.to_string())),
        None => Err(refused("None".to_owned())),
    }
}

/// The Python type name of a parsed JSON value (`None` for a missing key), for refusal texts.
pub(crate) fn python_type_name(value: Option<&serde_json::Value>) -> &'static str {
    match value {
        None | Some(serde_json::Value::Null) => "NoneType",
        Some(serde_json::Value::Bool(_)) => "bool",
        Some(serde_json::Value::Number(number)) => match number.is_f64() {
            true => "float",
            false => "int",
        },
        Some(serde_json::Value::String(_)) => "str",
        Some(serde_json::Value::Array(_)) => "list",
        Some(serde_json::Value::Object(_)) => "dict",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_json_has_python_keys_with_nulls_kept() {
        let summary = SubWorkflowSummary {
            status: WorkflowStatus::Failed,
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
            status: WorkflowStatus::Completed,
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

    /// Python `SubWorkflowSummary.from_json`: a stored summary reads into the typed status and the counts.
    #[test]
    fn a_stored_summary_reads_with_python_s_rules() {
        let read = SubWorkflowSummary::from_stored_value(&serde_json::json!({
            "status": "FAILED",
            "total_tasks": 2,
            "completed_tasks": 1,
            "failed_tasks": 1,
            "skipped_tasks": 0,
        }))
        .unwrap();
        assert_eq!(read.status, WorkflowStatus::Failed);
        assert_eq!(read.output, None);
        assert_eq!(read.error_summary, None);
        assert_eq!((read.total_tasks, read.failed_tasks), (2, 1));
    }

    /// Each value that Python's `from_json` refuses, with its refusal.
    #[test]
    fn a_stored_summary_that_breaks_python_s_rules_is_refused() {
        let counts = serde_json::json!({
            "total_tasks": 1, "completed_tasks": 1, "failed_tasks": 0, "skipped_tasks": 0,
        });
        let with = |key: &str, value: serde_json::Value| {
            let mut summary = counts.clone();
            summary["status"] = serde_json::json!("COMPLETED");
            summary[key] = value;
            summary
        };
        let cases = [
            (
                serde_json::json!([1]),
                SummaryRefused::NotAnObject {
                    python_type: "list",
                },
            ),
            (
                counts.clone(),
                SummaryRefused::StatusNotText {
                    python_type: "NoneType",
                },
            ),
            (
                with("status", serde_json::json!("NOT_A_STATUS")),
                SummaryRefused::UnknownStatus {
                    status: "NOT_A_STATUS".to_owned(),
                },
            ),
            (
                with("error_summary", serde_json::json!(5)),
                SummaryRefused::ErrorSummaryNotText { python_type: "int" },
            ),
            (
                with("failed_tasks", serde_json::json!(-1)),
                SummaryRefused::BadCount {
                    field: "failed_tasks",
                    found: "-1".to_owned(),
                },
            ),
            (
                with("total_tasks", serde_json::json!(true)),
                SummaryRefused::BadCount {
                    field: "total_tasks",
                    found: "true".to_owned(),
                },
            ),
        ];
        for (stored, refusal) in cases {
            assert_eq!(
                SubWorkflowSummary::from_stored_value(&stored),
                Err(refusal),
                "{stored}"
            );
        }
    }
}
