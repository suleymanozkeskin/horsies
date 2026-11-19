use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::core::codec::{check_json_text, object_members, JsonText, StrictJsonError};
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

    /// The child's output value, as its stored text: set only when the child is COMPLETED and its stored result
    /// is an ok value. An integer outside the `i64` and `u64` range keeps every digit.
    pub output: Option<JsonText>,

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
/// The texts are Python's `ValueError` messages, except for a text that is not strict JSON.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SummaryRefused {
    /// The text breaks the strict read rule (Python `loads_json` raises before `from_json`).
    #[error("SubWorkflowSummary payload does not read: {0}")]
    NotJson(StrictJsonError),
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
    /// A count is missing, not an integer, negative, or above the `i32` range of the stored count. `found` is
    /// the stored text of the member, or `None` for a missing one.
    #[error("SubWorkflowSummary {field} must be a non-negative integer, got {found}")]
    BadCount { field: &'static str, found: String },
}

impl SubWorkflowSummary {
    /// Reads a stored summary text with Python's rules (`loads_json`, then `SubWorkflowSummary.from_json`): strict
    /// JSON; an object; `status` a string that names a [`WorkflowStatus`]; the four counts non-negative integers;
    /// `error_summary` a string or `null` (missing reads as `null`); `output` any value (missing or `null` reads
    /// as `None`), kept as its stored text. Other keys are ignored, as Python ignores them. No member is read
    /// into a `serde_json::Value`. Pure.
    pub fn from_stored_text(text: &str) -> Result<Self, SummaryRefused> {
        check_json_text(text).map_err(SummaryRefused::NotJson)?;
        // The text is strict JSON, so it does not read as members only when it is not an object.
        let members =
            object_members(text).map_err(|_not_an_object| SummaryRefused::NotAnObject {
                python_type: python_type_of_text(text),
            })?;
        let status = match member(&members, "status").map(RawValue::get) {
            Some(status) if status.starts_with('"') => {
                let status = string_of_checked(status);
                WorkflowStatus::try_from(status.as_str())
                    .map_err(|status| SummaryRefused::UnknownStatus { status })?
            }
            other => {
                return Err(SummaryRefused::StatusNotText {
                    python_type: python_type_of_member(other),
                })
            }
        };
        let error_summary = match member(&members, "error_summary").map(RawValue::get) {
            None | Some("null") => None,
            Some(summary) if summary.starts_with('"') => Some(string_of_checked(summary)),
            other => {
                return Err(SummaryRefused::ErrorSummaryNotText {
                    python_type: python_type_of_member(other),
                })
            }
        };
        let output = match member(&members, "output") {
            None => None,
            Some(output) if output.get() == "null" => None,
            Some(output) => Some(JsonText::of_checked_part(output)),
        };
        Ok(Self {
            status,
            output,
            total_tasks: stored_count(&members, "total_tasks")?,
            completed_tasks: stored_count(&members, "completed_tasks")?,
            failed_tasks: stored_count(&members, "failed_tasks")?,
            skipped_tasks: stored_count(&members, "skipped_tasks")?,
            error_summary,
        })
    }
}

/// The text of the member `key`, or `None` when the object has no such member. Pure.
fn member<'a>(members: &[(String, &'a RawValue)], key: &str) -> Option<&'a RawValue> {
    members
        .iter()
        .find(|(name, _value)| name.as_str() == key)
        .map(|(_name, value)| *value)
}

/// The text of a JSON string of a checked text. Pure.
fn string_of_checked(text: &str) -> String {
    serde_json::from_str(text)
        .expect("a JSON string of a text that passed the strict read rule decodes")
}

/// One count of a stored summary: a JSON integer from 0 to `i32::MAX`. Python refuses a missing, boolean,
/// non-integer or negative count; the core also refuses one above the `i32` range of its count fields. Pure.
fn stored_count(
    members: &[(String, &RawValue)],
    field: &'static str,
) -> Result<i32, SummaryRefused> {
    let refused = |found: &str| SummaryRefused::BadCount {
        field,
        found: found.to_owned(),
    };
    let Some(text) = member(members, field).map(RawValue::get) else {
        return Err(refused("None"));
    };
    let digits = text.strip_prefix('-').unwrap_or(text);
    let is_integer = !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit());
    // `-0` is the integer 0, which Python reads as a count.
    match (is_integer, text.starts_with('-'), digits.parse::<u64>()) {
        (true, false, Ok(count)) => i32::try_from(count).map_err(|_above| refused(text)),
        (true, true, Ok(0)) => Ok(0),
        _ => Err(refused(text)),
    }
}

/// The Python type name of a member text, `NoneType` for a missing member, for refusal texts. Pure.
fn python_type_of_member(text: Option<&str>) -> &'static str {
    match text {
        None => "NoneType",
        Some(text) => python_type_of_text(text),
    }
}

/// The Python type name of the value of a JSON text that passed the strict read rule (`type(loads_json(text))`),
/// for refusal texts. A number with a fraction or an exponent is a `float`; any other number is an `int`, also
/// one outside the `i64` and `u64` range. Pure.
pub(crate) fn python_type_of_text(text: &str) -> &'static str {
    let text = text.trim_start_matches([' ', '\t', '\n', '\r']);
    match text.as_bytes().first() {
        Some(b'{') => "dict",
        Some(b'[') => "list",
        Some(b'"') => "str",
        Some(b't' | b'f') => "bool",
        Some(b'n') | None => "NoneType",
        Some(_) => match text.contains(['.', 'e', 'E']) {
            true => "float",
            false => "int",
        },
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
            output: Some(JsonText::parse(r#"{"result": 42}"#).unwrap()),
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

    /// The output keeps its stored text through the stored read and through the serde read and write: an integer
    /// outside the `i64` and `u64` range keeps every digit and decodes as `u128` or `i128`.
    #[test]
    fn an_output_outside_i64_and_u64_keeps_its_stored_text() {
        for (output, wide) in [
            ("340282366920938463463374607431768211455", None),
            ("-9223372036854775809", Some(i128::from(i64::MIN) - 1)),
        ] {
            let text = format!(
                r#"{{"status":"COMPLETED","output":{output},"total_tasks":1,"completed_tasks":1,"failed_tasks":0,"skipped_tasks":0,"error_summary":null}}"#
            );
            let read = SubWorkflowSummary::from_stored_text(&text).unwrap();
            assert_eq!(read.output.as_ref().map(JsonText::get), Some(output));
            let derived: SubWorkflowSummary = serde_json::from_str(&text).unwrap();
            assert_eq!(derived, read);
            assert_eq!(serde_json::to_string(&derived).unwrap(), text);
            let output = read.output.unwrap();
            match wide {
                None => assert_eq!(output.decode::<u128>().unwrap(), u128::MAX),
                Some(below) => assert_eq!(output.decode::<i128>().unwrap(), below),
            }
        }
    }

    /// Python `SubWorkflowSummary.from_json`: a stored summary reads into the typed status and the counts.
    #[test]
    fn a_stored_summary_reads_with_python_s_rules() {
        let read = SubWorkflowSummary::from_stored_text(
            r#"{"status": "FAILED", "total_tasks": 2, "completed_tasks": 1, "failed_tasks": 1, "skipped_tasks": 0, "other": 1e400}"#,
        );
        assert!(
            read.is_err(),
            "a text with a non-finite number is not strict JSON"
        );
        let read = SubWorkflowSummary::from_stored_text(
            r#"{"status": "FAILED", "total_tasks": 2, "completed_tasks": 1, "failed_tasks": 1, "skipped_tasks": -0, "output": null, "other": [1]}"#,
        )
        .unwrap();
        assert_eq!(read.status, WorkflowStatus::Failed);
        assert_eq!(read.output, None);
        assert_eq!(read.error_summary, None);
        assert_eq!(
            (read.total_tasks, read.failed_tasks, read.skipped_tasks),
            (2, 1, 0)
        );
    }

    /// The text of a stored summary: a valid one with each of `changes` applied; `None` removes the member.
    fn summary_text(changes: &[(&str, Option<&str>)]) -> String {
        let mut members: Vec<(&str, &str)> = vec![
            ("status", r#""COMPLETED""#),
            ("total_tasks", "1"),
            ("completed_tasks", "1"),
            ("failed_tasks", "0"),
            ("skipped_tasks", "0"),
        ];
        for (key, value) in changes {
            members.retain(|(name, _value)| name != key);
            if let Some(value) = value {
                members.push((*key, *value));
            }
        }
        let body: Vec<String> = members
            .iter()
            .map(|(key, value)| format!(r#""{key}": {value}"#))
            .collect();
        format!("{{{}}}", body.join(", "))
    }

    /// Each value that Python's `from_json` refuses, with its refusal.
    #[test]
    fn a_stored_summary_that_breaks_python_s_rules_is_refused() {
        let with = |key: &str, value: &str| summary_text(&[(key, Some(value))]);
        let cases = [
            (
                "[1]".to_owned(),
                SummaryRefused::NotAnObject {
                    python_type: "list",
                },
            ),
            (
                "340282366920938463463374607431768211455".to_owned(),
                SummaryRefused::NotAnObject { python_type: "int" },
            ),
            (
                summary_text(&[("status", None)]),
                SummaryRefused::StatusNotText {
                    python_type: "NoneType",
                },
            ),
            (
                with("status", r#""NOT_A_STATUS""#),
                SummaryRefused::UnknownStatus {
                    status: "NOT_A_STATUS".to_owned(),
                },
            ),
            (
                with("error_summary", "5"),
                SummaryRefused::ErrorSummaryNotText { python_type: "int" },
            ),
            (
                with("failed_tasks", "-1"),
                SummaryRefused::BadCount {
                    field: "failed_tasks",
                    found: "-1".to_owned(),
                },
            ),
            (
                with("total_tasks", "true"),
                SummaryRefused::BadCount {
                    field: "total_tasks",
                    found: "true".to_owned(),
                },
            ),
            (
                with("total_tasks", "2147483648"),
                SummaryRefused::BadCount {
                    field: "total_tasks",
                    found: "2147483648".to_owned(),
                },
            ),
            (
                with("completed_tasks", "1.0"),
                SummaryRefused::BadCount {
                    field: "completed_tasks",
                    found: "1.0".to_owned(),
                },
            ),
            (
                summary_text(&[("skipped_tasks", None)]),
                SummaryRefused::BadCount {
                    field: "skipped_tasks",
                    found: "None".to_owned(),
                },
            ),
        ];
        for (stored, refusal) in cases {
            assert_eq!(
                SubWorkflowSummary::from_stored_text(&stored),
                Err(refusal),
                "{stored}"
            );
        }
        assert!(matches!(
            SubWorkflowSummary::from_stored_text(r#"{"status": "COMPLETED", "status": "FAILED"}"#),
            Err(SummaryRefused::NotJson(_))
        ));
    }
}
