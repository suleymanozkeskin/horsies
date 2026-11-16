#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod args_merge;
pub mod bound_handle;
pub mod engine;
pub mod error;
pub mod info;
pub mod lifecycle;
pub mod phase2_recovery;
pub mod query;
pub mod recovery;
pub(crate) mod recovery_limit;
pub mod start;

#[cfg(test)]
mod port_tests;

pub use error::WorkflowError;

/// Why the `good_until` of a node's stored task options is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StoredDeadlineError {
    /// The task options text is not one JSON object.
    #[error("task_options is not a JSON object: {detail}")]
    OptionsNotAnObject { detail: String },
    /// `good_until` is a JSON value that is not a text (and not `null`).
    #[error("good_until is not a text: {value}")]
    NotAText { value: String },
    /// `good_until` is a text that is not an RFC 3339 time.
    #[error("good_until {text:?} is not an RFC 3339 time: {detail}")]
    NotATime { text: String, detail: String },
}

/// The deadline in a node's stored task options (`task_options` JSON, key
/// `good_until`).
///
/// - No options, no `good_until` key, or `good_until: null`: `Ok(None)`.
/// - An RFC 3339 text: `Ok(Some(deadline))`, in UTC.
/// - Anything else: [`StoredDeadlineError`]. A deadline that does not parse
///   is refused, never read as "no deadline": the node must not get a task
///   without its deadline. Python passes the text to the database, whose
///   timestamp parse refuses it, so the statement fails
///   (`core/workflows/engine.py:792-795`,
///   `core/workflows/lifecycle.py:193-194`).
///
/// Pure.
pub(crate) fn parse_good_until_from_options(
    task_options: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, StoredDeadlineError> {
    let Some(text) = task_options else {
        return Ok(None);
    };
    let options = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(serde_json::Value::Object(options)) => options,
        Ok(other) => {
            return Err(StoredDeadlineError::OptionsNotAnObject {
                detail: format!("got {other}"),
            })
        }
        Err(error) => {
            return Err(StoredDeadlineError::OptionsNotAnObject {
                detail: error.to_string(),
            })
        }
    };
    match options.get("good_until") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(deadline)) => chrono::DateTime::parse_from_rfc3339(deadline)
            .map(|deadline| Some(deadline.with_timezone(&chrono::Utc)))
            .map_err(|error| StoredDeadlineError::NotATime {
                text: deadline.clone(),
                detail: error.to_string(),
            }),
        Some(other) => Err(StoredDeadlineError::NotAText {
            value: other.to_string(),
        }),
    }
}

/// The error of a node whose stored deadline is refused at the mint of its
/// task (promotion, resume, recovery): [`WorkflowError::Validation`], not
/// retryable, naming the node. The statement that would mint the task does
/// not run; the caller's transaction rolls back.
pub(crate) fn stored_deadline_refused(
    workflow_id: uuid::Uuid,
    task_index: i32,
    error: &StoredDeadlineError,
) -> WorkflowError {
    WorkflowError::Validation(format!(
        "workflow {workflow_id} node {task_index}: stored deadline refused: {error}"
    ))
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
        assert_eq!(parse_good_until_from_options(None), Ok(None));
    }

    #[test]
    fn parse_good_until_empty_json() {
        assert_eq!(parse_good_until_from_options(Some("{}")), Ok(None));
    }

    #[test]
    fn parse_good_until_no_good_until_key() {
        assert_eq!(
            parse_good_until_from_options(Some(r#"{"retries": 3}"#)),
            Ok(None)
        );
    }

    /// Python reads `good_until: null` as no deadline (`options.get`).
    #[test]
    fn a_null_good_until_is_no_deadline() {
        assert_eq!(
            parse_good_until_from_options(Some(r#"{"good_until": null}"#)),
            Ok(None)
        );
    }

    #[test]
    fn parse_good_until_valid_rfc3339() {
        let opts = r#"{"good_until": "2025-06-15T12:00:00+00:00"}"#;
        let dt = parse_good_until_from_options(Some(opts))
            .unwrap()
            .expect("a deadline");
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 6);
        assert_eq!(dt.day(), 15);
    }

    /// A text that is not an RFC 3339 time is refused, not read as no
    /// deadline.
    #[test]
    fn a_good_until_text_that_is_not_a_time_is_refused() {
        let opts = r#"{"good_until": "not-a-date"}"#;
        assert!(matches!(
            parse_good_until_from_options(Some(opts)),
            Err(StoredDeadlineError::NotATime { ref text, .. }) if text == "not-a-date"
        ));
    }

    /// A `good_until` that is not a text is refused.
    #[test]
    fn a_good_until_that_is_not_a_text_is_refused() {
        assert_eq!(
            parse_good_until_from_options(Some(r#"{"good_until": 12345}"#)),
            Err(StoredDeadlineError::NotAText {
                value: "12345".to_owned()
            })
        );
    }

    /// Options that are not one JSON object are refused.
    #[test]
    fn options_that_are_not_an_object_are_refused() {
        for opts in ["not{json", "[]", "null"] {
            assert!(
                matches!(
                    parse_good_until_from_options(Some(opts)),
                    Err(StoredDeadlineError::OptionsNotAnObject { .. })
                ),
                "{opts}"
            );
        }
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
        assert_eq!(parsed, Ok(Some(deadline)));
    }
}
