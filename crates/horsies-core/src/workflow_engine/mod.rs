#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod args_merge;
pub mod child_spec;
pub(crate) mod child_start;
pub mod engine;
pub mod error;
pub mod info;
pub mod lifecycle;
pub mod phase2_recovery;
pub mod query;
pub mod recovery;
pub(crate) mod recovery_limit;
pub mod spec_data;
pub mod start;
pub(crate) mod start_statement;
pub(crate) mod steps;

#[cfg(test)]
mod port_tests;

pub use error::WorkflowError;

/// The `max_retries` of a task node's row and of the task row minted from
/// it, read from the node's `task_options` with the reader of a task row
/// ([`crate::worker::retry::stored_max_retries`]; Python
/// `python/horsies/core/workflows/engine.py:767-776`,
/// `python/horsies/core/workflows/lifecycle.py:182-188`).
///
/// Options that the reader refuses are [`WorkflowError::Validation`] naming
/// the node: no limit is guessed, and the caller writes no row from them.
/// Pure.
pub(crate) fn node_max_retries(
    task_index: impl std::fmt::Display,
    task_name: &str,
    task_options: Option<&str>,
) -> Result<i32, WorkflowError> {
    crate::worker::retry::stored_max_retries(task_options).map_err(|refusal| {
        WorkflowError::Validation(format!(
            "task_options of workflow node {task_index} ({task_name}) refused: {refusal}"
        ))
    })
}

/// The refusal of a node's `task_options`, naming the node.
fn node_options_refused(
    task_index: impl std::fmt::Display,
    task_name: &str,
    cause: impl std::fmt::Display,
) -> WorkflowError {
    WorkflowError::Validation(format!(
        "task_options of workflow node {task_index} ({task_name}) refused: {cause}"
    ))
}

/// The deadline that a workflow start writes on a task node row and on the
/// task row of a root, read from the node's merged `task_options`
/// ([`merge_good_until_into_options`]).
///
/// - No options, no `good_until` key, or `null`: no deadline.
/// - An RFC 3339 text: that time. The core writes this form
///   ([`merge_good_until_into_options`]); Python's `isoformat()` of an aware
///   time is this form too.
/// - Otherwise [`WorkflowError::Validation`] naming the node: options that
///   the options reader of a task row refuses (text that does not parse, is
///   not strict JSON, or is not an object), a `good_until` that is not text,
///   or text that is not an RFC 3339 time. No deadline is guessed.
///
/// The options of a start come from the task function, and the start reads
/// them here, before any statement (Python passes the text to the database,
/// which refuses what it cannot read, `python/horsies/core/workflows/lifecycle.py:181-183`).
/// A promotion or a recovery case reads the stored copy of the options
/// instead: its task insert casts the stored `good_until` text, as
/// `horsies_workflow_node_tasks_insert` does, and a text that the database
/// cannot read raises from that insert (`python/horsies/core/workflows/engine.py:831-835`).
/// Pure.
pub(crate) fn node_good_until(
    task_index: impl std::fmt::Display,
    task_name: &str,
    task_options: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, WorkflowError> {
    let options = crate::core::history::enqueue::parse_optional_object(task_options)
        .map_err(|error| node_options_refused(&task_index, task_name, error))?;
    match options.as_ref().and_then(|object| object.get("good_until")) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) => chrono::DateTime::parse_from_rfc3339(text)
            .map(|deadline| Some(deadline.with_timezone(&chrono::Utc)))
            .map_err(|error| {
                node_options_refused(
                    &task_index,
                    task_name,
                    format!("good_until {text:?} is not an RFC 3339 time: {error}"),
                )
            }),
        Some(_other) => Err(node_options_refused(
            &task_index,
            task_name,
            "good_until is not an RFC 3339 text",
        )),
    }
}

/// Merge a node's `good_until` into its `task_options` at workflow start
/// (Python `python/horsies/core/workflows/lifecycle.py:576-604`). The result is
/// what the node row stores and [`node_good_until`] reads back.
///
/// - No deadline: the options as given; their readers judge them later.
/// - A deadline and no options: `{"good_until": "<RFC 3339>"}`.
/// - A deadline and options that hold a JSON object: the object with
///   `good_until` set.
/// - A deadline and options that the options reader of a task row refuses:
///   [`WorkflowError::Validation`] naming the node; the options are not
///   replaced. Python raises here and the start rolls back.
///
/// Pure.
pub(crate) fn merge_good_until_into_options(
    task_index: impl std::fmt::Display,
    task_name: &str,
    good_until: Option<chrono::DateTime<chrono::Utc>>,
    task_options_json: Option<&str>,
) -> Result<Option<String>, WorkflowError> {
    let Some(deadline) = good_until else {
        return Ok(task_options_json.map(str::to_owned));
    };
    let mut object = crate::core::history::enqueue::parse_optional_object(task_options_json)
        .map_err(|error| node_options_refused(&task_index, task_name, error))?
        .unwrap_or_default();
    // Python merges `node.good_until.isoformat()`: 6 fraction digits when
    // the microsecond is not 0.
    object.insert(
        "good_until".to_owned(),
        serde_json::Value::String(crate::core::python_text::python_isoformat_utc(deadline)),
    );
    let merged = serde_json::to_string(&serde_json::Value::Object(object))
        .expect("an object of values parsed from JSON text serializes");
    Ok(Some(merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};

    fn deadline() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 6, 15, 12, 0, 0).unwrap()
    }

    fn refusal(result: Result<impl std::fmt::Debug, WorkflowError>) -> String {
        match result {
            Err(WorkflowError::Validation(message)) => message,
            other => panic!("expected a refusal of the node's options, got {other:?}"),
        }
    }

    // -- node_good_until --

    #[test]
    fn no_options_no_key_and_null_are_no_deadline() {
        for options in [
            None,
            Some("{}"),
            Some("null"),
            Some(r#"{"retries": 3}"#),
            Some(r#"{"good_until": null}"#),
        ] {
            assert_eq!(
                node_good_until(0, "t", options).unwrap(),
                None,
                "{options:?}"
            );
        }
    }

    #[test]
    fn an_rfc_3339_text_is_the_deadline() {
        for text in [
            "2025-06-15T12:00:00+00:00",
            "2025-06-15T14:00:00+02:00",
            "2025-06-15T12:00:00Z",
        ] {
            let options = format!(r#"{{"good_until": "{text}"}}"#);
            let read = node_good_until(0, "t", Some(&options)).unwrap();
            assert_eq!(read, Some(deadline()), "{text}");
            assert_eq!(read.unwrap().year(), 2025);
        }
    }

    /// Each of these read as no deadline before: the deadline was dropped
    /// with no error.
    #[test]
    fn a_deadline_that_does_not_read_is_refused_by_node() {
        for options in [
            r#"{"good_until": "not-a-date"}"#,
            r#"{"good_until": "2025-06-15 12:00:00"}"#,
            r#"{"good_until": 12345}"#,
            r#"{"good_until": true}"#,
        ] {
            let message = refusal(node_good_until(4, "deadline_task", Some(options)));
            assert!(
                message.contains("task_options of workflow node 4 (deadline_task) refused"),
                "{message}"
            );
            assert!(message.contains("good_until"), "{message}");
        }
    }

    #[test]
    fn options_that_do_not_read_are_refused() {
        for options in ["not{json", "[1]", "3", r#"{"a": 1, "a": 2}"#, ""] {
            let message = refusal(node_good_until(0, "t", Some(options)));
            assert!(message.contains("refused"), "{options:?}: {message}");
        }
    }

    // -- merge_good_until_into_options --

    #[test]
    fn merge_no_deadline_keeps_the_options_as_given() {
        assert_eq!(
            merge_good_until_into_options(0, "t", None, None).unwrap(),
            None
        );
        let opts = r#"{"retries": 3}"#;
        assert_eq!(
            merge_good_until_into_options(0, "t", None, Some(opts))
                .unwrap()
                .as_deref(),
            Some(opts)
        );
    }

    #[test]
    fn merge_deadline_no_existing_options() {
        let merged = merge_good_until_into_options(0, "t", Some(deadline()), None)
            .unwrap()
            .expect("options with the deadline");
        let json: serde_json::Value = serde_json::from_str(&merged).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"good_until": deadline().to_rfc3339()})
        );
    }

    #[test]
    fn merge_deadline_with_existing_options() {
        let opts = r#"{"retries": 3, "good_until": null}"#;
        let merged = merge_good_until_into_options(0, "t", Some(deadline()), Some(opts))
            .unwrap()
            .expect("options with the deadline");
        let json: serde_json::Value = serde_json::from_str(&merged).unwrap();
        assert_eq!(json.get("retries").unwrap().as_i64(), Some(3));
        assert_eq!(
            json.get("good_until").and_then(serde_json::Value::as_str),
            Some(deadline().to_rfc3339().as_str())
        );
    }

    #[test]
    fn merge_deadline_round_trips_through_the_reader() {
        let merged = merge_good_until_into_options(0, "t", Some(deadline()), None).unwrap();
        assert_eq!(
            node_good_until(0, "t", merged.as_deref()).unwrap(),
            Some(deadline())
        );
    }

    /// These were replaced by `{}` before, so the stored options lost their
    /// content and kept only the deadline.
    #[test]
    fn merge_refuses_options_that_do_not_read() {
        for options in ["not{json", "[1]", "\"text\"", r#"{"a": 1, "a": 2}"#] {
            let message = refusal(merge_good_until_into_options(
                2,
                "merge_task",
                Some(deadline()),
                Some(options),
            ));
            assert!(
                message.contains("task_options of workflow node 2 (merge_task) refused"),
                "{options}: {message}"
            );
        }
    }
}
