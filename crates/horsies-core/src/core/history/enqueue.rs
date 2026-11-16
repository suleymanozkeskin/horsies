//! Facts prepared in process for every new live task row.
//!
//! A task row stores its input as one JSON value in the `input` column, exactly
//! as the client encoded it. These functions check that text and derive the
//! other facts of the row; they do no I/O.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::ddl::classes::resolve_retention_class_key;
use super::identity::fingerprint::{
    EnqueueCommandV1, FingerprintError, COMMAND_FINGERPRINT_VERSION,
};
use super::identity::keys::{IdempotencyKeyError, ScopedIdempotencyKey};
use super::rerun::input_envelope::validate_integer_domain;
use crate::core::codec::{check_json_text, JsonReadError, StrictJsonError};
use crate::core::task::execution::{stored_execution, Execution, InvalidExecution};

/// Facts of a task row that a send, a schedule slot or a rerun writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEnqueueFacts {
    pub command_fingerprint_version: i16,
    pub command_fingerprint: [u8; 32],
    pub retention_class_key: String,
    pub idempotency_key_digest: Option<[u8; 32]>,
    pub retain_rerun_input: bool,
    pub execution: Execution,
}

/// Facts of a task node that workflow start stores on the node row, and that the
/// task row minted from the node carries.
///
/// An engine-minted task row has no command fingerprint, no idempotency key and
/// no retained input (`horsies_tasks_command_fingerprint_by_origin`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowTaskFacts {
    pub retention_class_key: String,
    pub execution: Execution,
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueuePreparationError {
    /// The text breaks the core's strict JSON read rule (`check_json_text`).
    #[error("{field} JSON is refused: {source}")]
    StrictJson {
        field: &'static str,
        source: StrictJsonError,
    },
    #[error("{field} JSON is corrupt: {source}")]
    Json {
        field: &'static str,
        source: serde_json::Error,
    },
    #[error("{field} JSON carries a number outside the supported domain: {detail}")]
    NumberDomain { field: &'static str, detail: String },
    #[error("task_options JSON must encode an object")]
    InvalidTaskOptions,
    #[error("task_options refused: {0}")]
    InvalidExecution(#[from] InvalidExecution),
    #[error("command fingerprint failed: {0}")]
    Fingerprint(#[from] FingerprintError),
    #[error("idempotency key is invalid: {0}")]
    Idempotency(#[from] IdempotencyKeyError),
}

/// Facts of a first-run send.
///
/// `input_json` is the encoded input as one JSON value. Any JSON value that
/// passes the strict read rule is accepted; the text is stored and
/// fingerprinted byte for byte.
#[allow(clippy::too_many_arguments)]
pub fn prepare_enqueue_facts(
    task_name: &str,
    queue_name: &str,
    priority: i32,
    input_json: &str,
    good_until: Option<DateTime<Utc>>,
    enqueue_delay_seconds: Option<i64>,
    task_options_json: Option<&str>,
    retention_class_key: Option<&str>,
    retain_rerun_input: bool,
    idempotency_key: Option<&str>,
) -> Result<PreparedEnqueueFacts, EnqueuePreparationError> {
    prepare_enqueue_facts_with_lineage(
        task_name,
        queue_name,
        priority,
        input_json,
        good_until,
        enqueue_delay_seconds,
        task_options_json,
        retention_class_key,
        retain_rerun_input,
        idempotency_key,
        None,
        None,
    )
}

/// Facts of a send with an optional rerun lineage (source and root together).
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_enqueue_facts_with_lineage(
    task_name: &str,
    queue_name: &str,
    priority: i32,
    input_json: &str,
    good_until: Option<DateTime<Utc>>,
    enqueue_delay_seconds: Option<i64>,
    task_options_json: Option<&str>,
    retention_class_key: Option<&str>,
    retain_rerun_input: bool,
    idempotency_key: Option<&str>,
    rerun_of_task_id: Option<Uuid>,
    rerun_root_task_id: Option<Uuid>,
) -> Result<PreparedEnqueueFacts, EnqueuePreparationError> {
    check_input(input_json)?;
    let options = parse_optional_object(task_options_json)?;
    let execution = stored_execution(options.as_ref())?;
    let retention_class_key = resolve_retention_class_key(retention_class_key).to_owned();
    // The version-1 command fingerprint keeps Python's field list. The one
    // input text takes the keyword slot and the positional slot is null: the
    // field values a Python send fingerprints for its keyword input.
    let command = EnqueueCommandV1::new(
        task_name,
        queue_name,
        priority,
        None,
        Some(input_json.to_owned()),
        good_until,
        enqueue_delay_seconds,
        task_options_json.map(str::to_owned),
        &retention_class_key,
        retain_rerun_input,
        rerun_of_task_id,
        rerun_root_task_id,
    )?;
    let idempotency_key_digest = idempotency_key
        .map(|key| ScopedIdempotencyKey::new(task_name, key).map(|scoped| scoped.digest()))
        .transpose()?;

    Ok(PreparedEnqueueFacts {
        command_fingerprint_version: COMMAND_FINGERPRINT_VERSION,
        command_fingerprint: command.fingerprint()?,
        retention_class_key,
        idempotency_key_digest,
        retain_rerun_input,
        execution,
    })
}

/// Facts of a task node and of the task row that the engine mints from it.
pub fn prepare_workflow_task_facts(
    input_json: &str,
    task_options_json: Option<&str>,
    retention_class_key: Option<&str>,
) -> Result<WorkflowTaskFacts, EnqueuePreparationError> {
    check_input(input_json)?;
    let options = parse_optional_object(task_options_json)?;
    let execution = stored_execution(options.as_ref())?;
    Ok(WorkflowTaskFacts {
        retention_class_key: resolve_retention_class_key(retention_class_key).to_owned(),
        execution,
    })
}

/// Refuse input text that breaks the strict read rule, so a refused text
/// never reaches storage. The text is stored as written, so integers keep
/// every digit.
fn check_input(input_json: &str) -> Result<(), EnqueuePreparationError> {
    check_json_text(input_json).map_err(|source| EnqueuePreparationError::StrictJson {
        field: "input",
        source,
    })
}

/// The task options as a JSON object, read with the strict rule; `None`
/// for no options or `null`. Refuses text that breaks the strict read rule,
/// does not parse, holds an integer the parse would round, or is not an
/// object.
pub(crate) fn parse_optional_object(
    value: Option<&str>,
) -> Result<Option<Map<String, Value>>, EnqueuePreparationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    // The options are parsed to read the execution class, so an integer that
    // the parse would round is refused. The core's strict reader gives the
    // decode 300 levels of room (Python `core/codec/json_value.py:54-75`).
    let parsed: Value = crate::core::codec::from_json_str(value).map_err(|error| match error {
        JsonReadError::Refused(source) => EnqueuePreparationError::StrictJson {
            field: "task_options",
            source,
        },
        JsonReadError::Decode(source) => EnqueuePreparationError::Json {
            field: "task_options",
            source,
        },
    })?;
    validate_integer_domain(value).map_err(|detail| EnqueuePreparationError::NumberDomain {
        field: "task_options",
        detail,
    })?;
    match parsed {
        Value::Object(object) => Ok(Some(object)),
        Value::Null => Ok(None),
        _ => Err(EnqueuePreparationError::InvalidTaskOptions),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "arbitrary-precision"))]
    #[test]
    fn a_long_integer_is_kept_in_input_and_refused_in_options() {
        // The input is stored as written, so every digit is kept (Python's
        // limit is 4300 digits). The options are parsed to read the
        // execution class, so an integer the parse would round is refused.
        let big = "123456789012345678901234567890";
        let input = format!("{{\"k\":{big}}}");
        prepare_enqueue_facts(
            "task",
            "default",
            50,
            &input,
            None,
            None,
            Some("{}"),
            Some("standard_30d"),
            true,
            None,
        )
        .expect("a long integer in the input is stored as written");
        let options = format!("{{\"k\":{big}}}");
        let error = prepare_enqueue_facts(
            "task",
            "default",
            50,
            "{}",
            None,
            None,
            Some(&options),
            Some("standard_30d"),
            true,
            None,
        )
        .expect_err("an out-of-domain integer in the options must not be rounded");
        assert!(
            matches!(
                error,
                EnqueuePreparationError::NumberDomain {
                    field: "task_options",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[test]
    fn ordinary_facts_pin_fingerprint_key_class_and_forever() {
        let keyed = prepare_enqueue_facts(
            "task",
            "default",
            50,
            "{\"z\":1}",
            None,
            None,
            Some("{}"),
            Some("standard_30d"),
            true,
            Some("request-1"),
        )
        .unwrap();
        assert_eq!(keyed.command_fingerprint_version, 1);
        assert_eq!(keyed.retention_class_key, "standard_30d");
        assert_eq!(keyed.command_fingerprint.len(), 32);
        assert!(keyed.idempotency_key_digest.is_some());
        assert!(keyed.retain_rerun_input);
        assert_eq!(keyed.execution, Execution::Isolated);

        let forever = prepare_enqueue_facts(
            "task", "default", 50, "null", None, None, None, None, false, None,
        )
        .unwrap();
        assert_eq!(forever.retention_class_key, "forever");
        assert!(!forever.retain_rerun_input);
    }

    #[test]
    fn input_text_that_breaks_the_strict_read_rule_is_refused() {
        for input in ["", "{", "[1] [2]", "NaN", "{\"a\":1,\"a\":2}", "[1e400]"] {
            let error = prepare_enqueue_facts(
                "task", "default", 50, input, None, None, None, None, false, None,
            )
            .expect_err("refused input text must not reach storage");
            assert!(
                matches!(
                    error,
                    EnqueuePreparationError::StrictJson { field: "input", .. }
                ),
                "{input:?}: {error}"
            );
        }
    }

    #[test]
    fn task_options_that_break_the_strict_read_rule_are_refused() {
        let error = prepare_enqueue_facts(
            "task",
            "default",
            50,
            "{}",
            None,
            None,
            Some("{\"execution\":\"ISOLATED\",\"execution\":\"SHARED_THREAD\"}"),
            None,
            false,
            None,
        )
        .expect_err("a repeated options key must be refused");
        assert!(matches!(
            error,
            EnqueuePreparationError::StrictJson {
                field: "task_options",
                ..
            }
        ));
    }

    #[test]
    fn null_and_empty_object_inputs_fingerprint_differently() {
        let null_input = prepare_enqueue_facts(
            "task", "default", 50, "null", None, None, None, None, false, None,
        )
        .unwrap();
        let empty_object = prepare_enqueue_facts(
            "task", "default", 50, "{}", None, None, None, None, false, None,
        )
        .unwrap();
        assert_ne!(
            null_input.command_fingerprint,
            empty_object.command_fingerprint
        );
    }

    #[test]
    fn workflow_facts_carry_class_and_execution() {
        let facts = prepare_workflow_task_facts(
            "[1]",
            Some("{\"execution\":\"SHARED_THREAD\"}"),
            Some("standard_7d"),
        )
        .unwrap();
        assert_eq!(facts.retention_class_key, "standard_7d");
        assert_eq!(facts.execution, Execution::SharedThread);
    }

    #[test]
    fn an_unknown_execution_class_is_refused() {
        let error = prepare_enqueue_facts(
            "task",
            "default",
            50,
            "{}",
            None,
            None,
            Some("{\"execution\":\"PARALLEL\"}"),
            None,
            false,
            None,
        )
        .expect_err("an unknown execution class must be refused");
        assert!(matches!(
            error,
            EnqueuePreparationError::InvalidExecution(_)
        ));
    }
}
