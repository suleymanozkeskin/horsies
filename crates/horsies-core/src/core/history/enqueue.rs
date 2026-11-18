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
use super::rerun::input_envelope::{end_of_number, end_of_string};
use crate::core::codec::{check_json_text, JsonReadError, StrictJsonError};
use crate::core::task::execution::{stored_execution, Execution, InvalidExecution};

/// The smallest integer that task options may hold (`i64::MIN`).
pub const MIN_OPTIONS_INTEGER: i64 = i64::MIN;
/// The largest integer that task options may hold (`u64::MAX`).
pub const MAX_OPTIONS_INTEGER: u64 = u64::MAX;

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

/// The part of a send that a refusal names (Python's `INVALID_INPUT` and
/// `INVALID_TASK_OPTIONS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendPart {
    /// The input, and the facts of the call that the input carries: the
    /// start, the sent instant, the idempotency key, the retention class
    /// key and the command fingerprint.
    Input,
    /// The task options.
    TaskOptions,
}

impl SendPart {
    /// The field name in a refusal message: `input` or `task_options`.
    pub const fn field_name(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::TaskOptions => "task_options",
        }
    }
}

impl std::fmt::Display for SendPart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.field_name())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueuePreparationError {
    /// The text breaks the core's strict JSON read rule (`check_json_text`).
    #[error("{field} JSON is refused: {source}")]
    StrictJson {
        field: SendPart,
        source: StrictJsonError,
    },
    #[error("{field} JSON is corrupt: {source}")]
    Json {
        field: SendPart,
        source: serde_json::Error,
    },
    /// The task options hold an integer outside [`MIN_OPTIONS_INTEGER`] to
    /// [`MAX_OPTIONS_INTEGER`]. No count, delay or limit of that size has a
    /// meaning. Python's send refuses it with the same text
    /// (`options_integer_out_of_range`, `python/horsies/core/codec/task_options.py`).
    #[error(
        "task_options JSON holds an integer outside {MIN_OPTIONS_INTEGER} to {MAX_OPTIONS_INTEGER}"
    )]
    OptionsIntegerOutOfRange,
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
        field: SendPart::Input,
        source,
    })
}

/// The task options as a JSON object, read with the strict rule; `None`
/// for no options or `null`. Refusals, in this order: text that breaks the
/// strict read rule; an integer outside [`MIN_OPTIONS_INTEGER`] to
/// [`MAX_OPTIONS_INTEGER`] (Python's send checks the same range after its
/// parse); text that does not decode; a value that is not an object.
pub(crate) fn parse_optional_object(
    value: Option<&str>,
) -> Result<Option<Map<String, Value>>, EnqueuePreparationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    check_json_text(value).map_err(|source| EnqueuePreparationError::StrictJson {
        field: SendPart::TaskOptions,
        source,
    })?;
    // Checked before the decode, which without `arbitrary-precision` rounds an
    // integer outside this range or refuses it with a serde_json error.
    if holds_integer_out_of_range(value) {
        return Err(EnqueuePreparationError::OptionsIntegerOutOfRange);
    }
    // The core's strict reader gives the decode 300 levels of room (Python
    // `core/codec/json_value.py:54-75`).
    let parsed: Value = crate::core::codec::from_json_str(value).map_err(|error| match error {
        JsonReadError::Refused(source) => EnqueuePreparationError::StrictJson {
            field: SendPart::TaskOptions,
            source,
        },
        JsonReadError::Decode(source) => EnqueuePreparationError::Json {
            field: SendPart::TaskOptions,
            source,
        },
    })?;
    match parsed {
        Value::Object(object) => Ok(Some(object)),
        Value::Null => Ok(None),
        _ => Err(EnqueuePreparationError::InvalidTaskOptions),
    }
}

/// Whether a JSON text holds an integer literal outside
/// [`MIN_OPTIONS_INTEGER`] to [`MAX_OPTIONS_INTEGER`]. A number with a
/// fraction or an exponent is a float, not an integer.
///
/// Precondition: the text passed the strict read rule, so it is one JSON
/// value and a number token starts only where this scan looks for one. Each
/// step moves forward, so the scan ends within the text. Pure.
fn holds_integer_out_of_range(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => index = end_of_string(bytes, index),
            b'-' | b'0'..=b'9' => {
                let start = index;
                index = end_of_number(bytes, index);
                let lexeme = &text[start..index];
                let is_integer = !lexeme.contains(['.', 'e', 'E']);
                let in_range = lexeme.parse::<i64>().is_ok() || lexeme.parse::<u64>().is_ok();
                if is_integer && !in_range {
                    return true;
                }
            }
            _ => index += 1,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_integer_is_kept_in_input_and_refused_in_options() {
        // The input is stored as written, so every digit is kept (Python's
        // limit is 4300 digits). An integer of that size in the options has
        // no meaning, so the send refuses it, with or without
        // `arbitrary-precision`.
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
        .expect_err("an integer outside the options range must be refused");
        assert!(
            matches!(error, EnqueuePreparationError::OptionsIntegerOutOfRange),
            "{error}"
        );
        assert_eq!(
            error.to_string(),
            "task_options JSON holds an integer outside -9223372036854775808 to \
             18446744073709551615"
        );
    }

    /// The range is `i64::MIN` to `u64::MAX`, at any depth; a float, and an
    /// integer in a string, are not judged. An integer of 400 digits is the
    /// named refusal, not a decode error.
    #[test]
    fn the_options_integer_range_is_i64_min_to_u64_max() {
        let accepted = [
            r#"{"retry_policy":{"max_retries":1,"intervals":[1],"max_delay_seconds":18446744073709551615,"auto_retry_for":["X"]}}"#,
            r#"{"a":[-9223372036854775808,18446744073709551615,-0]}"#,
            r#"{"a":1.8446744073709551616e19,"b":-1e300,"c":0.5}"#,
            r#"{"123456789012345678901234567890":"123456789012345678901234567890"}"#,
            r#"{"a":"escaped quote \" then 123456789012345678901234567890"}"#,
        ];
        for options in accepted {
            assert!(parse_optional_object(Some(options)).is_ok(), "{options}");
        }
        let long = "9".repeat(400);
        let refused = [
            r#"{"retry_policy":{"max_retries":1,"intervals":[1],"max_delay_seconds":18446744073709551616,"auto_retry_for":["X"]}}"#.to_owned(),
            r#"{"a":[1,{"b":-9223372036854775809}]}"#.to_owned(),
            format!(r#"{{"k":{long}}}"#),
            "18446744073709551616".to_owned(),
        ];
        for options in &refused {
            assert!(
                matches!(
                    parse_optional_object(Some(options.as_str())),
                    Err(EnqueuePreparationError::OptionsIntegerOutOfRange)
                ),
                "{options}"
            );
        }
    }

    /// A text that breaks the strict read rule is refused as such before
    /// its integers are judged.
    #[test]
    fn the_strict_read_rule_comes_before_the_integer_range() {
        let options = r#"{"a":18446744073709551616,"a":1}"#;
        assert!(matches!(
            parse_optional_object(Some(options)),
            Err(EnqueuePreparationError::StrictJson {
                field: SendPart::TaskOptions,
                ..
            })
        ));
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
                    EnqueuePreparationError::StrictJson {
                        field: SendPart::Input,
                        ..
                    }
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
                field: SendPart::TaskOptions,
                ..
            }
        ));
    }

    /// Python `test_broker_postgres.py::TestEnqueueAsync::test_empty_task_options_text_is_refused`:
    /// an empty options text is not JSON and is not "no options": the send
    /// refuses it before any statement.
    #[test]
    fn an_empty_task_options_text_is_refused() {
        let error = prepare_enqueue_facts(
            "task",
            "default",
            50,
            "{}",
            None,
            None,
            Some(""),
            None,
            false,
            None,
        )
        .expect_err("an empty options text must not reach storage");
        assert!(
            matches!(
                error,
                EnqueuePreparationError::StrictJson {
                    field: SendPart::TaskOptions,
                    ..
                }
            ),
            "{error}"
        );
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
