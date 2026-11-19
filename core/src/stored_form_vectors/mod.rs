//! The core's side of the shared golden vectors of the stored JSON forms
//! (`tests/fixtures/stored_forms/`, with its `README.md`).
//!
//! Each vector is a value and the exact text that Python stores for it. The
//! core encodes each value that it writes to the text, and reads each text to
//! the value. The Python client and the Rust client test the same files.

pub(crate) mod error_data;
pub(crate) mod result_envelope;
mod task_input;
mod task_options;
mod trace_carrier;
mod workflow_spec_data;

use serde_json::Value;
use uuid::Uuid;

use crate::core::codec::StrictJsonError;
use crate::core::task::error::ExceptionRecord;
use crate::core::{
    BuiltInTaskCode, SubWorkflowFailure, SubWorkflowSummary, TaskError, TaskErrorCode,
    TaskErrorKind, UserCode,
};

/// The text of the vector file `name` of the vector folder.
pub(crate) fn vector_file(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/stored_forms")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read the vector file {}: {error}", path.display()))
}

/// The variant name of a refusal of the strict read rule, as a vector file
/// names it.
fn refusal_name(refusal: &StrictJsonError) -> &'static str {
    match refusal {
        StrictJsonError::Empty => "Empty",
        StrictJsonError::NotUtf8 { .. } => "NotUtf8",
        StrictJsonError::Syntax { .. } => "Syntax",
        StrictJsonError::NonStandardConstant { .. } => "NonStandardConstant",
        StrictJsonError::IntegerTooLong { .. } => "IntegerTooLong",
        StrictJsonError::NonFiniteFloat { .. } => "NonFiniteFloat",
        StrictJsonError::LoneSurrogate { .. } => "LoneSurrogate",
        StrictJsonError::TooDeep { .. } => "TooDeep",
        StrictJsonError::EngineKey { .. } => "EngineKey",
        StrictJsonError::DuplicateKey { .. } => "DuplicateKey",
    }
}

/// The code of a task error notation: `{"built_in": <name>}` or
/// `{"user": <text>}`.
fn error_code(name: &str, notation: &Value) -> TaskErrorCode {
    match (notation["built_in"].as_str(), notation["user"].as_str()) {
        (Some(code), None) => TaskErrorCode::BuiltIn(
            BuiltInTaskCode::parse(code)
                .unwrap_or_else(|| panic!("{name}: {code} is not a built-in code")),
        ),
        (None, Some(code)) => TaskErrorCode::User(
            UserCode::parse(code).unwrap_or_else(|refused| panic!("{name}: {refused}")),
        ),
        _ => panic!("{name}: an error code is built_in or user"),
    }
}

/// The core's `TaskError` of a task error notation (the README), with
/// `stored_data` in place of its `data` when the vector names one: the data
/// that a writer stores for a value it refuses.
pub(crate) fn task_error_of(
    name: &str,
    notation: &Value,
    stored_data: Option<&Value>,
) -> TaskError {
    let data_value = match stored_data {
        Some(stored) => Some(stored),
        None => match &notation["data"] {
            Value::Null => None,
            given => Some(given),
        },
    };
    let data = data_value.map(|value| {
        crate::core::codec::JsonText::of(value)
            .unwrap_or_else(|refused| panic!("{name}: the data does not write: {refused}"))
    });
    let exception = match &notation["exception"] {
        Value::Null => None,
        record => Some(
            serde_json::from_value::<ExceptionRecord>(record.clone())
                .unwrap_or_else(|error| panic!("{name}: {error}")),
        ),
    };
    let kind = match notation.get("sub_workflow") {
        None => TaskErrorKind::Plain,
        Some(sub_workflow) => TaskErrorKind::SubWorkflow(SubWorkflowFailure {
            sub_workflow_id: Uuid::parse_str(
                sub_workflow["id"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{name}: a sub-workflow id is text")),
            )
            .unwrap_or_else(|error| panic!("{name}: {error}")),
            sub_workflow_summary: SubWorkflowSummary::from_stored_text(
                &sub_workflow["summary"].to_string(),
            )
            .unwrap_or_else(|refused| panic!("{name}: {refused}")),
        }),
    };
    TaskError {
        exception,
        error_code: error_code(name, &notation["error_code"]),
        data,
        message: notation["message"].as_str().map(str::to_owned),
        kind,
    }
}
