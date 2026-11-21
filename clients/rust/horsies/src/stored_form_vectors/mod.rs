//! The Rust client's side of the shared golden vectors of the stored JSON
//! forms (`core/tests/fixtures/stored_forms/`, with its
//! `README.md`).
//!
//! Each vector is a value and the exact text that Python stores for it. The
//! client encodes each value with its own types to the text, and reads the
//! text back to the value. A vector names what the client cannot express
//! (`rust_cannot_express`) or writes differently today (`rust_drift`, a
//! finding of the README); the tests check both, so a change in either client
//! fails a test.

mod error_data;
mod result_envelope;
mod task_input;
mod task_options;

use std::collections::BTreeMap;
use std::fmt::Debug;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use horsies_core::{
    BuiltInTaskCode, RawResultRecord, StoredEnvelopeText, SubWorkflowFailure, SubWorkflowSummary,
    TaskErrorCode, TaskErrorKind, TaskStatus, UserCode,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::StoredDateTime;

/// The text of the vector file `name` of the core's vector folder.
pub(crate) fn vector_file(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../core/tests/fixtures/stored_forms")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read the vector file {}: {error}", path.display()))
}

/// The text that the client writes today in place of Python's, and the name
/// of the finding in the README.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RustDrift {
    text: String,
    finding: String,
}

/// What the client does with a vector.
#[derive(Debug, Clone)]
pub(crate) enum RustCase {
    /// It writes Python's text.
    Same,
    /// It writes `RustDrift::text`.
    Drift(RustDrift),
    /// It has no type for the value, for the reason given.
    CannotExpress(String),
}

impl RustCase {
    /// The case of a vector row from its two optional notes. A row with both
    /// is a defect of the file.
    pub(crate) fn of(name: &str, cannot_express: Option<String>, drift: Option<RustDrift>) -> Self {
        match (cannot_express, drift) {
            (None, None) => Self::Same,
            (None, Some(drift)) => {
                assert!(
                    !drift.finding.is_empty(),
                    "{name}: a drift names its finding"
                );
                Self::Drift(drift)
            }
            (Some(reason), None) => {
                assert!(
                    !reason.is_empty(),
                    "{name}: a vector that Rust cannot express names why"
                );
                Self::CannotExpress(reason)
            }
            (Some(_), Some(_)) => panic!("{name}: a vector is either a drift or not expressible"),
        }
    }

    /// The text that the client writes for a vector whose Python text is
    /// `python`; `None` when it cannot express the value.
    pub(crate) fn expected_text<'a>(&'a self, python: &'a str) -> Option<&'a str> {
        match self {
            Self::Same => Some(python),
            Self::Drift(drift) => Some(drift.text.as_str()),
            Self::CannotExpress(_) => None,
        }
    }
}

/// The text of a value in the vectors' notation: decimal text for an
/// integer, the text of a float, ISO 8601 text, UUID text.
fn notation_text<'a>(name: &str, notation: &'a Value) -> &'a str {
    notation
        .as_str()
        .unwrap_or_else(|| panic!("{name}: expected the value as text, got {notation}"))
}

/// The enum with text values of the vectors (Python `Color`).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Color {
    Red,
    Green,
}

/// The record of the vectors (Python `Order`): two fields, in this order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Order {
    id: i64,
    amount: f64,
}

fn parsed<T>(name: &str, notation: &Value) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let text = notation_text(name, notation);
    text.parse()
        .unwrap_or_else(|error| panic!("{name}: {text} does not parse: {error}"))
}

fn offset_datetime(name: &str, notation: &Value) -> DateTime<FixedOffset> {
    let text = notation_text(name, notation);
    DateTime::parse_from_rfc3339(text)
        .unwrap_or_else(|error| panic!("{name}: {text} does not parse: {error}"))
}

fn naive_datetime(name: &str, notation: &Value) -> NaiveDateTime {
    let text = notation_text(name, notation);
    NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap_or_else(|error| panic!("{name}: {text} does not parse: {error}"))
}

fn color(name: &str, notation: &Value) -> Color {
    match notation_text(name, notation) {
        "red" => Color::Red,
        "green" => Color::Green,
        other => panic!("{name}: {other} is not a Color"),
    }
}

fn order(name: &str, notation: &Value) -> Order {
    Order {
        id: parsed(name, &notation["id"]),
        amount: parsed(name, &notation["amount"]),
    }
}

fn list(name: &str, notation: &Value) -> Vec<i64> {
    notation
        .as_array()
        .unwrap_or_else(|| panic!("{name}: expected a list"))
        .iter()
        .map(|item| parsed(name, item))
        .collect()
}

fn sorted_map(name: &str, notation: &Value) -> BTreeMap<String, i64> {
    notation
        .as_object()
        .unwrap_or_else(|| panic!("{name}: expected an object"))
        .iter()
        .map(|(key, item)| (key.clone(), parsed(name, item)))
        .collect()
}

/// A check of one typed value, generic over the Rust type of the value.
trait TypedCheck {
    fn check<T>(&mut self, value: T)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug;
}

/// Run `check` on the Rust value of `notation` under the vector type
/// `type_name` (the README's notation table). A type with no Rust type here
/// is a defect of the vector: it must be marked `rust_cannot_express`.
fn check_typed(name: &str, type_name: &str, notation: &Value, check: &mut impl TypedCheck) {
    match type_name {
        "none" => {
            assert!(notation.is_null(), "{name}: the none type holds null");
            check.check(());
        }
        "bool" => check.check(
            notation
                .as_bool()
                .unwrap_or_else(|| panic!("{name}: expected a bool")),
        ),
        "i64" => check.check(parsed::<i64>(name, notation)),
        "u64" => check.check(parsed::<u64>(name, notation)),
        "i128" => check.check(parsed::<i128>(name, notation)),
        "u128" => check.check(parsed::<u128>(name, notation)),
        "f64" => check.check(parsed::<f64>(name, notation)),
        "str" => check.check(notation_text(name, notation).to_owned()),
        // chrono's own serde form writes a whole millisecond with 3 digits;
        // `StoredDateTime` writes Python's text.
        "datetime_utc" => check.check(StoredDateTime(
            offset_datetime(name, notation).with_timezone(&Utc),
        )),
        "datetime_offset" => check.check(StoredDateTime(offset_datetime(name, notation))),
        "datetime_naive" => check.check(StoredDateTime(naive_datetime(name, notation))),
        "date" => check.check(parsed::<NaiveDate>(name, notation)),
        "uuid" => check.check(parsed::<Uuid>(name, notation)),
        "str_enum" => check.check(color(name, notation)),
        "optional_i64" => check.check(match notation {
            Value::Null => None,
            given => Some(parsed::<i64>(name, given)),
        }),
        "list_i64" => check.check(list(name, notation)),
        "map_str_i64" => check.check(sorted_map(name, notation)),
        "json" => check.check(notation.clone()),
        "record_order" => check.check(order(name, notation)),
        other => panic!("{name}: the vector type {other} has no Rust type here"),
    }
}

/// A record of a stored envelope, as the core's result read gives it.
fn record(name: &str, status: TaskStatus, text: &str) -> RawResultRecord {
    RawResultRecord {
        task_id: Uuid::nil(),
        task_name: "stored_form_vector".to_owned(),
        status,
        raw_result: StoredEnvelopeText::parse(text.to_owned())
            .unwrap_or_else(|refused| panic!("{name}: the core refused the envelope: {refused}")),
    }
}

/// The code of a task error notation.
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

/// The stored form of a task error notation, with `stored_data` in place of
/// its `data` when the vector names one.
fn stored_error(
    name: &str,
    notation: &Value,
    stored_data: Option<&Value>,
) -> horsies_core::TaskError {
    let data_value = match stored_data {
        Some(stored) => Some(stored),
        None => match &notation["data"] {
            Value::Null => None,
            given => Some(given),
        },
    };
    let data = data_value.map(|value| {
        horsies_core::JsonText::of(value)
            .unwrap_or_else(|refused| panic!("{name}: the data does not write: {refused}"))
    });
    let exception = match &notation["exception"] {
        Value::Null => None,
        record => Some(
            serde_json::from_value::<crate::ExceptionRecord>(record.clone())
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
    horsies_core::TaskError {
        exception,
        error_code: error_code(name, &notation["error_code"]),
        data,
        message: notation["message"].as_str().map(str::to_owned),
        kind,
    }
}
