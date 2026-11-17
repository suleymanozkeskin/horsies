//! The Rust client's side of the shared golden vectors of the stored JSON
//! forms (`crates/horsies-core/tests/fixtures/stored_forms/`, with its
//! `README.md`).
//!
//! Each vector is a value and the exact text that Python stores for it. The
//! client encodes each value with its own types to the text, and reads the
//! text back to the value. A vector names what the client cannot express
//! (`rust_cannot_express`) or writes differently today (`rust_drift`, a
//! finding of the README); the tests check both, so a change in either client
//! fails a test.

mod result_envelope;
mod task_input;

use std::collections::BTreeMap;
use std::fmt::Debug;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// The text of the vector file `name` of the core's vector folder.
fn vector_file(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../horsies-core/tests/fixtures/stored_forms")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read the vector file {}: {error}", path.display()))
}

/// The text that the client writes today in place of Python's, and the name
/// of the finding in the README.
#[derive(Debug, Clone, Deserialize)]
struct RustDrift {
    text: String,
    finding: String,
}

/// What the client does with a vector.
#[derive(Debug, Clone)]
enum RustCase {
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
    fn of(name: &str, cannot_express: Option<String>, drift: Option<RustDrift>) -> Self {
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
    fn expected_text<'a>(&'a self, python: &'a str) -> Option<&'a str> {
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
        "datetime_utc" => check.check(offset_datetime(name, notation).with_timezone(&Utc)),
        "datetime_offset" => check.check(offset_datetime(name, notation)),
        "datetime_naive" => check.check(naive_datetime(name, notation)),
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
