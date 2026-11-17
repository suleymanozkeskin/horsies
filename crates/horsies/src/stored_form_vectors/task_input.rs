//! The task input vectors (`task_input.json`) through the client's encoder
//! and decoder.
//!
//! The send encodes the input with `encode_read_back`; the child reads it with
//! `decode_input` (`decode_unit_input` for a task with no parameters). The
//! input of a task is the struct that the task attribute generates: one field
//! per parameter, keyed by its name, unknown keys refused. A one-parameter
//! vector names its parameter `value`, so [`Input`] is that struct for each
//! type; the record, the two-parameter and the no-parameter vectors use tasks
//! of the attribute itself.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Debug;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::{notation_text, vector_file, RustCase, RustDrift};
use crate::task::body::{decode_input, decode_unit_input, encode_read_back};
use crate::TaskError;

#[derive(Deserialize)]
struct TaskInputForm {
    vectors: Vec<VectorRow>,
    refused_values: Vec<RefusedValue>,
    refused_texts: Vec<RefusedText>,
    typed_refused_texts: Vec<TypedRefusedText>,
}

#[derive(Deserialize)]
struct VectorRow {
    name: String,
    parameters: Vec<Parameter>,
    text: String,
    rust_cannot_express: Option<String>,
    rust_drift: Option<RustDrift>,
}

#[derive(Deserialize)]
struct Parameter {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    value: Value,
}

#[derive(Deserialize)]
struct RefusedValue {
    name: String,
    parameters: Vec<Parameter>,
}

#[derive(Deserialize)]
struct RefusedText {
    name: String,
    text: String,
}

#[derive(Deserialize)]
struct TypedRefusedText {
    name: String,
    parameters: Vec<Parameter>,
    text: String,
}

/// A vector row with its case for this client.
struct Vector {
    name: String,
    parameters: Vec<Parameter>,
    text: String,
    case: RustCase,
}

fn form() -> TaskInputForm {
    serde_json::from_str(&vector_file("task_input.json")).expect("task_input.json parses")
}

fn vectors(rows: Vec<VectorRow>) -> Vec<Vector> {
    rows.into_iter()
        .map(|row| Vector {
            case: RustCase::of(&row.name, row.rust_cannot_express, row.rust_drift),
            name: row.name,
            parameters: row.parameters,
            text: row.text,
        })
        .collect()
}

/// The input of a task whose one parameter is `value: T`, as the task
/// attribute generates it (`crates/horsies-macros/src/codegen.rs`,
/// `input_struct`).
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input<T> {
    value: T,
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

#[horsies::task("stored_form_record_parameter")]
async fn record_parameter(order: Order) -> Result<i64, TaskError<Infallible>> {
    Ok(order.id)
}

#[horsies::task("stored_form_two_parameters")]
async fn two_parameters(b: i64, a: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("{b}{a}"))
}

#[horsies::task("stored_form_no_parameters")]
async fn no_parameters() -> Result<(), TaskError<Infallible>> {
    Ok(())
}

/// The client's text of `value`: the encode of a send, with its read-back.
fn written<T: Serialize + DeserializeOwned>(name: &str, value: &T) -> String {
    let bytes = encode_read_back(value)
        .unwrap_or_else(|cause| panic!("{name}: the client refused the value: {cause}"));
    String::from_utf8(bytes).expect("the encoder writes UTF-8")
}

/// The client's read of Python's text.
fn read<T: DeserializeOwned>(name: &str, text: &str) -> T {
    decode_input::<T>(text.as_bytes())
        .unwrap_or_else(|refused| panic!("{name}: the client refused Python's text: {refused}"))
}

/// The text that the client must write for `vector`.
fn expected(vector: &Vector) -> &str {
    vector
        .case
        .expected_text(&vector.text)
        .unwrap_or_else(|| panic!("{}: the vector is not expressible in Rust", vector.name))
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

/// `Input { value }` encodes to the client's text of the vector, and
/// Python's text reads back to it.
fn check_value<T>(vector: &Vector, value: T)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let input = Input { value };
    assert_eq!(
        written(&vector.name, &input),
        expected(vector),
        "{}",
        vector.name
    );
    assert_eq!(
        read::<Input<T>>(&vector.name, &vector.text),
        input,
        "{}",
        vector.name
    );
}

/// A one-parameter vector, by the type of its parameter.
fn check_one_parameter(vector: &Vector, parameter: &Parameter) {
    let name = vector.name.as_str();
    let notation = &parameter.value;
    match parameter.type_name.as_str() {
        "bool" => check_value(
            vector,
            notation
                .as_bool()
                .unwrap_or_else(|| panic!("{name}: expected a bool")),
        ),
        "i64" => check_value(vector, parsed::<i64>(name, notation)),
        "u64" => check_value(vector, parsed::<u64>(name, notation)),
        "i128" => check_value(vector, parsed::<i128>(name, notation)),
        "u128" => check_value(vector, parsed::<u128>(name, notation)),
        "f64" => check_value(vector, parsed::<f64>(name, notation)),
        "str" => check_value(vector, notation_text(name, notation).to_owned()),
        "datetime_utc" => check_value(vector, offset_datetime(name, notation).with_timezone(&Utc)),
        "datetime_offset" => check_value(vector, offset_datetime(name, notation)),
        "datetime_naive" => check_value(vector, naive_datetime(name, notation)),
        "date" => check_value(vector, parsed::<NaiveDate>(name, notation)),
        "uuid" => check_value(vector, parsed::<Uuid>(name, notation)),
        "str_enum" => check_value(vector, color(name, notation)),
        "optional_i64" => check_value(
            vector,
            match notation {
                Value::Null => None,
                given => Some(parsed::<i64>(name, given)),
            },
        ),
        "list_i64" => check_value(vector, list(name, notation)),
        "map_str_i64" => check_value(vector, sorted_map(name, notation)),
        "json" => check_value(vector, notation.clone()),
        "record_order" => check_value(vector, order(name, notation)),
        other => panic!("{name}: the vector type {other} has no Rust type here"),
    }
}

/// The vectors that use tasks of the attribute.
fn check_attribute_task(vector: &Vector) {
    let name = vector.name.as_str();
    match (name, vector.parameters.as_slice()) {
        ("no_parameters", []) => {
            // A task with no parameters has the input `()`.
            let _register: fn(
                &mut crate::Horsies,
            ) -> Result<
                crate::TaskFunction<(), (), Infallible>,
                crate::HorsiesError,
            > = no_parameters::register;
            assert_eq!(written(name, &()), expected(vector), "{name}");
            decode_unit_input(vector.text.as_bytes())
                .unwrap_or_else(|refused| panic!("{name}: refused Python's text: {refused}"));
        }
        ("record_parameter", [parameter]) => {
            let value = order(name, &parameter.value);
            let input = record_parameter::Input {
                order: value.clone(),
            };
            assert_eq!(written(name, &input), expected(vector), "{name}");
            let record_parameter::Input { order: read_back } =
                read::<record_parameter::Input>(name, &vector.text);
            assert_eq!(read_back, value, "{name}");
        }
        ("parameters_in_declaration_order", [b, a]) => {
            let (b_value, a_value) = (
                parsed::<i64>(name, &b.value),
                notation_text(name, &a.value).to_owned(),
            );
            let input = two_parameters::Input {
                b: b_value,
                a: a_value.clone(),
            };
            assert_eq!(written(name, &input), expected(vector), "{name}");
            let two_parameters::Input { b, a } = read::<two_parameters::Input>(name, &vector.text);
            assert_eq!((b, a), (b_value, a_value), "{name}");
        }
        (other, _) => panic!("{other}: no Rust case for this parameter list"),
    }
}

#[test]
fn each_value_encodes_to_its_text_and_reads_back() {
    for vector in vectors(form().vectors) {
        if let RustCase::CannotExpress(reason) = &vector.case {
            assert!(!reason.is_empty(), "{}", vector.name);
            continue;
        }
        match vector.parameters.as_slice() {
            [parameter] if parameter.name == "value" => check_one_parameter(&vector, parameter),
            _ => check_attribute_task(&vector),
        }
    }
}

#[test]
fn each_refused_value_is_refused_by_the_send() {
    for refused in form().refused_values {
        let name = refused.name.as_str();
        let [parameter] = refused.parameters.as_slice() else {
            panic!("{name}: a refused value has one parameter");
        };
        let outcome = match parameter.type_name.as_str() {
            "f64" => encode_read_back(&Input {
                value: parsed::<f64>(name, &parameter.value),
            }),
            "json" => encode_read_back(&Input {
                value: parameter.value.clone(),
            }),
            other => panic!("{name}: the vector type {other} has no Rust case here"),
        };
        assert!(outcome.is_err(), "{name}: the send took a refused value");
    }
}

#[test]
fn each_refused_text_is_refused_by_the_read() {
    for refused in form().refused_texts {
        assert!(
            decode_input::<Input<Value>>(refused.text.as_bytes()).is_err(),
            "{}",
            refused.name
        );
    }
}

#[test]
fn each_typed_refused_text_is_refused_by_the_decode() {
    for refused in form().typed_refused_texts {
        let name = refused.name.as_str();
        let [parameter] = refused.parameters.as_slice() else {
            panic!("{name}: a typed refused text has one parameter");
        };
        let bytes = refused.text.as_bytes();
        let refused_by_decode = match parameter.type_name.as_str() {
            "i64" => decode_input::<Input<i64>>(bytes).is_err(),
            "str_enum" => decode_input::<Input<Color>>(bytes).is_err(),
            other => panic!("{name}: the vector type {other} has no Rust case here"),
        };
        assert!(refused_by_decode, "{name}");
    }
}
