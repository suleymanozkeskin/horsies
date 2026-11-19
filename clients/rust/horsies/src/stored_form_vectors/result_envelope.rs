//! The result envelope vectors (`result_envelope.json`) through the client.
//!
//! A task body's value is encoded by `encode_output`; the core's child host
//! puts that text in the envelope as it is (`result_end`, mirrored by
//! [`envelope_of_value`]). A task body's error is stored by `body_result` and
//! written by the child host's `error_envelope` through
//! `task_result_to_json_string`. A read of a stored envelope is
//! `outcome_of_record`; an ok value is also read as a dependency result
//! (`TaskOutcome` in a task input) and by `WorkflowContext::result_for`. The
//! client builds an error from a code, a message and `data`; the exception
//! record and the sub-workflow failure are written by the library, so those
//! vectors are read only.

use std::fmt::Debug;

use horsies_core::core::codec::task_result_to_json_string;
use horsies_core::{
    RawResultRecord, StoredEnvelopeText, TaskErrorCode, TaskResult, TaskStatus, UserCode,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
use uuid::Uuid;

use super::{
    check_typed, error_code, record, stored_error, vector_file, RustCase, RustDrift, TypedCheck,
};
use crate::handle::outcome_of_record;
use crate::task::body::{body_result, decode_input, encode_output};
use crate::{NodeKey, TaskError, TaskOutcome, WorkflowContext};

#[derive(Deserialize)]
struct ResultEnvelopeForm {
    vectors: Vec<VectorRow>,
    refused_texts: Vec<RefusedText>,
}

#[derive(Deserialize)]
struct VectorRow {
    name: String,
    ok: Option<TypedValue>,
    err: Option<Value>,
    stored_data: Option<Value>,
    text: String,
    rust_cannot_express: Option<String>,
    rust_drift: Option<RustDrift>,
}

#[derive(Deserialize)]
struct TypedValue {
    #[serde(rename = "type")]
    type_name: String,
    value: Value,
}

#[derive(Deserialize)]
struct RefusedText {
    name: String,
    text: String,
}

fn form() -> ResultEnvelopeForm {
    serde_json::from_str(&vector_file("result_envelope.json")).expect("result_envelope.json parses")
}

/// The envelope that the core's child host writes for the text of an ok
/// value (`crates/horsies-core/src/worker/executor/child_host/run.rs`,
/// `result_end`): the text as it is, in the `ok` slot.
fn envelope_of_value(name: &str, bytes: &[u8]) -> String {
    let raw: &RawValue = serde_json::from_slice(bytes)
        .unwrap_or_else(|error| panic!("{name}: the encoded value is not JSON: {error}"));
    serde_json::to_string(&TaskResult::Ok(raw)).expect("an envelope of a JSON text encodes")
}

/// The check of an ok vector: the client's text of the value, and the three
/// reads of Python's text: the result read, the dependency read and the
/// context read.
struct OkValue<'a> {
    name: &'a str,
    python: &'a str,
    expected: &'a str,
}

impl TypedCheck for OkValue<'_> {
    fn check<T>(&mut self, value: T)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let name = self.name;
        let written = match encode_output(&value) {
            TaskResult::Ok(bytes) => envelope_of_value(name, &bytes),
            TaskResult::Err(error) => panic!("{name}: the client refused the value: {error}"),
        };
        assert_eq!(written, self.expected, "{name}");
        match outcome_of_record::<T, UserCode>(record(name, TaskStatus::Completed, self.python)) {
            Ok(TaskOutcome::Succeeded(read)) => assert_eq!(read, value, "{name}"),
            Ok(TaskOutcome::Failed(error)) => panic!("{name}: read an error: {error}"),
            Err(error) => panic!("{name}: the read refused Python's text: {error}"),
        }
        match decode_input::<TaskOutcome<T, UserCode>>(self.python.as_bytes()) {
            Ok(TaskOutcome::Succeeded(read)) => assert_eq!(read, value, "{name}: dependency"),
            Ok(TaskOutcome::Failed(error)) => panic!("{name}: dependency read an error: {error}"),
            Err(error) => panic!("{name}: the dependency read refused Python's text: {error}"),
        }
        let context = decode_input::<WorkflowContext>(context_text(self.python).as_bytes())
            .unwrap_or_else(|error| panic!("{name}: the context refused Python's text: {error}"));
        match context.result_for::<T, UserCode>(&NodeKey::new(CONTEXT_NODE_ID)) {
            Ok(TaskOutcome::Succeeded(read)) => assert_eq!(read, value, "{name}: context"),
            Ok(TaskOutcome::Failed(error)) => panic!("{name}: context read an error: {error}"),
            Err(error) => panic!("{name}: the context read refused Python's text: {error}"),
        }
    }
}

/// The node id of the one result of [`context_text`].
const CONTEXT_NODE_ID: &str = "node";

/// The text of a workflow context that holds `envelope` as the result of
/// the node [`CONTEXT_NODE_ID`], as the child of the worker writes it.
fn context_text(envelope: &str) -> String {
    format!(
        r#"{{"workflow_id":"{workflow_id}","task_index":0,"task_name":"","results_by_id":{{"{CONTEXT_NODE_ID}":{envelope}}},"summaries_by_id":{{}}}}"#,
        workflow_id = Uuid::nil(),
    )
}

/// The error that a task body builds for the notation, or `None` when the
/// client's API does not build it: an exception record or a sub-workflow
/// failure (written by the library), or no message (`TaskError::user` and
/// `TaskError::library` take one).
fn body_error(name: &str, notation: &Value) -> Option<TaskError<UserCode>> {
    let message = notation["message"].as_str()?;
    if !notation["exception"].is_null() || notation.get("sub_workflow").is_some() {
        return None;
    }
    let error = match error_code(name, &notation["error_code"]) {
        TaskErrorCode::BuiltIn(code) => TaskError::library(code, message),
        TaskErrorCode::User(code) => TaskError::user(code, message),
    };
    Some(match &notation["data"] {
        Value::Null => error,
        data => error.with_data(data.clone()),
    })
}

fn check_error(vector: &VectorRow, notation: &Value, expected: &str) {
    let name = vector.name.as_str();
    if let Some(error) = body_error(name, notation) {
        let written = match body_result::<(), UserCode>(Err(error)) {
            TaskResult::Err(stored) => {
                task_result_to_json_string(&TaskResult::<Value>::Err(stored))
                    .expect("a stored error encodes")
            }
            TaskResult::Ok(_) => panic!("{name}: an error result stored a value"),
        };
        assert_eq!(written, expected, "{name}");
    }
    match outcome_of_record::<(), UserCode>(record(name, TaskStatus::Failed, &vector.text)) {
        Ok(TaskOutcome::Failed(error)) => assert_eq!(
            error.into_stored(),
            stored_error(name, notation, vector.stored_data.as_ref()),
            "{name}"
        ),
        Ok(TaskOutcome::Succeeded(())) => panic!("{name}: read a value"),
        Err(error) => panic!("{name}: the read refused Python's text: {error}"),
    }
}

#[test]
fn each_vector_encodes_to_its_text_and_reads_back() {
    for row in form().vectors {
        let case = RustCase::of(
            &row.name,
            row.rust_cannot_express.clone(),
            row.rust_drift.clone(),
        );
        let expected = match case.expected_text(&row.text) {
            Some(text) => text.to_owned(),
            None => continue,
        };
        match (&row.ok, &row.err) {
            (Some(ok), None) => check_typed(
                &row.name,
                &ok.type_name,
                &ok.value,
                &mut OkValue {
                    name: &row.name,
                    python: &row.text,
                    expected: &expected,
                },
            ),
            (None, Some(err)) => check_error(&row, err, &expected),
            _ => panic!("{}: a vector has an ok or an err value", row.name),
        }
    }
}

#[test]
fn each_refused_text_is_refused_by_the_read() {
    for refused in form().refused_texts {
        let read = StoredEnvelopeText::parse(refused.text.clone()).map(|text| {
            text.map(|envelope| {
                outcome_of_record::<i64, UserCode>(RawResultRecord {
                    task_id: Uuid::nil(),
                    task_name: "stored_form_vector".to_owned(),
                    status: TaskStatus::Completed,
                    raw_result: Some(envelope),
                })
            })
        });
        let refused_by_the_read = match read {
            Err(_) => true,
            Ok(Some(Err(_))) => true,
            Ok(Some(Ok(_))) | Ok(None) => false,
        };
        assert!(refused_by_the_read, "{}", refused.name);
    }
}
