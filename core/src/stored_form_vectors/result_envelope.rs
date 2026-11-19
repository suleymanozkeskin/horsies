//! The result envelope vectors (`result_envelope.json`) through the core.
//!
//! The core reads every envelope (`read_task_result_text`, and the typed
//! read `task_result_from_json_str`) and writes the envelope of a task error
//! (`TaskError`'s `Serialize`, through the codec's
//! `task_result_to_json_string`). The envelope of an ok value is written by
//! the child host from the value's text (`child_host` vector tests). The
//! child host and the sub-workflow child build give a dependent each
//! envelope as stored.

use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::{task_error_of, vector_file};
use crate::core::codec::{task_result_from_json_str, task_result_to_json_string};
use crate::core::task::result::{read_task_result_text, TaskResultText};
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::TaskResult;
use crate::worker::execution::build_task_input;
use crate::workflow_engine::child_spec::ChildSpecRequest;
use crate::workflow_engine::start_statement::{merged_child_input, NodeInput};
use crate::workflow_engine::steps::ChildStartSource;

#[derive(Deserialize)]
pub(crate) struct ResultEnvelopeForm {
    pub(crate) vectors: Vec<ResultVector>,
    refused_texts: Vec<RefusedText>,
}

/// A vector: an `ok` value or an `err` notation, and Python's text.
#[derive(Deserialize)]
pub(crate) struct ResultVector {
    pub(crate) name: String,
    pub(crate) err: Option<Value>,
    pub(crate) stored_data: Option<Value>,
    pub(crate) text: String,
}

/// A text that every reader refuses. `refusal` is `envelope` when the shape
/// of the envelope is refused, `ok value` when the decode of its `ok` slot
/// is: `read_task_result_text` reads the shape and keeps the `ok` slot as
/// text, as Python's `validate_task_result_envelope`.
#[derive(Deserialize)]
struct RefusedText {
    name: String,
    text: String,
    refusal: String,
}

pub(crate) fn form() -> ResultEnvelopeForm {
    serde_json::from_str(&vector_file("result_envelope.json")).expect("result_envelope.json parses")
}

#[test]
fn each_error_encodes_to_its_text() {
    for vector in form().vectors {
        let Some(notation) = &vector.err else {
            continue;
        };
        let error = task_error_of(&vector.name, notation, None);
        let written = task_result_to_json_string(&TaskResult::<Value>::Err(error))
            .unwrap_or_else(|error| panic!("{}: {error}", vector.name));
        assert_eq!(written, vector.text, "{}", vector.name);
    }
}

#[test]
fn each_text_reads_back_to_its_slot() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let read = read_task_result_text(&vector.text)
            .unwrap_or_else(|refused| panic!("{name}: the core refused Python's text: {refused}"));
        match (read, &vector.err) {
            (TaskResultText::Ok(raw), None) => {
                // The ok slot is the text between `"ok":` and `,"err":null}`.
                let expected = vector
                    .text
                    .strip_prefix(r#"{"__h_task_result__":true,"ok":"#)
                    .and_then(|rest| rest.strip_suffix(r#","err":null}"#))
                    .unwrap_or_else(|| panic!("{name}: Python's ok envelope has another layout"));
                assert_eq!(raw.get(), expected, "{name}");
            }
            (TaskResultText::Err(error), Some(notation)) => assert_eq!(
                error,
                task_error_of(name, notation, vector.stored_data.as_ref()),
                "{name}"
            ),
            (TaskResultText::Ok(_), Some(_)) => panic!("{name}: read a value for an error"),
            (TaskResultText::Err(error), None) => panic!("{name}: read an error: {error}"),
        }
    }
}

#[test]
fn each_error_text_reads_back_through_the_typed_read() {
    for vector in form().vectors {
        let Some(notation) = &vector.err else {
            continue;
        };
        match task_result_from_json_str::<Value>(&vector.text) {
            Ok(TaskResult::Err(error)) => assert_eq!(
                error,
                task_error_of(&vector.name, notation, vector.stored_data.as_ref()),
                "{}",
                vector.name
            ),
            Ok(TaskResult::Ok(_)) => panic!("{}: read a value for an error", vector.name),
            Err(error) => panic!("{}: refused: {error}", vector.name),
        }
    }
}

#[test]
fn each_refused_text_is_refused_by_the_reads() {
    for refused in form().refused_texts {
        let name = refused.name.as_str();
        assert!(
            task_result_from_json_str::<i64>(&refused.text).is_err(),
            "{name}: the typed read took it"
        );
        let shape_read = read_task_result_text(&refused.text);
        match refused.refusal.as_str() {
            "envelope" => assert!(shape_read.is_err(), "{name}: the envelope read took it"),
            "ok value" => assert!(
                matches!(shape_read, Ok(TaskResultText::Ok(_))),
                "{name}: the envelope read keeps the ok slot as text"
            ),
            other => panic!("{name}: unknown refusal {other}"),
        }
    }
}

/// The text of an `args_from` entry around `envelope`, as the engine stores
/// it in a node input (Python `core/workflows/engine.py:891-896`).
fn args_from_entry(envelope: &str) -> String {
    format!(
        r#"{{"__h_taskresult_envelope__":true,"source_task_name":"produce","source_definition_key":null,"inner":{envelope}}}"#
    )
}

/// The child host gives a dependent task each envelope as stored, as an
/// `args_from` member and as a result of the workflow context
/// (`build_task_input`): an integer outside the `i64` and `u64` range
/// (`ok_u128_max`, `ok_i128_below_i64`) reaches the task as Python reads it.
#[test]
fn the_child_host_passes_each_envelope_as_stored() {
    let workflow_id = Uuid::nil();
    for vector in form().vectors {
        let envelope = vector.text.as_str();
        let input = format!(
            r#"{{"dep":{},"__h_workflow_ctx__":{{"workflow_id":"{workflow_id}","task_index":1,"task_name":"consume","results_by_id":{{"source":{envelope}}},"task_name_by_id":{{"source":"produce"}}}}}}"#,
            args_from_entry(envelope),
        );

        let built = build_task_input(&input, true)
            .unwrap_or_else(|refused| panic!("{}: the child host refused: {refused}", vector.name));

        assert_eq!(
            String::from_utf8(built).expect("the task input is UTF-8"),
            format!(
                r#"{{"dep":{envelope},"workflow_ctx":{{"workflow_id":"{workflow_id}","task_index":1,"task_name":"consume","results_by_id":{{"source":{envelope}}},"summaries_by_id":{{}}}}}}"#
            ),
            "{}",
            vector.name
        );
    }
}

/// The sub-workflow child build gives the child's builder each envelope as
/// stored, as an `args_from` member (`merged_child_input`).
#[test]
fn the_child_build_passes_each_envelope_as_stored() {
    for vector in form().vectors {
        let request = ChildSpecRequest {
            parent_workflow_id: Uuid::nil(),
            parent_workflow_name: "parent".to_owned(),
            task_index: 1,
            child_depth: 1,
            definition_key: Some("app.child".to_owned()),
            params: SubWorkflowParams::Given,
            node_input: "{}".to_owned(),
            args_from: Some(r#"{"dep": 0}"#.to_owned()),
            sources: vec![ChildStartSource {
                task_index: 0,
                node_id: Some("source".to_owned()),
                status: "COMPLETED".to_owned(),
                task_name: "produce".to_owned(),
                definition_key: None,
                result: Some(vector.text.clone()),
            }],
        };

        match merged_child_input(&request) {
            Ok(NodeInput::Input(input)) => assert_eq!(
                input.text(),
                format!(r#"{{"dep":{}}}"#, vector.text),
                "{}",
                vector.name
            ),
            Ok(NodeInput::NodeFails(error)) => {
                panic!("{}: the child build failed the node: {error}", vector.name)
            }
            Err(error) => panic!("{}: the child build failed: {error}", vector.name),
        }
    }
}
