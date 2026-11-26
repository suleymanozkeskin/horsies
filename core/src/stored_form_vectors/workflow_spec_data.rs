//! The workflow spec data vectors (`workflow_spec_data.json`) through the
//! core. A client writes the text; the core reads it
//! (`parse_workflow_spec_data`) into the spec whose rows it stores. Each
//! Python text reads as its document, and each refused text is refused at
//! its field path.

use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;
use serde_json::Value;

use super::vector_file;
use crate::core::{JoinType, OnError, SubWorkflowParameters};
use crate::workflow_engine::spec_data::parse_workflow_spec_data;

#[derive(Deserialize)]
struct SpecDataForm {
    vectors: Vec<SpecVector>,
    refused_texts: Vec<RefusedText>,
}

#[derive(Deserialize)]
struct SpecVector {
    name: String,
    document: Value,
    text: String,
}

#[derive(Deserialize)]
struct RefusedText {
    name: String,
    text: String,
    core_field_path: String,
}

fn form() -> SpecDataForm {
    serde_json::from_str(&vector_file("workflow_spec_data.json"))
        .expect("workflow_spec_data.json parses")
}

fn index(value: &Value) -> usize {
    value
        .as_u64()
        .and_then(|number| usize::try_from(number).ok())
        .unwrap_or_else(|| panic!("{value} is not an index"))
}

fn indexes(value: &Value) -> Vec<usize> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{value} is not a list"))
        .iter()
        .map(index)
        .collect()
}

fn texts(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{value} is not a list"))
        .iter()
        .map(|item| item.as_str().expect("a text").to_owned())
        .collect()
}

#[test]
fn each_python_text_reads_as_its_document() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let document = &vector.document;
        let client = parse_workflow_spec_data(&vector.text)
            .unwrap_or_else(|refused| panic!("{name}: the core refused Python's text: {refused}"));
        let spec = client.spec();
        assert_eq!(
            spec.name,
            document["name"].as_str().expect("a name"),
            "{name}"
        );
        assert_eq!(
            spec.definition_key.as_deref(),
            document["definition_key"]["key"].as_str(),
            "{name}"
        );
        let on_error = match document["on_error"].as_str() {
            Some("FAIL") => OnError::Fail,
            Some("PAUSE") => OnError::Pause,
            other => panic!("{name}: on_error {other:?}"),
        };
        assert_eq!(spec.on_error, on_error, "{name}");
        let output = match document["output"]["kind"].as_str() {
            Some("NODE") => Some(index(&document["output"]["index"])),
            Some("NO_OUTPUT") => None,
            other => panic!("{name}: output {other:?}"),
        };
        assert_eq!(spec.output_index, output, "{name}");
        match document["success_policy"]["kind"].as_str() {
            Some("NO_POLICY") => assert!(spec.success_policy.is_none(), "{name}"),
            Some("CASES") => {
                let policy = spec.success_policy.as_ref().expect("a policy");
                let required: Vec<Vec<usize>> = document["success_policy"]["cases"]
                    .as_array()
                    .expect("cases")
                    .iter()
                    .map(|case| indexes(&case["required"]))
                    .collect();
                let read: Vec<Vec<usize>> = policy
                    .cases
                    .iter()
                    .map(|case| case.required_indices.clone())
                    .collect();
                assert_eq!(read, required, "{name}");
                assert_eq!(
                    policy.optional_indices.clone().unwrap_or_default(),
                    indexes(&document["success_policy"]["optional"]),
                    "{name}"
                );
            }
            other => panic!("{name}: success_policy {other:?}"),
        }
        let classes: BTreeMap<String, String> =
            serde_json::from_value(document["queue_classes"].clone()).expect("queue classes");
        assert_eq!(client.queue_classes, classes, "{name}");
        let nodes = document["nodes"].as_array().expect("nodes");
        assert_eq!(spec.tasks.len(), nodes.len(), "{name}");
        for (node, task) in nodes.iter().zip(&spec.tasks) {
            let at = format!("{name} node {}", task.index);
            assert_eq!(task.index, index(&node["index"]), "{at}");
            assert_eq!(task.node_id.as_deref(), node["node_id"].as_str(), "{at}");
            assert_eq!(
                task.task_name,
                node["task_name"].as_str().expect("a name"),
                "{at}"
            );
            assert_eq!(
                task.input_json,
                node["input_json"].as_str().expect("an input"),
                "{at}"
            );
            assert_eq!(task.dependencies, indexes(&node["dependencies"]), "{at}");
            let args_from: HashMap<String, usize> = node["args_from"]
                .as_object()
                .expect("args_from")
                .iter()
                .map(|(member, source)| (member.clone(), index(source)))
                .collect();
            assert_eq!(task.args_from, args_from, "{at}");
            assert_eq!(
                task.workflow_ctx_from.clone().unwrap_or_default(),
                texts(&node["workflow_ctx_from"]),
                "{at}"
            );
            assert_eq!(
                task.allow_failed_deps,
                node["allow_failed_deps"].as_bool().expect("a bool"),
                "{at}"
            );
            let join = &node["join"];
            let (kind, min_success) = match join["kind"].as_str() {
                Some("ALL") => (JoinType::All, None),
                Some("ANY") => (JoinType::Any, None),
                Some("QUORUM") => (
                    JoinType::Quorum,
                    Some(i32::try_from(index(&join["min_success"])).expect("an i32")),
                ),
                other => panic!("{at}: join {other:?}"),
            };
            assert_eq!((task.join, task.min_success), (kind, min_success), "{at}");
            let body = &node["body"];
            match body["kind"].as_str() {
                Some("TASK") => {
                    assert!(!task.is_subworkflow, "{at}");
                    assert_eq!(task.queue.as_deref(), body["queue"].as_str(), "{at}");
                    assert_eq!(
                        task.priority.map(i64::from),
                        body["priority"].as_i64(),
                        "{at}"
                    );
                    assert_eq!(
                        task.task_options_json.as_deref(),
                        body["task_options"]["json"].as_str(),
                        "{at}"
                    );
                }
                Some("SUB_WORKFLOW") => {
                    assert!(task.is_subworkflow, "{at}");
                    assert_eq!(
                        task.sub_definition_key.as_deref(),
                        body["definition_key"].as_str(),
                        "{at}"
                    );
                    let params = match body["params"].as_str() {
                        Some("GIVEN") => SubWorkflowParameters::Given,
                        Some("NOT_GIVEN") => SubWorkflowParameters::NotGiven,
                        other => panic!("{at}: params {other:?}"),
                    };
                    assert_eq!(task.sub_workflow_params, params, "{at}");
                }
                other => panic!("{at}: body {other:?}"),
            }
        }
    }
}

#[test]
fn each_refused_text_is_refused_at_its_field_path() {
    for refused in form().refused_texts {
        match parse_workflow_spec_data(&refused.text) {
            Err(refusal) => assert_eq!(
                refusal.field_path, refused.core_field_path,
                "{}: {}",
                refused.name, refusal.cause
            ),
            Ok(_) => panic!("{}: the core read a refused text", refused.name),
        }
    }
}
