//! Tests of the parse of workflow spec data.

use serde_json::{json, Value};

use super::*;

/// A spec of a task node, a sub-workflow node that takes the task's result
/// as `report`, and a quorum task node after both.
fn sample() -> Value {
    json!({
        "data_version": 1,
        "name": "nightly",
        "definition_key": {"kind": "KEYED", "key": "tests.nightly.v1"},
        "on_error": "PAUSE",
        "output": {"kind": "NODE", "index": 2},
        "success_policy": {
            "kind": "CASES",
            "cases": [{"required": [0, 2]}],
            "optional": [1],
        },
        "queue_classes": {"default": "standard_30d", "bulk": "forever"},
        "nodes": [
            {
                "index": 0,
                "node_id": "fetch",
                "task_name": "fetch_rows",
                "input_json": "{\"day\":\"2026-11-17\"}",
                "dependencies": [],
                "args_from": {},
                "workflow_ctx_from": [],
                "allow_failed_deps": false,
                "join": {"kind": "ALL"},
                "body": {
                    "kind": "TASK",
                    "queue": "default",
                    "priority": 10,
                    "task_options": {"kind": "ENCODED", "json": "{\"good_until\":\"2026-11-18T00:00:00+00:00\"}"},
                },
            },
            {
                "index": 1,
                "node_id": "report",
                "task_name": "report_workflow",
                "input_json": "{}",
                "dependencies": [0],
                "args_from": {"rows": 0},
                "workflow_ctx_from": [],
                "allow_failed_deps": false,
                "join": {"kind": "ALL"},
                "body": {
                    "kind": "SUB_WORKFLOW",
                    "definition_key": "tests.report.v1",
                    "params": "NOT_GIVEN",
                },
            },
            {
                "index": 2,
                "node_id": "publish",
                "task_name": "publish",
                "input_json": "{}",
                "dependencies": [0, 1],
                "args_from": {},
                "workflow_ctx_from": ["fetch"],
                "allow_failed_deps": true,
                "join": {"kind": "QUORUM", "min_success": 1},
                "body": {
                    "kind": "TASK",
                    "queue": "bulk",
                    "priority": 100,
                    "task_options": {"kind": "NO_OPTIONS"},
                },
            },
        ],
    })
}

fn parsed(document: &Value) -> Result<ClientSpec, SpecDataRefused> {
    parse_workflow_spec_data(&document.to_string())
}

fn refusal(document: &Value) -> SpecDataRefused {
    match parsed(document) {
        Ok(spec) => panic!("the spec data was accepted: {:?}", spec.spec().name),
        Err(refused) => refused,
    }
}

#[test]
fn a_spec_reads_into_the_core_spec_and_its_queue_classes() {
    let client = parsed(&sample()).expect("the sample parses");
    let spec = client.spec();
    assert_eq!(spec.name, "nightly");
    assert_eq!(spec.definition_key.as_deref(), Some("tests.nightly.v1"));
    assert_eq!(spec.on_error, OnError::Pause);
    assert_eq!(spec.output_index, Some(2));
    assert_eq!(
        spec.success_policy,
        Some(SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0, 2],
            }],
            optional_indices: Some(vec![1]),
        })
    );
    assert_eq!(
        client.queue_classes,
        BTreeMap::from([
            ("bulk".to_owned(), "forever".to_owned()),
            ("default".to_owned(), "standard_30d".to_owned()),
        ])
    );

    let fetch = &spec.tasks[0];
    assert_eq!(fetch.queue.as_deref(), Some("default"));
    assert_eq!(fetch.priority, Some(10));
    assert_eq!(
        fetch.task_options_json.as_deref(),
        Some("{\"good_until\":\"2026-11-18T00:00:00+00:00\"}")
    );
    assert_eq!(fetch.good_until, None);
    assert_eq!(fetch.workflow_ctx_from, None);
    assert!(!fetch.is_subworkflow);

    let report = &spec.tasks[1];
    assert!(report.is_subworkflow);
    assert_eq!(report.queue, None);
    assert_eq!(report.priority, None);
    assert_eq!(
        report.sub_definition_key.as_deref(),
        Some("tests.report.v1")
    );
    assert_eq!(report.sub_workflow_params, SubWorkflowParams::NotGiven);
    assert_eq!(report.dependencies, vec![0]);
    assert_eq!(report.args_from, HashMap::from([("rows".to_owned(), 0)]));

    let publish = &spec.tasks[2];
    assert_eq!(publish.join, JoinType::Quorum);
    assert_eq!(publish.min_success, Some(1));
    assert!(publish.allow_failed_deps);
    assert_eq!(publish.workflow_ctx_from, Some(vec!["fetch".to_owned()]));
    assert_eq!(publish.task_options_json, None);
    assert_eq!(publish.node_id.as_deref(), Some("publish"));
}

#[test]
fn an_unkeyed_spec_with_no_output_and_no_policy_reads_as_none() {
    let mut document = sample();
    document["definition_key"] = json!({"kind": "UNKEYED"});
    document["output"] = json!({"kind": "NO_OUTPUT"});
    document["success_policy"] =
        json!({"kind": "CASES", "cases": [{"required": [2]}], "optional": []});
    let client = parsed(&document).expect("the spec parses");
    assert_eq!(client.spec().definition_key, None);
    assert_eq!(client.spec().output_index, None);
    assert_eq!(
        client
            .spec()
            .success_policy
            .as_ref()
            .and_then(|policy| policy.optional_indices.clone()),
        None
    );
    document["success_policy"] = json!({"kind": "NO_POLICY"});
    assert_eq!(
        parsed(&document).expect("parses").spec().success_policy,
        None
    );
}

#[test]
fn an_unknown_field_is_refused_with_its_path() {
    let mut document = sample();
    document["nodes"][1]["queue"] = json!("default");
    let refused = refusal(&document);
    assert_eq!(refused.field_path, "nodes[1].queue");
}

#[test]
fn another_data_version_is_refused() {
    let mut document = sample();
    document["data_version"] = json!(2);
    assert_eq!(refusal(&document).field_path, "data_version");
}

#[test]
fn a_node_away_from_its_index_is_refused() {
    let mut document = sample();
    document["nodes"][1]["index"] = json!(2);
    let refused = refusal(&document);
    assert_eq!(refused.field_path, "nodes[1].index");
    assert_eq!(refused.cause, "a node is not at its index");
}

#[test]
fn an_index_that_names_no_node_is_refused_with_its_path() {
    for (path, field, value) in [
        ("nodes[2].dependencies[1]", "dependencies", json!([0, 3])),
        ("nodes[2].args_from.late", "args_from", json!({"late": 7})),
    ] {
        let mut document = sample();
        document["nodes"][2][field] = value;
        assert_eq!(refusal(&document).field_path, path);
    }
    let mut document = sample();
    document["output"] = json!({"kind": "NODE", "index": 3});
    assert_eq!(refusal(&document).field_path, "output.index");
    let mut document = sample();
    document["success_policy"]["optional"] = json!([9]);
    assert_eq!(refusal(&document).field_path, "success_policy.optional[0]");
}

#[test]
fn a_task_queue_without_a_class_and_a_class_of_no_task_queue_are_refused() {
    let mut missing = sample();
    missing["queue_classes"] = json!({"default": "standard_30d"});
    let refused = refusal(&missing);
    assert_eq!(refused.field_path, "queue_classes");
    assert!(refused.cause.contains("\"bulk\""), "{}", refused.cause);

    let mut extra = sample();
    extra["queue_classes"]["idle"] = json!("forever");
    assert_eq!(refusal(&extra).field_path, "queue_classes.idle");
}

#[test]
fn a_value_outside_its_bounds_is_refused() {
    // A field of a tagged variant is read from a buffered copy, so its path
    // ends at the variant.
    let mut priority = sample();
    priority["nodes"][0]["body"]["priority"] = json!(0);
    let refused = refusal(&priority);
    assert!(
        refused.field_path.starts_with("nodes[0].body"),
        "{}",
        refused.field_path
    );

    let mut quorum = sample();
    quorum["nodes"][2]["join"]["min_success"] = json!(0);
    let refused = refusal(&quorum);
    assert!(
        refused.field_path.starts_with("nodes[2].join"),
        "{}",
        refused.field_path
    );

    let mut class = sample();
    class["queue_classes"]["default"] = json!("standard_0d");
    assert_eq!(refusal(&class).field_path, "queue_classes.default");
}
