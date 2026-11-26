//! Tests of the start arguments (pure) and of the one-statement start
//! (database, needs `horsies_workflow_start`).

use std::collections::HashMap;
use std::sync::Arc;

use super::*;
use crate::broker::workflow_start::ClientStartOutcome;
use crate::core::workflow::node::{JoinType, SubWorkflowParams};
use crate::core::{SuccessCase, SuccessPolicy};
use crate::test_support::child_specs::{TestBuildRefused, TestChildBuild, TestChildSpecs};
use crate::test_support::sql;

const CHILD_KEY: &str = "tests.start_statement.child.v1";
/// The key of a child whose one root is a sub-workflow node of [`CHILD_KEY`].
const NESTING_KEY: &str = "tests.start_statement.nesting.v1";

/// The start arguments of `spec` as a client sends it, with the children of
/// its sub-workflow roots from `children`. The test source answers when its
/// future is made, so the future ends on its first poll.
fn arguments_of(
    spec: &WorkflowSpec,
    workflow_id: Uuid,
    children: &TestChildSpecs,
) -> Result<StartArguments, WorkflowError> {
    futures::executor::block_on(client_start_arguments(
        &children.client_spec(spec),
        workflow_id,
        children,
    ))
}

fn task_node(index: usize, dependencies: Vec<usize>) -> AnyNode {
    AnyNode {
        task_name: format!("task_{index}"),
        input_json: "{}".to_owned(),
        dependencies,
        args_from: HashMap::new(),
        workflow_context_from: None,
        queue: Some("default".to_owned()),
        priority: Some(100),
        allow_failed_dependencies: false,
        join: JoinType::All,
        min_success: None,
        good_until: None,
        index,
        node_id: Some(format!("node_{index}")),
        task_options_json: None,
        is_subworkflow: false,
        sub_definition_key: None,
        sub_workflow_params: SubWorkflowParams::NotGiven,
    }
}

fn spec_of(name: &str, tasks: Vec<AnyNode>) -> WorkflowSpec {
    WorkflowSpec {
        name: name.to_owned(),
        definition_key: Some(format!("tests.start_statement.{name}.v1")),
        tasks,
        on_error: OnError::Fail,
        output_index: None,
        success_policy: None,
    }
}

/// The child under [`CHILD_KEY`]: one task node, its output. The node has
/// its queue and priority resolved and an object input, as the start and the
/// task row that SQL mints require.
fn child_spec() -> WorkflowSpec {
    let mut child_task = task_node(0, vec![]);
    child_task.task_name = "child_task".to_owned();
    child_task.node_id = Some("start_statement_child:0".to_owned());
    WorkflowSpec {
        name: "start_statement_child".to_owned(),
        definition_key: Some(CHILD_KEY.to_owned()),
        tasks: vec![child_task],
        on_error: OnError::Fail,
        output_index: Some(0),
        success_policy: None,
    }
}

/// The child specs with [`child_spec`] under [`CHILD_KEY`].
fn children_with_child() -> TestChildSpecs {
    TestChildSpecs::empty().with_spec(child_spec())
}

/// A sub-workflow root at `index` whose child is the one under
/// [`CHILD_KEY`], with no params: its stored input is `{}`, as Python and
/// the Rust client store a node that gives no params. Python refuses a
/// stored `null` (`Corrupt kwargs JSON for subworkflow task <index>`,
/// [`merged_child_input`]).
fn subworkflow_root(index: usize) -> AnyNode {
    AnyNode {
        task_name: "start_statement_child".to_owned(),
        input_json: "{}".to_owned(),
        queue: None,
        priority: None,
        node_id: None,
        is_subworkflow: true,
        sub_definition_key: Some(CHILD_KEY.to_owned()),
        ..task_node(index, vec![])
    }
}

#[test]
fn a_task_chain_gives_one_workflow_row_and_one_row_per_node() {
    let workflow_id = Uuid::new_v4();
    let mut dependent = task_node(1, vec![0]);
    dependent.args_from = HashMap::from([("value".to_owned(), 0)]);
    let spec = spec_of("chain", vec![task_node(0, vec![]), dependent]);

    let arguments = arguments_of(&spec, workflow_id, &TestChildSpecs::empty()).expect("arguments");

    assert_eq!(arguments.workflow_ids, vec![workflow_id]);
    assert_eq!(arguments.on_errors, vec!["fail".to_owned()]);
    assert_eq!(arguments.parent_workflow_ids, vec![None]);
    assert_eq!(arguments.parent_task_indexes, vec![None]);
    assert_eq!(arguments.depths, vec![0]);
    assert_eq!(arguments.node_workflow_ids, vec![workflow_id, workflow_id]);
    assert_eq!(arguments.node_task_indexes, vec![0, 1]);
    assert_eq!(
        arguments.node_dependencies,
        vec!["[]".to_owned(), "[0]".to_owned()]
    );
    assert_eq!(
        arguments.node_args_from,
        vec![None, Some(r#"{"value":0}"#.to_owned())]
    );
    assert_eq!(arguments.node_is_subworkflow, vec![false, false]);
    assert_eq!(arguments.node_sub_workflow_params, vec![None, None]);
    // Task nodes carry their typed facts.
    assert!(arguments.node_max_retries.iter().all(Option::is_some));
    assert!(arguments.node_executions.iter().all(Option::is_some));
    assert!(arguments
        .node_retention_class_keys
        .iter()
        .all(Option::is_some));
    // No sub-workflow root: no failed-node record.
    assert!(!arguments.has_failed_records());
    assert!(arguments.failed_workflow_ids.is_empty());
}

/// A node defined with an empty `workflow_context_from` takes no context: its row stores NULL,
/// as for no list (Python `_graph_columns`,
/// `clients/python/horsies/core/workflows/child_build.py:479-485`). The task input of the
/// baseline mint adds the context member for any stored array (`horsies_workflow_task_input`).
#[test]
fn an_empty_context_list_is_stored_as_null() {
    let workflow_id = Uuid::new_v4();
    let mut empty = task_node(1, vec![0]);
    empty.workflow_context_from = Some(Vec::new());
    let mut named = task_node(2, vec![0]);
    named.workflow_context_from = Some(vec!["node_0".to_owned()]);
    let spec = spec_of("ctx_lists", vec![task_node(0, vec![]), empty, named]);

    let arguments = arguments_of(&spec, workflow_id, &TestChildSpecs::empty()).expect("arguments");

    assert_eq!(
        arguments.node_workflow_context_from,
        vec![None, None, Some(r#"["node_0"]"#.to_owned())]
    );
}

/// The `args_from` text of a node is in key order, whatever order the map
/// took its members in: the start command fingerprint takes the text byte
/// for byte, so two equal starts must store one text.
#[test]
fn the_args_from_text_is_in_key_order() {
    let keys: Vec<String> = (0..16).map(|key| format!("key_{key:02}")).collect();
    let mut forward = task_node(1, vec![0]);
    for key in &keys {
        let _absent = forward.args_from.insert(key.clone(), 0);
    }
    let mut reverse = task_node(1, vec![0]);
    for key in keys.iter().rev() {
        let _absent = reverse.args_from.insert(key.clone(), 0);
    }
    let expected = format!(
        "{{{}}}",
        keys.iter()
            .map(|key| format!(r#""{key}":0"#))
            .collect::<Vec<_>>()
            .join(",")
    );

    for node in [forward, reverse] {
        let workflow_id = Uuid::new_v4();
        let spec = spec_of("args_from_order", vec![task_node(0, vec![]), node]);
        let arguments =
            arguments_of(&spec, workflow_id, &TestChildSpecs::empty()).expect("arguments");
        assert_eq!(
            arguments.node_args_from[1].as_deref(),
            Some(expected.as_str())
        );
    }
}

/// Two starts of one spec with `args_from` maps that took their members in
/// other orders, and with other workflow ids, have one start command
/// fingerprint: the id is not part of the command.
#[test]
fn equal_starts_have_one_fingerprint_whatever_the_args_from_order_and_the_id() {
    let fingerprint_of = |members: &[(&str, usize)]| {
        let mut dependent = task_node(2, vec![0, 1]);
        for (key, source) in members {
            let _absent = dependent.args_from.insert((*key).to_owned(), *source);
        }
        let spec = spec_of(
            "fingerprint_order",
            vec![task_node(0, vec![]), task_node(1, vec![]), dependent],
        );
        arguments_of(&spec, Uuid::new_v4(), &TestChildSpecs::empty())
            .expect("arguments")
            .top_start_command()
            .fingerprint()
    };

    assert_eq!(
        fingerprint_of(&[("left", 0), ("right", 1), ("middle", 0)]),
        fingerprint_of(&[("middle", 0), ("right", 1), ("left", 0)]),
    );
    assert_ne!(
        fingerprint_of(&[("left", 0), ("right", 1)]),
        fingerprint_of(&[("left", 1), ("right", 0)]),
    );
}

/// The start command holds the top row and its own nodes, in node index
/// order, with their stored values. The pre-built child and its node are not
/// in it.
#[test]
fn the_start_command_holds_the_top_row_and_its_nodes_only() {
    let workflow_id = Uuid::new_v4();
    let mut dependent = task_node(1, vec![0]);
    dependent.args_from = HashMap::from([("value".to_owned(), 0)]);
    dependent.workflow_context_from = Some(vec!["node_0".to_owned()]);
    let spec = spec_of(
        "command_rows",
        vec![dependent, task_node(0, vec![]), subworkflow_root(2)],
    );

    let arguments = arguments_of(&spec, workflow_id, &children_with_child()).expect("arguments");
    let command = arguments.top_start_command();

    assert_eq!(arguments.workflow_ids.len(), 2, "the top row and its child");
    assert_eq!(command.name, "command_rows");
    assert_eq!(
        command.definition_key,
        Some("tests.start_statement.command_rows.v1")
    );
    assert_eq!(command.on_error, "fail");
    let indexes: Vec<i32> = command.nodes.iter().map(|node| node.task_index).collect();
    assert_eq!(indexes, vec![0, 1, 2]);
    let dependent = &command.nodes[1];
    assert_eq!(dependent.task_name, "task_1");
    assert_eq!(dependent.dependencies, "[0]");
    assert_eq!(dependent.args_from, Some(r#"{"value":0}"#));
    assert_eq!(dependent.workflow_context_from, Some(r#"["node_0"]"#));
    assert_eq!(dependent.queue_name, "default");
    assert_eq!(dependent.priority, 100);
    assert!(dependent.retention_class_key.is_some());
    let root = &command.nodes[2];
    assert!(root.is_subworkflow);
    assert_eq!(root.sub_definition_key, Some(CHILD_KEY));
    assert_eq!(root.sub_workflow_params, Some("NOT_GIVEN"));
    assert_eq!(root.retention_class_key, None);
    assert!(command
        .nodes
        .iter()
        .all(|node| node.task_name != "child_task"));
}

#[test]
fn a_subworkflow_root_gets_a_prebuilt_child_after_its_parent() {
    let workflow_id = Uuid::new_v4();
    let spec = spec_of("parent", vec![subworkflow_root(0)]);

    let arguments = arguments_of(&spec, workflow_id, &children_with_child()).expect("arguments");

    assert_eq!(arguments.workflow_ids.len(), 2);
    assert_eq!(arguments.workflow_ids[0], workflow_id);
    let child_id = arguments.workflow_ids[1];
    assert_eq!(arguments.parent_workflow_ids, vec![None, Some(workflow_id)]);
    assert_eq!(arguments.parent_task_indexes, vec![None, Some(0)]);
    assert_eq!(arguments.depths, vec![0, 1]);
    // The parent's sub-workflow node, then the child's task node.
    assert_eq!(arguments.node_workflow_ids, vec![workflow_id, child_id]);
    assert_eq!(arguments.node_is_subworkflow, vec![true, false]);
    assert_eq!(
        arguments.node_sub_workflow_params,
        vec![Some("NOT_GIVEN".to_owned()), None]
    );
    assert_eq!(
        arguments.node_sub_definition_keys,
        vec![Some(CHILD_KEY.to_owned()), None]
    );
    // A sub-workflow node has no task facts.
    assert_eq!(arguments.node_max_retries[0], None);
    assert_eq!(arguments.node_executions[0], None);
}

/// A pre-built child whose root is a sub-workflow node gets its own
/// pre-built child: each child row names its parent workflow and the node
/// above it, one level deeper.
#[test]
fn a_prebuilt_child_with_a_subworkflow_root_gets_its_own_child() {
    let workflow_id = Uuid::new_v4();
    let nesting = WorkflowSpec {
        name: "start_statement_nesting".to_owned(),
        definition_key: Some(NESTING_KEY.to_owned()),
        tasks: vec![subworkflow_root(0)],
        on_error: OnError::Fail,
        output_index: None,
        success_policy: None,
    };
    let children = children_with_child().with_spec(nesting);
    let nesting_root = AnyNode {
        task_name: "start_statement_nesting".to_owned(),
        sub_definition_key: Some(NESTING_KEY.to_owned()),
        ..subworkflow_root(0)
    };
    let spec = spec_of("nesting_parent", vec![nesting_root]);

    let arguments = arguments_of(&spec, workflow_id, &children).expect("arguments");

    assert_eq!(arguments.workflow_ids.len(), 3);
    assert_eq!(arguments.workflow_ids[0], workflow_id);
    let child_id = arguments.workflow_ids[1];
    assert_eq!(
        arguments.parent_workflow_ids,
        vec![None, Some(workflow_id), Some(child_id)]
    );
    assert_eq!(arguments.parent_task_indexes, vec![None, Some(0), Some(0)]);
    assert_eq!(arguments.depths, vec![0, 1, 2]);
    assert!(!arguments.has_failed_records());
}

/// A sub-workflow node that waits for another node is not built at the
/// start, even when its child could not be built: no child row and no
/// failed-node record. Its node row is written as a sub-workflow node.
#[test]
fn a_dependent_subworkflow_node_is_not_built_at_the_start() {
    let workflow_id = Uuid::new_v4();
    let dependent = AnyNode {
        dependencies: vec![0],
        ..subworkflow_root(1)
    };
    let spec = spec_of(
        "dependent_sub_parent",
        vec![task_node(0, vec![]), dependent],
    );

    // No child spec under CHILD_KEY: a build would give a failed-node record.
    let arguments = arguments_of(&spec, workflow_id, &TestChildSpecs::empty()).expect("arguments");

    assert_eq!(arguments.workflow_ids, vec![workflow_id]);
    assert!(!arguments.has_failed_records());
    assert_eq!(arguments.node_workflow_ids, vec![workflow_id, workflow_id]);
    assert_eq!(arguments.node_is_subworkflow, vec![false, true]);
}

/// Each workflow array holds one element per workflow row and each node
/// array one element per node row, in the same order: the top workflow and
/// its nodes, then the pre-built child and its node. The failure policy,
/// the output index and the success policy stay with their own workflow.
#[test]
fn the_arguments_hold_one_aligned_element_per_row() {
    let workflow_id = Uuid::new_v4();
    let mut spec = spec_of(
        "aligned_parent",
        vec![
            task_node(0, vec![]),
            subworkflow_root(1),
            task_node(2, vec![0, 1]),
        ],
    );
    spec.on_error = OnError::Pause;
    spec.output_index = Some(2);
    let policy = SuccessPolicy {
        cases: vec![SuccessCase {
            required_indices: vec![2],
        }],
        optional_indices: None,
    };
    spec.success_policy = Some(policy.clone());

    let arguments = arguments_of(&spec, workflow_id, &children_with_child()).expect("arguments");

    let workflow_lengths = [
        arguments.workflow_ids.len(),
        arguments.names.len(),
        arguments.definition_keys.len(),
        arguments.on_errors.len(),
        arguments.output_task_indexes.len(),
        arguments.success_policies.len(),
        arguments.parent_workflow_ids.len(),
        arguments.parent_task_indexes.len(),
        arguments.depths.len(),
    ];
    assert_eq!(workflow_lengths, [2; 9]);
    let node_lengths = [
        arguments.node_workflow_ids.len(),
        arguments.node_task_indexes.len(),
        arguments.node_ids.len(),
        arguments.node_task_names.len(),
        arguments.node_inputs.len(),
        arguments.node_queue_names.len(),
        arguments.node_priorities.len(),
        arguments.node_dependencies.len(),
        arguments.node_args_from.len(),
        arguments.node_workflow_context_from.len(),
        arguments.node_allow_failed_dependencies.len(),
        arguments.node_join_types.len(),
        arguments.node_min_successes.len(),
        arguments.node_task_options.len(),
        arguments.node_max_retries.len(),
        arguments.node_executions.len(),
        arguments.node_good_untils.len(),
        arguments.node_retention_class_keys.len(),
        arguments.node_is_subworkflow.len(),
        arguments.node_sub_workflow_names.len(),
        arguments.node_sub_definition_keys.len(),
        arguments.node_sub_workflow_params.len(),
    ];
    assert_eq!(node_lengths, [4; 22]);
    let child_id = arguments.workflow_ids[1];
    assert_eq!(
        arguments.on_errors,
        vec!["pause".to_owned(), "fail".to_owned()]
    );
    assert_eq!(arguments.output_task_indexes, vec![Some(2), Some(0)]);
    assert_eq!(
        arguments.success_policies,
        vec![Some(policy.to_json().expect("the policy encodes")), None]
    );
    assert_eq!(arguments.depths, vec![0, 1]);
    assert_eq!(
        arguments.node_workflow_ids,
        vec![workflow_id, workflow_id, workflow_id, child_id]
    );
    assert_eq!(arguments.node_task_indexes, vec![0, 1, 2, 0]);
    assert_eq!(
        arguments.node_dependencies,
        vec![
            "[]".to_owned(),
            "[]".to_owned(),
            "[0,1]".to_owned(),
            "[]".to_owned()
        ]
    );
    assert_eq!(
        arguments.node_is_subworkflow,
        vec![false, true, false, false]
    );
    assert_eq!(
        arguments.node_sub_workflow_params,
        vec![None, Some("NOT_GIVEN".to_owned()), None, None]
    );
    assert_eq!(
        arguments
            .node_max_retries
            .iter()
            .map(Option::is_some)
            .collect::<Vec<bool>>(),
        vec![true, false, true, true]
    );
    assert!(!arguments.has_failed_records());
}

/// A sub-workflow root whose child has no registration under its key gets
/// no pre-built child: a failed-node record carries its error envelope
/// (`SUBWORKFLOW_LOAD_FAILED`, Python's message with the workflow name and
/// the definition key in `data`, `core/workflows/engine.py:577-581`), and the
/// start runs in a transaction.
#[test]
fn a_subworkflow_root_whose_child_is_not_registered_gets_a_failed_record() {
    let workflow_id = Uuid::new_v4();
    let spec = spec_of("orphan_parent", vec![subworkflow_root(0)]);

    let arguments = arguments_of(&spec, workflow_id, &TestChildSpecs::empty()).expect("arguments");

    assert_eq!(arguments.workflow_ids, vec![workflow_id]);
    assert!(arguments.has_failed_records());
    assert_eq!(arguments.failed_workflow_ids, vec![workflow_id]);
    assert_eq!(arguments.failed_task_indexes, vec![0]);
    let stored: crate::core::TaskResult<serde_json::Value> =
        serde_json::from_str(&arguments.failed_results[0]).expect("an error envelope");
    let crate::core::TaskResult::Err(error) = stored else {
        panic!("a failed record stores an error");
    };
    assert_eq!(error.error_code.to_string(), "SUBWORKFLOW_LOAD_FAILED");
    assert_eq!(
        error.message.as_deref(),
        Some("Failed to load subworkflow definition for orphan_parent:0")
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"definition_key": CHILD_KEY}))
    );
}

/// A sub-workflow root whose registered builder fails gets a failed-node
/// record with `WORKFLOW_ENQUEUE_FAILED`: the Python class of the builder's
/// error, its text, and the workflow by name in the message; the workflow id
/// and the node in `data` (Python `core/workflows/engine.py:1196-1211`).
#[test]
fn a_subworkflow_root_whose_builder_fails_gets_a_failed_record() {
    let workflow_id = Uuid::new_v4();
    let failing_builder: TestChildBuild =
        Arc::new(|_input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            Err(TestBuildRefused::Failed(crate::core::HorsiesError::new(
                "the builder refuses",
            )))
        });
    let children = TestChildSpecs::empty().with_build(CHILD_KEY, failing_builder);
    let spec = spec_of("failing_builder_parent", vec![subworkflow_root(0)]);

    let arguments = arguments_of(&spec, workflow_id, &children).expect("arguments");

    assert_eq!(arguments.workflow_ids, vec![workflow_id]);
    assert_eq!(arguments.failed_task_indexes, vec![0]);
    let stored: crate::core::TaskResult<serde_json::Value> =
        serde_json::from_str(&arguments.failed_results[0]).expect("an error envelope");
    let crate::core::TaskResult::Err(error) = stored else {
        panic!("a failed record stores an error");
    };
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert_eq!(
        error.message.as_deref(),
        Some(
            "Subworkflow build failed: HorsiesError: error: the builder refuses \
             (workflow failing_builder_parent:0)"
        )
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 0}))
    );
}

/// Params that the client's params type does not decode as a whole:
/// `WORKFLOW_ENQUEUE_FAILED` with Python's prefix and suffix around the
/// decode's text, cut at 200 characters, with no exception class; the
/// workflow id and the node in `data`.
#[test]
fn a_params_decode_failure_keeps_the_decode_text_without_a_class() {
    let workflow_id = Uuid::new_v4();
    let decode_text = format!(".: missing field `n` {}", "x".repeat(300));
    let error = params_decode_failed_error(workflow_id, "parent", 2, &decode_text);
    let kept: String = decode_text.chars().take(BUILD_FAILURE_TEXT_CHARS).collect();
    let expected = format!("Subworkflow build failed: {kept} (workflow parent:2)");

    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert_eq!(error.message.as_deref(), Some(expected.as_str()));
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 2}))
    );
}

#[test]
fn a_task_node_without_a_resolved_queue_refuses_the_start() {
    let mut unresolved = task_node(0, vec![]);
    unresolved.queue = None;
    let spec = spec_of("unresolved", vec![unresolved]);

    let refused = arguments_of(&spec, Uuid::new_v4(), &TestChildSpecs::empty());

    assert!(
        matches!(refused, Err(WorkflowError::Validation(_))),
        "{refused:?}"
    );
}

const CLIENT_CHILD_KEY: &str = "tests.start_statement.client_child.v1";
const CLIENT_MISSING_KEY: &str = "tests.start_statement.client_missing.v1";
/// The envelope that the scripted client stores for a child it cannot load.
const CLIENT_FAILURE_RESULT: &str =
    r#"{"err":{"error_code":"SUBWORKFLOW_LOAD_FAILED","message":"scripted"}}"#;

/// The spec data of a workflow `name` with the nodes `nodes` (JSON values)
/// and the queue classes `classes`.
fn client_spec(name: &str, nodes: &serde_json::Value, classes: &serde_json::Value) -> ClientSpec {
    let document = serde_json::json!({
        "data_version": 1,
        "name": name,
        "definition_key": {"kind": "KEYED", "key": format!("tests.start_statement.{name}.v1")},
        "on_error": "FAIL",
        "output": {"kind": "NO_OUTPUT"},
        "success_policy": {"kind": "NO_POLICY"},
        "queue_classes": classes,
        "nodes": nodes,
    });
    crate::workflow_engine::spec_data::parse_workflow_spec_data(&document.to_string())
        .expect("the spec data parses")
}

/// A root node of spec data: a task on `queue`, or a sub-workflow node of the
/// child under `definition_key`.
fn client_node(index: u32, body: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "index": index,
        "node_id": format!("node_{index}"),
        "task_name": format!("node_{index}"),
        "input_json": "{}",
        "dependencies": [],
        "args_from": {},
        "workflow_context_from": [],
        "allow_failed_dependencies": false,
        "join": {"kind": "ALL"},
        "body": body,
    })
}

fn task_body(queue: &str) -> serde_json::Value {
    serde_json::json!({
        "kind": "TASK",
        "queue": queue,
        "priority": 100,
        "task_options": {"kind": "NO_OPTIONS"},
    })
}

fn sub_body(definition_key: &str) -> serde_json::Value {
    serde_json::json!({
        "kind": "SUB_WORKFLOW",
        "definition_key": definition_key,
        "params": "NOT_GIVEN",
    })
}

/// A client that builds [`CLIENT_CHILD_KEY`] (one task on `bulk`, kept
/// forever), fails the node of [`CLIENT_MISSING_KEY`] with
/// [`CLIENT_FAILURE_RESULT`], and does not run any other build. It keeps
/// every request.
#[derive(Default)]
struct ScriptedClient {
    requests: std::sync::Mutex<Vec<ChildSpecRequest>>,
}

impl ChildSpecSource for ScriptedClient {
    fn child_spec(
        &self,
        request: ChildSpecRequest,
    ) -> crate::workflow_engine::child_spec::ChildSpecFuture<'_> {
        let answer = match request.definition_key.as_deref() {
            Some(CLIENT_CHILD_KEY) => Ok(ClientChildSpec::Built(client_spec(
                "client_child",
                &serde_json::json!([client_node(0, &task_body("bulk"))]),
                &serde_json::json!({"bulk": "forever"}),
            ))),
            Some(CLIENT_MISSING_KEY) => Ok(ClientChildSpec::NodeFails {
                message: "scripted load failure".to_owned(),
                failure_result: CLIENT_FAILURE_RESULT.to_owned(),
            }),
            _ => Err(
                crate::workflow_engine::child_spec::ChildSpecUnavailable::NotRun {
                    subject: "scripted build".to_owned(),
                    cause: "the callback queue is full".to_owned(),
                },
            ),
        };
        self.requests
            .lock()
            .expect("the request list is not poisoned")
            .push(request);
        Box::pin(async move { answer })
    }
}

/// A spec of a client: each root asks the client's source once; a built child
/// is pushed with the classes the client resolved for its queues, and a node
/// that the client fails stores the client's envelope as written.
#[test]
fn a_client_spec_takes_its_children_and_queue_classes_from_the_client() {
    let workflow_id = Uuid::new_v4();
    let spec = client_spec(
        "client_parent",
        &serde_json::json!([
            client_node(0, &sub_body(CLIENT_CHILD_KEY)),
            client_node(1, &sub_body(CLIENT_MISSING_KEY)),
            client_node(2, &task_body("default")),
        ]),
        &serde_json::json!({"default": "standard_7d"}),
    );
    let client = ScriptedClient::default();

    let arguments =
        futures::executor::block_on(client_start_arguments(&spec, workflow_id, &client))
            .expect("arguments");

    assert_eq!(arguments.workflow_ids.len(), 2);
    assert_eq!(arguments.workflow_ids[0], workflow_id);
    assert_eq!(arguments.parent_workflow_ids[1], Some(workflow_id));
    assert_eq!(arguments.parent_task_indexes[1], Some(0));
    assert_eq!(arguments.depths, vec![0, 1]);
    assert_eq!(arguments.failed_workflow_ids, vec![workflow_id]);
    assert_eq!(arguments.failed_task_indexes, vec![1]);
    assert_eq!(
        arguments.failed_results,
        vec![CLIENT_FAILURE_RESULT.to_owned()]
    );
    assert_eq!(
        arguments.node_retention_class_keys,
        vec![
            None,
            None,
            Some("standard_7d".to_owned()),
            Some("forever".to_owned()),
        ]
    );
    let requests = client
        .requests
        .lock()
        .expect("the request list is not poisoned");
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0],
        ChildSpecRequest {
            parent_workflow_id: workflow_id,
            parent_workflow_name: "client_parent".to_owned(),
            task_index: 0,
            child_depth: 1,
            definition_key: Some(CLIENT_CHILD_KEY.to_owned()),
            params: SubWorkflowParams::NotGiven,
            node_input: "{}".to_owned(),
            args_from: None,
            sources: vec![],
        }
    );
}

/// A client that does not run a build refuses the start before any
/// statement: a retryable `ChildSpecSource` error.
#[test]
fn a_build_that_the_client_did_not_run_refuses_the_start() {
    let spec = client_spec(
        "client_refused",
        &serde_json::json!([client_node(
            0,
            &sub_body("tests.start_statement.unknown.v1")
        )]),
        &serde_json::json!({}),
    );

    let refused = futures::executor::block_on(client_start_arguments(
        &spec,
        Uuid::new_v4(),
        &ScriptedClient::default(),
    ));

    match refused {
        Err(error @ WorkflowError::ChildSpecSource(_)) => assert!(error.is_retryable()),
        other => panic!("expected a ChildSpecSource error, got {other:?}"),
    }
}

/// A start with a carrier stores the checked carrier text and the trace id
/// on every workflow row of the tree (the parent and its pre-built child)
/// and on the task rows of their nodes.
#[tokio::test]
async fn a_traced_start_stores_its_carrier_on_every_row_of_the_tree() {
    use std::collections::BTreeMap;

    use crate::telemetry::trace_carrier::{PropagatedTrace, SendTrace};

    let pool = crate::test_support::migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));
    let workflow_id = Uuid::new_v4();
    let spec = spec_of(
        "traced_parent",
        vec![subworkflow_root(0), task_node(1, vec![])],
    );
    let trace_id: Vec<u8> = (1..=16).collect();
    let trace = SendTrace::Propagated(PropagatedTrace {
        carrier: BTreeMap::from([("traceparent".to_owned(), "00-0102-03-01".to_owned())]),
        trace_id: trace_id.clone(),
    });

    let children = children_with_child();
    let started = broker
        .start_client_workflow(
            &children.client_spec(&spec),
            workflow_id,
            chrono::Utc::now(),
            trace,
            false,
            &children,
        )
        .await
        .expect("start");
    assert_eq!(started, ClientStartOutcome::Started);

    let stored = (
        Some("{\"traceparent\":\"00-0102-03-01\"}".to_owned()),
        Some(trace_id),
    );
    let workflows: Vec<(Option<String>, Option<Vec<u8>>)> = sql::query_as(
        "SELECT trace_carrier, trace_id FROM horsies_workflows
         WHERE id = $1 OR parent_workflow_id = $1 ORDER BY depth",
    )
    .bind(workflow_id)
    .fetch_all(&pool)
    .await
    .expect("the workflow rows of the tree");
    assert_eq!(workflows, vec![stored.clone(), stored.clone()]);
    let tasks: Vec<(Option<String>, Option<Vec<u8>>)> = sql::query_as(
        "SELECT t.trace_carrier, t.trace_id FROM horsies_tasks t
         JOIN horsies_workflow_tasks n ON n.task_id = t.id
         JOIN horsies_workflows w ON w.id = n.workflow_id
         WHERE w.id = $1 OR w.parent_workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_all(&pool)
    .await
    .expect("the task rows of the tree");
    assert_eq!(tasks, vec![stored.clone(), stored]);
}

/// The one statement writes the parent, its pre-built child linked from the
/// RUNNING sub-workflow node, and the child's root task row (ENQUEUED node,
/// PENDING task).
#[tokio::test]
async fn the_start_writes_the_tree_in_one_statement() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));
    let workflow_id = Uuid::new_v4();
    let spec = spec_of("one_statement_parent", vec![subworkflow_root(0)]);

    let children = children_with_child();
    let started = broker
        .start_client_workflow(
            &children.client_spec(&spec),
            workflow_id,
            chrono::Utc::now(),
            crate::telemetry::trace_carrier::SendTrace::NoCarrier,
            false,
            &children,
        )
        .await
        .expect("start");
    assert_eq!(started, ClientStartOutcome::Started);

    let (node_status, child_id): (String, Option<Uuid>) = sql::query_as(
        "SELECT status, sub_workflow_id FROM horsies_workflow_tasks
         WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("parent node");
    assert_eq!(node_status, "RUNNING");
    let child_id = child_id.expect("the sub-workflow node links its child");
    let (child_status, parent_id, depth): (String, Option<Uuid>, i32) = sql::query_as(
        "SELECT status, parent_workflow_id, depth FROM horsies_workflows WHERE id = $1",
    )
    .bind(child_id)
    .fetch_one(&pool)
    .await
    .expect("child workflow");
    assert_eq!(
        (child_status.as_str(), parent_id, depth),
        ("RUNNING", Some(workflow_id), 1)
    );
    let (child_node_status, task_status): (String, String) = sql::query_as(
        "SELECT n.status, t.status FROM horsies_workflow_tasks n
         JOIN horsies_tasks t ON t.id = n.task_id
         WHERE n.workflow_id = $1 AND n.task_index = 0",
    )
    .bind(child_id)
    .fetch_one(&pool)
    .await
    .expect("child root task");
    assert_eq!(
        (child_node_status.as_str(), task_status.as_str()),
        ("ENQUEUED", "PENDING")
    );
}
