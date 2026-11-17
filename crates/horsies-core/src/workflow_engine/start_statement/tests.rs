//! Tests of the start arguments (pure) and of the one-statement start
//! (database, needs `horsies_workflow_start`).

use std::collections::HashMap;
use std::sync::Arc;

use serial_test::serial;

use super::*;
use crate::core::config::payload::PayloadPolicy;
use crate::core::registry::workflow::RegisteredWorkflowSpec;
use crate::core::workflow::node::{JoinType, SubWorkflowParams};
use crate::core::workflow::spec::WorkflowSpecBuilder;
use crate::core::workflow::sub_workflow::SubWorkflowNode;
use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::workflow_engine::start::start_workflow;
use crate::TaskNode;

const CHILD_KEY: &str = "tests.start_statement.child.v1";

/// The start arguments of a spec of the Rust client. The registry form
/// awaits nothing, so the future ends on its first poll.
fn arguments_of(
    spec: &WorkflowSpec,
    workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<StartArguments, WorkflowError> {
    futures::executor::block_on(start_arguments(spec, workflow_id, registry, retention))
}

fn task_node(index: usize, dependencies: Vec<usize>) -> AnyNode {
    AnyNode {
        task_name: format!("task_{index}"),
        input_json: "{}".to_owned(),
        dependencies,
        args_from: HashMap::new(),
        workflow_ctx_from: None,
        workflow_ctx_from_refs: None,
        queue: Some("default".to_owned()),
        priority: Some(100),
        allow_failed_deps: false,
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

/// A registry with one static child of one task node under [`CHILD_KEY`]. The
/// node has its queue and priority resolved and an object input, as the
/// start and the task row that SQL mints require.
fn registry_with_child() -> WorkflowSpecRegistry {
    let mut builder = WorkflowSpecBuilder::new("start_statement_child");
    builder.definition_key(CHILD_KEY);
    let node = builder.task(
        TaskNode::<String>::raw("child_task", "{}")
            .queue("default")
            .priority(100),
    );
    builder.output(node);
    let mut registry =
        WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    registry
        .register(RegisteredWorkflowSpec {
            spec: builder.build().expect("child spec"),
            spec_builder: None,
        })
        .expect("register the child");
    registry
}

/// A sub-workflow root at `index` whose child is registered under
/// [`CHILD_KEY`].
fn subworkflow_root(index: usize) -> AnyNode {
    SubWorkflowNode::<(), String>::typed(
        "start_statement_child",
        "null",
        SubWorkflowParams::NotGiven,
    )
    .definition_key(CHILD_KEY)
    .into_any_node(index)
}

#[test]
fn a_task_chain_gives_one_workflow_row_and_one_row_per_node() {
    let workflow_id = Uuid::new_v4();
    let mut dependent = task_node(1, vec![0]);
    dependent.args_from = HashMap::from([("value".to_owned(), 0)]);
    let spec = spec_of("chain", vec![task_node(0, vec![]), dependent]);

    let arguments = arguments_of(
        &spec,
        workflow_id,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &RetentionConfig::default(),
    )
    .expect("arguments");

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
}

/// A node defined with an empty `workflow_ctx_from` takes no context: its row stores NULL, as for
/// no list (Python `_graph_columns`, `python/horsies/core/workflows/child_build.py:479-485`). The task
/// input of the baseline mint adds the context member for any stored array
/// (`horsies_workflow_task_input`).
#[test]
fn an_empty_context_list_is_stored_as_null() {
    let workflow_id = Uuid::new_v4();
    let mut empty = task_node(1, vec![0]);
    empty.workflow_ctx_from = Some(Vec::new());
    let mut named = task_node(2, vec![0]);
    named.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    let spec = spec_of("ctx_lists", vec![task_node(0, vec![]), empty, named]);

    let arguments = arguments_of(
        &spec,
        workflow_id,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &RetentionConfig::default(),
    )
    .expect("arguments");

    assert_eq!(
        arguments.node_workflow_ctx_from,
        vec![None, None, Some(r#"["node_0"]"#.to_owned())]
    );
}

#[test]
fn a_subworkflow_root_gets_a_prebuilt_child_after_its_parent() {
    let workflow_id = Uuid::new_v4();
    let spec = spec_of("parent", vec![subworkflow_root(0)]);

    let arguments = arguments_of(
        &spec,
        workflow_id,
        &registry_with_child(),
        &RetentionConfig::default(),
    )
    .expect("arguments");

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

/// A sub-workflow root whose child has no registration under its key gets
/// no pre-built child: a failed-node record carries its error envelope
/// (`SUBWORKFLOW_LOAD_FAILED`, Python's message with the workflow name and
/// the definition key in `data`, `core/workflows/engine.py:577-581`), and the
/// start runs in a transaction.
#[test]
fn a_subworkflow_root_whose_child_is_not_registered_gets_a_failed_record() {
    let workflow_id = Uuid::new_v4();
    let spec = spec_of("orphan_parent", vec![subworkflow_root(0)]);

    let arguments = arguments_of(
        &spec,
        workflow_id,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &RetentionConfig::default(),
    )
    .expect("arguments");

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
        error.data,
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
    let mut registry =
        WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let mut builder = WorkflowSpecBuilder::new("start_statement_child");
    builder.definition_key(CHILD_KEY);
    let node = builder.task(
        TaskNode::<String>::raw("child_task", "{}")
            .queue("default")
            .priority(100),
    );
    builder.output(node);
    let failing_builder: crate::core::SpecBuilderFn = Arc::new(
        |_input: &str| -> Result<WorkflowSpec, crate::core::SpecBuildError> {
            Err(crate::core::SpecBuildError::Failed(
                crate::core::HorsiesError::new("the builder refuses"),
            ))
        },
    );
    registry
        .register(RegisteredWorkflowSpec {
            spec: builder.build().expect("child spec"),
            spec_builder: Some(failing_builder),
        })
        .expect("register the child");
    let spec = spec_of("failing_builder_parent", vec![subworkflow_root(0)]);

    let arguments = arguments_of(&spec, workflow_id, &registry, &RetentionConfig::default())
        .expect("arguments");

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
        error.data,
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 0}))
    );
}

#[test]
fn a_task_node_without_a_resolved_queue_refuses_the_start() {
    let mut unresolved = task_node(0, vec![]);
    unresolved.queue = None;
    let spec = spec_of("unresolved", vec![unresolved]);

    let refused = arguments_of(
        &spec,
        Uuid::new_v4(),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &RetentionConfig::default(),
    );

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
fn client_spec(name: &str, nodes: serde_json::Value, classes: serde_json::Value) -> ClientSpec {
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
fn client_node(index: u32, body: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "index": index,
        "node_id": format!("node_{index}"),
        "task_name": format!("node_{index}"),
        "input_json": "{}",
        "dependencies": [],
        "args_from": {},
        "workflow_ctx_from": [],
        "allow_failed_deps": false,
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
                serde_json::json!([client_node(0, task_body("bulk"))]),
                serde_json::json!({"bulk": "forever"}),
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
        serde_json::json!([
            client_node(0, sub_body(CLIENT_CHILD_KEY)),
            client_node(1, sub_body(CLIENT_MISSING_KEY)),
            client_node(2, task_body("default")),
        ]),
        serde_json::json!({"default": "standard_7d"}),
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
        serde_json::json!([client_node(0, sub_body("tests.start_statement.unknown.v1"))]),
        serde_json::json!({}),
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

/// The one statement writes the parent, its pre-built child linked from the
/// RUNNING sub-workflow node, and the child's root task row (ENQUEUED node,
/// PENDING task).
#[tokio::test]
#[serial]
async fn the_start_writes_the_tree_in_one_statement() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let workflow_id = Uuid::new_v4();
    let spec = spec_of("one_statement_parent", vec![subworkflow_root(0)]);

    let _handle: WorkflowHandle<serde_json::Value> = start_workflow(
        &broker,
        &spec,
        Some(workflow_id),
        &registry_with_child(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
    .expect("start");

    let (node_status, child_id): (String, Option<Uuid>) = sqlx::query_as(
        "SELECT status, sub_workflow_id FROM horsies_workflow_tasks
         WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("parent node");
    assert_eq!(node_status, "RUNNING");
    let child_id = child_id.expect("the sub-workflow node links its child");
    let (child_status, parent_id, depth): (String, Option<Uuid>, i32) = sqlx::query_as(
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
    let (child_node_status, task_status): (String, String) = sqlx::query_as(
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
