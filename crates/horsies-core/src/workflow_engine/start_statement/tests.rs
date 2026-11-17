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

    let arguments = start_arguments(
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
/// no list (Python `_graph_columns`, `horsies/core/workflows/child_build.py:479-485`). The task
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

    let arguments = start_arguments(
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

    let arguments = start_arguments(
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

    let arguments = start_arguments(
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

    let arguments = start_arguments(&spec, workflow_id, &registry, &RetentionConfig::default())
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

    let refused = start_arguments(
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
