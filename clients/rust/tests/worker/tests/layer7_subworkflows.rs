//! Layer 7: sub-workflows on every read surface (Python
//! `clients/python/tests/e2e/test_layer7_subworkflows.py`). The parent specs are built in
//! the test process with the typed DSL; their children are the definitions
//! of `horsies_test_worker::tasks::subworkflow`, which every process of the
//! instance registers, so the worker builds each child from its registry.

use std::fmt::Debug;
use std::time::Duration;

use horsies::{
    CustomCode, CustomErrorCode, HorsiesError, NodeKey, NodeResult, OnError, OutputlessResults,
    SubWorkflowFailure, SubWorkflowNode, SubWorkflowParams, SuccessCase, SuccessPolicy, TaskNode,
    TaskOutcome, TypedNodeRef, WorkflowDefinition, WorkflowHandle, WorkflowSpecBuilder,
    WorkflowStatus, WorkflowTaskStatus,
};
use horsies_test_support::rows::{self, NodeRow};
use horsies_test_support::tokio_postgres::Client;
use horsies_test_worker::harness::{within, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::tasks::subworkflow::{
    self, Pet, PetParams, SubFail, SubFailedInput, SubNestedFailMiddle, SubNestedOkMiddle, SubOk,
    SubParam, SubTypedKwarg, UpstreamParams, SUB_OK_VALUE,
};
use horsies_test_worker::tasks::workflow::{self, WfCode};
use serde::de::DeserializeOwned;
use serde_json::json;
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for a workflow to end, or to pause.
const WORKFLOW_WAIT: Duration = Duration::from_secs(30);

/// The builder of Python's `_workflow_spec(name, ...)`.
fn layer7(suite: &Suite, name: &str) -> WorkflowSpecBuilder {
    let mut builder = suite.app.workflow_builder(format!("e2e_layer7_{name}"));
    let _key = builder.definition_key(format!("tests.e2e.layer7.{name}.v1"));
    builder
}

/// Add `node` to `spec`, a workflow builder of the suite's app. The app
/// registers every task of the test binary, so a refusal is a defect of the
/// test.
fn add<T: 'static, I: 'static>(
    spec: &mut WorkflowSpecBuilder,
    node: TaskNode<T, I>,
) -> TypedNodeRef<T> {
    spec.task(node)
        .expect("the app of the suite registers the task of the node")
}

/// A sub-workflow node `node_id` of `D` that gives no params.
fn child<D>(node_id: &str) -> SubWorkflowNode<(), D::Output>
where
    D: WorkflowDefinition<Params = ()>,
{
    D::node(SubWorkflowParams::NotGiven)
        .unwrap_or_else(|refused| panic!("a node of {}: {refused}", D::name()))
        .node_id(node_id)
}

async fn start<T: DeserializeOwned + 'static>(
    suite: &Suite,
    builder: WorkflowSpecBuilder,
) -> WorkflowHandle<T> {
    let spec = builder
        .build()
        .unwrap_or_else(|refused: HorsiesError| panic!("build the spec: {refused}"));
    suite
        .app
        .start::<T>(&spec)
        .await
        .unwrap_or_else(|refused| panic!("start the workflow: {refused}"))
}

/// The nodes of a workflow once it ended with `status`.
async fn ended_nodes<T>(suite: &Suite, handle: &WorkflowHandle<T>, status: &str) -> Vec<NodeRow> {
    let client = suite.client().await;
    let ended = rows::wait_for_workflow_end(&client, handle.workflow_id(), WORKFLOW_WAIT).await;
    assert_eq!(ended, status, "the workflow status");
    rows::workflow_nodes(&client, handle.workflow_id()).await
}

/// The outcome of a workflow that ended.
async fn outcome<T: DeserializeOwned + 'static>(
    handle: &WorkflowHandle<T>,
) -> TaskOutcome<T, CustomCode> {
    handle
        .get(within(WORKFLOW_WAIT))
        .await
        .expect("read the workflow")
}

/// The output of a workflow that completed.
async fn output<T: DeserializeOwned + Debug + 'static>(handle: &WorkflowHandle<T>) -> T {
    match outcome(handle).await {
        TaskOutcome::Succeeded(value) => value,
        TaskOutcome::Failed(error) => panic!("the workflow failed: {error:?}"),
    }
}

/// The child workflow of a sub-workflow node that started it.
fn child_of(node: &NodeRow) -> Uuid {
    node.sub_workflow_id
        .unwrap_or_else(|| panic!("node {} has no child workflow", node.task_index))
}

/// The sub-workflow failure of `outcome` (Python `_assert_subworkflow_error`):
/// the code `SUB_WORKFLOW_FAILED`, the child `child_id`, which FAILED with at
/// least one failed task.
fn subworkflow_failure<T: Debug, E: CustomErrorCode + Debug>(
    outcome: &TaskOutcome<T, E>,
    child_id: Uuid,
) -> &SubWorkflowFailure {
    let TaskOutcome::Failed(error) = outcome else {
        panic!("expected a sub-workflow failure, got {outcome:?}");
    };
    assert_eq!(error.code().as_str(), "SUB_WORKFLOW_FAILED");
    let failure = error
        .sub_workflow()
        .unwrap_or_else(|| panic!("the error {error:?} is not a sub-workflow failure"));
    assert_eq!(failure.sub_workflow_id, child_id);
    assert_eq!(failure.sub_workflow_summary.status, WorkflowStatus::Failed);
    assert!(
        failure.sub_workflow_summary.failed_tasks >= 1,
        "the child has a failed task: {failure:?}"
    );
    failure
}

/// The place of one workflow in its tree.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TreeRow {
    id: Uuid,
    status: String,
    parent_workflow_id: Option<Uuid>,
    parent_task_index: Option<i32>,
    depth: i32,
    root_workflow_id: Option<Uuid>,
}

/// The workflow `root` and its descendants, by depth (Python
/// `_workflow_tree_rows`).
async fn tree(client: &Client, root: Uuid) -> Vec<TreeRow> {
    client
        .query(
            "SELECT id, status::text, parent_workflow_id, parent_task_index, depth, \
             root_workflow_id FROM horsies_workflows \
             WHERE id = $1 OR root_workflow_id = $1 ORDER BY depth, id",
            &[&root],
        )
        .await
        .unwrap_or_else(|error| panic!("read the tree of workflow {root}: {error}"))
        .iter()
        .map(|row| TreeRow {
            id: row.get(0),
            status: row.get(1),
            parent_workflow_id: row.get(2),
            parent_task_index: row.get(3),
            depth: row.get(4),
            root_workflow_id: row.get(5),
        })
        .collect()
}

/// The row of `workflow_id` in `rows`.
fn row_of(rows: &[TreeRow], workflow_id: Uuid) -> &TreeRow {
    rows.iter()
        .find(|row| row.id == workflow_id)
        .unwrap_or_else(|| panic!("no row of workflow {workflow_id} in {rows:?}"))
}

// ---------------------------------------------------------------------------
// One child on every read surface
// ---------------------------------------------------------------------------

/// Python: `test_layer7_subworkflows.py::test_subworkflow_success_full_surface_matrix`.
/// A child that completed reads the same through `get`, the node, the node
/// info with its summary, `result_for` and the child's row.
#[tokio::test]
#[serial]
async fn test_subworkflow_success_full_surface_matrix() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = layer7(&suite, "success_full_surface");
    let node = spec.sub_workflow(child::<SubOk>("e2e_sub_success_child"));
    let _output = spec.output(node);
    let handle = start::<i64>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(output(&handle).await, SUB_OK_VALUE);

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].status, "COMPLETED");
    let child_id = child_of(&nodes[0]);

    let infos = handle.tasks().await.expect("the node infos");
    assert_eq!(infos.len(), 1);
    let info = &infos[0];
    assert_eq!(info.status, WorkflowTaskStatus::Completed);
    assert_eq!(info.sub_workflow_id, Some(child_id));
    let Some(NodeResult::Outcome(TaskOutcome::Succeeded(value))) = &info.result else {
        panic!("the node info has no ok result: {:?}", info.result);
    };
    assert_eq!(
        value.decode::<i64>().expect("the node value reads as i64"),
        SUB_OK_VALUE
    );
    let summary = info
        .sub_workflow_summary
        .as_ref()
        .expect("the summary of the ended child");
    assert_eq!(summary.status, WorkflowStatus::Completed);
    let output = summary
        .output
        .as_ref()
        .expect("the output of the ended child");
    assert_eq!(
        output.decode::<i64>().expect("the output decodes as i64"),
        SUB_OK_VALUE
    );
    assert_eq!((summary.total_tasks, summary.completed_tasks), (1, 1));

    let by_node = handle
        .result_for::<i64, WfCode>(&NodeKey::new("e2e_sub_success_child"))
        .await
        .expect("read the node");
    assert_eq!(by_node, TaskOutcome::Succeeded(SUB_OK_VALUE));

    let rows = tree(&client, handle.workflow_id()).await;
    let child_row = row_of(&rows, child_id);
    assert_eq!(child_row.status, "COMPLETED");
    assert_eq!(child_row.parent_workflow_id, Some(handle.workflow_id()));
    assert_eq!(child_row.parent_task_index, Some(nodes[0].task_index));
    assert_eq!(child_row.depth, 1);
    assert_eq!(child_row.root_workflow_id, Some(handle.workflow_id()));
}

/// Python: `test_layer7_subworkflows.py::test_subworkflow_failure_fail_policy_preserves_error_everywhere`.
#[tokio::test]
#[serial]
async fn test_subworkflow_failure_fail_policy_preserves_error_everywhere() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = layer7(&suite, "failure_fail_policy");
    let _policy = spec.on_error(OnError::Fail);
    let node = spec.sub_workflow(child::<SubFail>("e2e_sub_failure_child"));
    let _output = spec.output(node);
    let handle = start::<i64>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].status, "FAILED");
    let child_id = child_of(&nodes[0]);

    let by_node = handle
        .result_for::<i64, WfCode>(&NodeKey::new("e2e_sub_failure_child"))
        .await
        .expect("read the node");
    let _failure = subworkflow_failure(&by_node, child_id);

    let stored = rows::workflow_error(&client, handle.workflow_id()).await;
    assert!(
        stored
            .as_deref()
            .is_some_and(|error| error.contains(&child_id.to_string())),
        "the workflow error names the child: {stored:?}"
    );
    let _failure = subworkflow_failure(&outcome(&handle).await, child_id);

    let infos = handle.tasks().await.expect("the node infos");
    assert_eq!(infos.len(), 1);
    let info = &infos[0];
    assert_eq!(info.status, WorkflowTaskStatus::Failed);
    assert_eq!(info.sub_workflow_id, Some(child_id));
    let summary = info
        .sub_workflow_summary
        .as_ref()
        .expect("the summary of the ended child");
    assert_eq!(summary.status, WorkflowStatus::Failed);
    match &info.result {
        Some(NodeResult::Outcome(stored)) => {
            let _failure = subworkflow_failure(stored, child_id);
        }
        other => panic!("the node info has no readable result: {other:?}"),
    }
}

/// Python: `test_layer7_subworkflows.py::test_subworkflow_failure_pause_policy_preserves_error_after_resume`.
#[tokio::test]
#[serial]
async fn test_subworkflow_failure_pause_policy_preserves_error_after_resume() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = layer7(&suite, "failure_pause_policy");
    let _policy = spec.on_error(OnError::Pause);
    let node = spec.sub_workflow(child::<SubFail>("e2e_sub_pause_child"));
    let _output = spec.output(node);
    let handle = start::<i64>(&suite, spec).await;
    rows::wait_for_workflow_status(&client, handle.workflow_id(), "PAUSED", WORKFLOW_WAIT).await;

    match outcome(&handle).await {
        TaskOutcome::Failed(error) => assert_eq!(error.code().as_str(), "WORKFLOW_PAUSED"),
        TaskOutcome::Succeeded(value) => panic!("the paused workflow read {value}"),
    }
    let parent = rows::workflow_node(&client, handle.workflow_id(), 0).await;
    assert_eq!(parent.status, "FAILED");
    let child_id = child_of(&parent);
    let by_node = handle
        .result_for::<i64, WfCode>(&NodeKey::new("e2e_sub_pause_child"))
        .await
        .expect("read the node");
    let _failure = subworkflow_failure(&by_node, child_id);
    let paused_error = rows::workflow_error(&client, handle.workflow_id()).await;
    assert!(
        paused_error
            .as_deref()
            .is_some_and(|error| error.contains(&child_id.to_string())),
        "the paused workflow stores the child's failure: {paused_error:?}"
    );

    assert!(
        handle.resume().await.expect("resume"),
        "the workflow resumed"
    );
    let _nodes = ended_nodes(&suite, &handle, "FAILED").await;
    let _failure = subworkflow_failure(&outcome(&handle).await, child_id);
}

// ---------------------------------------------------------------------------
// A child's result in the nodes after it
// ---------------------------------------------------------------------------

/// Python: `test_layer7_subworkflows.py::test_successful_subworkflow_is_available_through_workflow_ctx`.
#[tokio::test]
#[serial]
async fn test_successful_subworkflow_is_available_through_workflow_ctx() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = layer7(&suite, "success_ctx_probe");
    let node = spec.sub_workflow(child::<SubOk>(subworkflow::SUCCESS_CTX_CHILD));
    let probe = add(
        &mut spec,
        suite
            .function(subworkflow::sub_probe_success_ctx::handle)
            .node()
            .node_id("e2e_sub_success_ctx_probe")
            .waits_for(node)
            .workflow_context_from([node]),
    );
    let _output = spec.output(probe);
    let handle = start::<serde_json::Value>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(
        output(&handle).await,
        json!({
            "result_ok": true,
            "result_value": SUB_OK_VALUE,
            "result_error_code": null,
            "summary_status": "COMPLETED",
            "summary_output": SUB_OK_VALUE,
            "summary_completed_tasks": 1,
        })
    );
}

/// Python: `test_layer7_subworkflows.py::test_failed_subworkflow_flows_through_args_from_and_workflow_ctx`.
/// The node after a failed child reads the same sub-workflow failure
/// through `args_from` and through its context.
#[tokio::test]
#[serial]
async fn test_failed_subworkflow_flows_through_args_from_and_workflow_ctx() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = layer7(&suite, "failure_args_ctx_probe");
    let node = spec.sub_workflow(child::<SubFail>(subworkflow::FAILURE_PROBE_CHILD));
    let probe = add(
        &mut spec,
        suite
            .function(subworkflow::sub_probe_failed_child::handle)
            .node()
            .node_id("e2e_sub_failure_probe")
            .arg_from(
                subworkflow::sub_probe_failed_child::fields::child_result(),
                node,
            )
            .workflow_context_from([node])
            .allow_failed_dependencies(true),
    );
    let _output = spec.output(probe);
    let _policy =
        spec.success_policy(SuccessPolicy::new(vec![SuccessCase::new([probe])]).optional([node]));
    let handle = start::<serde_json::Value>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(
        output(&handle).await,
        json!({
            "args_is_subworkflow_error": true,
            "args_error_code": "SUB_WORKFLOW_FAILED",
            "ctx_is_subworkflow_error": true,
            "ids_match": true,
            "summary_status": "FAILED",
            "summary_failed_tasks": 1,
        })
    );
}

/// Python: `test_layer7_subworkflows.py::test_outputless_success_policy_keeps_failed_subworkflow_result`.
/// The outputless result holds the failure of the optional child.
#[tokio::test]
#[serial]
async fn test_outputless_success_policy_keeps_failed_subworkflow_result() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = layer7(&suite, "outputless_success_policy");
    let required = add(
        &mut spec,
        suite
            .function(workflow::mark::handle)
            .node()
            .node_id("e2e_sub_outputless_ok")
            .set(workflow::mark::fields::value(), "required_ok".to_owned())
            .expect("a mark input"),
    );
    let optional = spec.sub_workflow(child::<SubFail>("e2e_sub_outputless_fail_child"));
    let _policy = spec.success_policy(
        SuccessPolicy::new(vec![SuccessCase::new([required])]).optional([optional]),
    );
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    let results = output(&handle).await;
    let Some(TaskOutcome::Succeeded(required)) = results.get("e2e_sub_outputless_ok") else {
        panic!("the required child reads as Succeeded");
    };
    assert_eq!(
        required
            .decode::<String>()
            .expect("the required child's value decodes"),
        "required_ok"
    );
    let failed = results
        .get("e2e_sub_outputless_fail_child")
        .expect("the result of the failed child");
    let _failure = subworkflow_failure(failed, child_of(&nodes[1]));
}

/// Python: `test_layer7_subworkflows.py::test_parallel_subworkflows_preserve_independent_success_and_failure`.
#[tokio::test]
#[serial]
async fn test_parallel_subworkflows_preserve_independent_success_and_failure() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated4).await;
    let mut spec = layer7(&suite, "parallel_success_failure");
    let ok = spec.sub_workflow(child::<SubOk>("e2e_sub_parallel_ok_child"));
    let failing = spec.sub_workflow(child::<SubFail>("e2e_sub_parallel_fail_child"));
    let _policy =
        spec.success_policy(SuccessPolicy::new(vec![SuccessCase::new([ok])]).optional([failing]));
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].status, "COMPLETED");
    assert_eq!(nodes[1].status, "FAILED");
    let (ok_child, failed_child) = (child_of(&nodes[0]), child_of(&nodes[1]));
    assert_ne!(ok_child, failed_child);

    let ok_read = handle
        .result_for::<i64, WfCode>(&NodeKey::new("e2e_sub_parallel_ok_child"))
        .await
        .expect("read the completed child node");
    assert_eq!(ok_read, TaskOutcome::Succeeded(SUB_OK_VALUE));
    let failed_read = handle
        .result_for::<i64, WfCode>(&NodeKey::new("e2e_sub_parallel_fail_child"))
        .await
        .expect("read the failed child node");
    let _failure = subworkflow_failure(&failed_read, failed_child);

    let infos = handle.tasks().await.expect("the node infos");
    let summaries: Vec<Option<WorkflowStatus>> = infos
        .iter()
        .map(|info| {
            info.sub_workflow_summary
                .as_ref()
                .map(|summary| summary.status)
        })
        .collect();
    assert_eq!(
        summaries,
        vec![
            Some(WorkflowStatus::Completed),
            Some(WorkflowStatus::Failed)
        ]
    );
}

// ---------------------------------------------------------------------------
// Nested children
// ---------------------------------------------------------------------------

/// Python: `test_layer7_subworkflows.py::test_nested_subworkflow_success_records_depth_root_and_result`.
#[tokio::test]
#[serial]
async fn test_nested_subworkflow_success_records_depth_root_and_result() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = layer7(&suite, "nested_success");
    let node = spec.sub_workflow(child::<SubNestedOkMiddle>("e2e_sub_nested_success_child"));
    let _output = spec.output(node);
    let handle = start::<i64>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(output(&handle).await, SUB_OK_VALUE);

    let root_id = handle.workflow_id();
    let rows = tree(&client, root_id).await;
    assert_eq!(rows.len(), 3, "the root, the middle and the leaf: {rows:?}");
    let depths: Vec<i32> = rows.iter().map(|row| row.depth).collect();
    assert_eq!(depths, vec![0, 1, 2]);
    let (root, middle, leaf) = (row_of(&rows, root_id), &rows[1], &rows[2]);
    assert_eq!(root.parent_workflow_id, None);
    assert_eq!(middle.parent_workflow_id, Some(root_id));
    assert_eq!(middle.root_workflow_id, Some(root_id));
    assert_eq!(leaf.parent_workflow_id, Some(middle.id));
    assert_eq!(leaf.root_workflow_id, Some(root_id));
    assert_eq!(
        (middle.status.as_str(), leaf.status.as_str()),
        ("COMPLETED", "COMPLETED")
    );
}

/// Python: `test_layer7_subworkflows.py::test_nested_subworkflow_failure_surfaces_each_child_error`.
/// The root reads the failure of the middle child, and the middle child the
/// failure of the leaf.
#[tokio::test]
#[serial]
async fn test_nested_subworkflow_failure_surfaces_each_child_error() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = layer7(&suite, "nested_failure");
    let node = spec.sub_workflow(child::<SubNestedFailMiddle>("e2e_sub_nested_failure_child"));
    let _output = spec.output(node);
    let handle = start::<i64>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    let middle_id = child_of(&nodes[0]);

    let root_outcome = outcome(&handle).await;
    let root_failure = subworkflow_failure(&root_outcome, middle_id);
    assert_eq!(root_failure.sub_workflow_summary.failed_tasks, 1);

    let middle = suite.app.workflow_handle::<i64>(middle_id);
    let middle_outcome = outcome(&middle).await;
    let middle_nodes = rows::workflow_nodes(&client, middle_id).await;
    let leaf_id = child_of(&middle_nodes[0]);
    let middle_failure = subworkflow_failure(&middle_outcome, leaf_id);
    assert_eq!(middle_failure.sub_workflow_summary.failed_tasks, 1);
    let middle_node = middle
        .result_for::<i64, WfCode>(&NodeKey::new("child"))
        .await
        .expect("read the node of the middle child");
    let _failure = subworkflow_failure(&middle_node, leaf_id);
}

// ---------------------------------------------------------------------------
// The params of `build_with`
// ---------------------------------------------------------------------------

/// Python: `test_layer7_subworkflows.py::test_subworkflow_build_with_args_from_receives_typed_task_result`.
#[tokio::test]
#[serial]
async fn test_subworkflow_build_with_args_from_receives_typed_task_result() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = layer7(&suite, "build_with_args_from");
    let source = add(
        &mut spec,
        suite
            .function(workflow::produce_int::handle)
            .node()
            .node_id("e2e_sub_param_source")
            .set(workflow::produce_int::fields::value(), 37)
            .expect("a produce input"),
    );
    let node = spec.sub_workflow(
        SubParam::node(SubWorkflowParams::NotGiven)
            .expect("a param child node")
            .node_id("e2e_sub_param_child")
            .arg_from(UpstreamParams::field_value(), source),
    );
    let _output = spec.output(node);
    let handle = start::<i64>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(output(&handle).await, 37);
}

/// Python: `test_layer7_subworkflows.py::test_subworkflow_allow_failed_dependencies_passes_failed_task_result_to_build_with`.
#[tokio::test]
#[serial]
async fn test_subworkflow_allow_failed_dependencies_passes_failed_task_result_to_build_with() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = layer7(&suite, "allow_failed_dependencies_to_build_with");
    let source = add(
        &mut spec,
        suite
            .function(workflow::fail_with::handle)
            .node()
            .node_id("e2e_sub_failed_input_source")
            .set(
                workflow::fail_with::fields::error_code(),
                "E2E_UPSTREAM_FAIL".to_owned(),
            )
            .expect("a fail input"),
    );
    let node = spec.sub_workflow(
        SubFailedInput::node(SubWorkflowParams::NotGiven)
            .expect("a failed-input child node")
            .node_id("e2e_sub_failed_input_child")
            .arg_from(UpstreamParams::field_value(), source)
            .allow_failed_dependencies(true),
    );
    let _output = spec.output(node);
    let _policy =
        spec.success_policy(SuccessPolicy::new(vec![SuccessCase::new([node])]).optional([source]));
    let handle = start::<serde_json::Value>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(
        output(&handle).await,
        json!({
            "input_is_err": true,
            "input_error_code": "E2E_UPSTREAM_FAIL",
            "input_value": null,
        })
    );
}

/// Python: `test_layer7_subworkflows.py::test_subworkflow_static_kwargs_preserve_build_with_types`.
/// The static param keeps its variant through the stored form.
#[tokio::test]
#[serial]
async fn test_subworkflow_static_kwargs_preserve_build_with_types() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = layer7(&suite, "static_kwargs_preserve_types");
    let node = spec.sub_workflow(
        SubTypedKwarg::node(SubWorkflowParams::Given(PetParams {
            pet: Pet::Dog {
                name: "Ada".to_owned(),
                bark_volume: 7,
            },
        }))
        .expect("a typed child node")
        .node_id("e2e_sub_typed_kwarg_child"),
    );
    let _output = spec.output(node);
    let handle = start::<serde_json::Value>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(
        output(&handle).await,
        json!({
            "saw_dog_at_build": true,
            "type_name_at_build": "Dog",
            "kind_at_build": "dog",
            "bark_volume_at_build": 7,
        })
    );
}
