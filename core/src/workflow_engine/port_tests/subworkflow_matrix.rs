//! The sub-workflow matrix: read surfaces, nesting, joins, skips, success
//! policies and outputless maps with sub-workflow nodes. Python:
//! `clients/python/tests/e2e/test_layer7_subworkflows.py`.
//!
//! Python runs these against a worker. Here [`drive`] plays the worker: it
//! finishes every ENQUEUED node of a workflow tree with the outcome that the
//! Python task of the same name returns ([`worker_outcome`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::support::{
    args_from_inner, child_of, code, ctx_result, err, error_of, finish_node, finish_task, handle,
    node, node_status, node_task_input, ok, required, shared_pool, spec, stored_error,
    stored_result, sub_workflow_node, task_node, workflow, workflow_status,
};
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::workflow::node::{JoinType, SubWorkflowParameters};
use crate::core::workflow::policy::SuccessPolicy;
use crate::core::{
    AnyNode, HorsiesError, SubWorkflowSummary, TaskResult, WorkflowNodeStatus, WorkflowSpec,
    WorkflowStatus,
};
use crate::test_support::child_specs::{TestBuildRefused, TestChildBuild, TestChildSpecs};
use crate::test_support::sql::{self, TestPool};

/// How long `get()` waits; every workflow here is terminal or PAUSED before the call.
pub(super) const GET_WAIT: Duration = Duration::from_secs(2);

/// The most node tasks that [`drive`] finishes in one tree.
const DRIVE_STEPS: usize = 64;

/// The error code of the failing child task (Python `E2ESubFailWorkflow`).
pub(super) const CHILD_FAIL_CODE: &str = "E2E_SUB_CHILD_FAIL";

const FAILED_INPUT_KEY: &str = "tests.port.e2e_sub_failed_input.v1";
pub(super) const SLOW_KEY: &str = "tests.port.e2e_sub_slow.v1";

// ---------------------------------------------------------------------------
// Children (Python `clients/python/tests/e2e/tasks/workflows.py`)
// ---------------------------------------------------------------------------

fn one_node_child(name: &str, node: AnyNode) -> WorkflowSpec {
    let mut child = spec(name, vec![node]);
    child.output_index = Some(0);
    child
}

/// `E2ESubOkWorkflow`: one task that returns 11.
pub(super) fn ok_child() -> WorkflowSpec {
    one_node_child("e2e_sub_ok_workflow", task_node(0, "e2e_sub_int_ok"))
}

/// `E2ESubFailWorkflow`: one task that fails with [`CHILD_FAIL_CODE`].
pub(super) fn fail_child() -> WorkflowSpec {
    one_node_child("e2e_sub_fail_workflow", task_node(0, "e2e_sub_int_fail"))
}

/// `E2ESubNestedOkMiddleWorkflow`: holds [`ok_child`] as its output node.
pub(super) fn nested_ok_middle() -> WorkflowSpec {
    one_node_child(
        "e2e_sub_nested_ok_middle_workflow",
        sub_workflow_node(0, &ok_child()),
    )
}

/// `E2ESubNestedFailMiddleWorkflow`: holds [`fail_child`] as its output node.
pub(super) fn nested_fail_middle() -> WorkflowSpec {
    one_node_child(
        "e2e_sub_nested_fail_middle_workflow",
        sub_workflow_node(0, &fail_child()),
    )
}

/// `E2ESubNestedSlowMiddleWorkflow`: holds the slow child with fixed params.
pub(super) fn nested_slow_middle() -> WorkflowSpec {
    one_node_child(
        "e2e_sub_nested_slow_middle_workflow",
        given_params_node(
            0,
            "e2e_sub_slow_workflow",
            SLOW_KEY,
            &serde_json::json!({"value": "nested_slow_leaf", "delay_ms": 1000}),
        ),
    )
}

/// The stub spec that names the failed-input child in a parent node.
fn failed_input_child() -> WorkflowSpec {
    WorkflowSpec {
        definition_key: Some(FAILED_INPUT_KEY.to_owned()),
        ..spec("e2e_sub_failed_input_workflow", Vec::new())
    }
}

/// A sub-workflow node at `index` of the definition `definition_key`, which
/// gives `input` as params.
pub(super) fn given_params_node(
    index: usize,
    name: &str,
    definition_key: &str,
    input: &serde_json::Value,
) -> AnyNode {
    AnyNode {
        input_json: input.to_string(),
        sub_workflow_params: SubWorkflowParameters::Given,
        sub_definition_key: Some(definition_key.to_owned()),
        ..sub_workflow_node(index, &spec(name, Vec::new()))
    }
}

/// `E2ESubFailedInputWorkflow`: the builder reads the `value` result that the
/// parent passes through `args_from` and writes what it saw into the input of
/// its one task. The builder runs in the engine, so `value` is the result
/// envelope itself, not the wrap that crosses to a worker (Python
/// `clients/python/horsies/core/workflows/engine.py:1171-1185`; the core's
/// `child_build_input`).
fn failed_input_definition() -> TestChildBuild {
    Arc::new(|input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
        let params: serde_json::Value = serde_json::from_str(input).map_err(|error| {
            TestBuildRefused::Failed(HorsiesError::new(format!("params do not decode: {error}")))
        })?;
        let received: TaskResult<serde_json::Value> =
            serde_json::from_value(params["value"].clone()).map_err(|error| {
                TestBuildRefused::Failed(HorsiesError::new(format!(
                    "'value' is not a result envelope: {error}"
                )))
            })?;
        let report = match &received {
            TaskResult::Ok(value) => serde_json::json!({
                "input_is_err": false,
                "input_error_code": null,
                "input_value": value,
            }),
            TaskResult::Err(error) => serde_json::json!({
                "input_is_err": true,
                "input_error_code": code(error),
                "input_value": null,
            }),
        };
        let mut reporter = task_node(0, "e2e_sub_report_input");
        reporter.input_json = report.to_string();
        Ok(WorkflowSpec {
            definition_key: Some(FAILED_INPUT_KEY.to_owned()),
            ..one_node_child("e2e_sub_failed_input_workflow", reporter)
        })
    })
}

/// `E2ESubSlowWorkflow`: one `slow_mark_task` whose input is the params.
fn slow_definition() -> TestChildBuild {
    Arc::new(|input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
        let mut slow = task_node(0, "slow_mark_task");
        slow.input_json = input.to_owned();
        Ok(WorkflowSpec {
            definition_key: Some(SLOW_KEY.to_owned()),
            ..one_node_child("e2e_sub_slow_workflow", slow)
        })
    })
}

/// The child specs of every child of the matrix: the builds by their
/// definition keys, and the specs by theirs.
pub(super) fn matrix_children() -> TestChildSpecs {
    [
        ok_child(),
        fail_child(),
        nested_ok_middle(),
        nested_fail_middle(),
        nested_slow_middle(),
    ]
    .into_iter()
    .fold(
        TestChildSpecs::empty()
            .with_build(FAILED_INPUT_KEY, failed_input_definition())
            .with_build(SLOW_KEY, slow_definition()),
        TestChildSpecs::with_spec,
    )
}

// ---------------------------------------------------------------------------
// Parent nodes
// ---------------------------------------------------------------------------

/// A `mark_task` node that returns `value`.
pub(super) fn mark(index: usize, value: &str) -> AnyNode {
    let mut marked = task_node(index, "mark_task");
    marked.input_json = serde_json::json!({ "value": value }).to_string();
    marked
}

/// A `fail_task` node that fails with `error_code`.
pub(super) fn failing(index: usize, error_code: &str) -> AnyNode {
    let mut failed = task_node(index, "fail_task");
    failed.input_json = serde_json::json!({ "error_code": error_code }).to_string();
    failed
}

pub(super) fn after(mut node: AnyNode, dependencies: &[usize]) -> AnyNode {
    node.dependencies = dependencies.to_vec();
    node
}

fn joined(
    node: AnyNode,
    dependencies: &[usize],
    join: JoinType,
    min_success: Option<i32>,
) -> AnyNode {
    let mut joined = after(node, dependencies);
    joined.join = join;
    joined.min_success = min_success;
    joined
}

/// A policy whose cases require `cases` and that lists `optional`.
pub(super) fn policy(cases: &[&[usize]], optional: &[usize]) -> SuccessPolicy {
    SuccessPolicy {
        optional_indices: Some(optional.to_vec()),
        ..required(cases)
    }
}

/// A parent spec named after the Python layer-7 spec.
pub(super) fn parent(name: &str, tasks: Vec<AnyNode>, output: Option<usize>) -> WorkflowSpec {
    let mut parent = spec(&format!("e2e_layer7_{name}"), tasks);
    parent.output_index = output;
    parent
}

// ---------------------------------------------------------------------------
// The worker
// ---------------------------------------------------------------------------

/// The outcome that the Python task `task_name` returns for `input`.
fn worker_outcome(task_name: &str, input: &serde_json::Value) -> TaskResult<serde_json::Value> {
    match task_name {
        "e2e_sub_int_ok" => ok(serde_json::json!(11)),
        "e2e_sub_int_fail" => err(
            CHILD_FAIL_CODE,
            &format!("Subworkflow failed: {CHILD_FAIL_CODE}"),
        ),
        "mark_task" | "slow_mark_task" => ok(input["value"].clone()),
        "fail_task" => {
            let error_code = input["error_code"]
                .as_str()
                .unwrap_or_else(|| panic!("fail_task input without an error code: {input}"));
            err(error_code, &format!("failed: {error_code}"))
        }
        "e2e_sub_report_input" => ok(serde_json::json!({
            "input_is_err": input["input_is_err"],
            "input_error_code": input["input_error_code"],
            "input_value": input["input_value"],
        })),
        other => panic!("the port worker has no outcome for task '{other}'"),
    }
}

/// The next ENQUEUED node task of the tree under `root_id`, deepest first.
async fn next_enqueued(pool: &TestPool, root_id: Uuid) -> Option<(Uuid, String, String)> {
    sql::query_as(
        "SELECT n.task_id, n.task_name, t.input
         FROM horsies_workflow_tasks n
         JOIN horsies_workflows w ON w.id = n.workflow_id
         JOIN horsies_tasks t ON t.id = n.task_id
         WHERE (w.id = $1 OR w.root_workflow_id = $1)
           AND w.status = 'RUNNING'
           AND n.status = 'ENQUEUED'
         ORDER BY w.depth DESC, n.task_index
         LIMIT 1",
    )
    .bind(root_id)
    .fetch_optional(pool)
    .await
    .expect("read the next enqueued node")
}

/// Finishes every ENQUEUED node task of the RUNNING workflows under
/// `root_id`, as a worker would, until none is left.
pub(super) async fn drive(pool: &TestPool, root_id: Uuid, child_specs: &TestChildSpecs) {
    for _step in 0..DRIVE_STEPS {
        match next_enqueued(pool, root_id).await {
            None => return,
            Some((task_id, task_name, input)) => {
                let input: serde_json::Value =
                    serde_json::from_str(&input).expect("a task input decodes");
                finish_task(
                    pool,
                    task_id,
                    &worker_outcome(&task_name, &input),
                    child_specs,
                )
                .await;
            }
        }
    }
    panic!("the tree of {root_id} did not settle in {DRIVE_STEPS} tasks");
}

/// Starts `parent_spec` with [`matrix_children`] and drives it to its end.
async fn run(pool: &TestPool, parent_spec: &WorkflowSpec) -> Uuid {
    let child_specs = matrix_children();
    let workflow_id = super::support::start(pool, parent_spec, &child_specs).await;
    drive(pool, workflow_id, &child_specs).await;
    workflow_id
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Python `_assert_subworkflow_error`: a `SUB_WORKFLOW_FAILED` error of
/// `child_id` whose summary is FAILED with a failed task.
pub(super) fn assert_subworkflow_error(
    result: &TaskResult<serde_json::Value>,
    child_id: Option<Uuid>,
) -> SubWorkflowSummary {
    let error = error_of(result);
    assert_eq!(
        code(error).as_deref(),
        Some("SUB_WORKFLOW_FAILED"),
        "{error:?}"
    );
    let details = error
        .sub_workflow_details()
        .unwrap_or_else(|| panic!("the error has no sub-workflow details: {error:?}"));
    if let Some(child) = child_id {
        assert_eq!(details.sub_workflow_id, child);
    }
    assert_eq!(details.sub_workflow_summary.status, WorkflowStatus::Failed);
    assert!(details.sub_workflow_summary.failed_nodes >= 1);
    details.sub_workflow_summary.clone()
}

pub(super) fn assert_ok_value(
    result: &TaskResult<serde_json::Value>,
    expected: &serde_json::Value,
) {
    match result {
        TaskResult::Ok(value) => assert_eq!(value, expected),
        TaskResult::Err(error) => panic!("expected ok {expected}, got {error:?}"),
    }
}

pub(super) async fn get(pool: &TestPool, workflow_id: Uuid) -> TaskResult<serde_json::Value> {
    handle(pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read")
}

pub(super) fn node_result(text: Option<String>) -> TaskResult<serde_json::Value> {
    stored_result(&text.expect("a terminal node stores its result"))
}

fn summary_of(text: Option<&str>) -> SubWorkflowSummary {
    serde_json::from_str(text.expect("the node stores a child summary"))
        .expect("the child summary decodes")
}

/// The result of the node `node_id` as `result_for()` reads it.
pub(super) async fn node_entry(
    pool: &TestPool,
    workflow_id: Uuid,
    node_id: &str,
) -> TaskResult<serde_json::Value> {
    handle(pool, workflow_id)
        .result_for::<serde_json::Value>(node_id)
        .await
        .expect("the node result read")
}

/// `get()` of a COMPLETED workflow with no output node: the value `null`.
pub(super) async fn assert_no_workflow_output(pool: &TestPool, workflow_id: Uuid) {
    assert_ok_value(&get(pool, workflow_id).await, &serde_json::Value::Null);
}

/// A row of a workflow tree.
#[derive(Debug)]
pub(super) struct TreeRow {
    pub(super) id: Uuid,
    pub(super) parent_workflow_id: Option<Uuid>,
    pub(super) parent_task_index: Option<i32>,
    pub(super) root_workflow_id: Option<Uuid>,
    pub(super) depth: i32,
    pub(super) status: String,
}

impl sql::FromTestRow for TreeRow {
    fn from_row(row: &tokio_postgres::Row) -> Result<Self, sql::Error> {
        Ok(Self {
            id: sql::named(row, "id")?,
            parent_workflow_id: sql::named(row, "parent_workflow_id")?,
            parent_task_index: sql::named(row, "parent_task_index")?,
            root_workflow_id: sql::named(row, "root_workflow_id")?,
            depth: sql::named(row, "depth")?,
            status: sql::named(row, "status")?,
        })
    }
}

pub(super) async fn tree(pool: &TestPool, root_id: Uuid) -> Vec<TreeRow> {
    sql::query_as(
        "SELECT id, parent_workflow_id, parent_task_index, root_workflow_id, depth, status
         FROM horsies_workflows WHERE id = $1 OR root_workflow_id = $1 ORDER BY depth",
    )
    .bind(root_id)
    .fetch_all(pool)
    .await
    .expect("read the workflow tree")
}

// ---------------------------------------------------------------------------
// Read surfaces
// ---------------------------------------------------------------------------

pub(super) fn success_surface_parent() -> WorkflowSpec {
    parent(
        "subworkflow_success_full_surface",
        vec![sub_workflow_node(0, &ok_child())],
        Some(0),
    )
}

/// `test_subworkflow_success_full_surface_matrix`: `get()`, the node row, the
/// summary, the child row links, `tasks()` and `result_for()` agree.
#[tokio::test]
async fn a_child_success_reads_the_same_on_every_surface() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &success_surface_parent()).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(&get(&pool, workflow_id).await, &serde_json::json!(11));
    let sub = node(&pool, workflow_id, 0).await;
    assert_eq!(sub.status, "COMPLETED");
    let child_id = sub.sub_workflow_id.expect("the node holds its child");
    assert_ok_value(&node_result(sub.result), &serde_json::json!(11));
    let summary = summary_of(sub.sub_workflow_summary.as_deref());
    assert_eq!(summary.status, WorkflowStatus::Completed);
    assert_eq!(
        summary
            .output
            .as_ref()
            .map(crate::core::codec::JsonText::get),
        Some("11")
    );
    assert_eq!((summary.total_nodes, summary.completed_nodes), (1, 1));
    let child = tree(&pool, workflow_id)
        .await
        .into_iter()
        .find(|row| row.id == child_id)
        .expect("the child is in the tree");
    assert_eq!(child.status, "COMPLETED");
    assert_eq!(child.parent_workflow_id, Some(workflow_id));
    assert_eq!(child.parent_task_index, Some(0));
    assert_eq!(child.depth, 1);
    assert_eq!(child.root_workflow_id, Some(workflow_id));
    let infos = handle(&pool, workflow_id).tasks().await.expect("tasks()");
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].status, WorkflowNodeStatus::Completed);
    assert_eq!(infos[0].sub_workflow_id, Some(child_id));
    assert_eq!(
        infos[0]
            .sub_workflow_summary
            .as_ref()
            .expect("tasks() gives the child summary")
            .status,
        WorkflowStatus::Completed
    );
    assert_ok_value(
        infos[0].result.as_ref().expect("tasks() holds the result"),
        &serde_json::json!(11),
    );
    assert_ok_value(
        &handle(&pool, workflow_id)
            .result_for::<serde_json::Value>("node_0")
            .await
            .expect("the node result read"),
        &serde_json::json!(11),
    );
}

/// `test_subworkflow_reconstructed_handle_status`, `..._tasks` and `..._results`:
/// a handle built from the workflow id reads the finished workflow.
#[tokio::test]
async fn a_handle_built_from_the_id_reads_status_tasks_and_results() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &success_surface_parent()).await;
    let rebuilt = handle(&pool, workflow_id);

    assert_eq!(
        rebuilt.status().await.expect("status()"),
        WorkflowStatus::Completed
    );
    assert_ok_value(
        &rebuilt.get(Some(GET_WAIT)).await.expect("the result read"),
        &serde_json::json!(11),
    );
    let infos = rebuilt.tasks().await.expect("tasks()");
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].status, WorkflowNodeStatus::Completed);
    assert!(infos[0].sub_workflow_id.is_some());
    assert_eq!(
        infos[0]
            .sub_workflow_summary
            .as_ref()
            .expect("tasks() gives the child summary")
            .status,
        WorkflowStatus::Completed
    );
    assert_ok_value(
        infos[0].result.as_ref().expect("tasks() holds the result"),
        &serde_json::json!(11),
    );
    let results = rebuilt.results().await.expect("results()");
    assert_eq!(
        results.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["node_0"]
    );
    assert_ok_value(&results["node_0"], &serde_json::json!(11));
}

/// `test_subworkflow_failure_fail_policy_preserves_error_everywhere`: the node,
/// the workflow error column, `get()`, `tasks()` and `result_for()` hold the
/// same sub-workflow error.
#[tokio::test]
async fn a_child_failure_reads_the_same_sub_workflow_error_on_every_surface() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "subworkflow_failure_fail_policy",
            vec![sub_workflow_node(0, &fail_child())],
            Some(0),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    let sub = node(&pool, workflow_id, 0).await;
    assert_eq!(sub.status, "FAILED");
    let child_id = sub.sub_workflow_id.expect("the node holds its child");
    let _summary = assert_subworkflow_error(&node_result(sub.result), Some(child_id));
    let workflow_error = stored_error(
        &workflow(&pool, workflow_id)
            .await
            .error
            .expect("a FAILED workflow stores its error"),
    );
    assert_eq!(
        workflow_error
            .sub_workflow_details()
            .expect("the workflow error is a sub-workflow error")
            .sub_workflow_id,
        child_id
    );
    let _got = assert_subworkflow_error(&get(&pool, workflow_id).await, Some(child_id));
    let infos = handle(&pool, workflow_id).tasks().await.expect("tasks()");
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].status, WorkflowNodeStatus::Failed);
    assert_eq!(infos[0].sub_workflow_id, Some(child_id));
    assert_eq!(
        infos[0]
            .sub_workflow_summary
            .as_ref()
            .expect("tasks() gives the child summary")
            .status,
        WorkflowStatus::Failed
    );
    let _info = assert_subworkflow_error(
        infos[0].result.as_ref().expect("tasks() holds the result"),
        Some(child_id),
    );
    let _by_node = assert_subworkflow_error(
        &handle(&pool, workflow_id)
            .result_for::<serde_json::Value>("node_0")
            .await
            .expect("the node result read"),
        Some(child_id),
    );
}

/// `test_failed_subworkflow_flows_through_args_from_and_workflow_ctx`: the
/// dependent gets the same sub-workflow error through `args_from` and the
/// context, and the context holds the child summary.
#[tokio::test]
async fn a_failed_child_reaches_its_dependent_through_args_from_and_the_context() {
    let pool = shared_pool().await;
    let child_specs = matrix_children();
    let mut probe = after(task_node(1, "e2e_sub_probe_failed_child"), &[0]);
    probe.args_from.insert("child_result".to_owned(), 0);
    probe.workflow_context_from = Some(vec!["node_0".to_owned()]);
    probe.allow_failed_dependencies = true;
    let mut probe_parent = parent(
        "subworkflow_failure_args_ctx_probe",
        vec![sub_workflow_node(0, &fail_child()), probe],
        Some(1),
    );
    probe_parent.success_policy = Some(policy(&[&[1]], &[0]));
    let workflow_id = super::support::start(&pool, &probe_parent, &child_specs).await;

    let child_id = child_of(&pool, workflow_id, 0).await;
    let _task = finish_node(
        &pool,
        child_id,
        0,
        &err(CHILD_FAIL_CODE, "Subworkflow failed"),
        &child_specs,
    )
    .await;

    assert_eq!(node_status(&pool, workflow_id, 1).await, "ENQUEUED");
    let input = node_task_input(&pool, workflow_id, 1).await;
    let _through_args =
        assert_subworkflow_error(&args_from_inner(&input, "child_result"), Some(child_id));
    let _through_ctx = assert_subworkflow_error(&ctx_result(&input, "node_0"), Some(child_id));
    // The context holds the stored summary text as a JSON string (Python
    // `_build_workflow_context_data`, `clients/python/horsies/core/workflows/engine.py:1666-1677`;
    // the baseline `horsies_workflow_task_input`); the worker parses it.
    let summary_text = input[WORKFLOW_CTX_KWARG]["summaries_by_id"]["node_0"]
        .as_str()
        .unwrap_or_else(|| panic!("the context holds the child summary as text: {input}"));
    let summary: SubWorkflowSummary =
        serde_json::from_str(summary_text).expect("the child summary decodes");
    assert_eq!(summary.status, WorkflowStatus::Failed);
    assert_eq!(summary.failed_nodes, 1);
}

pub(super) fn outputless_parent() -> WorkflowSpec {
    let mut outputless = parent(
        "subworkflow_outputless_success_policy",
        vec![mark(0, "required_ok"), sub_workflow_node(1, &fail_child())],
        None,
    );
    outputless.success_policy = Some(policy(&[&[0]], &[1]));
    outputless
}

/// `test_outputless_success_policy_keeps_failed_subworkflow_result`
#[tokio::test]
async fn an_outputless_workflow_keeps_the_optional_child_error_in_its_node() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &outputless_parent()).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_no_workflow_output(&pool, workflow_id).await;
    assert_ok_value(
        &node_entry(&pool, workflow_id, "node_0").await,
        &serde_json::json!("required_ok"),
    );
    let child_id = child_of(&pool, workflow_id, 1).await;
    let _summary = assert_subworkflow_error(
        &node_entry(&pool, workflow_id, "node_1").await,
        Some(child_id),
    );
}

/// `test_nested_subworkflow_success_records_depth_root_and_result`
#[tokio::test]
async fn a_nested_child_success_records_its_links_depth_and_root() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "subworkflow_nested_success",
            vec![sub_workflow_node(0, &nested_ok_middle())],
            Some(0),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(&get(&pool, workflow_id).await, &serde_json::json!(11));
    let rows = tree(&pool, workflow_id).await;
    assert_eq!(rows.len(), 3);
    let (root, middle, leaf) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!((root.id, root.parent_workflow_id), (workflow_id, None));
    assert_eq!((middle.depth, leaf.depth), (1, 2));
    assert_eq!(middle.parent_workflow_id, Some(workflow_id));
    assert_eq!(middle.root_workflow_id, Some(workflow_id));
    assert_eq!(leaf.parent_workflow_id, Some(middle.id));
    assert_eq!(leaf.root_workflow_id, Some(workflow_id));
    assert_eq!(
        (middle.status.as_str(), leaf.status.as_str()),
        ("COMPLETED", "COMPLETED")
    );
}

/// `test_nested_subworkflow_failure_surfaces_each_child_error`: the root, the
/// middle workflow and the middle node each hold a sub-workflow error.
#[tokio::test]
async fn a_nested_child_failure_gives_a_sub_workflow_error_at_each_level() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "subworkflow_nested_failure",
            vec![sub_workflow_node(0, &nested_fail_middle())],
            Some(0),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    let middle_id = child_of(&pool, workflow_id, 0).await;
    let root_summary = assert_subworkflow_error(&get(&pool, workflow_id).await, Some(middle_id));
    assert_eq!(root_summary.failed_nodes, 1);
    let middle_error = stored_error(
        &workflow(&pool, middle_id)
            .await
            .error
            .expect("the middle workflow stores its error"),
    );
    assert_eq!(
        middle_error
            .sub_workflow_details()
            .expect("the middle error is a sub-workflow error")
            .sub_workflow_summary
            .failed_nodes,
        1
    );
    let leaf_id = child_of(&pool, middle_id, 0).await;
    let _middle_node = assert_subworkflow_error(
        &node_result(node(&pool, middle_id, 0).await.result),
        Some(leaf_id),
    );
}

/// `test_subworkflow_allow_failed_dependencies_passes_failed_task_result_to_build_with`
/// and `test_subworkflow_skip_allow_failed_dependencies`: the child builder receives
/// the failed upstream result with its code, and the child completes.
#[tokio::test]
async fn an_allow_failed_dependencies_child_builder_receives_the_failed_upstream_result() {
    let pool = shared_pool().await;
    let mut child = after(sub_workflow_node(1, &failed_input_child()), &[0]);
    child.args_from.insert("value".to_owned(), 0);
    child.allow_failed_dependencies = true;
    child.sub_workflow_params = SubWorkflowParameters::Given;
    let mut allow_failed = parent(
        "subworkflow_allow_failed_dependencies_to_build_with",
        vec![failing(0, "E2E_UPSTREAM_FAIL"), child],
        Some(1),
    );
    allow_failed.success_policy = Some(policy(&[&[1]], &[0]));
    let workflow_id = run(&pool, &allow_failed).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_eq!(node_status(&pool, workflow_id, 0).await, "FAILED");
    assert_eq!(node_status(&pool, workflow_id, 1).await, "COMPLETED");
    assert_ok_value(
        &get(&pool, workflow_id).await,
        &serde_json::json!({
            "input_is_err": true,
            "input_error_code": "E2E_UPSTREAM_FAIL",
            "input_value": null,
        }),
    );
}

// ---------------------------------------------------------------------------
// Joins and skips
// ---------------------------------------------------------------------------

async fn statuses(pool: &TestPool, workflow_id: Uuid, count: i32) -> HashMap<String, String> {
    let mut read = HashMap::new();
    for index in 0..count {
        let _replaced = read.insert(
            format!("node_{index}"),
            node_status(pool, workflow_id, index).await,
        );
    }
    read
}

fn expected(statuses: &[&str]) -> HashMap<String, String> {
    statuses
        .iter()
        .enumerate()
        .map(|(index, status)| (format!("node_{index}"), (*status).to_owned()))
        .collect()
}

/// `test_subworkflow_join_any_first_success`: the child starts while the slow
/// dependency still runs, and keeps its child to the end.
#[tokio::test]
async fn a_join_any_child_starts_on_the_first_success() {
    let pool = shared_pool().await;
    let child_specs = matrix_children();
    let workflow_id = super::support::start(
        &pool,
        &parent(
            "join_any_first_success",
            vec![
                mark(0, "fast"),
                mark(1, "slow"),
                joined(
                    sub_workflow_node(2, &ok_child()),
                    &[0, 1],
                    JoinType::Any,
                    None,
                ),
            ],
            Some(2),
        ),
        &child_specs,
    )
    .await;

    let _fast = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!("fast")),
        &child_specs,
    )
    .await;
    let child_id = child_of(&pool, workflow_id, 2).await;
    assert_eq!(node_status(&pool, workflow_id, 1).await, "ENQUEUED");
    drive(&pool, workflow_id, &child_specs).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(&get(&pool, workflow_id).await, &serde_json::json!(11));
    let sub = node(&pool, workflow_id, 2).await;
    assert_eq!(sub.sub_workflow_id, Some(child_id));
    assert_eq!(sub.status, "COMPLETED");
}

/// `test_subworkflow_join_any_all_fail`
#[tokio::test]
async fn a_join_any_child_with_every_dependency_failed_is_skipped_with_no_child() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "join_any_all_fail",
            vec![
                failing(0, "L7_ANY_FAIL_A"),
                failing(1, "L7_ANY_FAIL_B"),
                joined(
                    sub_workflow_node(2, &ok_child()),
                    &[0, 1],
                    JoinType::Any,
                    None,
                ),
            ],
            Some(2),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        statuses(&pool, workflow_id, 3).await,
        expected(&["FAILED", "FAILED", "SKIPPED"])
    );
    assert_eq!(node(&pool, workflow_id, 2).await.sub_workflow_id, None);
}

/// `test_subworkflow_join_quorum_met`
#[tokio::test]
async fn a_quorum_child_starts_when_its_quorum_is_met() {
    let pool = shared_pool().await;
    let mut quorum = parent(
        "join_quorum_met",
        vec![
            mark(0, "A"),
            mark(1, "B"),
            failing(2, "L7_QUORUM_OPTIONAL_FAIL"),
            joined(
                sub_workflow_node(3, &ok_child()),
                &[0, 1, 2],
                JoinType::Quorum,
                Some(2),
            ),
        ],
        Some(3),
    );
    quorum.success_policy = Some(policy(&[&[3]], &[2]));
    let workflow_id = run(&pool, &quorum).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(&get(&pool, workflow_id).await, &serde_json::json!(11));
    assert_eq!(
        statuses(&pool, workflow_id, 4).await,
        expected(&["COMPLETED", "COMPLETED", "FAILED", "COMPLETED"])
    );
}

/// `test_subworkflow_join_quorum_unmet`
#[tokio::test]
async fn a_quorum_child_is_skipped_when_its_quorum_fails() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "join_quorum_unmet",
            vec![
                mark(0, "A"),
                failing(1, "L7_QUORUM_FAIL_B"),
                failing(2, "L7_QUORUM_FAIL_C"),
                joined(
                    sub_workflow_node(3, &ok_child()),
                    &[0, 1, 2],
                    JoinType::Quorum,
                    Some(2),
                ),
            ],
            Some(3),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        statuses(&pool, workflow_id, 4).await,
        expected(&["COMPLETED", "FAILED", "FAILED", "SKIPPED"])
    );
}

/// `test_subworkflow_join_quorum_impossible`: the child is SKIPPED with no
/// child once two of three dependencies failed, while the third still runs.
#[tokio::test]
async fn a_quorum_child_is_skipped_once_its_quorum_is_impossible() {
    let pool = shared_pool().await;
    let child_specs = matrix_children();
    let workflow_id = super::support::start(
        &pool,
        &parent(
            "join_quorum_impossible",
            vec![
                failing(0, "L7_QI_FAIL_A"),
                failing(1, "L7_QI_FAIL_B"),
                mark(2, "slow_c"),
                joined(
                    sub_workflow_node(3, &ok_child()),
                    &[0, 1, 2],
                    JoinType::Quorum,
                    Some(2),
                ),
            ],
            Some(3),
        ),
        &child_specs,
    )
    .await;

    for index in [0, 1] {
        let _failed = finish_node(
            &pool,
            workflow_id,
            index,
            &err("L7_QI_FAIL", "failed"),
            &child_specs,
        )
        .await;
    }

    let skipped = node(&pool, workflow_id, 3).await;
    assert_eq!(skipped.status, "SKIPPED");
    assert_eq!(skipped.sub_workflow_id, None);
    assert_eq!(node_status(&pool, workflow_id, 2).await, "ENQUEUED");
    drive(&pool, workflow_id, &child_specs).await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// `test_subworkflow_skip_task_fail_to_sub_skip`
#[tokio::test]
async fn a_failed_task_skips_a_dependent_child_node() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "skip_task_fail_to_sub_skip",
            vec![
                failing(0, "L7_TASK_FAILS_BEFORE_SUB"),
                after(sub_workflow_node(1, &ok_child()), &[0]),
            ],
            Some(1),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        statuses(&pool, workflow_id, 2).await,
        expected(&["FAILED", "SKIPPED"])
    );
}

/// `test_subworkflow_skip_sub_fail_to_task_skip`
#[tokio::test]
async fn a_failed_child_skips_a_dependent_task_node() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "skip_sub_fail_to_task_skip",
            vec![
                sub_workflow_node(0, &fail_child()),
                after(mark(1, "should_skip"), &[0]),
            ],
            Some(1),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        statuses(&pool, workflow_id, 2).await,
        expected(&["FAILED", "SKIPPED"])
    );
}

/// `test_subworkflow_skip_chain_skip`
#[tokio::test]
async fn a_skip_cascades_through_child_and_task_nodes() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &parent(
            "skip_chain_skip",
            vec![
                failing(0, "L7_CHAIN_SOURCE_FAIL"),
                after(sub_workflow_node(1, &ok_child()), &[0]),
                after(mark(2, "middle"), &[1]),
                after(sub_workflow_node(3, &ok_child()), &[2]),
            ],
            Some(3),
        ),
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        statuses(&pool, workflow_id, 4).await,
        expected(&["FAILED", "SKIPPED", "SKIPPED", "SKIPPED"])
    );
}

// ---------------------------------------------------------------------------
// Success policies and outputless maps
// ---------------------------------------------------------------------------

/// `test_subworkflow_success_policy_required_fail_workflow_fail`
#[tokio::test]
async fn a_required_failed_child_fails_the_workflow() {
    let pool = shared_pool().await;
    let mut required_fail = parent(
        "success_policy_required_fail_workflow_fail",
        vec![sub_workflow_node(0, &fail_child())],
        Some(0),
    );
    required_fail.success_policy = Some(required(&[&[0]]));
    let workflow_id = run(&pool, &required_fail).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    let child_id = child_of(&pool, workflow_id, 0).await;
    let _summary = assert_subworkflow_error(&get(&pool, workflow_id).await, Some(child_id));
}

/// `test_subworkflow_success_policy_multi_case_any`
#[tokio::test]
async fn a_second_case_met_by_a_child_completes_the_workflow() {
    let pool = shared_pool().await;
    let mut multi = parent(
        "success_policy_multi_case_any",
        vec![
            sub_workflow_node(0, &fail_child()),
            sub_workflow_node(1, &ok_child()),
        ],
        Some(1),
    );
    multi.success_policy = Some(required(&[&[0], &[1]]));
    let workflow_id = run(&pool, &multi).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(&get(&pool, workflow_id).await, &serde_json::json!(11));
    assert_eq!(
        statuses(&pool, workflow_id, 2).await,
        expected(&["FAILED", "COMPLETED"])
    );
}

/// `test_subworkflow_success_policy_mixed_required_optional`: `results()` holds
/// the required child's output and the optional child's error.
#[tokio::test]
async fn required_and_optional_children_hold_their_results() {
    let pool = shared_pool().await;
    let mut mixed = parent(
        "success_policy_mixed_required_optional",
        vec![
            sub_workflow_node(0, &ok_child()),
            mark(1, "task_ok"),
            sub_workflow_node(2, &fail_child()),
        ],
        Some(1),
    );
    mixed.success_policy = Some(policy(&[&[0, 1]], &[2]));
    let workflow_id = run(&pool, &mixed).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(
        &get(&pool, workflow_id).await,
        &serde_json::json!("task_ok"),
    );
    let results = handle(&pool, workflow_id)
        .results()
        .await
        .expect("results()");
    assert_ok_value(&results["node_0"], &serde_json::json!(11));
    let optional_child = child_of(&pool, workflow_id, 2).await;
    let _summary = assert_subworkflow_error(&results["node_2"], Some(optional_child));
}

/// `test_subworkflow_deep_outputless_nested_failed_optional`
#[tokio::test]
async fn an_optional_nested_failed_child_keeps_its_error_in_its_node() {
    let pool = shared_pool().await;
    let mut deep = parent(
        "deep_outputless_nested_failed_optional",
        vec![
            mark(0, "required_ok"),
            sub_workflow_node(1, &nested_fail_middle()),
        ],
        None,
    );
    deep.success_policy = Some(policy(&[&[0]], &[1]));
    let workflow_id = run(&pool, &deep).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_no_workflow_output(&pool, workflow_id).await;
    assert_ok_value(
        &node_entry(&pool, workflow_id, "node_0").await,
        &serde_json::json!("required_ok"),
    );
    let middle_id = child_of(&pool, workflow_id, 1).await;
    let _summary = assert_subworkflow_error(
        &node_entry(&pool, workflow_id, "node_1").await,
        Some(middle_id),
    );
    assert!(tree(&pool, workflow_id)
        .await
        .iter()
        .any(|row| row.depth == 2));
}

/// `test_subworkflow_deep_outputless_all_failed_required`
#[tokio::test]
async fn two_required_failed_children_fail_the_workflow_with_sub_workflow_errors() {
    let pool = shared_pool().await;
    let mut deep = parent(
        "deep_outputless_all_failed_required",
        vec![
            sub_workflow_node(0, &fail_child()),
            sub_workflow_node(1, &nested_fail_middle()),
        ],
        None,
    );
    deep.success_policy = Some(required(&[&[0, 1]]));
    let workflow_id = run(&pool, &deep).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    for index in [0, 1] {
        let failed = node(&pool, workflow_id, index).await;
        assert_eq!(failed.status, "FAILED");
        let child_id = failed.sub_workflow_id.expect("the node holds its child");
        let _summary = assert_subworkflow_error(&node_result(failed.result), Some(child_id));
    }
    let got = get(&pool, workflow_id).await;
    assert!(error_of(&got).sub_workflow_details().is_some(), "{got:?}");
}

/// `test_subworkflow_deep_mixed_paths`: a child success, an allow-failed child
/// that receives the failed source, and an optional nested failure in one
/// outputless workflow.
#[tokio::test]
async fn success_allow_failed_and_nested_optional_failure_in_one_outputless_workflow() {
    let pool = shared_pool().await;
    let mut allow_failed = after(sub_workflow_node(2, &failed_input_child()), &[1]);
    allow_failed.args_from.insert("value".to_owned(), 1);
    allow_failed.allow_failed_dependencies = true;
    allow_failed.sub_workflow_params = SubWorkflowParameters::Given;
    let mut mixed = parent(
        "deep_mixed_paths",
        vec![
            sub_workflow_node(0, &ok_child()),
            failing(1, "L7_DEEP_MIXED_SOURCE_FAIL"),
            allow_failed,
            sub_workflow_node(3, &nested_fail_middle()),
        ],
        None,
    );
    mixed.success_policy = Some(policy(&[&[0, 2]], &[1, 3]));
    let workflow_id = run(&pool, &mixed).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_no_workflow_output(&pool, workflow_id).await;
    assert_ok_value(
        &node_entry(&pool, workflow_id, "node_0").await,
        &serde_json::json!(11),
    );
    assert_ok_value(
        &node_entry(&pool, workflow_id, "node_2").await,
        &serde_json::json!({
            "input_is_err": true,
            "input_error_code": "L7_DEEP_MIXED_SOURCE_FAIL",
            "input_value": null,
        }),
    );
    let nested_id = child_of(&pool, workflow_id, 3).await;
    let _summary = assert_subworkflow_error(
        &node_entry(&pool, workflow_id, "node_3").await,
        Some(nested_id),
    );
}
