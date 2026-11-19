//! Layer 6: joins, pause and resume, success policies, recovery and
//! retries of workflows (Python
//! `clients/python/tests/e2e/test_layer6_workflow_advanced.py`). The specs are built in the
//! test process with the typed DSL and started with `Horsies::start`; the
//! worker runs their nodes, and its reaper runs the workflow recovery.

use std::convert::Infallible;
use std::time::Duration;

use horsies::{
    HorsiesError, NodeKey, OnError, OutputlessResults, SubWorkflowParams, SuccessCase,
    SuccessPolicy, TaskNode, TaskOutcome, TypedNodeRef, WorkflowDefinition, WorkflowHandle,
    WorkflowSpecBuilder,
};
use horsies_test_support::poll;
use horsies_test_support::rows::{self, NodeRow};
use horsies_test_support::tokio_postgres::Client;
use horsies_test_worker::harness::{within, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::tasks::{recovery, subworkflow, workflow};
use serde::de::DeserializeOwned;
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for a workflow to end, or to reach a node status.
const WORKFLOW_WAIT: Duration = Duration::from_secs(30);

/// The longest wait for a workflow whose node retries three times, 1 s,
/// 2 s and 4 s apart.
const RETRY_WAIT: Duration = Duration::from_secs(60);

/// The longest wait for the reaper of a worker to finalize a workflow
/// again: one recovery pass each second.
const RECOVERY_WAIT: Duration = Duration::from_secs(30);

/// How long a paused workflow must stay paused.
const PAUSED_WINDOW: Duration = Duration::from_secs(1);

fn builder(name: &str) -> WorkflowSpecBuilder {
    let mut builder = WorkflowSpecBuilder::new(name);
    let _key = builder.definition_key(format!("tests.e2e.{name}.v1"));
    builder
}

/// A step node `id` that returns `completed_<id>`.
fn step(suite: &Suite, id: &str) -> TaskNode<String, workflow::step::Input> {
    suite
        .function(workflow::step::handle)
        .node()
        .node_id(id)
        .set(workflow::step::params::step(), id.to_owned())
        .expect("a step input")
}

/// A slow step node `id` that runs `delay_ms` and returns `completed_<id>`.
fn slow_step(
    suite: &Suite,
    id: &str,
    delay_ms: u64,
) -> TaskNode<String, workflow::slow_step::Input> {
    suite
        .function(workflow::slow_step::handle)
        .node()
        .node_id(id)
        .set(workflow::slow_step::params::step(), id.to_owned())
        .and_then(|node| node.set(workflow::slow_step::params::delay_ms(), delay_ms))
        .expect("a slow step input")
}

/// A node `id` that returns `value`.
fn mark(suite: &Suite, id: &str, value: &str) -> TaskNode<String, workflow::mark::Input> {
    suite
        .function(workflow::mark::handle)
        .node()
        .node_id(id)
        .set(workflow::mark::params::value(), value.to_owned())
        .expect("a mark input")
}

/// A node `id` that returns `value`.
fn produce(suite: &Suite, id: &str, value: i64) -> TaskNode<i64, workflow::produce_int::Input> {
    suite
        .function(workflow::produce_int::handle)
        .node()
        .node_id(id)
        .set(workflow::produce_int::params::value(), value)
        .expect("a produce input")
}

/// A node `id` that fails with `DELIBERATE_FAILURE`.
fn fail(suite: &Suite, id: &str) -> TaskNode<i64, ()> {
    suite.function(workflow::fail::handle).node().node_id(id)
}

/// A node `id` that fails with the code named `error_code`.
fn fail_with(
    suite: &Suite,
    id: &str,
    error_code: &str,
) -> TaskNode<i64, workflow::fail_with::Input> {
    suite
        .function(workflow::fail_with::handle)
        .node()
        .node_id(id)
        .set(
            workflow::fail_with::params::error_code(),
            error_code.to_owned(),
        )
        .expect("a fail input")
}

/// A node `id` that needs `succeed_on_attempt` attempts of `token`.
fn retry_then_ok(
    suite: &Suite,
    id: &str,
    token: &str,
    succeed_on_attempt: u32,
) -> TaskNode<String, workflow::retry_then_ok::Input> {
    suite
        .function(workflow::retry_then_ok::handle)
        .node()
        .node_id(id)
        .set(workflow::retry_then_ok::params::token(), token.to_owned())
        .and_then(|node| {
            node.set(
                workflow::retry_then_ok::params::succeed_on_attempt(),
                succeed_on_attempt,
            )
        })
        .expect("a retry input")
}

/// A node `id` of the recovery queue instance that runs `delay_ms`.
fn recovery_step(
    suite: &Suite,
    id: &str,
    delay_ms: u64,
) -> TaskNode<String, recovery::recovery_wf_step::Input> {
    suite
        .function(recovery::recovery_wf_step::handle)
        .node()
        .node_id(id)
        .set(recovery::recovery_wf_step::params::step(), id.to_owned())
        .and_then(|node| node.set(recovery::recovery_wf_step::params::delay_ms(), delay_ms))
        .expect("a recovery step input")
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

/// The nodes of a workflow once it ended with `status` within `limit`.
async fn ended_nodes_within<T>(
    suite: &Suite,
    handle: &WorkflowHandle<T>,
    status: &str,
    limit: Duration,
) -> Vec<NodeRow> {
    let client = suite.client().await;
    let ended = rows::wait_for_workflow_end(&client, handle.workflow_id(), limit).await;
    assert_eq!(ended, status, "the workflow status");
    rows::workflow_nodes(&client, handle.workflow_id()).await
}

/// The nodes of a workflow once it ended with `status`.
async fn ended_nodes<T>(suite: &Suite, handle: &WorkflowHandle<T>, status: &str) -> Vec<NodeRow> {
    ended_nodes_within(suite, handle, status, WORKFLOW_WAIT).await
}

fn statuses(nodes: &[NodeRow]) -> Vec<&str> {
    nodes.iter().map(|node| node.status.as_str()).collect()
}

/// The output of a workflow that completed.
async fn output<T: DeserializeOwned + Clone + std::fmt::Debug + 'static>(
    handle: &WorkflowHandle<T>,
) -> T {
    match handle
        .get(within(WORKFLOW_WAIT))
        .await
        .expect("read the workflow")
    {
        TaskOutcome::Succeeded(value) => value,
        TaskOutcome::Failed(error) => panic!("the workflow failed: {error:?}"),
    }
}

/// The failure code of a workflow read through its handle.
async fn failed_code<T: DeserializeOwned + Clone + std::fmt::Debug + 'static>(
    handle: &WorkflowHandle<T>,
) -> String {
    match handle
        .get(within(WORKFLOW_WAIT))
        .await
        .expect("read the workflow")
    {
        TaskOutcome::Failed(error) => error.code().text(),
        TaskOutcome::Succeeded(value) => panic!("the workflow succeeded with {value:?}"),
    }
}

/// Python `spec_pausable`: A (100 ms), B (200 ms), C (200 ms), D (100 ms),
/// each after the one before.
fn pausable(suite: &Suite) -> WorkflowSpecBuilder {
    let mut spec = builder("pausable");
    let a = spec.task(slow_step(suite, "a", 100));
    let b = spec.task(slow_step(suite, "b", 200).waits_for(a));
    let c = spec.task(slow_step(suite, "c", 200).waits_for(b));
    let _d = spec.task(slow_step(suite, "d", 100).waits_for(c));
    spec
}

/// Wait until node A of a pausable workflow completed while node D is
/// still PENDING: the workflow runs and has work left.
async fn wait_for_pause_point(client: &Client, workflow_id: Uuid) {
    poll::poll_until(
        WORKFLOW_WAIT,
        &format!("workflow {workflow_id} completes node A with node D pending"),
        async || {
            let nodes = rows::workflow_nodes(client, workflow_id).await;
            (nodes.len() == 4 && nodes[0].status == "COMPLETED" && nodes[3].status == "PENDING")
                .then_some(())
        },
    )
    .await;
}

/// Python `spec_on_error_pause`: A, then B that fails, then C; the workflow
/// pauses on a failure.
fn pausing_on_error(suite: &Suite) -> WorkflowSpecBuilder {
    let mut spec = builder("on_error_pause");
    let _policy = spec.on_error(OnError::Pause);
    let a = spec.task(step(suite, "a"));
    let b = spec.task(fail(suite, "b").waits_for(a));
    let _c = spec.task(step(suite, "c").waits_for(b));
    spec
}

/// Python `spec_linear`: A, B, C, each after the one before; the output is
/// C.
fn linear(suite: &Suite, name: &str) -> WorkflowSpecBuilder {
    let mut spec = builder(name);
    let a = spec.task(step(suite, "a"));
    let b = spec.task(step(suite, "b").waits_for(a));
    let c = spec.task(step(suite, "c").waits_for(b));
    let _output = spec.output(c);
    spec
}

/// Put an ended workflow back to RUNNING with its nodes as they are, as a
/// finalize that never committed leaves it (Python
/// `test_recovery_preserves_results`).
async fn unfinalize(client: &Client, workflow_id: Uuid) {
    let changed = client
        .execute(
            "UPDATE horsies_workflows SET status = 'RUNNING', completed_at = NULL WHERE id = $1",
            &[&workflow_id],
        )
        .await
        .unwrap_or_else(|error| panic!("put workflow {workflow_id} back to RUNNING: {error}"));
    assert_eq!(changed, 1, "workflow {workflow_id} has a row");
}

/// The task of node `task_index` of `workflow_id`, once the node has one.
async fn node_task(client: &Client, workflow_id: Uuid, task_index: i32) -> Uuid {
    poll::poll_until(
        WORKFLOW_WAIT,
        &format!("node {task_index} of workflow {workflow_id} gets its task"),
        async || {
            rows::workflow_node(client, workflow_id, task_index)
                .await
                .task_id
        },
    )
    .await
}

/// The child workflow of node `task_index` of `workflow_id`, once it has
/// one.
async fn child_workflow(client: &Client, workflow_id: Uuid, task_index: i32) -> Uuid {
    poll::poll_until(
        WORKFLOW_WAIT,
        &format!("node {task_index} of workflow {workflow_id} starts its child"),
        async || {
            rows::workflow_node(client, workflow_id, task_index)
                .await
                .sub_workflow_id
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

/// Python: `test_layer6_workflow_advanced.py::test_quorum_ctx_gating`. A and
/// B meet the quorum of two before C ends; D also waits for C, its context
/// node.
#[tokio::test]
#[serial]
async fn test_quorum_ctx_gating() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated4).await;
    let mut spec = builder("quorum_ctx_gating");
    let a = spec.task(slow_step(&suite, "a", 100));
    let b = spec.task(slow_step(&suite, "b", 150));
    let c = spec.task(slow_step(&suite, "c", 300));
    let _d = spec.task(
        suite
            .function(workflow::ctx_reader_str::handle)
            .node()
            .node_id("d")
            .set(workflow::ctx_reader_str::params::source(), "c".to_owned())
            .expect("a reader input")
            .waits_for_all([a, b, c])
            .workflow_ctx_from([c])
            .join_quorum(2),
    );
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED"; 4]);
    assert!(
        nodes[3].completed_at >= nodes[2].completed_at,
        "D ended after C: {nodes:?}"
    );
    let read = handle
        .result_for::<String, workflow::WfCode>(&NodeKey::new("d"))
        .await
        .expect("read node d");
    assert_eq!(read, TaskOutcome::Succeeded("completed_c".to_owned()));
}

/// Python: `test_layer6_workflow_advanced.py::test_quorum_impossible_skips`.
/// Two of three dependencies fail, so a quorum of three cannot be met.
#[tokio::test]
#[serial]
async fn test_quorum_impossible_skips() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder("quorum_impossible");
    let a = spec.task(fail(&suite, "a"));
    let b = spec.task(fail(&suite, "b"));
    let c = spec.task(step(&suite, "c"));
    let _d = spec.task(
        step(&suite, "d")
            .waits_for(a)
            .waits_for(b)
            .waits_for(c)
            .join_quorum(3),
    );
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(
        statuses(&nodes),
        vec!["FAILED", "FAILED", "COMPLETED", "SKIPPED"]
    );
}

/// Python: `test_layer6_workflow_advanced.py::test_join_any_all_deps_fail_skips`.
#[tokio::test]
#[serial]
async fn test_join_any_all_deps_fail_skips() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder("join_any_all_fail");
    let a = spec.task(fail(&suite, "a"));
    let b = spec.task(fail(&suite, "b"));
    let _c = spec.task(step(&suite, "c").waits_for_all([a, b]).join_any());
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(statuses(&nodes), vec!["FAILED", "FAILED", "SKIPPED"]);
}

// ---------------------------------------------------------------------------
// Pause, resume and cancel
// ---------------------------------------------------------------------------

/// Python: `test_layer6_workflow_advanced.py::test_pause_blocks_ready_transitions`.
#[tokio::test]
#[serial]
async fn test_pause_blocks_ready_transitions() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<OutputlessResults>(&suite, pausable(&suite)).await;
    let workflow_id = handle.workflow_id();
    wait_for_pause_point(&client, workflow_id).await;
    assert!(handle.pause().await.expect("pause"), "the workflow paused");
    assert_eq!(
        rows::workflow_status(&client, workflow_id).await.as_deref(),
        Some("PAUSED")
    );
    poll::never_during(
        PAUSED_WINDOW,
        "the paused workflow leaves PAUSED",
        async || rows::workflow_status(&client, workflow_id).await.as_deref() != Some("PAUSED"),
    )
    .await;
}

/// Python: `test_layer6_workflow_advanced.py::test_resume_continues_workflow`.
#[tokio::test]
#[serial]
async fn test_resume_continues_workflow() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<OutputlessResults>(&suite, pausable(&suite)).await;
    let workflow_id = handle.workflow_id();
    wait_for_pause_point(&client, workflow_id).await;
    assert!(handle.pause().await.expect("pause"), "the workflow paused");
    assert_eq!(
        rows::workflow_status(&client, workflow_id).await.as_deref(),
        Some("PAUSED")
    );
    assert!(
        handle.resume().await.expect("resume"),
        "the workflow resumed"
    );
    let resumed = rows::workflow_status(&client, workflow_id).await;
    assert!(
        matches!(resumed.as_deref(), Some("RUNNING" | "COMPLETED")),
        "the workflow left PAUSED: {resumed:?}"
    );
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED"; 4]);
}

/// Python: `test_layer6_workflow_advanced.py::test_pause_then_cancel`. The
/// node that was PENDING at the pause is skipped.
#[tokio::test]
#[serial]
async fn test_pause_then_cancel() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<OutputlessResults>(&suite, pausable(&suite)).await;
    wait_for_pause_point(&client, handle.workflow_id()).await;
    assert!(handle.pause().await.expect("pause"), "the workflow paused");
    handle.cancel().await.expect("cancel the workflow");
    let nodes = ended_nodes(&suite, &handle, "CANCELLED").await;
    assert_eq!(nodes[3].status, "SKIPPED", "node D: {nodes:?}");
    assert_eq!(failed_code(&handle).await, "WORKFLOW_CANCELLED");
}

/// Python: `test_layer6_workflow_advanced.py::test_pause_idempotent`.
#[tokio::test]
#[serial]
async fn test_pause_idempotent() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<OutputlessResults>(&suite, pausable(&suite)).await;
    wait_for_pause_point(&client, handle.workflow_id()).await;
    assert!(handle.pause().await.expect("pause"), "the first pause");
    assert!(
        !handle.pause().await.expect("pause again"),
        "a second pause of a PAUSED workflow returns false"
    );
}

/// Python: `test_layer6_workflow_advanced.py::test_resume_on_running_noop`.
#[tokio::test]
#[serial]
async fn test_resume_on_running_noop() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<OutputlessResults>(&suite, pausable(&suite)).await;
    rows::wait_for_workflow_status(&client, handle.workflow_id(), "RUNNING", WORKFLOW_WAIT).await;
    assert!(
        !handle.resume().await.expect("resume"),
        "a resume of a RUNNING workflow returns false"
    );
}

/// Python: `test_layer6_workflow_advanced.py::test_on_error_pause_stops_workflow`.
#[tokio::test]
#[serial]
async fn test_on_error_pause_stops_workflow() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<OutputlessResults>(&suite, pausing_on_error(&suite)).await;
    rows::wait_for_workflow_status(&client, handle.workflow_id(), "PAUSED", WORKFLOW_WAIT).await;
    let nodes = rows::workflow_nodes(&client, handle.workflow_id()).await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED", "FAILED", "PENDING"]);
    assert_eq!(failed_code(&handle).await, "WORKFLOW_PAUSED");
}

/// Python: `test_layer6_workflow_advanced.py::test_on_error_pause_resume_completes`.
/// The resume skips C, whose dependency failed, and the workflow fails.
#[tokio::test]
#[serial]
async fn test_on_error_pause_resume_completes() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<OutputlessResults>(&suite, pausing_on_error(&suite)).await;
    rows::wait_for_workflow_status(&client, handle.workflow_id(), "PAUSED", WORKFLOW_WAIT).await;
    assert!(
        handle.resume().await.expect("resume"),
        "the workflow resumed"
    );
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED", "FAILED", "SKIPPED"]);
}

/// Python: `test_layer7_subworkflows.py::test_subworkflow_cancel_running_child_cascade`.
/// The cancel of a workflow cancels its running child workflow.
#[tokio::test]
#[serial]
async fn test_cancel_cascades_to_child_workflow() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = builder("cancel_running_child_cascade");
    let child = spec.sub_workflow(
        subworkflow::SubSlow::node(SubWorkflowParams::Given(subworkflow::SlowParams {
            value: "cancel_me".to_owned(),
            delay_ms: 2_000,
        }))
        .expect("a slow child node")
        .node_id("l7_cancel_running_child"),
    );
    let _output = spec.output(child);
    let handle = start::<String>(&suite, spec).await;
    let child_id = child_workflow(&client, handle.workflow_id(), 0).await;
    rows::wait_for_workflow_status(&client, child_id, "RUNNING", WORKFLOW_WAIT).await;
    handle.cancel().await.expect("cancel the workflow");
    let _nodes = ended_nodes(&suite, &handle, "CANCELLED").await;
    rows::wait_for_workflow_status(&client, child_id, "CANCELLED", WORKFLOW_WAIT).await;
}

// ---------------------------------------------------------------------------
// Success policies
// ---------------------------------------------------------------------------

/// Python: `test_layer6_workflow_advanced.py::test_success_policy_satisfied`.
/// A is required, B is optional and fails: the workflow completes.
#[tokio::test]
#[serial]
async fn test_success_policy_satisfied() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder("success_policy_satisfied");
    let a = spec.task(mark(&suite, "a", "required_A"));
    let b = spec.task(fail_with(&suite, "b", "OPTIONAL_FAIL"));
    let _policy =
        spec.success_policy(SuccessPolicy::new(vec![SuccessCase::new([a])]).optional([b]));
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED", "FAILED"]);
}

/// A and B are both required; B fails with `REQUIRED_FAIL`.
fn policy_not_met(suite: &Suite) -> WorkflowSpecBuilder {
    let mut spec = builder("success_policy_not_met");
    let a = spec.task(mark(suite, "a", "required_A"));
    let b = spec.task(fail_with(suite, "b", "REQUIRED_FAIL"));
    let _policy = spec.success_policy(SuccessPolicy::new(vec![SuccessCase::new([
        a.untyped(),
        b.untyped(),
    ])]));
    spec
}

/// Python: `test_layer6_workflow_advanced.py::test_success_policy_not_met`.
#[tokio::test]
#[serial]
async fn test_success_policy_not_met() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = start::<OutputlessResults>(&suite, policy_not_met(&suite)).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED", "FAILED"]);
}

/// Python: `test_layer6_workflow_advanced.py::test_success_policy_not_met_error_content`.
/// The workflow error is the error of the failed required node.
#[tokio::test]
#[serial]
async fn test_success_policy_not_met_error_content() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = start::<OutputlessResults>(&suite, policy_not_met(&suite)).await;
    let _nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(failed_code(&handle).await, "REQUIRED_FAIL");
}

/// Python: `test_layer6_workflow_advanced.py::test_success_policy_multiple_cases`.
/// The first case requires A, which fails; the second requires B, which
/// completes.
#[tokio::test]
#[serial]
async fn test_success_policy_multiple_cases() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder("success_policy_multi");
    let a = spec.task(fail_with(&suite, "a", "CASE1_FAIL"));
    let b = spec.task(mark(&suite, "b", "case2_ok"));
    let _policy = spec.success_policy(SuccessPolicy::new(vec![
        SuccessCase::new([a]),
        SuccessCase::new([b]),
    ]));
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(statuses(&nodes), vec!["FAILED", "COMPLETED"]);
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

/// Python: `test_layer6_workflow_advanced.py::test_recovery_preserves_results`.
/// A COMPLETED workflow put back to RUNNING is finalized again by the
/// reaper's workflow recovery, with its output.
#[tokio::test]
#[serial]
async fn test_recovery_preserves_results() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let handle = start::<String>(&suite, linear(&suite, "recovery_linear")).await;
    let _nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(output(&handle).await, "completed_c");
    unfinalize(&client, handle.workflow_id()).await;
    rows::wait_for_workflow_status(&client, handle.workflow_id(), "COMPLETED", RECOVERY_WAIT).await;
    assert_eq!(output(&handle).await, "completed_c");
}

/// Python: `test_layer6_workflow_advanced.py::test_recovery_preserves_failed_state`.
#[tokio::test]
#[serial]
async fn test_recovery_preserves_failed_state() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = builder("recovery_linear_fail_mid");
    let a = spec.task(step(&suite, "a"));
    let b = spec.task(fail(&suite, "b").waits_for(a));
    let c = spec.task(step(&suite, "c").waits_for(b));
    let _output = spec.output(c);
    let handle = start::<String>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "FAILED").await;
    unfinalize(&client, handle.workflow_id()).await;
    rows::wait_for_workflow_status(&client, handle.workflow_id(), "FAILED", RECOVERY_WAIT).await;
    assert_eq!(failed_code(&handle).await, "DELIBERATE_FAILURE");
}

/// Python: `clients/python/tests/integration/test_workflow_recovery.py::test_recover_failed_recomputes_first_failed_error`.
/// A workflow put back to RUNNING with the error of its second failed node
/// is finalized with the error of its first failed node by index.
#[tokio::test]
#[serial]
async fn test_recovery_recomputes_first_failed_error() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = builder("recovery_first_error");
    let _first = spec.task(fail_with(&suite, "first", "FIRST_ERROR"));
    let _second = spec.task(fail_with(&suite, "second", "SECOND_ERROR"));
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let workflow_id = handle.workflow_id();
    let _nodes = ended_nodes(&suite, &handle, "FAILED").await;
    let changed = client
        .execute(
            "UPDATE horsies_workflows SET status = 'RUNNING', completed_at = NULL, \
             result = NULL, error = (SELECT (result::jsonb -> 'err')::text \
             FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 1) \
             WHERE id = $1",
            &[&workflow_id],
        )
        .await
        .unwrap_or_else(|error| panic!("store the later error on {workflow_id}: {error}"));
    assert_eq!(changed, 1, "workflow {workflow_id} has a row");
    let stale = rows::workflow_error(&client, workflow_id).await;
    assert!(
        stale
            .as_deref()
            .is_some_and(|error| error.contains("SECOND_ERROR")),
        "the stored error is the later one: {stale:?}"
    );
    rows::wait_for_workflow_status(&client, workflow_id, "FAILED", RECOVERY_WAIT).await;
    assert_eq!(failed_code(&handle).await, "FIRST_ERROR");
    let recomputed = rows::workflow_error(&client, workflow_id).await;
    assert!(
        recomputed
            .as_deref()
            .is_some_and(|error| !error.contains("SECOND_ERROR")),
        "the later error was replaced: {recomputed:?}"
    );
}

/// Python: `test_layer6_workflow_advanced.py::test_workflow_recovers_after_worker_crash`.
/// The worker dies while C and D run. The reaper of the next worker fails
/// them with `WORKER_CRASHED`; E, which allows failed dependencies, and F
/// then run on the `recovery` queue, and the workflow fails.
#[tokio::test]
#[serial]
async fn test_workflow_recovers_after_worker_crash() {
    let suite = Suite::start(BIN, Instance::RecoveryQueues).await;
    let client = suite.client().await;
    let mut crashed = suite.worker(Profile::Isolated2).await;
    let mut spec = builder("recovery_crash");
    let a = spec.task(recovery_step(&suite, "a", 50));
    let b = spec.task(recovery_step(&suite, "b", 50).waits_for(a));
    let c = spec.task(recovery_step(&suite, "c", 60_000).waits_for(b));
    let d = spec.task(recovery_step(&suite, "d", 60_000).waits_for(b));
    let e = spec.task(
        recovery_step(&suite, "e", 50)
            .waits_for_all([c, d])
            .allow_failed_deps(true)
            .queue("recovery"),
    );
    let _f = spec.task(
        recovery_step(&suite, "f", 50)
            .waits_for(e)
            .queue("recovery"),
    );
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let workflow_id = handle.workflow_id();
    rows::wait_for_node_status(&client, workflow_id, 1, "COMPLETED", WORKFLOW_WAIT).await;
    for task_index in [2, 3] {
        let task_id = node_task(&client, workflow_id, task_index).await;
        rows::wait_for_task_status(&client, task_id, "RUNNING", WORKFLOW_WAIT).await;
    }
    crashed.kill_group();
    let _exit = crashed.wait_exit(Duration::from_secs(5)).await;
    let _recovering = suite.worker(Profile::Isolated2).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(
        statuses(&nodes),
        vec![
            "COMPLETED",
            "COMPLETED",
            "FAILED",
            "FAILED",
            "COMPLETED",
            "COMPLETED"
        ]
    );
    for node_id in ["c", "d"] {
        let read = handle
            .result_for::<String, Infallible>(&NodeKey::new(node_id))
            .await
            .unwrap_or_else(|error| panic!("read node {node_id}: {error}"));
        match read {
            TaskOutcome::Failed(error) => {
                assert_eq!(error.code().text(), "WORKER_CRASHED", "node {node_id}");
            }
            TaskOutcome::Succeeded(value) => panic!("node {node_id} succeeded with {value}"),
        }
    }
    for node in &nodes[4..] {
        let task_id = node.task_id.expect("a node that ran has its task");
        let task = rows::read_task(&client, task_id)
            .await
            .unwrap_or_else(|| panic!("node {} has no task row", node.task_index));
        assert_eq!(task.queue_name, "recovery", "node {}", node.task_index);
    }
}

// ---------------------------------------------------------------------------
// Retries
// ---------------------------------------------------------------------------

/// Python: `test_layer6_workflow_advanced.py::test_workflow_task_retries`. The
/// node carries no options: its retry policy is the one of the task's
/// registration.
#[tokio::test]
#[serial]
async fn test_workflow_task_retries() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder("retry_test");
    let a = spec.task(retry_then_ok(&suite, "a", "wf-retry-ok", 2));
    let _output = spec.output(a);
    let handle = start::<String>(&suite, spec).await;
    let nodes = ended_nodes_within(&suite, &handle, "COMPLETED", RETRY_WAIT).await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED"]);
    assert_eq!(output(&handle).await, "succeeded_on_attempt_2");
    assert_eq!(suite.markers.count("wf-retry-ok-attempt-"), 2);
}

/// Python: `test_layer6_workflow_advanced.py::test_workflow_task_retries_exhausted`.
/// One attempt and three retries, then the node and the workflow fail.
#[tokio::test]
#[serial]
async fn test_workflow_task_retries_exhausted() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder("retry_exhausted");
    let a = spec.task(retry_then_ok(&suite, "a", "wf-retry-exhausted", 10));
    let _output = spec.output(a);
    let handle = start::<String>(&suite, spec).await;
    let nodes = ended_nodes_within(&suite, &handle, "FAILED", RETRY_WAIT).await;
    assert_eq!(statuses(&nodes), vec!["FAILED"]);
    assert_eq!(failed_code(&handle).await, "RETRY_NEEDED");
    assert_eq!(suite.markers.count("wf-retry-exhausted-attempt-"), 4);
}

// ---------------------------------------------------------------------------
// Sub-workflow params
// ---------------------------------------------------------------------------

/// The old Rust `test_subworkflow_mixed_explicit_and_args_from`: one
/// sub-workflow node gives its child a static param and the result of an
/// upstream node (Python `test_layer7_subworkflows.py`,
/// `test_subworkflow_static_kwargs_preserve_build_with_types` and
/// `test_subworkflow_build_with_args_from_receives_typed_task_result`, in
/// one node).
#[tokio::test]
#[serial]
async fn test_subworkflow_mixed_explicit_and_args_from() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder("subworkflow_mixed_input");
    let source: TypedNodeRef<i64> = spec.task(produce(&suite, "produce", 21));
    let child = spec.sub_workflow(
        subworkflow::SubLabel::node(SubWorkflowParams::NotGiven)
            .expect("a label child node")
            .node_id("child")
            .set(subworkflow::LabelParams::field_label(), "count".to_owned())
            .expect("a label param")
            .arg_from(subworkflow::LabelParams::field_value(), source),
    );
    let _output = spec.output(child);
    let handle = start::<String>(&suite, spec).await;
    assert_eq!(output(&handle).await, "count=21");
}
