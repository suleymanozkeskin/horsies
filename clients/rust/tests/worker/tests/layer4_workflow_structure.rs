//! Layer 4: workflow structure (Python
//! `clients/python/tests/e2e/test_layer4_workflow_structure.py`). The specs are built in
//! the test process with the typed DSL and started with `Horsies::start`;
//! the worker runs their nodes.

use std::time::Duration;

use chrono::Utc;
use horsies::{
    HorsiesError, OutputlessResults, TaskNode, TaskOutcome, TypedNodeRef, WorkflowHandle,
    WorkflowSpecBuilder,
};
use horsies_test_support::poll;
use horsies_test_support::rows::{self, NodeRow};
use horsies_test_worker::harness::{within, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::tasks::workflow;
use serde::de::DeserializeOwned;
use serial_test::serial;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for a workflow to end.
const WORKFLOW_WAIT: Duration = Duration::from_secs(30);

fn builder(suite: &Suite, name: &str) -> WorkflowSpecBuilder {
    let mut builder = suite.app.workflow_builder(name);
    let _key = builder.definition_key(format!("tests.e2e.{name}.v1"));
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

/// A step node `id` that returns `completed_<id>`.
fn step(suite: &Suite, id: &str) -> TaskNode<String, workflow::step::Input> {
    suite
        .function(workflow::step::handle)
        .node()
        .node_id(id)
        .set(workflow::step::params::step(), id.to_owned())
        .expect("a step input")
}

/// A slow step node `id` that runs `delay_ms`.
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

/// A node `id` that fails with `DELIBERATE_FAILURE`.
fn fail(suite: &Suite, id: &str) -> TaskNode<i64, ()> {
    suite.function(workflow::fail::handle).node().node_id(id)
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

fn statuses(nodes: &[NodeRow]) -> Vec<&str> {
    nodes.iter().map(|node| node.status.as_str()).collect()
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
        TaskOutcome::Failed(error) => error.code().as_str().to_owned(),
        TaskOutcome::Succeeded(value) => panic!("the workflow succeeded with {value:?}"),
    }
}

/// Python: `test_layer4_workflow_structure.py::test_linear_workflow`.
#[tokio::test]
#[serial]
async fn test_linear_workflow() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "linear");
    let a = add(&mut spec, step(&suite, "a"));
    let b = add(&mut spec, step(&suite, "b").waits_for(a));
    let c = add(&mut spec, step(&suite, "c").waits_for(b));
    let _output = spec.output(c);
    let handle = start::<String>(&suite, spec).await;
    let outcome = handle
        .get(within(WORKFLOW_WAIT))
        .await
        .expect("read the workflow");
    assert_eq!(outcome, TaskOutcome::Succeeded("completed_c".to_owned()));
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert_eq!(statuses(&nodes), vec!["COMPLETED"; 3]);
    assert!(nodes[0].started_at < nodes[1].started_at);
    assert!(nodes[1].started_at < nodes[2].started_at);
}

/// Python: `test_layer4_workflow_structure.py::test_fanout_parallel_execution`.
#[tokio::test]
#[serial]
async fn test_fanout_parallel_execution() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated4).await;
    let mut spec = builder(&suite, "fanout");
    let root = add(&mut spec, step(&suite, "root"));
    for id in ["b", "c", "d"] {
        let _branch = add(&mut spec, slow_step(&suite, id, 1_000).waits_for(root));
    }
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    let branches = &nodes[1..];
    let latest_start = branches.iter().map(|node| node.started_at).max().flatten();
    let first_end = branches
        .iter()
        .map(|node| node.completed_at)
        .min()
        .flatten();
    assert!(
        latest_start < first_end,
        "the branches ran at once: {branches:?}"
    );
}

/// Python: `test_layer4_workflow_structure.py::test_fanin_waits_for_all`.
#[tokio::test]
#[serial]
async fn test_fanin_waits_for_all() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated4).await;
    let mut spec = builder(&suite, "fanin");
    let sources: Vec<_> = [("a", 200), ("b", 400), ("c", 600)]
        .into_iter()
        .map(|(id, delay)| add(&mut spec, slow_step(&suite, id, delay)))
        .collect();
    let aggregate = add(
        &mut spec,
        step(&suite, "aggregate").waits_for_all(sources.iter().copied()),
    );
    let _output = spec.output(aggregate);
    let handle = start::<String>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    let last_source_end = nodes[..3]
        .iter()
        .map(|node| node.completed_at)
        .max()
        .flatten();
    assert!(
        nodes[3].started_at > last_source_end,
        "the aggregate waited: {nodes:?}"
    );
}

/// Python: `test_layer4_workflow_structure.py::test_diamond_structure`.
#[tokio::test]
#[serial]
async fn test_diamond_structure() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let mut spec = builder(&suite, "diamond");
    let a = add(&mut spec, step(&suite, "a"));
    let b = add(&mut spec, step(&suite, "b").waits_for(a));
    let c = add(&mut spec, step(&suite, "c").waits_for(a));
    let d = add(&mut spec, step(&suite, "d").waits_for(b).waits_for(c));
    let _output = spec.output(d);
    let handle = start::<String>(&suite, spec).await;
    let outcome = handle
        .get(within(WORKFLOW_WAIT))
        .await
        .expect("read the workflow");
    assert_eq!(outcome, TaskOutcome::Succeeded("completed_d".to_owned()));
    let nodes = ended_nodes(&suite, &handle, "COMPLETED").await;
    assert!(nodes[3].started_at > nodes[1].completed_at.max(nodes[2].completed_at));
}

/// Python: `test_layer4_workflow_structure.py::test_single_node_workflow`.
#[tokio::test]
#[serial]
async fn test_single_node_workflow() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "single");
    let only = add(&mut spec, step(&suite, "only"));
    let _output = spec.output(only);
    let handle = start::<String>(&suite, spec).await;
    let outcome = handle
        .get(within(WORKFLOW_WAIT))
        .await
        .expect("read the workflow");
    assert_eq!(outcome, TaskOutcome::Succeeded("completed_only".to_owned()));
}

/// Python: `test_layer4_workflow_structure.py::test_linear_failure_cascades_to_skip`.
#[tokio::test]
#[serial]
async fn test_linear_failure_cascades_to_skip() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "linear_fail_first");
    let a = add(&mut spec, fail(&suite, "a"));
    let b = add(&mut spec, step(&suite, "b").waits_for(a));
    let _c = add(&mut spec, step(&suite, "c").waits_for(b));
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(statuses(&nodes), vec!["FAILED", "SKIPPED", "SKIPPED"]);
}

/// Python: `test_layer4_workflow_structure.py::test_handle_get_returns_error_on_failed_workflow`.
#[tokio::test]
#[serial]
async fn test_workflow_fails_with_error() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "linear_fail_mid");
    let a = add(&mut spec, step(&suite, "a"));
    let b = add(&mut spec, fail(&suite, "b").waits_for(a));
    let c = add(&mut spec, step(&suite, "c").waits_for(b));
    let _output = spec.output(c);
    let handle = start::<String>(&suite, spec).await;
    let _nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(failed_code(&handle).await, "DELIBERATE_FAILURE");
}

/// Python: `test_layer4_workflow_structure.py::test_fanout_one_branch_fails`.
#[tokio::test]
#[serial]
async fn test_fanout_one_branch_fails() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated4).await;
    let mut spec = builder(&suite, "fanout_one_fail");
    let root = add(&mut spec, step(&suite, "root"));
    let _b = add(&mut spec, fail(&suite, "b").waits_for(root));
    let _c = add(&mut spec, step(&suite, "c").waits_for(root));
    let _d = add(&mut spec, step(&suite, "d").waits_for(root));
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(
        statuses(&nodes),
        vec!["COMPLETED", "FAILED", "COMPLETED", "COMPLETED"]
    );
}

/// Python: `test_layer4_workflow_structure.py::test_fanin_one_dep_fails_skips_aggregator`.
#[tokio::test]
#[serial]
async fn test_fanin_one_dep_fails_skips_aggregator() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated4).await;
    let mut spec = builder(&suite, "fanin_one_fail");
    let a = add(&mut spec, fail(&suite, "a"));
    let b = add(&mut spec, step(&suite, "b"));
    let c = add(&mut spec, step(&suite, "c"));
    let _aggregate = add(
        &mut spec,
        step(&suite, "aggregate")
            .waits_for(a)
            .waits_for(b)
            .waits_for(c),
    );
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(
        statuses(&nodes),
        vec!["FAILED", "COMPLETED", "COMPLETED", "SKIPPED"]
    );
    assert!(nodes[3].started_at.is_none(), "the aggregate never started");
    assert!(nodes[3].completed_at.is_some(), "the skip has its time");
}

/// Python: `test_layer4_workflow_structure.py::test_workflow_cancellation`.
#[tokio::test]
#[serial]
async fn test_workflow_cancellation() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = builder(&suite, "slow_linear");
    let a = add(&mut spec, slow_step(&suite, "a", 500));
    let b = add(&mut spec, slow_step(&suite, "b", 3_000).waits_for(a));
    let c = add(&mut spec, slow_step(&suite, "c", 3_000).waits_for(b));
    let _output = spec.output(c);
    let handle = start::<String>(&suite, spec).await;
    rows::wait_for_node_status(&client, handle.workflow_id(), 0, "COMPLETED", WORKFLOW_WAIT).await;
    handle.cancel().await.expect("cancel the workflow");
    let nodes = ended_nodes(&suite, &handle, "CANCELLED").await;
    for node in &nodes[1..] {
        assert!(
            ["SKIPPED", "COMPLETED", "RUNNING"].contains(&node.status.as_str()),
            "node {} is {}",
            node.task_index,
            node.status
        );
    }
    assert_eq!(failed_code(&handle).await, "WORKFLOW_CANCELLED");
}

/// The reaper interval of the suite's instance: the worker runs a reaper
/// pass at its start, then one each `check_interval_ms`.
fn reaper_interval(suite: &Suite) -> Duration {
    Duration::from_millis(suite.config().recovery.check_interval_ms)
}

/// The age that a phase-2 record of an expired workflow task must reach
/// before a reaper pass recovers its node and ends its workflow
/// (`crashed_worker_recovery_grace_ms`).
fn phase2_grace(suite: &Suite) -> Duration {
    Duration::from_millis(suite.config().recovery.crashed_worker_recovery_grace_ms)
}

/// The time on top of a reaper interval for one reaper step to show: the
/// pass itself, the write that the step waits for, and the polls of the
/// wait.
const REAPER_PASS_MARGIN: Duration = Duration::from_secs(10);

/// Python: `test_layer4_workflow_structure.py::test_workflow_task_expires_before_claim`.
/// The second node's deadline passes while the first node holds the only
/// child, so the task of the second node is PENDING with a passed deadline
/// when the first node completes. A claim never takes such a row: the
/// pending expiry of the next reaper pass ends the task, at most one reaper
/// interval later. The pass that recovers its phase-2 record, at least
/// `phase2_grace` after the expiry, fails the node and the workflow.
#[tokio::test]
#[serial]
async fn test_workflow_task_expired_while_claimed_before_start() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    let mut spec = builder(&suite, "expiring_node");
    let a = add(&mut spec, slow_step(&suite, "a", 3_000));
    let b = add(
        &mut spec,
        step(&suite, "b")
            .waits_for(a)
            .good_until(Utc::now() + chrono::Duration::seconds(1)),
    );
    let _output = spec.output(b);
    let handle = start::<String>(&suite, spec).await;
    let expiring = poll::poll_until(WORKFLOW_WAIT, "node b gets its task", async || {
        rows::workflow_node(&client, handle.workflow_id(), 1)
            .await
            .task_id
    })
    .await;
    rows::wait_for_task_status(
        &client,
        expiring,
        "EXPIRED",
        reaper_interval(&suite) + REAPER_PASS_MARGIN,
    )
    .await;
    let expired = rows::read_task(&client, expiring)
        .await
        .expect("the task of node b");
    assert_eq!(expired.error_code.as_deref(), Some("TASK_EXPIRED"));
    assert_eq!(
        (expired.claimed_at, expired.worker_id, expired.started_at),
        (None, None, None),
        "no claim took the task of node b"
    );
    let ended_by: String = client
        .query_one(
            "SELECT terminalization_kind::text FROM horsies_task_history WHERE task_id = $1",
            &[&expiring],
        )
        .await
        .unwrap_or_else(|error| panic!("read how task {expiring} ended: {error}"))
        .get(0);
    assert_eq!(
        ended_by, "EXPIRE_PENDING",
        "the pending expiry of the reaper ended the task of node b"
    );
    let ended = rows::wait_for_workflow_end(
        &client,
        handle.workflow_id(),
        phase2_grace(&suite) + reaper_interval(&suite) + REAPER_PASS_MARGIN,
    )
    .await;
    assert_eq!(ended, "FAILED", "the workflow status");
    let nodes = rows::workflow_nodes(&client, handle.workflow_id()).await;
    assert_eq!(nodes[0].status, "COMPLETED");
    assert_eq!(nodes[1].status, "FAILED");
    assert_eq!(failed_code(&handle).await, "TASK_EXPIRED");
}
