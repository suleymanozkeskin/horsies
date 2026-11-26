//! Layer 5: workflow data flow, `args_from` and the workflow context
//! (Python `clients/python/tests/e2e/test_layer5_workflow_dataflow.py`), and the strict
//! node input of the old `wrapped_input_strict.rs`.

use std::collections::BTreeMap;
use std::time::Duration;

use horsies::{
    HorsiesError, OutputlessResults, TaskNode, TaskOutcome, TypedNodeRef, WorkflowHandle,
    WorkflowSpecBuilder,
};
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

/// A node `id` that returns `value`.
fn produce(suite: &Suite, id: &str, value: i64) -> TaskNode<i64, workflow::produce_int::Input> {
    suite
        .function(workflow::produce_int::handle)
        .node()
        .node_id(id)
        .set(workflow::produce_int::params::value(), value)
        .expect("a produce input")
}

/// A node `id` that doubles the result of `source`.
fn double(
    suite: &Suite,
    id: &str,
    source: TypedNodeRef<i64>,
) -> TaskNode<i64, workflow::double::Input> {
    suite
        .function(workflow::double::handle)
        .node()
        .node_id(id)
        .arg_from(workflow::double::params::input_result(), source)
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

/// Python: `test_layer5_workflow_dataflow.py::test_args_from_single_dependency`.
#[tokio::test]
#[serial]
async fn test_args_from_single_dependency() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "args_from_single");
    let a = add(&mut spec, produce(&suite, "a", 5));
    let b = add(&mut spec, double(&suite, "b", a));
    let _output = spec.output(b);
    let handle = start::<i64>(&suite, spec).await;
    assert_eq!(output(&handle).await, 10);
}

/// Python: `test_layer5_workflow_dataflow.py::test_args_from_multiple_dependencies`.
#[tokio::test]
#[serial]
async fn test_args_from_multiple_dependencies() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let mut spec = builder(&suite, "args_from_multiple");
    let a = add(&mut spec, produce(&suite, "a", 3));
    let b = add(&mut spec, produce(&suite, "b", 4));
    let sum = add(
        &mut spec,
        suite
            .function(workflow::sum_two::handle)
            .node()
            .node_id("sum")
            .arg_from(workflow::sum_two::params::first(), a)
            .arg_from(workflow::sum_two::params::second(), b),
    );
    let _output = spec.output(sum);
    let handle = start::<i64>(&suite, spec).await;
    assert_eq!(output(&handle).await, 7);
}

/// Python: `test_layer5_workflow_dataflow.py::test_failed_propagation`. With
/// `allow_failed_dependencies`, the node runs and reads the upstream failure.
#[tokio::test]
#[serial]
async fn test_failed_propagation_via_args_from() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "failed_propagation");
    let a = add(&mut spec, fail(&suite, "a"));
    let b = add(
        &mut spec,
        double(&suite, "b", a).allow_failed_dependencies(true),
    );
    let _output = spec.output(b);
    let handle = start::<i64>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(statuses(&nodes), vec!["FAILED", "FAILED"]);
    assert!(
        nodes[1].started_at.is_some(),
        "the node ran with the failure"
    );
    let read = handle
        .result_for::<i64, workflow::WfCode>(&horsies::NodeKey::new("b"))
        .await
        .expect("read node b");
    match read {
        TaskOutcome::Failed(error) => assert_eq!(error.code().as_str(), "DELIBERATE_FAILURE"),
        TaskOutcome::Succeeded(value) => panic!("node b succeeded with {value}"),
    }
}

/// Python: `test_layer5_workflow_dataflow.py::test_skipped_propagation`.
/// A node that allows failed dependencies reads a skipped upstream as
/// `UPSTREAM_SKIPPED`.
#[tokio::test]
#[serial]
async fn test_skipped_propagation() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "skipped_propagation");
    let a = add(&mut spec, fail(&suite, "a"));
    let b = add(&mut spec, double(&suite, "b", a));
    let c = add(
        &mut spec,
        suite
            .function(workflow::check_upstream_skipped::handle)
            .node()
            .node_id("c")
            .arg_from(workflow::check_upstream_skipped::params::input_result(), b)
            .allow_failed_dependencies(true),
    );
    let _output = spec.output(c);
    let handle = start::<String>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(statuses(&nodes), vec!["FAILED", "SKIPPED", "COMPLETED"]);
    let read = handle
        .result_for::<String, workflow::WfCode>(&horsies::NodeKey::new("c"))
        .await
        .expect("read node c");
    assert_eq!(
        read,
        TaskOutcome::Succeeded("received_upstream_skipped".to_owned())
    );
}

/// Python: `test_layer5_workflow_dataflow.py::test_args_from_transitive_chain`.
#[tokio::test]
#[serial]
async fn test_args_from_transitive_chain() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "transitive_chain");
    let a = add(&mut spec, produce(&suite, "a", 2));
    let b = add(&mut spec, double(&suite, "b", a));
    let c = add(&mut spec, double(&suite, "c", b));
    let _output = spec.output(c);
    let handle = start::<i64>(&suite, spec).await;
    assert_eq!(output(&handle).await, 8);
}

/// Python: `test_layer5_workflow_dataflow.py::test_args_from_failed_dep_skips_and_fails_workflow`.
#[tokio::test]
#[serial]
async fn test_args_from_failed_dep_skips_and_fails() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "failed_dep_skips");
    let a = add(&mut spec, fail(&suite, "a"));
    let _b = add(&mut spec, double(&suite, "b", a));
    let handle = start::<OutputlessResults>(&suite, spec).await;
    let nodes = ended_nodes(&suite, &handle, "FAILED").await;
    assert_eq!(statuses(&nodes), vec!["FAILED", "SKIPPED"]);
    assert!(nodes[1].started_at.is_none(), "the skipped node never ran");
}

/// Python: `test_layer5_workflow_dataflow.py::test_dual_injection_same_node`.
/// One upstream reaches a node through `args_from` and through the context.
#[tokio::test]
#[serial]
async fn test_dual_injection_same_node() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "dual_injection");
    let a = add(&mut spec, produce(&suite, "a", 5));
    let reader = add(
        &mut spec,
        suite
            .function(workflow::mixed_reader::handle)
            .node()
            .node_id("reader")
            .arg_from(workflow::mixed_reader::params::from_args(), a)
            .set(workflow::mixed_reader::params::ctx_source(), "a".to_owned())
            .expect("a reader input")
            .workflow_context_from([a]),
    );
    let _output = spec.output(reader);
    let handle = start::<BTreeMap<String, i64>>(&suite, spec).await;
    let value = output(&handle).await;
    assert_eq!(
        value,
        BTreeMap::from([("from_args".to_owned(), 5), ("from_ctx".to_owned(), 5)])
    );
}

/// Python: `test_layer5_workflow_dataflow.py::test_workflow_ctx_single_dependency`.
#[tokio::test]
#[serial]
async fn test_workflow_ctx_single_dependency() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "ctx_single");
    let a = add(&mut spec, produce(&suite, "a", 7));
    let reader = add(
        &mut spec,
        suite
            .function(workflow::ctx_reader::handle)
            .node()
            .node_id("reader")
            .set(workflow::ctx_reader::params::source(), "a".to_owned())
            .expect("a reader input")
            .waits_for(a)
            .workflow_context_from([a]),
    );
    let _output = spec.output(reader);
    let handle = start::<i64>(&suite, spec).await;
    assert_eq!(output(&handle).await, 7);
}

/// Python: `test_layer5_workflow_dataflow.py::test_workflow_ctx_multiple_dependencies`.
#[tokio::test]
#[serial]
async fn test_workflow_ctx_multiple_dependencies() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let mut spec = builder(&suite, "ctx_multiple");
    let a = add(&mut spec, produce(&suite, "a", 3));
    let b = add(&mut spec, produce(&suite, "b", 4));
    let sum = add(
        &mut spec,
        suite
            .function(workflow::ctx_sum::handle)
            .node()
            .node_id("sum")
            .set(workflow::ctx_sum::params::first(), "a".to_owned())
            .and_then(|node| node.set(workflow::ctx_sum::params::second(), "b".to_owned()))
            .expect("a sum input")
            .waits_for_all([a, b])
            .workflow_context_from([a, b]),
    );
    let _output = spec.output(sum);
    let handle = start::<i64>(&suite, spec).await;
    assert_eq!(output(&handle).await, 7);
}

/// Python: `test_layer5_workflow_dataflow.py::test_mixed_args_from_and_ctx`.
#[tokio::test]
#[serial]
async fn test_mixed_args_from_and_ctx() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let mut spec = builder(&suite, "mixed_args_ctx");
    let a = add(&mut spec, produce(&suite, "a", 10));
    let b = add(&mut spec, produce(&suite, "b", 20));
    let reader = add(
        &mut spec,
        suite
            .function(workflow::mixed_reader::handle)
            .node()
            .node_id("reader")
            .arg_from(workflow::mixed_reader::params::from_args(), a)
            .set(workflow::mixed_reader::params::ctx_source(), "b".to_owned())
            .expect("a reader input")
            .waits_for(b)
            .workflow_context_from([b]),
    );
    let _output = spec.output(reader);
    let handle = start::<BTreeMap<String, i64>>(&suite, spec).await;
    assert_eq!(
        output(&handle).await,
        BTreeMap::from([("from_args".to_owned(), 10), ("from_ctx".to_owned(), 20)])
    );
}

/// Python: `test_layer5_workflow_dataflow.py::test_args_from_dict_serialization`.
#[tokio::test]
#[serial]
async fn test_args_from_dict_serialization() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let mut spec = builder(&suite, "dict_serialization");
    let a = add(
        &mut spec,
        suite
            .function(workflow::produce_dict::handle)
            .node()
            .node_id("a")
            .set(workflow::produce_dict::params::value(), "hello".to_owned())
            .expect("a dict input"),
    );
    let b = add(
        &mut spec,
        suite
            .function(workflow::receive_dict::handle)
            .node()
            .node_id("b")
            .arg_from(workflow::receive_dict::params::input_result(), a),
    );
    let _output = spec.output(b);
    let handle = start::<serde_json::Value>(&suite, spec).await;
    assert_eq!(
        output(&handle).await,
        serde_json::json!({ "key": "hello", "count": 42, "nested": { "inner": true } })
    );
}

/// Python: `test_layer5_workflow_dataflow.py::test_join_ctx_gating`. A
/// node that joins on any dependency runs once one succeeded, and reads it
/// from the context.
#[tokio::test]
#[serial]
async fn test_join_ctx_gating() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let mut spec = builder(&suite, "join_ctx_gating");
    let a = add(&mut spec, fail(&suite, "a"));
    let b = add(&mut spec, produce(&suite, "b", 9));
    let reader = add(
        &mut spec,
        suite
            .function(workflow::ctx_reader::handle)
            .node()
            .node_id("reader")
            .set(workflow::ctx_reader::params::source(), "b".to_owned())
            .expect("a reader input")
            .waits_for_all([a, b])
            .join_any()
            .workflow_context_from([a, b]),
    );
    let _output = spec.output(reader);
    let handle = start::<i64>(&suite, spec).await;
    let client = suite.client().await;
    let _ended = rows::wait_for_workflow_end(&client, handle.workflow_id(), WORKFLOW_WAIT).await;
    let nodes = rows::workflow_nodes(&client, handle.workflow_id()).await;
    assert_eq!(
        nodes[2].status, "COMPLETED",
        "the any-join node ran: {nodes:?}"
    );
    let read = handle
        .result_for::<i64, workflow::WfCode>(&horsies::NodeKey::new("reader"))
        .await
        .expect("read the reader");
    assert_eq!(read, TaskOutcome::Succeeded(9));
}

/// Old Rust `wrapped_input_strict.rs`: the result injected by `args_from`
/// decodes into the strict input of the node, which refuses unknown keys.
#[tokio::test]
#[serial]
async fn wrapped_input_args_from_deserializes_under_strict() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let mut spec = builder(&suite, "wrapped_strict");
    let a = add(&mut spec, produce(&suite, "a", 6));
    let b = add(&mut spec, produce(&suite, "b", 1));
    let sum = add(
        &mut spec,
        suite
            .function(workflow::sum_two::handle)
            .node()
            .node_id("sum")
            .arg_from(workflow::sum_two::params::first(), a)
            .arg_from(workflow::sum_two::params::second(), b),
    );
    let _output = spec.output(sum);
    let handle = start::<i64>(&suite, spec).await;
    assert_eq!(output(&handle).await, 7);
}

/// Old Rust `wrapped_input_strict.rs::wrapped_input_extra_kwarg_rejected_at_execution`.
/// A node input with a key that the task's input does not have is refused
/// when the spec is built, before anything is written.
#[tokio::test]
#[serial]
async fn wrapped_input_extra_kwarg_is_refused_at_build() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let mut spec = builder(&suite, "wrapped_extra_kwarg");
    let refused = match suite
        .function(workflow::produce_int::handle)
        .node()
        .node_id("a")
        .input_json(r#"{"value": 1, "extra": 2}"#)
    {
        Err(refused) => refused,
        Ok(node) => {
            let _a = add(&mut spec, node);
            spec.build()
                .expect_err("an input key that the task does not have")
        }
    };
    assert!(
        refused.to_string().contains("extra"),
        "the refusal names the key: {refused}"
    );
}
