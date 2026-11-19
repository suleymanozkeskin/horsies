//! What a promoted task receives: `args_from` members, the workflow context
//! and the workflow meta; and the e2e shapes of layers 4 and 5 at the engine
//! level. Python: `tests/integration/test_workflow_args_from.py`,
//! `test_workflow_ctx.py`, `test_workflow_container_kwargs.py`,
//! `tests/e2e/test_layer5_workflow_dataflow.py`,
//! `tests/e2e/test_layer4_workflow_structure.py`.

use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;
use uuid::Uuid;

use super::support::{
    args_from_inner, child_of, claim_once, claim_pass, code, ctx_result, err, error_of,
    finish_node, handle, node, node_status, node_task_input, ok, shared_pool, spec, start,
    sub_workflow_node, task_node, workflow_status, IsolatedDatabase, PORT_QUEUE,
};
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::workflow::policy::{SuccessCase, SuccessPolicy};
use crate::core::{AnyNode, TaskResult, WorkflowSpec};
use crate::test_support::child_specs::{TestBuildRefused, TestChildBuild, TestChildSpecs};
use crate::test_support::sql::{self, TestPool};

fn after(index: usize, task_name: &str, dependencies: &[usize]) -> AnyNode {
    let mut node = task_node(index, task_name);
    node.dependencies = dependencies.to_vec();
    node
}

async fn run(pool: &TestPool, workflow_spec: &WorkflowSpec) -> Uuid {
    start(pool, workflow_spec, &TestChildSpecs::empty()).await
}

async fn succeed(pool: &TestPool, workflow_id: Uuid, index: i32, value: serde_json::Value) {
    let _task = finish_node(
        pool,
        workflow_id,
        index,
        &ok(value),
        &TestChildSpecs::empty(),
    )
    .await;
}

async fn fail(pool: &TestPool, workflow_id: Uuid, index: i32, error_code: &str, message: &str) {
    let _task = finish_node(
        pool,
        workflow_id,
        index,
        &err(error_code, message),
        &TestChildSpecs::empty(),
    )
    .await;
}

fn ok_value(result: &TaskResult<serde_json::Value>) -> &serde_json::Value {
    match result {
        TaskResult::Ok(value) => value,
        TaskResult::Err(error) => panic!("expected ok, got {error:?}"),
    }
}

/// A → B where B takes A's result as `input_result` and runs after a failure.
fn recovering_pair(name: &str) -> WorkflowSpec {
    let mut recovery = after(1, "recovery_task", &[0]);
    recovery.args_from.insert("input_result".to_owned(), 0);
    recovery.allow_failed_deps = true;
    spec(name, vec![after(0, "failing_task", &[]), recovery])
}

/// `test_workflow_args_from.py::TestArgsFromInjection.test_args_from_err_result`
/// and `.test_failed_dep_passes_original_error_not_skipped_sentinel`
#[tokio::test]
#[serial]
async fn a_failed_dependency_passes_its_own_error_to_an_allow_failed_deps_node() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &recovering_pair("err_result_test")).await;

    fail(&pool, workflow_id, 0, "ORIGINAL_ERROR", "Original failure").await;

    assert_eq!(node_status(&pool, workflow_id, 1).await, "ENQUEUED");
    let input = node_task_input(&pool, workflow_id, 1).await;
    assert_eq!(
        input["input_result"]["__h_taskresult_envelope__"],
        serde_json::json!(true)
    );
    let injected = args_from_inner(&input, "input_result");
    let error = error_of(&injected);
    assert_eq!(code(error).as_deref(), Some("ORIGINAL_ERROR"));
    assert_eq!(error.message.as_deref(), Some("Original failure"));
}

/// `TestArgsFromInjection.test_args_from_same_dep_multiple_kwargs`
#[tokio::test]
#[serial]
async fn one_dependency_mapped_to_two_keys_fills_both() {
    let pool = shared_pool().await;
    let mut receiver = after(1, "dual_receiver", &[0]);
    receiver.args_from.insert("first_copy".to_owned(), 0);
    receiver.args_from.insert("second_copy".to_owned(), 0);
    let workflow_id = run(
        &pool,
        &spec("same_dep_test", vec![after(0, "same_dep_a", &[]), receiver]),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(50)).await;

    let input = node_task_input(&pool, workflow_id, 1).await;
    assert_eq!(input["first_copy"], input["second_copy"]);
    assert_eq!(
        ok_value(&args_from_inner(&input, "first_copy")),
        &serde_json::json!(50)
    );
}

/// `TestArgsFromInjection.test_skipped_dep_injects_upstream_skipped_sentinel`
#[tokio::test]
#[serial]
async fn a_skipped_dependency_is_injected_as_upstream_skipped_with_its_index() {
    let pool = shared_pool().await;
    let mut receiver = after(2, "skipped_sentinel_c", &[1]);
    receiver.args_from.insert("input_result".to_owned(), 1);
    receiver.allow_failed_deps = true;
    let workflow_id = run(
        &pool,
        &spec(
            "skipped_sentinel_test",
            vec![
                after(0, "skipped_sentinel_a", &[]),
                after(1, "skipped_sentinel_b", &[0]),
                receiver,
            ],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "A_FAILED", "A failed").await;

    assert_eq!(node_status(&pool, workflow_id, 1).await, "SKIPPED");
    let input = node_task_input(&pool, workflow_id, 2).await;
    let injected = args_from_inner(&input, "input_result");
    let error = error_of(&injected);
    assert_eq!(code(error).as_deref(), Some("UPSTREAM_SKIPPED"));
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"dependency_index": 1}))
    );
}

/// `TestArgsFromInjection.test_partial_args_from_only_mapped_deps_injected`
#[tokio::test]
#[serial]
async fn only_mapped_dependencies_are_injected() {
    let pool = shared_pool().await;
    let mut receiver = after(2, "partial_map_c", &[0, 1]);
    receiver.args_from.insert("from_a".to_owned(), 0);
    let workflow_id = run(
        &pool,
        &spec(
            "partial_args_from_test",
            vec![
                after(0, "partial_map_a", &[]),
                after(1, "partial_map_b", &[]),
                receiver,
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(10)).await;
    succeed(&pool, workflow_id, 1, serde_json::json!(20)).await;

    let input = node_task_input(&pool, workflow_id, 2).await;
    let wrapped: Vec<&String> = input
        .as_object()
        .expect("a task input with members is an object")
        .iter()
        .filter(|(_, value)| value.get("__h_taskresult_envelope__").is_some())
        .map(|(key, _)| key)
        .collect();
    assert_eq!(wrapped, vec!["from_a"]);
}

/// `test_workflow_ctx.py::TestWorkflowCtx.test_ctx_injected_and_result_for_works`:
/// the context names the workflow, the node and its task, and holds the
/// dependency's result.
#[tokio::test]
#[serial]
async fn the_context_names_the_node_and_holds_its_dependency_result() {
    let pool = shared_pool().await;
    let mut reader = after(1, "ctx_inject_b", &[0]);
    reader.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    let workflow_id = run(
        &pool,
        &spec(
            "ctx_inject_test",
            vec![after(0, "ctx_inject_a", &[]), reader],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(10)).await;

    let input = node_task_input(&pool, workflow_id, 1).await;
    assert_eq!(
        ok_value(&ctx_result(&input, "node_0")),
        &serde_json::json!(10)
    );
    let context = &input[WORKFLOW_CTX_KWARG];
    assert_eq!(
        context["workflow_id"],
        serde_json::json!(workflow_id.to_string())
    );
    assert_eq!(context["task_index"], serde_json::json!(1));
    assert_eq!(context["task_name"], serde_json::json!("ctx_inject_b"));
}

/// `TestWorkflowCtx.test_ctx_not_injected_without_ctx_from` and
/// `.test_ctx_empty_ctx_from_no_injection`
#[tokio::test]
#[serial]
async fn no_context_is_injected_without_a_context_dependency() {
    let pool = shared_pool().await;
    for ctx_from in [None, Some(Vec::new())] {
        let mut reader = after(1, "no_ctx_from_b", &[0]);
        reader.workflow_ctx_from = ctx_from.clone();
        let workflow_id = run(
            &pool,
            &spec(
                "no_ctx_from_test",
                vec![after(0, "no_ctx_from_a", &[]), reader],
            ),
        )
        .await;

        succeed(&pool, workflow_id, 0, serde_json::json!(5)).await;

        let input = node_task_input(&pool, workflow_id, 1).await;
        assert!(
            input.get(WORKFLOW_CTX_KWARG).is_none(),
            "{ctx_from:?}: {input}"
        );
    }
}

/// `TestWorkflowCtx.test_workflow_meta_injected_without_ctx_from`
#[tokio::test]
#[serial]
async fn the_workflow_meta_is_injected_without_a_context_dependency() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "meta_only_test",
            vec![after(0, "meta_only_a", &[]), after(1, "meta_only_b", &[0])],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(5)).await;

    let meta = &node_task_input(&pool, workflow_id, 1).await["__h_workflow_meta__"];
    assert_eq!(
        meta["workflow_id"],
        serde_json::json!(workflow_id.to_string())
    );
    assert_eq!(meta["task_index"], serde_json::json!(1));
    assert_eq!(meta["task_name"], serde_json::json!("meta_only_b"));
}

/// `TestWorkflowCtx.test_ctx_skipped_dep_returns_upstream_skipped`
#[tokio::test]
#[serial]
async fn a_skipped_context_dependency_reads_as_upstream_skipped_with_its_index() {
    let pool = shared_pool().await;
    let mut reader = after(2, "ctx_skipped_c", &[1]);
    reader.workflow_ctx_from = Some(vec!["node_1".to_owned()]);
    reader.allow_failed_deps = true;
    let workflow_id = run(
        &pool,
        &spec(
            "ctx_skipped_test",
            vec![
                after(0, "ctx_skipped_failing", &[]),
                after(1, "ctx_skipped_b", &[0]),
                reader,
            ],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "A_FAILED", "A failed").await;

    let input = node_task_input(&pool, workflow_id, 2).await;
    let entry = ctx_result(&input, "node_1");
    let error = error_of(&entry);
    assert_eq!(code(error).as_deref(), Some("UPSTREAM_SKIPPED"));
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"dependency_index": 1}))
    );
}

/// `TestWorkflowCtx.test_ctx_failed_dep_returns_original_error`
#[tokio::test]
#[serial]
async fn a_failed_context_dependency_reads_as_its_original_error() {
    let pool = shared_pool().await;
    let mut reader = after(1, "ctx_failed_c", &[0]);
    reader.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    reader.allow_failed_deps = true;
    let workflow_id = run(
        &pool,
        &spec(
            "ctx_failed_test",
            vec![after(0, "ctx_failed_failing", &[]), reader],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "A_FAILED", "A failed").await;

    let input = node_task_input(&pool, workflow_id, 1).await;
    let entry = ctx_result(&input, "node_0");
    let error = error_of(&entry);
    assert_eq!(code(error).as_deref(), Some("A_FAILED"));
    assert_eq!(error.message.as_deref(), Some("A failed"));
}

/// A build that copies the node's input into its nodes: one task node, and
/// one sub-workflow node of `grandchild_key` that gives the same input as
/// params (Python `_ChildWorkflow`).
fn container_child(grandchild_key: &'static str) -> TestChildBuild {
    Arc::new(
        move |input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            let mut copy = task_node(0, "container_child_task");
            copy.input_json = input.to_owned();
            let grandchild = WorkflowSpec {
                definition_key: Some(grandchild_key.to_owned()),
                ..spec(
                    "container_grandchild",
                    vec![task_node(0, "container_grandchild_task")],
                )
            };
            let mut nested = sub_workflow_node(1, &grandchild);
            nested.input_json = input.to_owned();
            nested.sub_workflow_params = SubWorkflowParams::Given;
            let mut child = spec("container_child", vec![copy, nested]);
            child.output_index = Some(0);
            Ok(child)
        },
    )
}

/// A build that takes params and builds one task node.
fn container_grandchild() -> TestChildBuild {
    Arc::new(|_input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
        let mut grandchild = spec(
            "container_grandchild",
            vec![task_node(0, "container_grandchild_task")],
        );
        grandchild.output_index = Some(0);
        Ok(grandchild)
    })
}

fn stored_input(text: &str) -> serde_json::Value {
    serde_json::from_str(text).expect("a stored input decodes")
}

/// `test_workflow_container_kwargs.py::TestContainerKwargsAtStart.test_a_sub_workflow_node_and_its_children_store_their_kwargs`:
/// a root sub-workflow node, a child task node and a child sub-workflow node
/// store their container input, and the child task gets it.
#[tokio::test]
#[serial]
async fn a_sub_workflow_node_and_its_children_store_their_container_input() {
    let pool = shared_pool().await;
    let child_key = "tests.port.container_child.v1";
    let grandchild_key = "tests.port.container_grandchild.v1";
    let child_specs = TestChildSpecs::empty()
        .with_build(grandchild_key, container_grandchild())
        .with_build(child_key, container_child(grandchild_key));
    let child_stub = WorkflowSpec {
        definition_key: Some(child_key.to_owned()),
        ..spec(
            "container_child",
            vec![task_node(0, "container_child_task")],
        )
    };
    let mut root = sub_workflow_node(0, &child_stub);
    root.input_json = r#"{"values": [1, 2]}"#.to_owned();
    root.sub_workflow_params = SubWorkflowParams::Given;
    let mut parent = spec("container_parent", vec![root]);
    parent.output_index = Some(0);
    let values = serde_json::json!({"values": [1, 2]});

    let parent_id = start(&pool, &parent, &child_specs).await;

    assert_eq!(stored_input(&node(&pool, parent_id, 0).await.input), values);
    let child_id = child_of(&pool, parent_id, 0).await;
    assert_eq!(stored_input(&node(&pool, child_id, 0).await.input), values);
    assert_eq!(
        node_task_input(&pool, child_id, 0).await["values"],
        values["values"]
    );
    assert_eq!(stored_input(&node(&pool, child_id, 1).await.input), values);
    super::support::end_workflows(&pool, &[parent_id]).await;
}

/// A policy whose one case requires `required_index`, with `optional` listed.
fn policy_with_optional(required_index: usize, optional: Vec<usize>) -> SuccessPolicy {
    SuccessPolicy {
        cases: vec![SuccessCase {
            required_indices: vec![required_index],
        }],
        optional_indices: Some(optional),
    }
}

/// `tests/e2e/test_layer5_workflow_dataflow.py::test_failed_propagation`:
/// B runs after A fails, receives A's own error, and the case on B completes
/// the workflow.
#[tokio::test]
#[serial]
async fn a_node_after_a_failure_receives_the_original_error() {
    let pool = shared_pool().await;
    let mut workflow_spec = recovering_pair("e2e_failed_propagation");
    workflow_spec.success_policy = Some(policy_with_optional(1, vec![0]));
    let workflow_id = run(&pool, &workflow_spec).await;

    fail(&pool, workflow_id, 0, "DELIBERATE_FAIL", "deliberate").await;
    let input = node_task_input(&pool, workflow_id, 1).await;
    assert_eq!(
        code(error_of(&args_from_inner(&input, "input_result"))).as_deref(),
        Some("DELIBERATE_FAIL")
    );
    succeed(
        &pool,
        workflow_id,
        1,
        serde_json::json!("received_error_DELIBERATE_FAIL"),
    )
    .await;

    assert_eq!(node_status(&pool, workflow_id, 0).await, "FAILED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `tests/e2e/test_layer5_workflow_dataflow.py::test_skipped_propagation`
#[tokio::test]
#[serial]
async fn a_node_after_a_skip_receives_upstream_skipped() {
    let pool = shared_pool().await;
    let mut checker = after(2, "check_upstream_skipped", &[1]);
    checker.args_from.insert("input_result".to_owned(), 1);
    checker.allow_failed_deps = true;
    let mut workflow_spec = spec(
        "e2e_skipped_propagation",
        vec![
            after(0, "fail_with", &[]),
            after(1, "passthrough", &[0]),
            checker,
        ],
    );
    workflow_spec.success_policy = Some(policy_with_optional(2, vec![0, 1]));
    let workflow_id = run(&pool, &workflow_spec).await;

    fail(&pool, workflow_id, 0, "CAUSE_SKIP", "cause").await;
    let input = node_task_input(&pool, workflow_id, 2).await;
    assert_eq!(
        code(error_of(&args_from_inner(&input, "input_result"))).as_deref(),
        Some("UPSTREAM_SKIPPED")
    );
    succeed(
        &pool,
        workflow_id,
        2,
        serde_json::json!("received_upstream_skipped"),
    )
    .await;

    assert_eq!(node_status(&pool, workflow_id, 1).await, "SKIPPED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `tests/e2e/test_layer5_workflow_dataflow.py::test_mixed_args_from_and_ctx`:
/// D gets B's result through `args_from` and C's through the context.
#[tokio::test]
#[serial]
async fn one_node_gets_args_from_of_one_dependency_and_the_context_of_another() {
    let pool = shared_pool().await;
    let mut reader = after(3, "mixed_reader", &[1, 2]);
    reader.args_from.insert("from_args".to_owned(), 1);
    reader.workflow_ctx_from = Some(vec!["node_2".to_owned()]);
    let workflow_id = run(
        &pool,
        &spec(
            "e2e_mixed_dataflow",
            vec![
                after(0, "produce", &[]),
                after(1, "double", &[0]),
                after(2, "produce_c", &[]),
                reader,
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(7)).await;
    succeed(&pool, workflow_id, 1, serde_json::json!(14)).await;
    succeed(&pool, workflow_id, 2, serde_json::json!(3)).await;

    let input = node_task_input(&pool, workflow_id, 3).await;
    assert_eq!(
        ok_value(&args_from_inner(&input, "from_args")),
        &serde_json::json!(14)
    );
    assert_eq!(
        ok_value(&ctx_result(&input, "node_2")),
        &serde_json::json!(3)
    );
}

/// `tests/e2e/test_layer5_workflow_dataflow.py::test_dual_injection_same_node`
#[tokio::test]
#[serial]
async fn one_dependency_read_through_args_from_and_the_context_gives_one_value() {
    let pool = shared_pool().await;
    let mut reader = after(1, "dual_reader", &[0]);
    reader.args_from.insert("from_args".to_owned(), 0);
    reader.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    let workflow_id = run(
        &pool,
        &spec("e2e_dual_injection", vec![after(0, "produce", &[]), reader]),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(77)).await;

    let input = node_task_input(&pool, workflow_id, 1).await;
    assert_eq!(
        ok_value(&args_from_inner(&input, "from_args")),
        &serde_json::json!(77)
    );
    assert_eq!(
        ok_value(&ctx_result(&input, "node_0")),
        &serde_json::json!(77)
    );
}

/// `tests/e2e/test_layer4_workflow_structure.py::test_fanout_sequential_with_single_process`:
/// a fan-out completes when its nodes finish one at a time.
#[tokio::test]
#[serial]
async fn a_fan_out_completes_when_its_nodes_finish_one_at_a_time() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "fanout_single",
            vec![
                after(0, "root", &[]),
                after(1, "leaf", &[0]),
                after(2, "leaf", &[0]),
                after(3, "leaf", &[0]),
            ],
        ),
    )
    .await;

    for index in 0..4 {
        succeed(&pool, workflow_id, index, serde_json::json!(index)).await;
    }

    for index in 0..4 {
        assert_eq!(node_status(&pool, workflow_id, index).await, "COMPLETED");
    }
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `tests/e2e/test_layer4_workflow_structure.py::test_fanin_aggregator_pending_during_execution`
#[tokio::test]
#[serial]
async fn a_fan_in_aggregator_stays_pending_while_some_roots_run() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "fanin_pending",
            vec![
                after(0, "root", &[]),
                after(1, "root", &[]),
                after(2, "root", &[]),
                after(3, "agg", &[0, 1, 2]),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(0)).await;
    assert_eq!(node_status(&pool, workflow_id, 3).await, "PENDING");
    succeed(&pool, workflow_id, 1, serde_json::json!(1)).await;
    assert_eq!(node_status(&pool, workflow_id, 3).await, "PENDING");
    succeed(&pool, workflow_id, 2, serde_json::json!(2)).await;
    succeed(&pool, workflow_id, 3, serde_json::json!(3)).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `tests/e2e/test_layer4_workflow_structure.py::test_complex_dag`: a node
/// starts (is enqueued) only after each of its dependencies completed.
#[tokio::test]
#[serial]
async fn a_seven_node_dag_starts_each_level_after_the_one_before() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "complex_dag",
            vec![
                after(0, "a", &[]),
                after(1, "b", &[0]),
                after(2, "c", &[0]),
                after(3, "d", &[0]),
                after(4, "e", &[1, 2]),
                after(5, "f", &[2, 3]),
                after(6, "g", &[4, 5]),
            ],
        ),
    )
    .await;

    for index in 0..7 {
        succeed(&pool, workflow_id, index, serde_json::json!(index)).await;
    }

    let mut rows = Vec::new();
    for index in 0..7 {
        let row = node(&pool, workflow_id, index).await;
        assert_eq!(row.status, "COMPLETED", "node {index}");
        rows.push((
            row.started_at.expect("a finished node started"),
            row.completed_at.expect("a finished node completed"),
        ));
    }
    for (index, dependencies) in [
        (1, vec![0]),
        (2, vec![0]),
        (3, vec![0]),
        (4, vec![1, 2]),
        (5, vec![2, 3]),
        (6, vec![4, 5]),
    ] {
        for dependency in dependencies {
            assert!(
                rows[index].0 >= rows[dependency].1,
                "node {index} started before node {dependency} completed"
            );
        }
    }
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `tests/e2e/test_layer4_workflow_structure.py::test_workflow_task_expires_before_claim`
#[tokio::test]
#[serial]
async fn a_workflow_task_past_its_deadline_is_not_claimed() {
    let db = IsolatedDatabase::create().await;
    let mut expiring = after(0, "step", &[]);
    expiring.good_until = Some(chrono::Utc::now() + chrono::Duration::milliseconds(300));
    let workflow_id = run(&db.pool, &spec("e2e_expiring_task", vec![expiring])).await;
    let task_id = node(&db.pool, workflow_id, 0).await.task_id;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let claimed = claim_once(&db.pool, &claim_pass(PORT_QUEUE, 10)).await;

    assert!(claimed.is_empty(), "the expired task was claimed");
    let live: Option<(String, Option<chrono::DateTime<chrono::Utc>>)> =
        sql::query_as("SELECT status, good_until FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&db.pool)
            .await
            .expect("read the task");
    match live {
        Some((status, good_until)) => {
            assert_eq!(status, "PENDING");
            assert!(good_until.is_some());
        }
        None => assert_eq!(
            super::support::history_row(&db.pool, task_id.expect("the root has its task"))
                .await
                .map(|(status, _kind)| status)
                .as_deref(),
            Some("EXPIRED")
        ),
    }
    db.drop().await;
}

/// `tests/e2e/test_layer4_workflow_structure.py::test_workflow_task_completes_before_expiry`
#[tokio::test]
#[serial]
async fn a_workflow_task_with_a_future_deadline_completes() {
    let pool = shared_pool().await;
    let mut valid = after(0, "step", &[]);
    valid.good_until = Some(chrono::Utc::now() + chrono::Duration::seconds(60));
    let workflow_id = run(&pool, &spec("e2e_valid_expiry", vec![valid])).await;

    succeed(&pool, workflow_id, 0, serde_json::json!("valid")).await;

    assert_eq!(node_status(&pool, workflow_id, 0).await, "COMPLETED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// A chain A → B → C (Python `spec_slow_linear`).
fn linear(name: &str) -> WorkflowSpec {
    spec(
        name,
        vec![
            after(0, "slow_a", &[]),
            after(1, "slow_b", &[0]),
            after(2, "slow_c", &[1]),
        ],
    )
}

/// `tests/e2e/test_layer4_workflow_structure.py::test_workflow_cancellation`
#[tokio::test]
#[serial]
async fn a_cancel_skips_waiting_nodes_and_get_gives_workflow_cancelled() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &linear("slow_linear_cancel")).await;
    succeed(&pool, workflow_id, 0, serde_json::json!("A")).await;
    let workflow_handle = handle(&pool, workflow_id);

    workflow_handle.cancel().await.expect("cancel()");

    assert_eq!(workflow_status(&pool, workflow_id).await, "CANCELLED");
    for index in 1..3 {
        let status = node_status(&pool, workflow_id, index).await;
        assert!(
            matches!(status.as_str(), "SKIPPED" | "COMPLETED" | "RUNNING"),
            "node {index}: {status}"
        );
    }
    let got = workflow_handle
        .get(Some(Duration::from_secs(2)))
        .await
        .expect("the result read");
    assert_eq!(code(error_of(&got)).as_deref(), Some("WORKFLOW_CANCELLED"));
}

/// `tests/e2e/test_layer4_workflow_structure.py::test_handle_get_timeout`
#[tokio::test]
#[serial]
async fn get_of_a_workflow_that_does_not_finish_gives_wait_timeout() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &linear("slow_linear_timeout")).await;

    let got = handle(&pool, workflow_id)
        .get(Some(Duration::from_millis(200)))
        .await
        .expect("the result read");

    assert_eq!(code(error_of(&got)).as_deref(), Some("WAIT_TIMEOUT"));
}
