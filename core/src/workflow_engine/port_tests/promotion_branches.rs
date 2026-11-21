//! Promotion branches of one node on the step path: the stored data of a
//! PENDING node that recovery case 0 promotes (`horsies_workflow_node_reevaluate`
//! through the tree recovery pass), for a task node and for a sub-workflow
//! node whose child start the step owes.

use uuid::Uuid;

use super::support::{
    broker, child_specs_of, code, ctx_result, error_of, node, node_task_input, ok, seed_node,
    seed_workflow, shared_pool, spec, stored_result_error, task_node, NodeSeed, WorkflowSeed,
};
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::workflow::node::JoinType;
use crate::core::WorkflowSpec;
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};
use crate::workflow_engine::recovery::recover_workflow_tree;

/// The child of the sub-workflow node: one task node.
fn child() -> WorkflowSpec {
    spec(
        "promotion_branch_child",
        vec![task_node(0, "promotion_branch_child_task")],
    )
}

/// Node 0 COMPLETED with an ok result, and node 1, which waits for node 0,
/// PENDING: a task node, or a sub-workflow node of [`child`]. `change` sets
/// the stored data of node 1. Returns the workflow id.
async fn seed_completed_root_and_dependent(
    pool: &TestPool,
    sub_workflow: bool,
    change: impl FnOnce(NodeSeed) -> NodeSeed,
) -> Uuid {
    let workflow_id = seed_workflow(pool, &WorkflowSeed::running()).await;
    seed_node(
        pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED")
            .result(serde_json::to_string(&ok(serde_json::json!(7))).expect("encode the result")),
    )
    .await;
    let dependent = match sub_workflow {
        true => NodeSeed::sub_workflow(
            workflow_id,
            1,
            "PENDING",
            child()
                .definition_key
                .as_deref()
                .expect("the child has a definition key"),
            None,
        ),
        false => NodeSeed::task(workflow_id, 1, "PENDING"),
    };
    seed_node(pool, &change(dependent.deps(&[0]))).await;
    workflow_id
}

/// The tree recovery pass of `workflow_id`: case 0 promotes node 1.
async fn recover(pool: &TestPool, workflow_id: Uuid, child_specs: &TestChildSpecs) {
    let _report = recover_workflow_tree(&broker(pool), child_specs, workflow_id)
        .await
        .expect("the tree recovery pass");
}

/// Child workflows of `workflow_id`.
async fn child_count(pool: &TestPool, workflow_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_workflows WHERE parent_workflow_id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("count the child workflows")
}

/// Node 1 is FAILED with `WORKER_SERIALIZATION_ERROR`, Python's `message`,
/// and the workflow id and the node in `data` (Python `_fail_enqueued_task`,
/// `node_error`); it has no task row, and no child workflow was written.
async fn assert_node_1_failed_alone(pool: &TestPool, workflow_id: Uuid, case: &str, message: &str) {
    let row = node(pool, workflow_id, 1).await;
    assert_eq!(row.status, "FAILED", "{case}");
    assert_eq!(row.task_id, None, "{case}");
    let error = stored_result_error(row.result.as_deref().expect("the node stores its error"));
    assert_eq!(
        code(&error).as_deref(),
        Some("WORKER_SERIALIZATION_ERROR"),
        "{case}"
    );
    assert_eq!(error.message.as_deref(), Some(message), "{case}");
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1})),
        "{case}"
    );
    assert_eq!(child_count(pool, workflow_id).await, 0, "{case}");
}

/// `TestValidateArgsFromMap::test_non_int_value_returns_none`,
/// `test_float_value_returns_none`, `test_none_value_returns_none`,
/// `test_mixed_valid_and_invalid_returns_none`, `test_bool_value_returns_none`
/// (`true` and `false` are not the indexes 1 and 0);
/// `TestEnqueueWorkflowTask::test_args_from_non_int_values_fails_task`,
/// `test_corrupt_args_from_string_fails_task`: a stored `args_from` that is
/// not a map of integer indexes fails the node alone with
/// `WORKER_SERIALIZATION_ERROR`, and no task row is written.
#[tokio::test]
async fn a_promoted_node_whose_args_from_is_not_a_map_of_indexes_fails_alone() {
    let pool = shared_pool().await;
    let child_specs = child_specs_of(&[]);
    let non_int = "args_from has non-int values for task 1";
    for (args_from, message) in [
        (serde_json::json!({"x": "not_an_int"}), non_int),
        (serde_json::json!({"x": 1.5}), non_int),
        (serde_json::json!({"x": null}), non_int),
        (serde_json::json!({"x": 0, "y": "bad"}), non_int),
        (serde_json::json!({"x": true}), non_int),
        (serde_json::json!({"x": false}), non_int),
        (
            serde_json::json!("{broken"),
            "Corrupt args_from JSON for task 1",
        ),
    ] {
        let case = args_from.to_string();
        let workflow_id = seed_completed_root_and_dependent(&pool, false, |mut dependent| {
            dependent.args_from = Some(args_from);
            dependent
        })
        .await;

        recover(&pool, workflow_id, &child_specs).await;

        assert_node_1_failed_alone(&pool, workflow_id, &case, message).await;
    }
}

/// `TestEnqueueWorkflowTask::test_corrupt_kwargs_json_fails_task`: a stored
/// node input that does not parse fails the node alone with
/// `WORKER_SERIALIZATION_ERROR`, and no task row is written.
#[tokio::test]
async fn a_promoted_node_whose_stored_input_does_not_parse_fails_alone() {
    let pool = shared_pool().await;
    let workflow_id =
        seed_completed_root_and_dependent(&pool, false, |dependent| dependent.input("broken{json"))
            .await;

    recover(&pool, workflow_id, &child_specs_of(&[])).await;

    assert_node_1_failed_alone(
        &pool,
        workflow_id,
        "broken{json",
        "Corrupt kwargs JSON for task 1",
    )
    .await;
}

/// `TestEnqueueSubworkflowTask::test_corrupt_kwargs_json_fails`: a stored
/// input of a sub-workflow node that does not parse fails the node with
/// `WORKER_SERIALIZATION_ERROR`, and no child starts.
#[tokio::test]
async fn a_sub_workflow_node_whose_stored_input_does_not_parse_fails_alone() {
    let pool = shared_pool().await;
    let workflow_id =
        seed_completed_root_and_dependent(&pool, true, |dependent| dependent.input("broken{json"))
            .await;

    recover(&pool, workflow_id, &child_specs_of(&[child()])).await;

    assert_node_1_failed_alone(
        &pool,
        workflow_id,
        "broken{json",
        "Corrupt kwargs JSON for subworkflow task 1",
    )
    .await;
}

/// `TestEnqueueSubworkflowTask::test_args_from_non_int_values_fails_task`:
/// an `args_from` value that is not an integer fails the sub-workflow node
/// with `WORKER_SERIALIZATION_ERROR`, and no child starts.
#[tokio::test]
async fn a_sub_workflow_node_whose_args_from_holds_a_value_that_is_not_an_index_fails_alone() {
    let pool = shared_pool().await;
    let workflow_id = seed_completed_root_and_dependent(&pool, true, |mut dependent| {
        dependent.args_from = Some(serde_json::json!({"x": "not_an_int"}));
        dependent
    })
    .await;

    recover(&pool, workflow_id, &child_specs_of(&[child()])).await;

    assert_node_1_failed_alone(
        &pool,
        workflow_id,
        "not_an_int",
        "args_from has non-int values for subworkflow task 1",
    )
    .await;
}

/// `TestEnqueueSubworkflowTask::test_corrupt_args_from_string_fails`: an
/// `args_from` stored as a JSON string fails the sub-workflow node with
/// `WORKER_SERIALIZATION_ERROR`, and no child starts.
#[tokio::test]
async fn a_sub_workflow_node_whose_args_from_is_a_json_string_fails_alone() {
    let pool = shared_pool().await;
    let workflow_id = seed_completed_root_and_dependent(&pool, true, |mut dependent| {
        dependent.args_from = Some(serde_json::json!("{broken"));
        dependent
    })
    .await;

    recover(&pool, workflow_id, &child_specs_of(&[child()])).await;

    assert_node_1_failed_alone(
        &pool,
        workflow_id,
        "\"{broken\"",
        "Corrupt args_from JSON for subworkflow task 1",
    )
    .await;
}

/// `TestCheckAndPromoteTask::test_no_dependencies_returns`, with the core's
/// rule (decided on #1224): a PENDING node with no dependencies meets its
/// join `all` (0 of 0 dependencies terminal), so recovery case 0 promotes it
/// and mints its task row. Python leaves such a node PENDING; the core keeps
/// its rule, because it recovers a root that a start left PENDING.
#[tokio::test]
async fn a_pending_node_without_dependencies_is_promoted() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
    seed_node(&pool, &NodeSeed::task(workflow_id, 0, "PENDING")).await;

    recover(&pool, workflow_id, &child_specs_of(&[])).await;

    let row = node(&pool, workflow_id, 0).await;
    assert_eq!(row.status, "ENQUEUED");
    assert!(
        row.task_id.is_some(),
        "the promoted node links its task row"
    );
}

/// A context node whose stored result is NULL or empty gets the entry of
/// `horsies_workflow_source_value` and Python `_missing_result_entry`:
/// `RESULT_DESERIALIZATION_ERROR` naming the node, its index, its task and its
/// status, with the dependency index in `data`. A context of task nodes only
/// holds no summary (`TestBuildWorkflowContextData::test_empty_ctx_from_ids_no_summaries`).
#[tokio::test]
async fn a_context_node_with_no_stored_result_gets_the_missing_result_entry() {
    let pool = shared_pool().await;
    for status in ["COMPLETED", "FAILED"] {
        for stored in [None, Some(String::new())] {
            let case = format!("{status} {stored:?}");
            let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
            let mut source = NodeSeed::task(workflow_id, 0, status);
            source.result = stored;
            seed_node(&pool, &source).await;
            let mut dependent = NodeSeed::task(workflow_id, 1, "PENDING")
                .deps(&[0])
                .allow_failed_deps();
            dependent.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
            seed_node(&pool, &dependent).await;

            recover(&pool, workflow_id, &child_specs_of(&[])).await;

            let input = node_task_input(&pool, workflow_id, 1).await;
            let entry = ctx_result(&input, "node_0");
            let error = error_of(&entry);
            assert_eq!(
                code(error).as_deref(),
                Some("RESULT_DESERIALIZATION_ERROR"),
                "{case}"
            );
            let expected = format!(
                "Dependency node node_0 (index 0, task port_task) is {status} with no stored result"
            );
            assert_eq!(error.message.as_deref(), Some(expected.as_str()), "{case}");
            assert_eq!(
                error.data_value(),
                Some(serde_json::json!({"dependency_index": 0})),
                "{case}"
            );
            assert_eq!(
                input[WORKFLOW_CTX_KWARG]["summaries_by_id"],
                serde_json::json!({}),
                "{case}"
            );
        }
    }
}

/// `TestBuildWorkflowContextData::test_fetches_subworkflow_summaries`: the
/// context of a node carries the stored summary text of a sub-workflow
/// context node.
#[tokio::test]
async fn a_context_entry_of_a_sub_workflow_node_carries_its_stored_summary() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
    let summary = r#"{"status":"COMPLETED","output":7,"total_tasks":1,"completed_tasks":1,"failed_tasks":0,"skipped_tasks":0,"error_summary":null}"#;
    let mut source = NodeSeed::sub_workflow(
        workflow_id,
        0,
        "COMPLETED",
        child()
            .definition_key
            .as_deref()
            .expect("the child has a definition key"),
        None,
    )
    .result(serde_json::to_string(&ok(serde_json::json!(7))).expect("encode the result"));
    source.sub_workflow_summary = Some(summary.to_owned());
    seed_node(&pool, &source).await;
    let mut dependent = NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0]);
    dependent.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    seed_node(&pool, &dependent).await;

    recover(&pool, workflow_id, &child_specs_of(&[])).await;

    let input = node_task_input(&pool, workflow_id, 1).await;
    assert_eq!(
        input[WORKFLOW_CTX_KWARG]["summaries_by_id"],
        serde_json::json!({ "node_0": summary })
    );
}

/// `TestValidateArgsFromMap::test_valid_map_returns_typed` and
/// `test_empty_map_returns_empty`: a stored `args_from` that maps keys to
/// integer indexes, or that is an empty map, is read: the promoted node is
/// enqueued with its task, and a mapped key holds the dependency's result.
#[tokio::test]
async fn a_promoted_node_whose_args_from_is_a_map_of_indexes_or_empty_is_enqueued() {
    let pool = shared_pool().await;
    for args_from in [serde_json::json!({"x": 0}), serde_json::json!({})] {
        let case = args_from.to_string();
        let mapped = args_from.get("x").is_some();
        let workflow_id = seed_completed_root_and_dependent(&pool, false, |mut dependent| {
            dependent.args_from = Some(args_from);
            dependent
        })
        .await;

        recover(&pool, workflow_id, &child_specs_of(&[])).await;

        let row = node(&pool, workflow_id, 1).await;
        assert_eq!(row.status, "ENQUEUED", "{case}");
        assert!(row.task_id.is_some(), "{case}");
        let input = node_task_input(&pool, workflow_id, 1).await;
        assert_eq!(input.get("x").is_some(), mapped, "{case}: {input}");
    }
}

/// Stored task options that do not parse, or that are not a JSON object, fail
/// the promoted node alone with `WORKER_SERIALIZATION_ERROR` and Python's
/// message (`horsies_workflow_task_options_refusal`), and no task row is
/// written.
#[tokio::test]
async fn a_promoted_node_whose_stored_options_do_not_read_fails_alone() {
    let pool = shared_pool().await;
    for (options, message) in [
        ("not{json", "Corrupt task_options JSON for task 1"),
        (
            "\"just_a_string\"",
            "Invalid task_options payload for task 1: expected object",
        ),
    ] {
        let workflow_id =
            seed_completed_root_and_dependent(&pool, false, |dependent| dependent.options(options))
                .await;

        recover(&pool, workflow_id, &child_specs_of(&[])).await;

        assert_node_1_failed_alone(&pool, workflow_id, options, message).await;
    }
}

/// `TestCheckAndPromoteTask::test_a_pending_node_without_dependencies_and_join_any_is_skipped`:
/// join `any` with no dependencies can never be met. Recovery case 0 skips
/// the node (`horsies_workflow_promote_level`), and the skip cascade
/// processes its dependents: the dependent that waits for it (join `all`) is
/// SKIPPED too.
#[tokio::test]
async fn a_pending_join_any_node_without_dependencies_is_skipped_with_its_dependent() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
    let mut lone = NodeSeed::task(workflow_id, 0, "PENDING");
    lone.join = JoinType::Any;
    seed_node(&pool, &lone).await;
    seed_node(&pool, &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0])).await;

    recover(&pool, workflow_id, &child_specs_of(&[])).await;

    let skipped = node(&pool, workflow_id, 0).await;
    assert_eq!(skipped.status, "SKIPPED");
    assert_eq!(skipped.task_id, None);
    assert_eq!(node(&pool, workflow_id, 1).await.status, "SKIPPED");
}
