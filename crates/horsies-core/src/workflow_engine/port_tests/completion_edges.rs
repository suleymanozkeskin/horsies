//! Completion edges: a success case and live nodes, the summary a parent node
//! gets, the parent node of an ended parent, the stored output, and workflows
//! with no node. The completion check runs as the tree recovery pass runs it
//! (`horsies_workflow_try_complete` under the workflow row lock), and the
//! parent node advances through `horsies_workflow_child_finish`. Python:
//! `tests/integration/test_success_policy_completion_repro.py`,
//! `tests/integration/test_workflow_finalization_contract.py`,
//! `tests/integration/test_workflow_sql_equivalence.py`.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    advance_parent_of, code, complete_workflow, node, ok, required, seed_node, seed_workflow,
    shared_pool, stored_error, stored_result, workflow, IsolatedDatabase, NodeSeed, ParentLink,
    WorkflowSeed,
};
use crate::core::{SubWorkflowSummary, TaskError, TaskResult, WorkflowStatus};
use crate::test_support::child_specs::TestChildSpecs;

async fn complete(pool: &PgPool, workflow_id: Uuid) {
    complete_workflow(pool, workflow_id, &TestChildSpecs::empty()).await;
}

/// The summary stored on node 0 of `parent_id`.
async fn parent_summary(pool: &PgPool, parent_id: Uuid) -> SubWorkflowSummary {
    let text = node(pool, parent_id, 0)
        .await
        .sub_workflow_summary
        .expect("the parent node stores its child's summary");
    serde_json::from_str(&text).expect("the summary decodes")
}

/// A RUNNING parent with node 0 linked to `child`, and its child row.
async fn seed_parent_and_child(pool: &PgPool, child: WorkflowSeed) -> (Uuid, Uuid) {
    let parent_id = seed_workflow(pool, &WorkflowSeed::running()).await;
    let mut child = child;
    child.parent = Some(ParentLink::of_root(parent_id, 0));
    let child_id = seed_workflow(pool, &child).await;
    seed_node(
        pool,
        &NodeSeed::sub_workflow(
            parent_id,
            0,
            "RUNNING",
            "tests.port.child.v1",
            Some(child_id),
        ),
    )
    .await;
    (parent_id, child_id)
}

/// `test_success_policy_does_not_finalize_with_live_non_required_task`: a
/// satisfied case is evaluated only once every node is terminal.
#[tokio::test]
#[serial]
async fn a_met_success_case_waits_for_a_live_node_that_it_does_not_require() {
    let db = IsolatedDatabase::create().await;
    let pool = db.pool.clone();
    let workflow_id = seed_workflow(
        &pool,
        &WorkflowSeed::running()
            .policy(serde_json::to_value(required(&[&[0]])).expect("encode the policy")),
    )
    .await;
    seed_node(&pool, &NodeSeed::task(workflow_id, 0, "COMPLETED")).await;
    seed_node(&pool, &NodeSeed::task(workflow_id, 1, "ENQUEUED")).await;

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "RUNNING");
    assert_eq!(row.completed_at, None);
    db.drop().await;
}

/// `test_a_failed_child_gives_its_parent_a_summary_without_output`: the
/// child's output node completed, but the child FAILED, so the parent's
/// summary has no output.
#[tokio::test]
#[serial]
async fn a_failed_child_gives_its_parent_a_summary_without_output() {
    let pool = shared_pool().await;
    let (parent_id, child_id) =
        seed_parent_and_child(&pool, WorkflowSeed::running().output(1)).await;
    let failure = serde_json::to_string(&super::support::err("CONTRACT_FAIL", "node failed"))
        .expect("encode the failure");
    let success = serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the success");
    seed_node(
        &pool,
        &NodeSeed::task(child_id, 0, "FAILED").result(failure),
    )
    .await;
    seed_node(
        &pool,
        &NodeSeed::task(child_id, 1, "COMPLETED").result(success.clone()),
    )
    .await;
    seed_node(
        &pool,
        &NodeSeed::task(child_id, 2, "COMPLETED").result(success),
    )
    .await;

    complete(&pool, child_id).await;

    assert_eq!(workflow(&pool, child_id).await.status, "FAILED");
    let summary = parent_summary(&pool, parent_id).await;
    assert_eq!(summary.status, WorkflowStatus::Failed);
    assert_eq!(summary.output, None);
}

/// `test_child_info_single_pass_matches_correlated_form`: the counts of the
/// summary equal the child's node statuses, over ten mixes.
#[tokio::test]
#[serial]
async fn child_summary_counts_follow_the_child_node_statuses() {
    let pool = shared_pool().await;
    let statuses = ["COMPLETED", "FAILED", "SKIPPED"];
    for trial in 1..=10_usize {
        let (parent_id, child_id) =
            seed_parent_and_child(&pool, WorkflowSeed::running().status("FAILED")).await;
        let node_count = 4 * trial;
        let mut expected = [0_i32; 3];
        for index in 0..node_count {
            let kind = (index * trial + index / 3) % statuses.len();
            expected[kind] += 1;
            let task_index = i32::try_from(index).expect("a small index");
            seed_node(&pool, &NodeSeed::task(child_id, task_index, statuses[kind])).await;
        }

        advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

        let summary = parent_summary(&pool, parent_id).await;
        assert_eq!(
            (
                summary.total_tasks,
                summary.completed_tasks,
                summary.failed_tasks,
                summary.skipped_tasks,
            ),
            (
                i32::try_from(node_count).expect("a small count"),
                expected[0],
                expected[1],
                expected[2],
            ),
            "trial {trial}"
        );
    }
}

/// `test_terminal_two_step_handles_zero_task_workflow`: a workflow with no
/// node has an empty terminal set, and its completion reads no row and fails
/// on nothing. It has no output node, so it stores Python's outputless result
/// with three empty maps (`python/horsies/core/workflows/engine.py:3935-3983`; the
/// baseline `horsies_workflow_outputless_result`).
#[tokio::test]
#[serial]
async fn a_workflow_with_no_node_completes_with_an_empty_terminal_set() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "COMPLETED");
    let stored = row.result.expect("a COMPLETED workflow stores its result");
    let result: serde_json::Value =
        serde_json::from_str(&stored).expect("the stored result is JSON");
    assert_eq!(
        result,
        serde_json::json!({
            "__h_task_result__": true,
            "__h_outputless_terminals__": true,
            "ok": {"results_by_id": {}, "task_name_by_id": {}, "definition_key_by_id": {}},
            "err": null,
        })
    );
}

/// `test_child_info_counts_zero_for_taskless_workflow`: a child with no node
/// reports a total of 0.
#[tokio::test]
#[serial]
async fn a_child_with_no_node_reports_a_total_of_zero() {
    let pool = shared_pool().await;
    let (parent_id, child_id) =
        seed_parent_and_child(&pool, WorkflowSeed::running().status("FAILED")).await;

    advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

    let summary = parent_summary(&pool, parent_id).await;
    assert_eq!(
        (
            summary.total_tasks,
            summary.completed_tasks,
            summary.failed_tasks,
            summary.skipped_tasks,
        ),
        (0, 0, 0, 0)
    );
}

/// An outputless child (no output node) that COMPLETED stores the outputless
/// result; its parent node COMPLETES with `Ok(null)`, not with the child's
/// terminal map, and the summary has no output (Python `_parse_child_output`
/// and `_completed_child_node`, `python/horsies/core/workflows/engine.py`; the baseline
/// `horsies_workflow_child_node_result`).
#[tokio::test]
#[serial]
async fn an_outputless_child_completes_its_parent_node_with_an_ok_null() {
    let pool = shared_pool().await;
    let outputless = serde_json::json!({
        "__h_task_result__": true,
        "__h_outputless_terminals__": true,
        "ok": {
            "results_by_id": {"node_0": {"__h_task_result__": true, "ok": 42, "err": null}},
            "task_name_by_id": {"node_0": "port_task"},
            "definition_key_by_id": {"node_0": null},
        },
        "err": null,
    });
    let (parent_id, child_id) = seed_parent_and_child(
        &pool,
        WorkflowSeed::running()
            .status("COMPLETED")
            .result(outputless.to_string()),
    )
    .await;

    advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

    let parent_node = node(&pool, parent_id, 0).await;
    assert_eq!(parent_node.status, "COMPLETED");
    match stored_result(
        parent_node
            .result
            .as_deref()
            .expect("the parent node stores a result"),
    ) {
        TaskResult::Ok(value) => assert!(value.is_null(), "{value}"),
        TaskResult::Err(error) => panic!("the parent node failed: {error:?}"),
    }
    assert_eq!(parent_summary(&pool, parent_id).await.output, None);
}

/// A child that ends after its parent ended still writes its parent node, as
/// in Python (`UPDATE_PARENT_NODE_RESULT_SQL` has no parent status gate). The
/// ended parent keeps its status, and a second advance does not write the
/// terminal node again.
#[tokio::test]
#[serial]
async fn the_parent_node_of_an_ended_parent_is_written_once() {
    let pool = shared_pool().await;
    let (parent_id, child_id) = seed_parent_and_child(
        &pool,
        WorkflowSeed::running()
            .status("COMPLETED")
            .output(0)
            .result(serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the output")),
    )
    .await;
    sqlx::query(
        "UPDATE horsies_workflows SET status = 'CANCELLED', completed_at = NOW() WHERE id = $1",
    )
    .bind(parent_id)
    .execute(&pool)
    .await
    .expect("end the parent");

    advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

    let written = node(&pool, parent_id, 0).await;
    assert_eq!(written.status, "COMPLETED");
    assert!(written.completed_at.is_some());
    assert_eq!(workflow(&pool, parent_id).await.status, "CANCELLED");

    sqlx::query("UPDATE horsies_workflows SET result = $2 WHERE id = $1")
        .bind(child_id)
        .bind(serde_json::to_string(&ok(serde_json::json!(2))).expect("encode the output"))
        .execute(&pool)
        .await
        .expect("change the child result");
    advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

    let again = node(&pool, parent_id, 0).await;
    assert_eq!(again.status, "COMPLETED");
    assert_eq!(again.result, written.result);
    assert_eq!(again.completed_at, written.completed_at);
}

/// The error summary of a FAILED child, from its stored workflow error, as
/// Python takes it (`python/horsies/core/workflows/engine.py:3030-3038`): the `message` of a
/// JSON object; the first 200 characters of a text that is not JSON; none for
/// other JSON, an empty text and no text.
#[tokio::test]
#[serial]
async fn the_error_summary_of_a_failed_child_follows_python() {
    let pool = shared_pool().await;
    let long = "x".repeat(300);
    let cases: [(Option<&str>, Option<String>); 6] = [
        (
            Some(r#"{"message": "first failure"}"#),
            Some("first failure".to_owned()),
        ),
        (Some(r#"{"error_code": "X"}"#), None),
        (Some("42"), None),
        (Some(long.as_str()), Some("x".repeat(200))),
        (Some(""), None),
        (None, None),
    ];
    for (stored, expected) in cases {
        let mut child = WorkflowSeed::running().status("FAILED");
        child.error = stored.map(str::to_owned);
        let (parent_id, child_id) = seed_parent_and_child(&pool, child).await;

        advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

        assert_eq!(
            parent_summary(&pool, parent_id).await.error_summary,
            expected,
            "stored child error {stored:?}"
        );
    }
}

/// The stored child error `null` is the JSON null: Python's reader gives
/// `None` for it and takes the first 200 characters of the text, `"null"`
/// (`horsies_workflow_child_node_result` reads it the same way).
#[tokio::test]
#[serial]
async fn a_child_error_stored_as_json_null_is_summarized_as_its_text() {
    let pool = shared_pool().await;
    let mut child = WorkflowSeed::running().status("FAILED");
    child.error = Some("null".to_owned());
    let (parent_id, child_id) = seed_parent_and_child(&pool, child).await;

    advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

    assert_eq!(
        parent_summary(&pool, parent_id).await.error_summary,
        Some("null".to_owned())
    );
}

/// A RUNNING workflow that stores an error and has no FAILED node completes
/// FAILED and keeps that error (Python `check_workflow_completion`: a stored
/// error fails the default success rule, and the failure error is kept).
#[tokio::test]
#[serial]
async fn a_completion_with_a_stored_error_and_no_failed_node_keeps_that_error() {
    let pool = shared_pool().await;
    let first_error =
        serde_json::to_string(&TaskError::new("FIRST", "first failure").expect("a user code"))
            .expect("encode the error");
    let workflow_id =
        seed_workflow(&pool, &WorkflowSeed::running().error(first_error.clone())).await;
    seed_node(
        &pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED")
            .result(serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the output")),
    )
    .await;

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "FAILED");
    assert_eq!(row.error, Some(first_error));
}

/// `test_workflow_finalization_contract.py`: a workflow with eight terminal
/// nodes completes with its explicit output copied unchanged, or with
/// Python's outputless result (`python/horsies/core/workflows/engine.py:3973-3983`), for a
/// small and a 64 KiB result; with a FAILED node it fails and stores no
/// result. Each completion sends one `workflow_done` notification with the
/// workflow id.
#[tokio::test]
#[serial]
async fn finalization_preserves_explicit_and_default_outputs() {
    let pool = shared_pool().await;
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("connect the listener");
    listener
        .listen("workflow_done")
        .await
        .expect("listen on workflow_done");
    for success in [false, true] {
        for output in [None, Some(0_i32)] {
            for bytes in [64_usize, 65536] {
                let mut seed = WorkflowSeed::running();
                seed.output_task_index = output;
                let id = seed_workflow(&pool, &seed).await;
                let result = serde_json::to_string(&TaskResult::Ok(serde_json::Value::String(
                    "x".repeat(bytes),
                )))
                .expect("encode the node result");
                for index in 0..8_i32 {
                    let status = match (success, index) {
                        (false, 0) => "FAILED",
                        _ => "COMPLETED",
                    };
                    let mut node_seed = NodeSeed::task(id, index, status).result(result.clone());
                    node_seed.task_name = "finalization_contract".to_owned();
                    seed_node(&pool, &node_seed).await;
                }

                complete(&pool, id).await;

                let row = workflow(&pool, id).await;
                assert_eq!(row.status, if success { "COMPLETED" } else { "FAILED" });
                match (success, output) {
                    (false, _) => assert_eq!(row.result, None),
                    (true, Some(_)) => assert_eq!(row.result, Some(result.clone())),
                    (true, None) => {
                        let entry: serde_json::Value =
                            serde_json::from_str(&result).expect("the node result is JSON");
                        let per_node = |value: serde_json::Value| {
                            (0..8)
                                .map(|index| (format!("node_{index}"), value.clone()))
                                .collect::<serde_json::Map<_, _>>()
                        };
                        let stored: serde_json::Value = serde_json::from_str(
                            row.result
                                .as_deref()
                                .expect("a COMPLETED workflow stores its result"),
                        )
                        .expect("the stored result is JSON");
                        assert_eq!(
                            stored,
                            serde_json::json!({
                                "__h_task_result__": true,
                                "__h_outputless_terminals__": true,
                                "ok": {
                                    "results_by_id": per_node(entry),
                                    "task_name_by_id": per_node(serde_json::json!(
                                        "finalization_contract"
                                    )),
                                    "definition_key_by_id": per_node(serde_json::Value::Null),
                                },
                                "err": null,
                            })
                        );
                    }
                }
                let notification =
                    tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
                        .await
                        .expect("a workflow_done notification")
                        .expect("the listener reads");
                assert_eq!(notification.payload(), id.to_string());
                assert!(tokio::time::timeout(
                    std::time::Duration::from_millis(25),
                    listener.recv(),
                )
                .await
                .is_err());
            }
        }
    }
}

/// `TestDecodeErrOnly::test_a_missing_marker_names_the_cause`,
/// `test_a_false_marker_names_the_cause`,
/// `test_an_extra_key_in_the_envelope_names_the_cause`,
/// `test_both_slots_populated_names_the_cause`,
/// `test_a_missing_err_key_names_the_cause`,
/// `test_a_missing_ok_key_names_the_cause`,
/// `test_json_that_is_not_an_object_names_the_cause`,
/// `test_text_that_is_not_json_names_the_cause`: a workflow whose first
/// FAILED node stores a result that is not a strict result envelope fails
/// with the error `RESULT_DESERIALIZATION_ERROR` (the baseline
/// `horsies_workflow_err_slot`).
#[tokio::test]
#[serial]
async fn a_first_failed_node_result_that_does_not_decode_gives_result_deserialization_error() {
    let pool = shared_pool().await;
    let error = serde_json::json!({"error_code": "BOOM", "message": "boom", "data": null});
    let undecodable = [
        (
            "missing marker",
            serde_json::json!({"ok": null, "err": error}).to_string(),
        ),
        (
            "false marker",
            serde_json::json!({"__h_task_result__": false, "ok": null, "err": error}).to_string(),
        ),
        (
            "extra key",
            serde_json::json!({
                "__h_task_result__": true, "ok": null, "err": error, "extra_key": "smuggled",
            })
            .to_string(),
        ),
        (
            "both slots",
            serde_json::json!({"__h_task_result__": true, "ok": 42, "err": error}).to_string(),
        ),
        (
            "missing err",
            serde_json::json!({"__h_task_result__": true, "ok": null}).to_string(),
        ),
        (
            "missing ok",
            serde_json::json!({"__h_task_result__": true, "err": error}).to_string(),
        ),
        ("null", "null".to_owned()),
        ("a string", "\"a string\"".to_owned()),
        ("an array", "[]".to_owned()),
        ("not JSON", "not json at all".to_owned()),
    ];
    for (case, stored) in undecodable {
        let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
        seed_node(
            &pool,
            &NodeSeed::task(workflow_id, 0, "FAILED").result(stored),
        )
        .await;

        complete(&pool, workflow_id).await;

        let row = workflow(&pool, workflow_id).await;
        assert_eq!(row.status, "FAILED", "{case}");
        let error = stored_error(row.error.as_deref().expect("the workflow stores its error"));
        assert_eq!(
            code(&error).as_deref(),
            Some("RESULT_DESERIALIZATION_ERROR"),
            "{case}"
        );
        assert!(error.message.is_some(), "{case}");
    }
}

/// A late completion check does not end a CANCELLED workflow again, also
/// when its `completed_at` is NULL: only a RUNNING workflow is finalized
/// (Python `check_workflow_completion`; the baseline
/// `horsies_workflow_try_complete`).
#[tokio::test]
#[serial]
async fn a_completion_check_does_not_resurrect_a_cancelled_workflow() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
    seed_node(
        &pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED")
            .result(serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the output")),
    )
    .await;
    sqlx::query(
        "UPDATE horsies_workflows SET status = 'CANCELLED', completed_at = NULL WHERE id = $1",
    )
    .bind(workflow_id)
    .execute(&pool)
    .await
    .expect("cancel the workflow with no completed_at");

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "CANCELLED");
    assert_eq!(row.result, None);
    assert_eq!(row.completed_at, None);
}

/// `TestOnChildWorkflowComplete::test_child_not_found_returns`: the advance
/// of a child that has no row writes nothing (`CHILD_ABSENT`).
#[tokio::test]
#[serial]
async fn a_parent_advance_of_a_child_with_no_row_writes_nothing() {
    let pool = shared_pool().await;
    let (parent_id, child_id) =
        seed_parent_and_child(&pool, WorkflowSeed::running().status("COMPLETED")).await;
    let before = node(&pool, parent_id, 0).await;

    advance_parent_of(&pool, Uuid::new_v4(), &TestChildSpecs::empty()).await;

    let after = node(&pool, parent_id, 0).await;
    assert_eq!(after.status, "RUNNING");
    assert_eq!(after.result, before.result);
    assert_eq!(after.sub_workflow_summary, None);
    assert_eq!(workflow(&pool, child_id).await.status, "COMPLETED");
}

/// `TestOnChildWorkflowComplete::test_parent_paused_stops_propagation`: the
/// end of a child writes its parent node, and a PAUSED parent promotes
/// nothing: the dependent of the node stays PENDING with no task row.
#[tokio::test]
#[serial]
async fn a_child_end_under_a_paused_parent_writes_the_node_and_promotes_nothing() {
    let pool = shared_pool().await;
    let (parent_id, child_id) = seed_parent_and_child(
        &pool,
        WorkflowSeed::running()
            .status("COMPLETED")
            .output(0)
            .result(serde_json::to_string(&ok(serde_json::json!(5))).expect("encode the output")),
    )
    .await;
    seed_node(&pool, &NodeSeed::task(parent_id, 1, "PENDING").deps(&[0])).await;
    sqlx::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
        .bind(parent_id)
        .execute(&pool)
        .await
        .expect("pause the parent");

    advance_parent_of(&pool, child_id, &TestChildSpecs::empty()).await;

    assert_eq!(node(&pool, parent_id, 0).await.status, "COMPLETED");
    let dependent = node(&pool, parent_id, 1).await;
    assert_eq!(dependent.status, "PENDING");
    assert_eq!(dependent.task_id, None);
    assert_eq!(workflow(&pool, parent_id).await.status, "PAUSED");
}

/// `TestEvaluateWorkflowSuccess::test_empty_required_indices_skipped`: a case
/// that requires no node is never met. With no other case the workflow
/// fails with `WORKFLOW_SUCCESS_CASE_NOT_MET`.
#[tokio::test]
#[serial]
async fn a_case_that_requires_no_node_is_never_met() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(
        &pool,
        &WorkflowSeed::running().policy(serde_json::json!({"cases": [{"required_indices": []}]})),
    )
    .await;
    seed_node(
        &pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED")
            .result(serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the output")),
    )
    .await;

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "FAILED");
    let error = stored_error(row.error.as_deref().expect("the workflow stores its error"));
    assert_eq!(
        code(&error).as_deref(),
        Some("WORKFLOW_SUCCESS_CASE_NOT_MET")
    );
}

/// `TestDecodeErrOnly::test_returns_none_on_ok_only_envelope`: a first
/// FAILED node whose stored envelope holds an ok value gives no failure
/// error; the workflow fails and keeps its stored error (none here).
#[tokio::test]
#[serial]
async fn a_first_failed_node_with_an_ok_envelope_gives_no_failure_error() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
    seed_node(
        &pool,
        &NodeSeed::task(workflow_id, 0, "FAILED")
            .result(serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the result")),
    )
    .await;

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "FAILED");
    assert_eq!(row.error, None);
}

/// `TestGetWorkflowFinalResult::test_non_string_node_id_skipped`,
/// `test_task_result_parse_fails_stores_none`: the outputless result has no
/// entry for a terminal node with no node id, and a null entry for a
/// terminal node whose stored text is not a result envelope; it is still
/// built.
#[tokio::test]
#[serial]
async fn the_outputless_result_skips_a_node_without_an_id_and_reads_other_text_as_null() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
    let two = serde_json::to_string(&ok(serde_json::json!(2))).expect("encode the result");
    seed_node(
        &pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED")
            .result(serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the result")),
    )
    .await;
    seed_node(
        &pool,
        &NodeSeed::task(workflow_id, 1, "COMPLETED").result("not an envelope".to_owned()),
    )
    .await;
    seed_node(
        &pool,
        &NodeSeed::task(workflow_id, 2, "COMPLETED").result(two.clone()),
    )
    .await;
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET node_id = NULL WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(workflow_id)
    .execute(&pool)
    .await
    .expect("drop the node id of node 0");

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "COMPLETED");
    let stored: serde_json::Value = serde_json::from_str(
        row.result
            .as_deref()
            .expect("a COMPLETED workflow stores its result"),
    )
    .expect("the stored result is JSON");
    let two: serde_json::Value = serde_json::from_str(&two).expect("the result is JSON");
    assert_eq!(
        stored["ok"]["results_by_id"],
        serde_json::json!({"node_1": null, "node_2": two})
    );
    assert_eq!(
        stored["ok"]["task_name_by_id"],
        serde_json::json!({"node_1": "port_task", "node_2": "port_task"})
    );
}
