//! The completion branches of the engine: the success rule
//! (`evaluate_workflow_success`), the workflow error of a failed workflow
//! (`get_workflow_failure_error`), the result of an outputless workflow
//! (`outputless_result`) and the failure policy (`handle_workflow_task_failure`).
//! Python: the completion branches of
//! `tests/unit/test_workflow_engine_coverage.py`, and `TestDecodeErrOnly`,
//! whose `_decode_err_only` reads the first failed node for the workflow
//! error. Each test names the Python tests that it ports.
//!
//! The database tests use the seeds of [`super::promotion_branch_tests`] on
//! the shared migrated database and remove the rows that they write.

use serial_test::serial;

use super::promotion_branch_tests::{remove_tree, seed_task_node, seed_workflow, shared_pool};
use super::*;

/// The stored envelope of a failure with the user code `code`.
fn failure(code: &str, message: &str) -> String {
    serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        TaskError::new(code, message).expect("a user error code"),
    ))
    .unwrap()
}

fn statuses(values: &[&str]) -> Vec<TaskStatusRow> {
    values
        .iter()
        .enumerate()
        .map(|(index, status)| TaskStatusRow {
            task_index: index as i32,
            status: (*status).to_owned(),
        })
        .collect()
}

/// A workflow whose nodes have the given (status, stored result), in index
/// order, none depending on another. Returns the workflow id.
async fn seed_terminal_nodes(pool: &PgPool, nodes: &[(&str, Option<&str>)]) -> Uuid {
    let workflow_id = seed_workflow(pool).await;
    for (task_index, (status, result)) in (0_i32..).zip(nodes) {
        seed_task_node(pool, workflow_id, task_index, &[], status).await;
        sqlx::query(
            "UPDATE horsies_workflow_tasks SET result = $3 WHERE workflow_id = $1 AND task_index = $2",
        )
        .bind(workflow_id)
        .bind(task_index)
        .bind(*result)
        .execute(pool)
        .await
        .expect("store the node result");
    }
    workflow_id
}

/// `get_workflow_failure_error` on one connection of `pool`.
async fn failure_error(
    pool: &PgPool,
    workflow_id: Uuid,
    policy: Option<serde_json::Value>,
) -> Result<Option<String>, WorkflowError> {
    let mut connection = pool.acquire().await.expect("a connection");
    get_workflow_failure_error(&mut connection, workflow_id, &policy).await
}

/// The error object of a workflow error text.
fn error_object(text: &str) -> TaskError {
    serde_json::from_str(text).unwrap_or_else(|error| panic!("{error}: {text}"))
}

async fn stored_workflow_error(pool: &PgPool, workflow_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT error FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("read the workflow error")
}

fn terminal_row(node_id: Option<&str>, result: &str) -> NodeResult {
    NodeResult {
        node_id: node_id.map(str::to_owned),
        result: Some(result.to_owned()),
        status: "COMPLETED".to_owned(),
        task_index: 0,
        task_name: "task_a".to_owned(),
        sub_definition_key: None,
    }
}

/// `TestEvaluateWorkflowSuccess::test_string_policy_parsed`: a stored policy
/// whose one case requires a node that COMPLETED is met.
#[test]
fn a_stored_policy_whose_required_node_completed_is_met() {
    let policy = Some(serde_json::json!({"cases": [{"required_indices": [0]}]}));

    let succeeded = evaluate_workflow_success(&policy, false, false, &statuses(&["COMPLETED"]));

    assert!(matches!(succeeded, Ok(true)), "{succeeded:?}");
}

/// `TestEvaluateWorkflowSuccess::test_empty_required_indices_skipped`: a case
/// that requires no node is never met.
#[test]
fn a_case_that_requires_no_node_is_never_met() {
    let policy = Some(serde_json::json!({"cases": [{"required_indices": []}]}));

    let succeeded = evaluate_workflow_success(&policy, false, false, &statuses(&[]));

    assert!(matches!(succeeded, Ok(false)), "{succeeded:?}");
}

/// `TestGetWorkflowFailureError::test_no_policy_extracts_first_failed_error`;
/// `TestDecodeErrOnly::test_decodes_canonical_err_envelope`: with no policy,
/// the workflow error is the error of the first FAILED node by index, read
/// from the err slot of its stored envelope.
#[tokio::test]
#[serial]
async fn without_a_policy_the_failure_error_is_the_error_of_the_first_failed_node() {
    let pool = shared_pool().await;
    let first = failure("FIRST_ERROR", "first failure");
    let second = failure("SECOND_ERROR", "second failure");
    let workflow_id = seed_terminal_nodes(
        &pool,
        &[
            ("COMPLETED", None),
            ("FAILED", Some(first.as_str())),
            ("FAILED", Some(second.as_str())),
        ],
    )
    .await;

    let read = failure_error(&pool, workflow_id, None).await;

    remove_tree(&pool, workflow_id).await;
    let text = read.expect("the read").expect("a failure error");
    let error = error_object(&text);
    assert_eq!(error.error_code.to_string(), "FIRST_ERROR");
    assert_eq!(error.message.as_deref(), Some("first failure"));
}

/// `TestGetWorkflowFailureError::test_no_policy_no_failed_returns_none`: with
/// no policy and no FAILED node there is no failure error; the caller keeps
/// the stored one.
#[tokio::test]
#[serial]
async fn without_a_policy_or_a_failed_node_there_is_no_failure_error() {
    let pool = shared_pool().await;
    let workflow_id = seed_terminal_nodes(&pool, &[("COMPLETED", None)]).await;

    let read = failure_error(&pool, workflow_id, None).await;

    remove_tree(&pool, workflow_id).await;
    assert!(matches!(read, Ok(None)), "{read:?}");
}

/// `TestGetWorkflowFailureError::test_string_policy_parsed`: with a policy and
/// no FAILED required node, the failure error is
/// `WORKFLOW_SUCCESS_CASE_NOT_MET`.
#[tokio::test]
#[serial]
async fn with_a_policy_and_no_failed_required_node_the_failure_error_is_success_case_not_met() {
    let pool = shared_pool().await;
    let workflow_id = seed_terminal_nodes(&pool, &[("SKIPPED", None)]).await;

    let read = failure_error(
        &pool,
        workflow_id,
        Some(serde_json::json!({"cases": [{"required_indices": [0]}]})),
    )
    .await;

    remove_tree(&pool, workflow_id).await;
    let text = read.expect("the read").expect("a failure error");
    assert_eq!(
        error_object(&text).error_code.to_string(),
        "WORKFLOW_SUCCESS_CASE_NOT_MET"
    );
}

/// `TestGetWorkflowFailureError::test_corrupt_policy_fails_the_step`: a stored
/// policy that is not an object fails the read.
#[tokio::test]
#[serial]
async fn a_stored_policy_that_is_not_an_object_fails_the_failure_error_read() {
    let pool = shared_pool().await;
    let workflow_id = seed_terminal_nodes(&pool, &[("COMPLETED", None)]).await;

    let read = failure_error(&pool, workflow_id, Some(serde_json::json!([1, 2]))).await;

    remove_tree(&pool, workflow_id).await;
    assert!(
        matches!(read, Err(WorkflowError::Serialization(_))),
        "{read:?}"
    );
}

/// `TestDecodeErrOnly::test_returns_none_on_ok_only_envelope`: a first FAILED
/// node whose stored envelope holds an ok value gives no failure error.
#[tokio::test]
#[serial]
async fn a_first_failed_node_with_an_ok_envelope_gives_no_failure_error() {
    let pool = shared_pool().await;
    let ok = serde_json::to_string(&TaskResult::Ok(serde_json::json!(42))).unwrap();
    let workflow_id = seed_terminal_nodes(&pool, &[("FAILED", Some(ok.as_str()))]).await;

    let read = failure_error(&pool, workflow_id, None).await;

    remove_tree(&pool, workflow_id).await;
    assert!(matches!(read, Ok(None)), "{read:?}");
}

/// `TestDecodeErrOnly::test_preserves_sub_workflow_error_subclass_fields`: the
/// failure error of a first FAILED sub-workflow node keeps its child id and
/// summary.
#[tokio::test]
#[serial]
async fn a_first_failed_sub_workflow_error_keeps_its_child_fields() {
    let pool = shared_pool().await;
    let child_id = Uuid::new_v4();
    let summary = SubWorkflowSummary {
        status: "FAILED".to_owned(),
        output: None,
        total_tasks: 1,
        completed_tasks: 0,
        failed_tasks: 1,
        skipped_tasks: 0,
        error_summary: Some("downstream".to_owned()),
    };
    let stored = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        subworkflow_failed_error(child_id, "child failed".to_owned(), &summary),
    ))
    .unwrap();
    let workflow_id = seed_terminal_nodes(&pool, &[("FAILED", Some(stored.as_str()))]).await;

    let read = failure_error(&pool, workflow_id, None).await;

    remove_tree(&pool, workflow_id).await;
    let text = read.expect("the read").expect("a failure error");
    let error = error_object(&text);
    let details = error
        .sub_workflow_details()
        .expect("the error keeps its child fields");
    assert_eq!(details.sub_workflow_id, child_id);
    assert_eq!(details.sub_workflow_summary, summary);
}

/// Stored node results that are not a strict result envelope, built from a
/// canonical err envelope.
fn undecodable_results() -> Vec<(&'static str, String)> {
    let canonical: serde_json::Value = serde_json::from_str(&failure("BOOM", "boom")).unwrap();
    let object = canonical.as_object().expect("an envelope object").clone();
    assert!(
        ["__h_task_result__", "ok", "err"]
            .iter()
            .all(|key| object.contains_key(*key)),
        "{canonical}"
    );
    let edited = |edit: &dyn Fn(&mut serde_json::Map<String, serde_json::Value>)| {
        let mut copy = object.clone();
        edit(&mut copy);
        serde_json::Value::Object(copy).to_string()
    };
    vec![
        (
            "missing marker",
            edited(&|o| drop(o.remove("__h_task_result__"))),
        ),
        (
            "false marker",
            edited(&|o| drop(o.insert("__h_task_result__".to_owned(), serde_json::json!(false)))),
        ),
        (
            "extra key",
            edited(&|o| drop(o.insert("extra_key".to_owned(), serde_json::json!("smuggled")))),
        ),
        (
            "both slots",
            edited(&|o| drop(o.insert("ok".to_owned(), serde_json::json!(42)))),
        ),
        ("missing err", edited(&|o| drop(o.remove("err")))),
        ("missing ok", edited(&|o| drop(o.remove("ok")))),
        ("null", "null".to_owned()),
        ("a string", "\"a string\"".to_owned()),
        ("an array", "[]".to_owned()),
        ("not JSON", "not json at all".to_owned()),
    ]
}

/// `TestDecodeErrOnly::test_a_missing_marker_names_the_cause`,
/// `test_a_false_marker_names_the_cause`,
/// `test_an_extra_key_in_the_envelope_names_the_cause`,
/// `test_both_slots_populated_names_the_cause`,
/// `test_a_missing_err_key_names_the_cause`,
/// `test_a_missing_ok_key_names_the_cause`,
/// `test_json_that_is_not_an_object_names_the_cause`,
/// `test_text_that_is_not_json_names_the_cause`: a first FAILED node whose
/// stored result is not a strict result envelope gives the failure error
/// `RESULT_DESERIALIZATION_ERROR`.
#[tokio::test]
#[serial]
#[ignore = "waits on #1234"]
async fn a_first_failed_node_result_that_does_not_decode_gives_result_deserialization_error() {
    let pool = shared_pool().await;
    for (case, stored) in undecodable_results() {
        let workflow_id = seed_terminal_nodes(&pool, &[("FAILED", Some(stored.as_str()))]).await;

        let read = failure_error(&pool, workflow_id, None).await;

        remove_tree(&pool, workflow_id).await;
        let text = read
            .unwrap_or_else(|error| panic!("{case}: {error}"))
            .unwrap_or_else(|| panic!("{case}: no failure error"));
        let error = error_object(&text);
        assert_eq!(
            error.error_code.to_string(),
            "RESULT_DESERIALIZATION_ERROR",
            "{case}"
        );
        assert!(error.message.is_some(), "{case}");
    }
}

/// `TestGetWorkflowFinalResult::test_non_string_node_id_skipped`: a terminal
/// node with no node id has no entry; the outputless result is still built.
#[test]
fn a_terminal_node_without_a_node_id_has_no_outputless_entry() {
    let ok = serde_json::to_string(&TaskResult::Ok(serde_json::Value::Null)).unwrap();

    let result = outputless_result(Uuid::nil(), &[terminal_row(None, &ok)]).unwrap();

    assert!(result.ok.results_by_id.is_empty());
    assert!(result.ok.task_name_by_id.is_empty());
}

/// `TestGetWorkflowFinalResult::test_task_result_parse_fails_stores_none`: a
/// terminal node whose stored text is not a result envelope gets a null
/// entry, and the outputless result is still built.
#[test]
fn a_terminal_node_whose_result_is_not_an_envelope_has_a_null_entry() {
    let row = terminal_row(Some("node-0"), r#"{"valid": "but_not_taskresult"}"#);

    let result = outputless_result(Uuid::nil(), &[row]).unwrap();

    assert_eq!(
        result.ok.results_by_id.get("node-0"),
        Some(&serde_json::Value::Null)
    );
    assert_eq!(
        result.ok.task_name_by_id.get("node-0").map(String::as_str),
        Some("task_a")
    );
}

/// `TestHandleWorkflowTaskFailure::test_workflow_not_found_returns_true`: the
/// failure policy of a workflow that has no row lets the step go on.
#[tokio::test]
#[serial]
async fn the_failure_policy_of_a_workflow_with_no_row_lets_the_step_go_on() {
    let pool = shared_pool().await;
    let error_json = serde_json::to_string(&TaskError::new("ERR", "fail").unwrap()).unwrap();

    let go_on =
        handle_workflow_task_failure(&pool, Uuid::new_v4(), 0, "fail", &error_json, false).await;

    assert!(matches!(go_on, Ok(true)), "{go_on:?}");
}

/// `TestHandleWorkflowTaskFailure::test_unknown_on_error_returns_true`: an
/// `on_error` policy that is not known is applied as `fail`: the step goes
/// on and the first failed error is stored.
#[tokio::test]
#[serial]
async fn an_unknown_on_error_policy_is_applied_as_fail() {
    let pool = shared_pool().await;
    let first = failure("FIRST_ERROR", "first failure");
    let workflow_id = seed_terminal_nodes(&pool, &[("FAILED", Some(first.as_str()))]).await;
    let error_json = serde_json::to_string(&TaskError::new("ERR", "fail").unwrap()).unwrap();

    let go_on =
        handle_workflow_task_failure(&pool, workflow_id, 0, "unknown_value", &error_json, false)
            .await;

    let stored = stored_workflow_error(&pool, workflow_id).await;
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(go_on, Ok(true)), "{go_on:?}");
    let stored = stored.expect("the workflow stores an error");
    assert_eq!(error_object(&stored).error_code.to_string(), "FIRST_ERROR");
}

/// `TestHandleWorkflowTaskFailure::test_fail_policy_locks_and_uses_first_failed_error`:
/// the `fail` policy stores the error of the first failed node by index, not
/// the error of the failure that triggered it.
#[tokio::test]
#[serial]
async fn the_fail_policy_stores_the_error_of_the_first_failed_node() {
    let pool = shared_pool().await;
    let first = failure("FIRST_ERROR", "first failure");
    let second = failure("SECOND_ERROR", "second failure");
    let workflow_id = seed_terminal_nodes(
        &pool,
        &[
            ("FAILED", Some(first.as_str())),
            ("FAILED", Some(second.as_str())),
        ],
    )
    .await;
    let triggering =
        serde_json::to_string(&TaskError::new("SECOND_ERROR", "second failure").unwrap()).unwrap();

    let go_on =
        handle_workflow_task_failure(&pool, workflow_id, 1, "fail", &triggering, false).await;

    let stored = stored_workflow_error(&pool, workflow_id).await;
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(go_on, Ok(true)), "{go_on:?}");
    let stored = stored.expect("the workflow stores an error");
    assert_eq!(error_object(&stored).error_code.to_string(), "FIRST_ERROR");
}
