//! The reads of the workflow handle: status, get, results, result_for, tasks,
//! and their errors. Python: `tests/integration/test_workflow_handle.py`
//! (the read classes).

use std::time::Duration;

use serial_test::serial;
use uuid::Uuid;

use super::support::{
    broker, code, end_workflows, err, error_of, finish_node, handle, ok, seed_node, seed_workflow,
    shared_pool, spec, start, task_node, unreachable_handle, IsolatedDatabase, NodeSeed,
    ParentLink, WorkflowSeed,
};
use crate::core::workflow::handle_types::HandleErrorCode;
use crate::core::PausedWorkflowAge;
use crate::core::{
    OnError, SubWorkflowSummary, TaskResult, WorkflowSpec, WorkflowStatus, WorkflowTaskStatus,
};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::lifecycle::expire_paused_workflows;
use crate::workflow_engine::query::NodesRead;

/// How long `get()` waits for a workflow that is terminal before the call.
const GET_WAIT: Duration = Duration::from_secs(1);

/// A → B: B waits for A.
fn chain(name: &str) -> WorkflowSpec {
    let mut second = task_node(1, &format!("{name}_b"));
    second.dependencies = vec![0];
    spec(name, vec![task_node(0, &format!("{name}_a")), second])
}

/// One node, and the given failure policy.
fn single(name: &str, on_error: OnError) -> WorkflowSpec {
    let mut single = spec(name, vec![task_node(0, &format!("{name}_a"))]);
    single.on_error = on_error;
    single
}

async fn status(pool: &TestPool, workflow_id: Uuid) -> WorkflowStatus {
    handle(pool, workflow_id).status().await.expect("status()")
}

fn ok_value(result: &TaskResult<serde_json::Value>) -> &serde_json::Value {
    match result {
        TaskResult::Ok(value) => value,
        TaskResult::Err(error) => panic!("expected ok, got {error:?}"),
    }
}

/// `TestWorkflowHandleStatus.test_status_running`
#[tokio::test]
#[serial]
async fn status_reads_running() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("status_running"), &TestChildSpecs::empty()).await;

    assert_eq!(status(&pool, workflow_id).await, WorkflowStatus::Running);
}

/// `TestWorkflowHandleStatus.test_status_completed`
#[tokio::test]
#[serial]
async fn status_reads_completed() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(
        &pool,
        &single("status_completed", OnError::Fail),
        &child_specs,
    )
    .await;
    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(10)),
        &child_specs,
    )
    .await;

    assert_eq!(status(&pool, workflow_id).await, WorkflowStatus::Completed);
}

/// `TestWorkflowHandleStatus.test_status_failed`
#[tokio::test]
#[serial]
async fn status_reads_failed() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(&pool, &single("status_failed", OnError::Fail), &child_specs).await;
    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &err("TEST_FAIL", "failed"),
        &child_specs,
    )
    .await;

    assert_eq!(status(&pool, workflow_id).await, WorkflowStatus::Failed);
}

/// `TestWorkflowHandleStatus.test_status_paused`
#[tokio::test]
#[serial]
async fn status_reads_paused() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(
        &pool,
        &single("status_paused", OnError::Pause),
        &child_specs,
    )
    .await;
    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &err("TEST_FAIL", "failed"),
        &child_specs,
    )
    .await;

    assert_eq!(status(&pool, workflow_id).await, WorkflowStatus::Paused);
}

/// `TestWorkflowHandleStatus.test_status_nonexistent_returns_not_found`
#[tokio::test]
#[serial]
async fn status_of_an_unknown_id_is_workflow_not_found() {
    let pool = shared_pool().await;
    let workflow_id = Uuid::new_v4();

    let error = handle(&pool, workflow_id)
        .status()
        .await
        .expect_err("an unknown id has no status");

    assert_eq!(error.code(), HandleErrorCode::WorkflowNotFound);
    assert!(!error.retryable());
    assert_eq!(error.workflow_id(), workflow_id);
}

/// A PAUSED workflow of one failed node, set back an hour so that a paused
/// expiry with a short age selects it.
async fn paused_an_hour_ago(pool: &TestPool, name: &str) -> Uuid {
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(pool, &single(name, OnError::Pause), &child_specs).await;
    let _task = finish_node(pool, workflow_id, 0, &err("FAIL", "pause"), &child_specs).await;
    assert_eq!(status(pool, workflow_id).await, WorkflowStatus::Paused);
    sql::query("UPDATE horsies_workflows SET updated_at = NOW() - INTERVAL '1 hour' WHERE id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("set the pause back an hour");
    workflow_id
}

/// `TestWorkflowHandleGet.test_auto_expired_pause_returns_workflow_expired_error`:
/// an expired pause reads EXPIRED, and `get()` gives `WORKFLOW_EXPIRED`. The
/// Python test's second half (a 0.x text error read as `WORKFLOW_EXPIRED`) is
/// not ported: the clean slate keeps no 0.x stored values, and the core fails
/// closed on such a text (`the_result_read_fails_closed_on_a_malformed_expiry_error`).
#[tokio::test]
#[serial]
async fn an_expired_pause_reads_expired_and_get_gives_workflow_expired() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = paused_an_hour_ago(&db.pool, "get_expired_pause").await;

    let age = PausedWorkflowAge::parse(chrono::Duration::milliseconds(1)).expect("an age of 1 ms");
    let expired = expire_paused_workflows(&broker(&db.pool), age, 1)
        .await
        .expect("paused expiry");

    assert_eq!(expired, 1);
    assert_eq!(status(&db.pool, workflow_id).await, WorkflowStatus::Expired);
    let got = handle(&db.pool, workflow_id)
        .get(Some(Duration::ZERO))
        .await
        .expect("the result read");
    assert_eq!(code(error_of(&got)).as_deref(), Some("WORKFLOW_EXPIRED"));
    db.drop().await;
}

/// The call of `test_auto_expired_pause_returns_workflow_expired_error`,
/// which passes a zero age: the smallest age the core takes (1 µs) expires a
/// PAUSED workflow. A zero age is refused where the age enters, as Python's
/// config refuses it (`config::retention` tests); the sweep takes only an
/// age of the config.
#[tokio::test]
#[serial]
async fn the_smallest_age_expires_a_paused_workflow() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = paused_an_hour_ago(&db.pool, "get_expired_pause_zero_age").await;

    let age = PausedWorkflowAge::parse(chrono::Duration::microseconds(1)).expect("an age of 1 µs");
    let expired = expire_paused_workflows(&broker(&db.pool), age, 1)
        .await
        .expect("paused expiry with the smallest age");

    assert_eq!(expired, 1);
    assert_eq!(status(&db.pool, workflow_id).await, WorkflowStatus::Expired);
    db.drop().await;
}

/// The longest age the config takes (`PausedWorkflowAge::MAX_DAYS`) runs
/// the candidate read without a timestamp or interval range error: an hour
/// old pause is not a candidate, and nothing is expired.
#[tokio::test]
#[serial]
async fn the_longest_age_reads_its_candidates_without_a_range_error() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = paused_an_hour_ago(&db.pool, "longest_paused_age").await;

    let age = PausedWorkflowAge::parse(chrono::Duration::days(PausedWorkflowAge::MAX_DAYS))
        .expect("the longest age");
    let expired = expire_paused_workflows(&broker(&db.pool), age, 1)
        .await
        .expect("paused expiry with the longest age");

    assert_eq!(expired, 0);
    assert_eq!(status(&db.pool, workflow_id).await, WorkflowStatus::Paused);
    db.drop().await;
}

/// `TestWorkflowHandleGet.test_get_timeout`
#[tokio::test]
#[serial]
async fn get_gives_wait_timeout_when_the_workflow_does_not_finish() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("get_timeout"), &TestChildSpecs::empty()).await;

    let got = handle(&pool, workflow_id)
        .get(Some(Duration::from_millis(500)))
        .await
        .expect("the result read");

    assert_eq!(code(error_of(&got)).as_deref(), Some("WAIT_TIMEOUT"));
}

/// `TestWorkflowHandleGet.test_get_cancelled_returns_cancelled_error`
#[tokio::test]
#[serial]
async fn get_after_cancel_gives_workflow_cancelled() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("get_cancelled"), &TestChildSpecs::empty()).await;
    let workflow_handle = handle(&pool, workflow_id);
    workflow_handle.cancel().await.expect("cancel()");

    let got = workflow_handle
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");

    assert_eq!(code(error_of(&got)).as_deref(), Some("WORKFLOW_CANCELLED"));
}

/// `TestWorkflowHandleGet.test_get_completed_returns_result`: a workflow of
/// one node whose output is that node gives the node's result at `get()`.
#[tokio::test]
#[serial]
async fn get_of_a_completed_workflow_gives_the_result_of_its_output_node() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let mut workflow_spec = spec("get_completed", vec![task_node(0, "get_completed_a")]);
    workflow_spec.output_index = Some(0);
    let workflow_id = start(&pool, &workflow_spec, &child_specs).await;
    let _a = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(100)),
        &child_specs,
    )
    .await;

    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");

    match got {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(100)),
        TaskResult::Err(error) => panic!("a completed workflow gave an error: {error:?}"),
    }
}

/// `TestWorkflowHandleGet.test_get_failed_returns_error`: a workflow whose
/// one node fails gives the node's error at `get()`.
#[tokio::test]
#[serial]
async fn get_of_a_failed_workflow_gives_the_error_of_its_node() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_spec = spec("get_failed", vec![task_node(0, "get_failed_a")]);
    let workflow_id = start(&pool, &workflow_spec, &child_specs).await;
    let _a = finish_node(
        &pool,
        workflow_id,
        0,
        &err("TEST_ERROR", "Test failure"),
        &child_specs,
    )
    .await;

    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");

    assert_eq!(code(error_of(&got)).as_deref(), Some("TEST_ERROR"));
}

/// A finished chain: A gave 10 and B gave 20.
async fn finished_chain(pool: &TestPool, name: &str) -> Uuid {
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(pool, &chain(name), &child_specs).await;
    let _a = finish_node(
        pool,
        workflow_id,
        0,
        &ok(serde_json::json!(10)),
        &child_specs,
    )
    .await;
    let _b = finish_node(
        pool,
        workflow_id,
        1,
        &ok(serde_json::json!(20)),
        &child_specs,
    )
    .await;
    workflow_id
}

/// `TestWorkflowHandleResults.test_results_returns_all`
#[tokio::test]
#[serial]
async fn results_gives_every_stored_node_result_by_node_id() {
    let pool = shared_pool().await;
    let workflow_id = finished_chain(&pool, "results_all").await;

    let results = handle(&pool, workflow_id)
        .results()
        .await
        .expect("results()");

    assert_eq!(results.len(), 2);
    assert_eq!(ok_value(&results["node_0"]), &serde_json::json!(10));
    assert_eq!(ok_value(&results["node_1"]), &serde_json::json!(20));
}

/// `TestWorkflowHandleResults.test_result_for_returns_typed_result`
#[tokio::test]
#[serial]
async fn result_for_gives_the_typed_result_of_one_node() {
    let pool = shared_pool().await;
    let workflow_id = finished_chain(&pool, "result_for").await;
    let workflow_handle = handle(&pool, workflow_id);

    for (node_id, expected) in [("node_0", 10_i64), ("node_1", 20_i64)] {
        match workflow_handle
            .result_for::<i64>(node_id)
            .await
            .expect("the node result read")
        {
            TaskResult::Ok(value) => assert_eq!(value, expected, "{node_id}"),
            TaskResult::Err(error) => panic!("result_for({node_id}): {error:?}"),
        }
    }
}

/// `TestWorkflowHandleResults.test_result_for_returns_error_when_not_ready`:
/// the node of an unfinished workflow stores no result, which a client reads
/// as `RESULT_NOT_READY`.
#[tokio::test]
#[serial]
async fn an_unfinished_node_stores_no_result() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &single("result_for_not_ready", OnError::Fail),
        &TestChildSpecs::empty(),
    )
    .await;

    let stored = handle(&pool, workflow_id)
        .node_result("node_0")
        .await
        .expect("the node result read");

    assert_eq!(stored, None);
}

/// `TestWorkflowHandleResults.test_results_empty_when_no_tasks_completed`
#[tokio::test]
#[serial]
async fn results_is_empty_before_any_node_finishes() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("results_empty"), &TestChildSpecs::empty()).await;

    let results = handle(&pool, workflow_id)
        .results()
        .await
        .expect("results()");

    assert!(results.is_empty(), "{results:?}");
}

/// `TestWorkflowHandleResults.test_tasks_returns_info`
#[tokio::test]
#[serial]
async fn tasks_gives_one_entry_per_node_with_its_task_name() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("tasks_info"), &TestChildSpecs::empty()).await;

    let tasks = handle(&pool, workflow_id).tasks().await.expect("tasks()");

    let names: Vec<&str> = tasks.iter().map(|task| task.name.as_str()).collect();
    assert_eq!(names, vec!["tasks_info_a", "tasks_info_b"]);
}

/// `TestWorkflowHandleResults.test_tasks_ordered_by_index`
#[tokio::test]
#[serial]
async fn tasks_is_ordered_by_node_index() {
    let pool = shared_pool().await;
    let mut join = task_node(2, "tasks_order_c");
    join.dependencies = vec![0, 1];
    let workflow_id = start(
        &pool,
        &spec(
            "tasks_order",
            vec![
                task_node(0, "tasks_order_a"),
                task_node(1, "tasks_order_b"),
                join,
            ],
        ),
        &TestChildSpecs::empty(),
    )
    .await;

    let tasks = handle(&pool, workflow_id).tasks().await.expect("tasks()");

    let indexes: Vec<i32> = tasks.iter().map(|task| task.index).collect();
    assert_eq!(indexes, vec![0, 1, 2]);
}

/// `TestWorkflowHandleResults.test_tasks_status_fields`
#[tokio::test]
#[serial]
async fn tasks_gives_enqueued_for_a_root_and_pending_for_a_dependent() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("tasks_status"), &TestChildSpecs::empty()).await;

    let tasks = handle(&pool, workflow_id).tasks().await.expect("tasks()");

    assert_eq!(tasks[0].status, WorkflowTaskStatus::Enqueued);
    assert_eq!(tasks[1].status, WorkflowTaskStatus::Pending);
    assert!(tasks[0].result.is_none());
    assert!(tasks[1].result.is_none());
}

/// `TestWorkflowHandleResults.test_tasks_completed_fields`
#[tokio::test]
#[serial]
async fn tasks_gives_the_result_and_times_of_a_finished_node() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(&pool, &single("tasks_fields", OnError::Fail), &child_specs).await;
    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(42)),
        &child_specs,
    )
    .await;

    let tasks = handle(&pool, workflow_id).tasks().await.expect("tasks()");

    assert_eq!(tasks.len(), 1);
    let info = &tasks[0];
    assert_eq!(info.status, WorkflowTaskStatus::Completed);
    let result = info.result.as_ref().expect("a finished node has a result");
    assert_eq!(ok_value(result), &serde_json::json!(42));
    assert!(info.started_at.is_some(), "the node started at its enqueue");
    assert!(info.completed_at.is_some());
}

/// `TestWorkflowHandleSubWorkflowFields.test_regular_task_has_none_subworkflow_fields`
#[tokio::test]
#[serial]
async fn a_task_node_has_no_child_and_no_summary() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &single("subwf_regular", OnError::Fail),
        &TestChildSpecs::empty(),
    )
    .await;

    let tasks = handle(&pool, workflow_id).tasks().await.expect("tasks()");

    assert_eq!(tasks[0].sub_workflow_id, None);
    assert_eq!(tasks[0].sub_workflow_summary, None);
}

/// A RUNNING parent whose sub-workflow node 0 has `status` and is linked to
/// a child workflow with `child_status`. Returns the parent and the child.
async fn seed_linked_child(
    pool: &TestPool,
    node_status: &'static str,
    child_status: &'static str,
    summary: Option<&str>,
) -> (Uuid, Uuid) {
    let parent = seed_workflow(pool, &WorkflowSeed::running()).await;
    let child = seed_workflow(
        pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0)).status(child_status),
    )
    .await;
    let mut sub =
        NodeSeed::sub_workflow(parent, 0, node_status, "tests.port.child.v1", Some(child));
    sub.sub_workflow_summary = summary.map(str::to_owned);
    seed_node(pool, &sub).await;
    (parent, child)
}

/// `TestWorkflowHandleSubWorkflowFields.test_subworkflow_node_exposes_sub_workflow_id`
#[tokio::test]
#[serial]
async fn a_started_sub_workflow_node_gives_its_child_id() {
    let pool = shared_pool().await;
    let (parent, child) = seed_linked_child(&pool, "RUNNING", "RUNNING", None).await;

    let tasks = handle(&pool, parent).tasks().await.expect("tasks()");

    assert_eq!(tasks[0].sub_workflow_id, Some(child));
    assert_eq!(tasks[0].sub_workflow_summary, None);
    end_workflows(&pool, &[parent, child]).await;
}

/// `TestWorkflowHandleSubWorkflowFields.test_subworkflow_node_with_valid_summary`
#[tokio::test]
#[serial]
async fn a_finished_sub_workflow_node_gives_its_summary() {
    let pool = shared_pool().await;
    let summary = r#"{"status": "COMPLETED", "output": null, "total_tasks": 3, "completed_tasks": 3, "failed_tasks": 0, "skipped_tasks": 0, "error_summary": null}"#;
    let (parent, child) = seed_linked_child(&pool, "COMPLETED", "COMPLETED", Some(summary)).await;

    let tasks = handle(&pool, parent).tasks().await.expect("tasks()");

    assert_eq!(tasks[0].sub_workflow_id, Some(child));
    let summary: SubWorkflowSummary = tasks[0]
        .sub_workflow_summary
        .clone()
        .expect("the node gives its summary");
    assert_eq!(summary.status, WorkflowStatus::Completed);
    assert_eq!(
        (
            summary.total_tasks,
            summary.completed_tasks,
            summary.failed_tasks,
            summary.skipped_tasks,
        ),
        (3, 3, 0, 0)
    );
    assert_eq!(summary.error_summary, None);
    end_workflows(&pool, &[parent, child]).await;
}

/// The stored sub-workflow summary of the node 0 of `parent`, as the node
/// read returns it.
async fn stored_summary_of(pool: &TestPool, parent: Uuid) -> Option<String> {
    match handle(pool, parent).nodes().await.expect("the node read") {
        NodesRead::Nodes(nodes) => nodes[0].sub_workflow_summary.clone(),
        NodesRead::WorkflowAbsent => panic!("the parent {parent} has a row"),
    }
}

/// `TestWorkflowHandleSubWorkflowFields.test_subworkflow_node_with_corrupt_summary_returns_err`:
/// the node read returns a summary that is not JSON as stored; a client
/// refuses it with `STORED_DATA_INVALID`.
#[tokio::test]
#[serial]
async fn a_corrupt_summary_is_read_as_stored() {
    let pool = shared_pool().await;
    let (parent, child) =
        seed_linked_child(&pool, "FAILED", "FAILED", Some("{not valid json!!!")).await;

    let summary = stored_summary_of(&pool, parent).await;

    end_workflows(&pool, &[parent, child]).await;
    assert_eq!(summary.as_deref(), Some("{not valid json!!!"));
}

/// `TestWorkflowHandleSubWorkflowFields.test_subworkflow_node_with_bad_status_summary_returns_err`:
/// the node read returns a summary with an unknown status as stored; a
/// client refuses it with `STORED_DATA_INVALID` and never reads it as FAILED.
#[tokio::test]
#[serial]
async fn a_summary_with_an_unknown_status_is_read_as_stored() {
    let pool = shared_pool().await;
    let stored = r#"{"status": "NOT_A_STATUS", "total_tasks": 1}"#;
    let (parent, child) = seed_linked_child(&pool, "COMPLETED", "COMPLETED", Some(stored)).await;

    let summary = stored_summary_of(&pool, parent).await;

    end_workflows(&pool, &[parent, child]).await;
    assert_eq!(summary.as_deref(), Some(stored));
}

/// `TestWorkflowHandleOutput.test_output_failed_returns_failed`
#[tokio::test]
#[serial]
async fn a_failed_output_node_makes_get_give_its_error() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let mut workflow_spec = chain("output_failed");
    workflow_spec.output_index = Some(1);
    let workflow_id = start(&pool, &workflow_spec, &child_specs).await;
    let _a = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(10)),
        &child_specs,
    )
    .await;
    let _b = finish_node(
        &pool,
        workflow_id,
        1,
        &err("OUTPUT_FAIL", "Output failed"),
        &child_specs,
    )
    .await;

    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");

    assert_eq!(code(error_of(&got)).as_deref(), Some("OUTPUT_FAIL"));
}

/// `TestWorkflowHandleOutput.test_duplicate_task_names_preserved`
#[tokio::test]
#[serial]
async fn results_keys_two_nodes_of_one_task_by_node_id() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(
        &pool,
        &spec(
            "dup_tasks",
            vec![task_node(0, "fetch_data"), task_node(1, "fetch_data")],
        ),
        &child_specs,
    )
    .await;
    let _a = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(100)),
        &child_specs,
    )
    .await;
    let _b = finish_node(
        &pool,
        workflow_id,
        1,
        &ok(serde_json::json!(200)),
        &child_specs,
    )
    .await;

    let results = handle(&pool, workflow_id)
        .results()
        .await
        .expect("results()");

    assert_eq!(results.len(), 2);
    assert_eq!(ok_value(&results["node_0"]), &serde_json::json!(100));
    assert_eq!(ok_value(&results["node_1"]), &serde_json::json!(200));
}

/// `TestUnknownStoredWorkflowStatus.test_the_database_refuses_an_unknown_status`:
/// the baseline CHECK `horsies_workflows_status_known` refuses a status
/// outside the catalog, so no handle call can read one. The guarantee moved
/// into the schema. The value fits `status varchar(9)`, as Python's `BOGUS`,
/// so the CHECK and not the length limit refuses it.
#[tokio::test]
#[serial]
async fn the_database_refuses_an_unknown_workflow_status() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;

    let refused = sql::query("UPDATE horsies_workflows SET status = 'BOGUS' WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect_err("the status CHECK refuses the value");

    match refused {
        sql::Error::Database(error) => {
            assert_eq!(Some(error.code().code()), Some("23514"));
            assert_eq!(error.constraint(), Some("horsies_workflows_status_known"));
        }
        other => panic!("expected a check violation, got {other:?}"),
    }
    assert_eq!(status(&pool, workflow_id).await, WorkflowStatus::Running);
    end_workflows(&pool, &[workflow_id]).await;
}

/// `TestWorkflowHandleInfraErrors.test_status_db_error_returns_err_db_operation_failed`
#[tokio::test]
async fn a_lost_database_makes_status_give_a_retryable_db_operation_failed() {
    let workflow_id = Uuid::new_v4();

    let error = unreachable_handle(workflow_id)
        .status()
        .await
        .expect_err("status() with no database");

    assert_eq!(error.code(), HandleErrorCode::DbOperationFailed);
    assert!(error.retryable());
    assert_eq!(error.workflow_id(), workflow_id);
}

/// `TestWorkflowHandleInfraErrors.test_get_db_error_returns_taskresult_broker_error`:
/// the result read on a lost database fails with a retryable database
/// error, which a client's handle names `BROKER_ERROR`.
#[tokio::test]
async fn a_lost_database_makes_the_result_read_fail_retryably() {
    let error = unreachable_handle(Uuid::new_v4())
        .get(Some(Duration::from_millis(500)))
        .await
        .expect_err("the result read with no database");

    assert!(
        matches!(error, WorkflowError::Database(_) | WorkflowError::Broker(_)),
        "{error:?}"
    );
    assert!(error.is_retryable(), "{error:?}");
}

/// `TestWorkflowHandleInfraErrors.test_cancel_db_error_returns_err_db_operation_failed`
#[tokio::test]
async fn a_lost_database_makes_cancel_give_a_retryable_db_operation_failed() {
    let error = unreachable_handle(Uuid::new_v4())
        .cancel()
        .await
        .expect_err("cancel() with no database");

    assert_eq!(error.code(), HandleErrorCode::DbOperationFailed);
    assert!(error.retryable());
}

/// `TestWorkflowHandleInfraErrors.test_results_db_error_returns_err_db_operation_failed`
#[tokio::test]
async fn a_lost_database_makes_results_give_db_operation_failed() {
    let workflow_id = Uuid::new_v4();

    let error = unreachable_handle(workflow_id)
        .results()
        .await
        .expect_err("results() with no database");

    assert_eq!(error.code(), HandleErrorCode::DbOperationFailed);
    assert_eq!(error.workflow_id(), workflow_id);
}

/// `TestListenerFallbackContract.test_handle_get_async_polls_on_listener_err`:
/// the listener's new connection is refused while the pool keeps its open
/// connections; `get()` reads the finished workflow by polling.
#[tokio::test]
#[serial]
async fn get_polls_when_the_listener_cannot_connect() {
    let mut db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let mut workflow_spec = single("listen_fallback", OnError::Fail);
    workflow_spec.output_index = Some(0);
    let workflow_id = start(&db.pool, &workflow_spec, &child_specs).await;
    let _task = finish_node(
        &db.pool,
        workflow_id,
        0,
        &ok(serde_json::json!(100)),
        &child_specs,
    )
    .await;
    // The handle's reads run on the core pool of its broker: a status read
    // opens that pool's connection, which the pool keeps. The listener of
    // the broker needs a new connection.
    let workflow_handle = handle(&db.pool, workflow_id);
    let _status = workflow_handle
        .status()
        .await
        .expect("a status read opens the core pool's connection");
    db.allow_connections(false).await;

    let got = workflow_handle.get(Some(Duration::from_secs(5))).await;

    db.allow_connections(true).await;
    let got = got.expect("the result read polls");
    assert_eq!(ok_value(&got), &serde_json::json!(100));
    db.drop().await;
}
