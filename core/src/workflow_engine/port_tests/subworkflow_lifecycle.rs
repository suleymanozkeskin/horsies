//! The sub-workflow matrix: cancel, pause, recovery, child retries and child
//! expiry. Python: `clients/python/tests/e2e/test_layer7_subworkflows.py`.
//!
//! [`drive`] plays the worker, as in `subworkflow_matrix.rs`. A recovery pass
//! is Python's `_run_workflow_recovery`: the phase-2 recovery drain, then the
//! stuck-workflow scan. Tests that run a pass use their own database.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::subworkflow_matrix::{
    after, assert_ok_value, assert_subworkflow_error, drive, fail_child, get, given_params_node,
    mark, matrix_children, nested_slow_middle, node_result, ok_child, outputless_entry,
    outputless_parent, parent, success_surface_parent, SLOW_KEY,
};
use super::support::{
    child_of, code, err, error_of, finish_phase1, handle, node, node_status, ok, shared_pool, spec,
    start, sub_workflow_node, task_node, workflow_status, IsolatedDatabase, WORKER,
};
use crate::broker::attempt_writes::{
    finish_failed_attempt, AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryPlacement,
    RetryRequest,
};
use crate::broker::core_rows::StatementLog;
use crate::broker::expiry_and_orphans::expire_pending_pass;
use crate::broker::pools::core_pool_for_test_url;
use crate::core::lifecycle::{FailedAttemptOutcome, OwnedClaim};
use crate::core::WorkflowSpec;
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};
use crate::workflow_engine::engine::on_workflow_task_complete_with;
use crate::workflow_engine::phase2_recovery::drive_phase2_recovery_with;
use crate::workflow_engine::recovery::{
    recover_stuck_workflows_observed_with, GLOBAL_SCAN_ROW_CAP,
};

/// Python `_run_workflow_recovery`: quarantine bound of the phase-2 drain.
const QUARANTINE_AFTER_ATTEMPTS: u32 = 25;

/// `workflow_recovery_failure_limit` of each pass.
const FAILURE_LIMIT: u32 = 3;

/// The retries that the retried child task's policy allows (Python
/// `retry_then_ok_task`).
const CHILD_MAX_RETRIES: i32 = 3;

/// The most attempts a retry loop makes; the schema bounds `max_retries` to 20.
const MAX_ATTEMPTS: usize = 21;

/// A parent whose one root node is the slow child with `value`.
fn slow_parent(name: &str, value: &str) -> WorkflowSpec {
    parent(
        name,
        vec![given_params_node(
            0,
            "e2e_sub_slow_workflow",
            SLOW_KEY,
            serde_json::json!({"value": value, "delay_ms": 1000}),
        )],
        Some(0),
    )
}

/// One recovery pass; returns the work it applied.
async fn recovery_pass(pool: &TestPool, child_specs: &TestChildSpecs) -> u64 {
    let phase2 = drive_phase2_recovery_with(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        child_specs,
        0,
        GLOBAL_SCAN_ROW_CAP,
        QUARANTINE_AFTER_ATTEMPTS,
    )
    .await
    .expect("the phase-2 drain");
    let report = recover_stuck_workflows_observed_with(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        child_specs,
        FAILURE_LIMIT,
    )
    .await
    .expect("the stuck-workflow scan");
    phase2.applied + u64::from(report.total())
}

/// Python `_mark_workflow_task_completed`: the task moves to history and the
/// node is COMPLETED with `value`, and nothing advances the workflow.
async fn mark_node_completed(
    pool: &TestPool,
    workflow_id: Uuid,
    task_index: i32,
    value: serde_json::Value,
) {
    let task_id = node(pool, workflow_id, task_index)
        .await
        .task_id
        .expect("the node has a task");
    let result_json = finish_phase1(pool, task_id, &ok(value)).await;
    sql::query(
        "UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = $3, completed_at = NOW()
         WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .bind(&result_json)
    .execute(pool)
    .await
    .expect("mark the node COMPLETED");
}

/// Takes `workflow_id` and its node 0 back to before the child's result was
/// written to the parent (`revert_node`) and before the workflow ended.
async fn revert_end(pool: &TestPool, workflow_id: Uuid, revert_node: bool) {
    sql::query(
        "UPDATE horsies_workflows
         SET status = 'RUNNING', result = NULL, error = NULL, completed_at = NULL, updated_at = NOW()
         WHERE id = $1",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("revert the workflow end");
    if revert_node {
        sql::query(
            "UPDATE horsies_workflow_tasks
             SET status = 'RUNNING', result = NULL, sub_workflow_summary = NULL, completed_at = NULL
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("revert the parent node");
    }
}

// ---------------------------------------------------------------------------
// Cancel and pause
// ---------------------------------------------------------------------------

/// `test_subworkflow_cancel_before_start`: a PENDING child node is SKIPPED and
/// starts no child.
#[tokio::test]
async fn cancel_before_the_child_starts_skips_the_node_and_starts_no_child() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &parent(
            "cancel_before_start",
            vec![
                mark(0, "gate"),
                after(sub_workflow_node(1, &ok_child()), &[0]),
            ],
            Some(1),
        ),
        &matrix_children(),
    )
    .await;

    handle(&pool, workflow_id).cancel().await.expect("cancel()");

    assert_eq!(workflow_status(&pool, workflow_id).await, "CANCELLED");
    let sub = node(&pool, workflow_id, 1).await;
    assert_eq!(sub.status, "SKIPPED");
    assert_eq!(sub.sub_workflow_id, None);
}

/// `test_subworkflow_cancel_after_complete_noop`
#[tokio::test]
async fn cancel_of_a_completed_workflow_changes_nothing() {
    let pool = shared_pool().await;
    let child_specs = matrix_children();
    let workflow_id = start(&pool, &success_surface_parent(), &child_specs).await;
    drive(&pool, workflow_id, &child_specs).await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");

    handle(&pool, workflow_id).cancel().await.expect("cancel()");

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(&get(&pool, workflow_id).await, serde_json::json!(11));
}

/// `test_subworkflow_cancel_nested_cascade`: cancel reaches the middle and the
/// leaf workflow.
#[tokio::test]
async fn cancel_reaches_the_middle_and_the_leaf_workflow() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &parent(
            "cancel_nested_cascade",
            vec![sub_workflow_node(0, &nested_slow_middle())],
            Some(0),
        ),
        &matrix_children(),
    )
    .await;
    let middle_id = child_of(&pool, workflow_id, 0).await;
    let leaf_id = child_of(&pool, middle_id, 0).await;

    handle(&pool, workflow_id).cancel().await.expect("cancel()");

    for (workflow, level) in [
        (workflow_id, "root"),
        (middle_id, "middle"),
        (leaf_id, "leaf"),
    ] {
        assert_eq!(
            workflow_status(&pool, workflow).await,
            "CANCELLED",
            "{level}"
        );
    }
}

/// `test_subworkflow_pause_running_child`
#[tokio::test]
async fn pause_of_the_parent_pauses_its_running_child() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &slow_parent("pause_running_child", "pause_me"),
        &matrix_children(),
    )
    .await;
    let child_id = child_of(&pool, workflow_id, 0).await;

    let paused = handle(&pool, workflow_id).pause().await.expect("pause()");

    assert!(paused);
    assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");
    assert_eq!(workflow_status(&pool, child_id).await, "PAUSED");
    assert_eq!(
        code(error_of(&get(&pool, workflow_id).await)).as_deref(),
        Some("WORKFLOW_PAUSED")
    );
    super::support::end_workflows(&pool, &[workflow_id, child_id]).await;
}

/// `test_subworkflow_pause_resume_completes`
#[tokio::test]
async fn pause_then_resume_of_the_parent_completes_parent_and_child() {
    let pool = shared_pool().await;
    let child_specs = matrix_children();
    let workflow_id = start(
        &pool,
        &slow_parent("pause_resume_completes", "resume_me"),
        &child_specs,
    )
    .await;
    let child_id = child_of(&pool, workflow_id, 0).await;
    let workflow_handle = handle(&pool, workflow_id);

    assert!(workflow_handle.pause().await.expect("pause()"));
    assert!(workflow_handle
        .resume(&TestChildSpecs::empty())
        .await
        .expect("resume()"));
    drive(&pool, workflow_id, &child_specs).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_eq!(workflow_status(&pool, child_id).await, "COMPLETED");
    assert_ok_value(
        &get(&pool, workflow_id).await,
        serde_json::json!("resume_me"),
    );
}

/// `test_subworkflow_pause_then_cancel`
#[tokio::test]
async fn cancel_of_a_paused_parent_cancels_its_paused_child() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &slow_parent("pause_then_cancel", "pause_cancel_me"),
        &matrix_children(),
    )
    .await;
    let child_id = child_of(&pool, workflow_id, 0).await;
    let workflow_handle = handle(&pool, workflow_id);

    assert!(workflow_handle.pause().await.expect("pause()"));
    workflow_handle.cancel().await.expect("cancel()");

    assert_eq!(workflow_status(&pool, workflow_id).await, "CANCELLED");
    assert_eq!(workflow_status(&pool, child_id).await, "CANCELLED");
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

/// `test_subworkflow_recovery_ready_unstarted`: recovery starts the child of a
/// READY sub-workflow node, and the workflow then completes.
#[tokio::test]
async fn recovery_starts_the_child_of_a_ready_node_and_the_workflow_completes() {
    let db = IsolatedDatabase::create().await;
    let child_specs = matrix_children();
    let workflow_id = start(
        &db.pool,
        &parent(
            "recovery_ready_unstarted",
            vec![
                mark(0, "gate"),
                after(sub_workflow_node(1, &ok_child()), &[0]),
            ],
            Some(1),
        ),
        &child_specs,
    )
    .await;
    mark_node_completed(&db.pool, workflow_id, 0, serde_json::json!("gate")).await;
    sql::query(
        "UPDATE horsies_workflow_tasks SET status = 'READY' WHERE workflow_id = $1 AND task_index = 1",
    )
    .bind(workflow_id)
    .execute(&db.pool)
    .await
    .expect("leave the child node READY");

    assert!(recovery_pass(&db.pool, &child_specs).await >= 1);
    let child_id = child_of(&db.pool, workflow_id, 1).await;
    drive(&db.pool, workflow_id, &child_specs).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
    assert_eq!(workflow_status(&db.pool, child_id).await, "COMPLETED");
    assert_ok_value(&get(&db.pool, workflow_id).await, serde_json::json!(11));
    db.drop().await;
}

/// `test_subworkflow_recovery_child_complete_parent_not_updated` and
/// `test_subworkflow_recovery_failed_child_parent_not_updated`: recovery
/// writes an ended child into its RUNNING parent node and ends the parent.
#[tokio::test]
async fn recovery_writes_an_ended_child_into_its_running_parent_node() {
    let db = IsolatedDatabase::create().await;
    let child_specs = matrix_children();
    let succeeded = start(&db.pool, &success_surface_parent(), &child_specs).await;
    let failed = start(
        &db.pool,
        &parent(
            "subworkflow_failure_fail_policy",
            vec![sub_workflow_node(0, &fail_child())],
            Some(0),
        ),
        &child_specs,
    )
    .await;
    for workflow_id in [succeeded, failed] {
        drive(&db.pool, workflow_id, &child_specs).await;
        revert_end(&db.pool, workflow_id, true).await;
    }

    assert!(recovery_pass(&db.pool, &child_specs).await >= 1);

    assert_eq!(workflow_status(&db.pool, succeeded).await, "COMPLETED");
    assert_ok_value(&get(&db.pool, succeeded).await, serde_json::json!(11));
    assert_eq!(workflow_status(&db.pool, failed).await, "FAILED");
    let failed_child = child_of(&db.pool, failed, 0).await;
    let _summary = assert_subworkflow_error(&get(&db.pool, failed).await, Some(failed_child));
    db.drop().await;
}

/// `test_subworkflow_recovery_parent_crashed_mid_child`: the child task
/// completed and nothing advanced it; recovery completes the child, then the
/// parent.
#[tokio::test]
async fn recovery_completes_a_child_whose_task_completed_and_then_its_parent() {
    let db = IsolatedDatabase::create().await;
    let child_specs = matrix_children();
    let workflow_id = start(
        &db.pool,
        &slow_parent("recovery_parent_crashed_mid_child", "recovered_child"),
        &child_specs,
    )
    .await;
    let child_id = child_of(&db.pool, workflow_id, 0).await;
    mark_node_completed(&db.pool, child_id, 0, serde_json::json!("recovered_child")).await;

    assert!(recovery_pass(&db.pool, &child_specs).await >= 1);

    assert_eq!(workflow_status(&db.pool, child_id).await, "COMPLETED");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
    assert_ok_value(
        &get(&db.pool, workflow_id).await,
        serde_json::json!("recovered_child"),
    );
    db.drop().await;
}

/// `test_subworkflow_recovery_outputless_recovery`: recovery rebuilds the
/// outputless map with the failed child's error.
#[tokio::test]
async fn recovery_rebuilds_an_outputless_map_with_the_failed_child_error() {
    let db = IsolatedDatabase::create().await;
    let child_specs = matrix_children();
    let workflow_id = start(&db.pool, &outputless_parent(), &child_specs).await;
    drive(&db.pool, workflow_id, &child_specs).await;
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
    revert_end(&db.pool, workflow_id, false).await;

    assert!(recovery_pass(&db.pool, &child_specs).await >= 1);

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
    let got = get(&db.pool, workflow_id).await;
    assert_ok_value(
        &outputless_entry(&got, "node_0"),
        serde_json::json!("required_ok"),
    );
    let _summary = assert_subworkflow_error(&outputless_entry(&got, "node_1"), None);
    db.drop().await;
}

// ---------------------------------------------------------------------------
// Child retries and expiry
// ---------------------------------------------------------------------------

/// The child of the retry rows: one task that the test fails or completes.
fn retry_child() -> WorkflowSpec {
    let mut child = spec(
        "e2e_sub_retry_workflow",
        vec![task_node(0, "retry_then_ok")],
    );
    child.output_index = Some(0);
    child
}

/// What one failed attempt of the retry loop did.
enum FailedAttempt {
    /// The write scheduled a retry; the task is PENDING again.
    Retried,
    /// The write ended the task; phase 2 is owed with this result.
    Ended { result_json: String },
}

/// One failed attempt of `task_id`, held by [`WORKER`] as the worker holds it
/// while it finalizes. The retry follows the row's counts, as the worker's
/// policy check does for a code that the policy lists.
async fn fail_attempt(pool: &TestPool, task_id: Uuid) -> FailedAttempt {
    let (claimed_at, retry_count, max_retries): (DateTime<Utc>, i32, i32) = sql::query_as(
        "UPDATE horsies_tasks
         SET status = 'RUNNING', claimed_at = NOW(), claimed_by_worker_id = $2,
             claim_expires_at = NULL, started_at = NOW(), finalizing_at = NOW(),
             finalizing_by_worker_id = $2, worker_pid = 4242, worker_hostname = 'port-host',
             worker_process_name = 'port-process', next_retry_at = NULL, updated_at = NOW()
         WHERE id = $1 AND status IN ('PENDING', 'CLAIMED', 'RUNNING')
         RETURNING claimed_at, retry_count, max_retries",
    )
    .bind(task_id)
    .bind(WORKER)
    .fetch_one(pool)
    .await
    .expect("hold the attempt");
    let retry = match retry_count < max_retries {
        true => RetryRequest::RetryAfter {
            expected_retry_count: retry_count,
            delay_seconds: 0.0,
            placement: RetryPlacement::SameClass,
        },
        false => RetryRequest::NoRetry,
    };
    let result_json =
        serde_json::to_string(&err("RETRY_PROBE", "attempt failed")).expect("encode the failure");
    let outcome = finish_failed_attempt(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        &FinishFailedAttempt {
            task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at,
            },
            retry,
            result_json: result_json.clone(),
            error_code: Some("RETRY_PROBE".to_owned()),
            failed_reason: None,
            attempt: AttemptFailure {
                kind: AttemptFailureKind::Failed,
                error_message: Some("attempt failed".to_owned()),
                failed_reason: None,
            },
        },
    )
    .await
    .expect("the failed attempt write");
    match outcome {
        FailedAttemptOutcome::RetryScheduled { .. } => FailedAttempt::Retried,
        FailedAttemptOutcome::FailureApplied { .. } => FailedAttempt::Ended { result_json },
        FailedAttemptOutcome::Refused(refusal) => panic!("the attempt write refused: {refusal:?}"),
    }
}

/// Starts a parent of [`retry_child`] and gives the child task
/// [`CHILD_MAX_RETRIES`]. Returns the parent, the child and the child task.
async fn start_retry_parent(
    pool: &TestPool,
    name: &str,
    child_specs: &TestChildSpecs,
) -> (Uuid, Uuid, Uuid) {
    let workflow_id = start(
        pool,
        &parent(name, vec![sub_workflow_node(0, &retry_child())], Some(0)),
        child_specs,
    )
    .await;
    let child_id = child_of(pool, workflow_id, 0).await;
    let task_id = node(pool, child_id, 0)
        .await
        .task_id
        .expect("the child task is enqueued");
    sql::query("UPDATE horsies_tasks SET max_retries = $2 WHERE id = $1")
        .bind(task_id)
        .bind(CHILD_MAX_RETRIES)
        .execute(pool)
        .await
        .expect("give the child task its retries");
    (workflow_id, child_id, task_id)
}

fn retry_children() -> TestChildSpecs {
    matrix_children().with_spec(retry_child())
}

/// `test_subworkflow_child_retries_succeed_eventually`: the child task fails
/// once, is retried, and its second attempt completes child and parent.
#[tokio::test]
async fn a_child_task_that_succeeds_on_its_retry_completes_the_parent() {
    let pool = shared_pool().await;
    let child_specs = retry_children();
    let (workflow_id, child_id, task_id) =
        start_retry_parent(&pool, "child_retries_succeed_eventually", &child_specs).await;

    match fail_attempt(&pool, task_id).await {
        FailedAttempt::Retried => {}
        FailedAttempt::Ended { .. } => panic!("the first failure must be retried"),
    }
    assert_eq!(node_status(&pool, workflow_id, 0).await, "RUNNING");
    super::support::finish_task(
        &pool,
        task_id,
        &ok(serde_json::json!("succeeded_on_attempt_2")),
        &child_specs,
    )
    .await;

    assert_eq!(workflow_status(&pool, child_id).await, "COMPLETED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_ok_value(
        &get(&pool, workflow_id).await,
        serde_json::json!("succeeded_on_attempt_2"),
    );
}

/// `test_subworkflow_child_retries_exhaust`: the child task fails 1 + 3 times;
/// the last failure ends it, and the parent fails with the child error.
#[tokio::test]
async fn a_child_task_that_spends_its_retries_fails_the_parent() {
    let pool = shared_pool().await;
    let child_specs = retry_children();
    let (workflow_id, child_id, task_id) =
        start_retry_parent(&pool, "child_retries_exhaust", &child_specs).await;

    let mut attempts = 0;
    let mut ended = None;
    while ended.is_none() && attempts < MAX_ATTEMPTS {
        attempts += 1;
        match fail_attempt(&pool, task_id).await {
            FailedAttempt::Retried => {}
            FailedAttempt::Ended { result_json } => ended = Some(result_json),
        }
    }
    let _ = ended.expect("the retries end");
    on_workflow_task_complete_with(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        task_id,
        &child_specs,
    )
    .await
    .expect("phase 2 of the last failure");

    assert_eq!(attempts, 4);
    assert_eq!(workflow_status(&pool, child_id).await, "FAILED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    let _summary = assert_subworkflow_error(&get(&pool, workflow_id).await, Some(child_id));
}

/// `test_subworkflow_child_good_until_expired`: the child task expires before
/// a claim; recovery fails the child and the parent with a sub-workflow error.
#[tokio::test]
async fn an_expired_child_task_fails_the_child_and_the_parent() {
    let db = IsolatedDatabase::create().await;
    let mut expiring = task_node(0, "mark_task");
    expiring.input_json = serde_json::json!({"value": "expired_child"}).to_string();
    expiring.good_until = Some(Utc::now() - chrono::Duration::milliseconds(100));
    let mut expired_child = spec("e2e_sub_expired_workflow", vec![expiring]);
    expired_child.output_index = Some(0);
    let child_specs = matrix_children().with_spec(expired_child.clone());
    let workflow_id = start(
        &db.pool,
        &parent(
            "child_good_until_expired",
            vec![sub_workflow_node(0, &expired_child)],
            Some(0),
        ),
        &child_specs,
    )
    .await;
    let child_id = child_of(&db.pool, workflow_id, 0).await;

    assert!(
        expire_pending_pass(
            &core_pool_for_test_url(db.pool.url()),
            StatementLog::Off,
            |_rows| {}
        )
        .await
        .expect("expire_pending_pass")
        .expired()
            >= 1
    );
    assert!(recovery_pass(&db.pool, &child_specs).await >= 1);

    assert_eq!(workflow_status(&db.pool, child_id).await, "FAILED");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    assert_eq!(node_status(&db.pool, child_id, 0).await, "FAILED");
    let _summary = assert_subworkflow_error(
        &node_result(node(&db.pool, workflow_id, 0).await.result),
        Some(child_id),
    );
    db.drop().await;
}
