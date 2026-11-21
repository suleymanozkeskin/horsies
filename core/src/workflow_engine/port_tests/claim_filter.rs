//! The post-claim filter of the worker: a claimed task of a PAUSED or
//! CANCELLED workflow is not dispatched; this worker's claim of it ends, and
//! its node goes back to READY (PAUSED) or is skipped (CANCELLED).
//!
//! The filter (`PostgresBroker::filter_non_runnable_workflow_tasks`) returns
//! the ids it filtered out. Each test leaves claimed rows, so it uses its own database.

use crate::test_support::sql::{self, TestPool};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::support::{
    broker, history_row, node, seed_node, seed_node_task, seed_workflow, IsolatedDatabase,
    LiveStatus, NodeSeed, WorkflowSeed, WORKER,
};

/// A claimed task of a sent command (not a workflow task) that no node
/// links: a fingerprinted row of `default`, held by [`WORKER`] for one
/// more minute. Returns its claim generation.
async fn seed_plain_claimed_task(pool: &TestPool, task_id: Uuid) -> DateTime<Utc> {
    sql::query_scalar(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at, command_fingerprint_version,
             command_fingerprint, idempotency_key_digest, status, enqueued_at,
             claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
             finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
             worker_process_name, retry_count, max_retries, next_retry_at, good_until,
             is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
             rerun_root_task_id
         ) VALUES (
             $1, 'filter_test', 'default', 100, '{}', NULL, 'ISOLATED', 'forever',
             NOW(), NOW(), 1, $2, NULL, 'CLAIMED', NOW(),
             NOW(), $3, NOW() + INTERVAL '1 minute', NULL,
             NULL, NULL, NULL, NULL, NULL, 0, 0, NULL, NULL,
             FALSE, NOW(), FALSE, NULL, NULL
         )
         RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(vec![9_u8; 32])
    .bind(WORKER)
    .fetch_one(pool)
    .await
    .expect("seed the plain claimed task")
}

/// A workflow of `workflow_status` whose node 0 (`node_status`) links a task
/// that [`WORKER`] claimed. Returns the workflow, the task and its claim
/// generation.
async fn seed_claimed_node(
    pool: &TestPool,
    workflow_status: &'static str,
    node_status: &'static str,
) -> (Uuid, Uuid, DateTime<Utc>) {
    let workflow_id = seed_workflow(pool, &WorkflowSeed::running().status(workflow_status)).await;
    let task_id = Uuid::new_v4();
    let claimed_at = seed_node_task(pool, task_id, LiveStatus::Claimed)
        .await
        .expect("a CLAIMED row has a claim generation");
    seed_node(
        pool,
        &NodeSeed::task(workflow_id, 0, node_status).task_id(task_id),
    )
    .await;
    (workflow_id, task_id, claimed_at)
}

/// The status of a task's live row, or `None` when it has none.
async fn live_status(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live task status")
}

/// The history status and terminalization kind of a task.
fn moved(status: &str, kind: &str) -> Option<(String, String)> {
    Some((status.to_owned(), kind.to_owned()))
}

/// `test_paused_retry_window_task_cancels_and_resets_running_node`: a claimed
/// task of a PAUSED workflow whose node is RUNNING (a retry window) ends,
/// and its node goes back to READY with no task.
#[tokio::test]
async fn a_paused_claim_with_a_running_node_ends_and_the_node_goes_back_to_ready() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id, claimed_at) = seed_claimed_node(&db.pool, "PAUSED", "RUNNING").await;

    let filtered = broker(&db.pool)
        .filter_non_runnable_workflow_tasks(&[(task_id, claimed_at)], WORKER)
        .await
        .expect("the claim filter");

    assert_eq!(filtered, vec![task_id]);
    assert_eq!(live_status(&db.pool, task_id).await, None);
    assert_eq!(
        history_row(&db.pool, task_id).await,
        moved("CANCELLED", "PAUSE_ABANDON_CLAIM_BATCH")
    );
    let reset = node(&db.pool, workflow_id, 0).await;
    assert_eq!((reset.status.as_str(), reset.task_id), ("READY", None));
    db.drop().await;
}

/// `test_cancelled_workflow_cancels_and_skips`: a claimed task of a
/// CANCELLED workflow ends, and its node is skipped.
#[tokio::test]
async fn a_cancelled_claim_ends_and_its_node_is_skipped() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id, claimed_at) =
        seed_claimed_node(&db.pool, "CANCELLED", "ENQUEUED").await;

    let filtered = broker(&db.pool)
        .filter_non_runnable_workflow_tasks(&[(task_id, claimed_at)], WORKER)
        .await
        .expect("the claim filter");

    assert_eq!(filtered, vec![task_id]);
    assert_eq!(live_status(&db.pool, task_id).await, None);
    assert_eq!(
        history_row(&db.pool, task_id).await,
        moved("CANCELLED", "WORKFLOW_CANCEL_CLAIM_BATCH")
    );
    assert_eq!(node(&db.pool, workflow_id, 0).await.status, "SKIPPED");
    db.drop().await;
}

/// `test_mixed_runnable_paused_cancelled`: in one batch, the claim of a task
/// with no workflow stays and is dispatched; the claims of a PAUSED and of
/// a CANCELLED workflow end, and the cancelled one's node is skipped.
#[tokio::test]
async fn one_batch_keeps_the_plain_claim_and_ends_the_paused_and_cancelled_ones() {
    let db = IsolatedDatabase::create().await;
    let plain_task = Uuid::new_v4();
    let plain_claimed_at = seed_plain_claimed_task(&db.pool, plain_task).await;
    let (paused_workflow, paused_task, paused_claimed_at) =
        seed_claimed_node(&db.pool, "PAUSED", "ENQUEUED").await;
    let (cancelled_workflow, cancelled_task, cancelled_claimed_at) =
        seed_claimed_node(&db.pool, "CANCELLED", "ENQUEUED").await;

    let mut filtered = broker(&db.pool)
        .filter_non_runnable_workflow_tasks(
            &[
                (plain_task, plain_claimed_at),
                (paused_task, paused_claimed_at),
                (cancelled_task, cancelled_claimed_at),
            ],
            WORKER,
        )
        .await
        .expect("the claim filter");

    filtered.sort();
    let mut expected = vec![paused_task, cancelled_task];
    expected.sort();
    assert_eq!(filtered, expected, "the plain task is dispatched");
    assert_eq!(
        live_status(&db.pool, plain_task).await.as_deref(),
        Some("CLAIMED")
    );
    for task_id in [paused_task, cancelled_task] {
        assert_eq!(live_status(&db.pool, task_id).await, None, "{task_id}");
        assert_eq!(
            history_row(&db.pool, task_id)
                .await
                .map(|(status, _kind)| status)
                .as_deref(),
            Some("CANCELLED"),
            "{task_id}"
        );
    }
    assert_eq!(node(&db.pool, paused_workflow, 0).await.status, "READY");
    assert_eq!(
        node(&db.pool, cancelled_workflow, 0).await.status,
        "SKIPPED"
    );
    db.drop().await;
}

/// The worker that holds the claims of
/// [`another_workers_claim_in_a_paused_or_cancelled_workflow_is_left_alone`].
const OTHER_WORKER: &str = "workflow-port-other-worker";

/// `test_paused_workflow_does_not_reset_task_owned_by_another_worker` and
/// `test_cancelled_workflow_does_not_reset_task_owned_by_another_worker`:
/// this worker does not dispatch a claim that another worker holds in a
/// PAUSED or CANCELLED workflow, and leaves that claim and its node as they
/// are.
#[tokio::test]
async fn another_workers_claim_in_a_paused_or_cancelled_workflow_is_left_alone() {
    let db = IsolatedDatabase::create().await;
    for workflow_status in ["PAUSED", "CANCELLED"] {
        let (workflow_id, task_id, claimed_at) =
            seed_claimed_node(&db.pool, workflow_status, "ENQUEUED").await;
        sql::query("UPDATE horsies_tasks SET claimed_by_worker_id = $2 WHERE id = $1")
            .bind(task_id)
            .bind(OTHER_WORKER)
            .execute(&db.pool)
            .await
            .expect("give the claim to another worker");

        let filtered = broker(&db.pool)
            .filter_non_runnable_workflow_tasks(&[(task_id, claimed_at)], WORKER)
            .await
            .expect("the claim filter");

        assert_eq!(filtered, vec![task_id], "{workflow_status}: not dispatched");
        assert_eq!(
            live_status(&db.pool, task_id).await.as_deref(),
            Some("CLAIMED"),
            "{workflow_status}"
        );
        assert_eq!(
            history_row(&db.pool, task_id).await,
            None,
            "{workflow_status}"
        );
        let kept = node(&db.pool, workflow_id, 0).await;
        assert_eq!(
            (kept.status.as_str(), kept.task_id),
            ("ENQUEUED", Some(task_id)),
            "{workflow_status}"
        );
    }
    db.drop().await;
}
