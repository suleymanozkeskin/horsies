//! The success of a plain task through the finalize batcher, on the test
//! database.

use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::*;
use crate::broker::lifecycle_tests::support::{single_session_pool, WireProxy};
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::db::{PoolSizing, Reserve, WORKER_RESERVED_CONNECTIONS};
use crate::test_support::sql::{self, TestPool};
use crate::worker::executor::protocol::{ResultEnvelopeText, RunEnd, StopReason};
use crate::worker::finalize_batch::statement::WorkerPoolSuccessWrite;
use crate::worker::finalize_batch::{FinalizeBatcher, FlusherBound};
use crate::worker::retry::read_stored_options;

const WORKER: &str = "plain-success-worker";
const OTHER_WORKER: &str = "plain-success-other";

fn writer(broker: Arc<PostgresBroker>) -> OutcomeWriter {
    let sizing = PoolSizing::new(5, Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS))
        .expect("a worker pool of 5 keeps batch connections");
    let successes = FinalizeBatcher::new(
        WorkerPoolSuccessWrite::new(Arc::clone(&broker), WORKER.to_owned()),
        NonZeroUsize::new(16).expect("16 is not zero"),
        FlusherBound::of_worker_pool(&sizing).expect("a pool of 5 leaves 2 flushers"),
    );
    OutcomeWriter::new(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        WORKER.to_owned(),
        PayloadPolicy::default(),
        successes,
    )
}

/// A claimed plain task with its own id and claim generation.
fn claimed_now() -> ClaimedTask {
    ClaimedTask {
        task_id: Uuid::new_v4(),
        task_name: "plain_success_test".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: Utc::now(),
        execution: ExecutionClass::Isolated,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(Some("{}")),
        trace: crate::events::RunTrace::NoCarrier,
    }
}

/// A RUNNING plain row of `owner` at the claim generation `claimed_at`.
async fn seed_running(pool: &TestPool, task_id: Uuid, owner: &str, claimed_at: DateTime<Utc>) {
    sql::query(
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
            $1, 'plain_success_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(), 1,
            decode(repeat('00', 32), 'hex'), NULL, 'RUNNING', NOW(),
            $2, $3, NULL, NOW(),
            NULL, NULL, 1, 'host',
            'worker', 0, 0, NULL, NULL,
            FALSE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(owner)
    .execute(pool)
    .await
    .expect("seed a RUNNING row");
}

async fn history_status(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar("SELECT status::text FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the history row")
}

async fn live_status(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar("SELECT status::text FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live row")
}

fn reported_value() -> AttemptEnd {
    AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(
        r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
    )))
}

/// Two plain successes settled at once both end through the batcher: each
/// settles as written, and each row is COMPLETED in history.
#[tokio::test]
#[serial_test::serial]
async fn plain_successes_settle_through_the_batcher() {
    let pool = crate::test_support::migrated_pool().await;
    let first = claimed_now();
    let second = claimed_now();
    seed_running(&pool, first.task_id, WORKER, first.claimed_at).await;
    seed_running(&pool, second.task_id, WORKER, second.claimed_at).await;
    let writer = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )));

    let (first_settled, second_settled) = tokio::join!(
        writer.settle(&first, reported_value()),
        writer.settle(&second, reported_value()),
    );

    assert_eq!(first_settled, Settlement::Written);
    assert_eq!(second_settled, Settlement::Written);
    for task in [&first, &second] {
        assert_eq!(live_status(&pool, task.task_id).await, None);
        assert_eq!(
            history_status(&pool, task.task_id).await.as_deref(),
            Some("COMPLETED")
        );
    }
}

/// A plain success on a row that another worker holds writes nothing and
/// settles as not held (Python: the refusal answers `Ok(None)`).
#[tokio::test]
#[serial_test::serial]
async fn a_plain_success_on_a_row_this_claim_does_not_hold_writes_nothing() {
    let pool = crate::test_support::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, OTHER_WORKER, claimed.claimed_at).await;
    let writer = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )));

    let settled = writer.settle(&claimed, reported_value()).await;

    assert_eq!(settled, Settlement::NotHeld);
    assert_eq!(
        live_status(&pool, claimed.task_id).await.as_deref(),
        Some("RUNNING")
    );
    assert_eq!(history_status(&pool, claimed.task_id).await, None);
}

/// The plain rows of one batch.
const BATCH_ROWS: usize = 8;
/// The batch queue bound of the writer of the deadlock test (Python
/// `QUEUE_ROOM`).
const QUEUE_ROOM: usize = 4 * BATCH_ROWS;
/// The bound of the wait for every settle of the deadlock test.
const SETTLE_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// The writer of [`writer`] with a batch queue of [`QUEUE_ROOM`] rows.
fn writer_with_queue_room(broker: Arc<PostgresBroker>) -> OutcomeWriter {
    let sizing = PoolSizing::new(5, Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS))
        .expect("a worker pool of 5 keeps batch connections");
    let successes = FinalizeBatcher::new(
        WorkerPoolSuccessWrite::new(Arc::clone(&broker), WORKER.to_owned()),
        NonZeroUsize::new(QUEUE_ROOM).expect("the queue room is not zero"),
        FlusherBound::of_worker_pool(&sizing).expect("a pool of 5 leaves 2 flushers"),
    );
    OutcomeWriter::new(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        WORKER.to_owned(),
        PayloadPolicy::default(),
        successes,
    )
}

/// A RUNNING workflow with no nodes yet.
async fn seed_running_workflow(pool: &TestPool, workflow_id: Uuid) {
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'batch_wf', NULL, 'RUNNING', 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed a workflow");
}

/// A RUNNING workflow task of [`WORKER`] at `claimed_at`, linked to the
/// RUNNING node `index` of `workflow_id`, with no dependencies.
async fn seed_workflow_node_task(
    pool: &TestPool,
    workflow_id: Uuid,
    index: i32,
    task_id: Uuid,
    claimed_at: DateTime<Utc>,
) {
    sql::query(
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
            $1, 'plain_success_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(), NULL,
            NULL, NULL, 'RUNNING', NOW(),
            $2, $3, NULL, NOW(),
            NULL, NULL, 1, 'host',
            'worker', 0, 0, NULL, NULL,
            TRUE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("seed a RUNNING workflow task");
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow,
             id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, allow_failed_deps, join_type, max_retries, execution,
             retention_class_key, status, task_id, is_subworkflow, created_at, started_at
         ) VALUES ('NONE', $1, $2, $3, $4, 'plain_success_test', '{}', 'default', 100,
                   '{}', FALSE, 'all', 0, 'ISOLATED',
                   'forever', 'RUNNING', $5, FALSE, NOW(), NOW())",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(index)
    .bind(format!("node_{index}"))
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed a RUNNING workflow node");
}

/// The plain successes of two batches (the batcher of a worker pool of 5
/// runs two flushers) and the successes of four nodes of one workflow
/// (phase 1, then phase 2) settle at the same time. Every write ends, no
/// row is refused, and no row stays live.
#[tokio::test]
#[serial_test::serial]
async fn two_flushers_and_workflow_node_successes_do_not_deadlock() {
    let pool = crate::test_support::migrated_pool().await;
    let writer = Arc::new(writer_with_queue_room(Arc::new(
        PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
    )));
    let mut plain = Vec::with_capacity(2 * BATCH_ROWS);
    for _ in 0..2 * BATCH_ROWS {
        let task = claimed_now();
        seed_running(&pool, task.task_id, WORKER, task.claimed_at).await;
        plain.push(task);
    }
    let workflow_id = Uuid::new_v4();
    seed_running_workflow(&pool, workflow_id).await;
    let mut nodes = Vec::with_capacity(4);
    for index in 0..4 {
        let task = ClaimedTask {
            kind: TaskKind::WorkflowTask,
            ..claimed_now()
        };
        seed_workflow_node_task(&pool, workflow_id, index, task.task_id, task.claimed_at).await;
        nodes.push(task);
    }
    let every_task: Vec<ClaimedTask> = plain.iter().chain(nodes.iter()).cloned().collect();

    let mut settling = tokio::task::JoinSet::new();
    for task in every_task.clone() {
        let writer = Arc::clone(&writer);
        settling.spawn(async move {
            let settled = writer.settle(&task, reported_value()).await;
            (task.task_id, settled)
        });
    }
    let mut settled = Vec::with_capacity(every_task.len());
    while let Some(joined) = tokio::time::timeout(SETTLE_WAIT, settling.join_next())
        .await
        .expect("every write ends within the bound")
    {
        settled.push(joined.expect("a settle task ended without a panic"));
    }

    assert_eq!(settled.len(), every_task.len());
    for (task_id, settlement) in &settled {
        assert_eq!(*settlement, Settlement::Written, "{task_id}");
    }
    for task in &every_task {
        assert_eq!(live_status(&pool, task.task_id).await, None);
        assert_eq!(
            history_status(&pool, task.task_id).await.as_deref(),
            Some("COMPLETED")
        );
    }
    let node_statuses: Vec<String> = sql::query_scalar(
        "SELECT status::text FROM horsies_workflow_tasks
         WHERE workflow_id = $1 ORDER BY task_index",
    )
    .bind(workflow_id)
    .fetch_all(&pool)
    .await
    .expect("read the node statuses");
    assert_eq!(node_statuses, vec!["COMPLETED".to_owned(); 4]);
    let task_ids: Vec<Uuid> = every_task.iter().map(|task| task.task_id).collect();
    for statement in [
        "DELETE FROM horsies_workflow_phase2_pending WHERE task_id = ANY($1)",
        "DELETE FROM horsies_task_history WHERE task_id = ANY($1)",
    ] {
        sql::query(statement)
            .bind(&task_ids)
            .execute(&pool)
            .await
            .expect("remove the test rows");
    }
    sql::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("remove the workflow");
}

/// The `pre_ping_on` case (the `pre_ping_off` case is
/// `broker::lifecycle_tests::finalize_batch_rows::a_batch_of_rows_is_one_round_trip`):
/// a plain success that the outcome writer settles through the batcher is
/// one round trip on a warm connection of a broker whose pools ping a
/// connection only after it was idle past their limit.
#[tokio::test]
#[serial_test::serial]
async fn a_plain_success_settle_is_one_round_trip() {
    let pool = crate::test_support::migrated_pool().await;
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = single_session_pool(&proxy.url(upstream)).await;
    let writer = writer(Arc::new(PostgresBroker::from_test_url(
        proxied.url(),
        crate::ClientFamily::Rust,
    )));
    let warm_up = claimed_now();
    seed_running(&pool, warm_up.task_id, WORKER, warm_up.claimed_at).await;
    assert_eq!(
        writer.settle(&warm_up, reported_value()).await,
        Settlement::Written
    );
    let task = claimed_now();
    seed_running(&pool, task.task_id, WORKER, task.claimed_at).await;

    proxy.reset_round_trips();
    let settled = writer.settle(&task, reported_value()).await;
    let round_trips = proxy.round_trips();

    assert_eq!(settled, Settlement::Written);
    assert_eq!(round_trips, 1);
    assert_eq!(live_status(&pool, task.task_id).await, None);
    assert_eq!(
        history_status(&pool, task.task_id).await.as_deref(),
        Some("COMPLETED")
    );
    proxied.close().await;
}

/// A workflow task whose child stopped before start because no node links
/// it (the orphan self-heal off) owes nothing, and its settle sends no
/// statement. The row stays CLAIMED
/// (`settle_tests::a_task_whose_node_is_not_linked_stays_claimed`).
#[tokio::test]
#[serial_test::serial]
async fn a_stop_for_a_node_not_linked_sends_no_statement() {
    let pool = crate::test_support::migrated_pool().await;
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = single_session_pool(&proxy.url(upstream)).await;
    let writer = writer(Arc::new(PostgresBroker::from_test_url(
        proxied.url(),
        crate::ClientFamily::Rust,
    )));
    let task = ClaimedTask {
        kind: TaskKind::WorkflowTask,
        ..claimed_now()
    };

    proxy.reset_round_trips();
    let settled = writer
        .settle(
            &task,
            AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(StopReason::NodeNotLinked)),
        )
        .await;
    let round_trips = proxy.round_trips();

    assert_eq!(settled, Settlement::NothingOwed);
    assert_eq!(round_trips, 0);
    proxied.close().await;
}
