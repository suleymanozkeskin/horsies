//! The success of a plain task through the finalize batcher, on the test
//! database (Python `tests/unit/test_worker_helpers.py`,
//! `TestFinalizeAfterSlotOutcome::test_finished_task_goes_to_persist`, and
//! `python/horsies/core/worker/finalize.py:1386-1436`).

use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::db::{PoolSizing, Reserve, WORKER_RESERVED_CONNECTIONS};
use crate::worker::executor::protocol::{ResultEnvelopeText, RunEnd};
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
        crate::workflow_engine::child_spec::ChildSpecs::Registry(Arc::new(
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        )),
        WORKER.to_owned(),
        PayloadPolicy::default(),
        RetentionConfig::default(),
        successes,
        OrphanSelfHeal::On,
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
    }
}

/// A RUNNING plain row of `owner` at the claim generation `claimed_at`.
async fn seed_running(pool: &PgPool, task_id: Uuid, owner: &str, claimed_at: DateTime<Utc>) {
    sqlx::query(
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

async fn history_status(pool: &PgPool, task_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT status::text FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the history row")
}

async fn live_status(pool: &PgPool, task_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT status::text FROM horsies_tasks WHERE id = $1")
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
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let first = claimed_now();
    let second = claimed_now();
    seed_running(&pool, first.task_id, WORKER, first.claimed_at).await;
    seed_running(&pool, second.task_id, WORKER, second.claimed_at).await;
    let writer = writer(Arc::new(PostgresBroker::from_pool(pool.clone())));

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
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, OTHER_WORKER, claimed.claimed_at).await;
    let writer = writer(Arc::new(PostgresBroker::from_pool(pool.clone())));

    let settled = writer.settle(&claimed, reported_value()).await;

    assert_eq!(settled, Settlement::NotHeld);
    assert_eq!(
        live_status(&pool, claimed.task_id).await.as_deref(),
        Some("RUNNING")
    );
    assert_eq!(history_status(&pool, claimed.task_id).await, None);
}
