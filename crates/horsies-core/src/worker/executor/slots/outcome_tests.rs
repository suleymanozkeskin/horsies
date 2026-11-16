//! The writes of [`OutcomeWriter::settle`] for a child that stopped before
//! RUNNING, on a migrated database.
//!
//! Python specification: `tests/unit/test_unconfirmed_start_release.py` and
//! the unconfirmed-start cases of
//! `tests/integration/test_worker_persist_terminal_state.py` (an
//! `OWNERSHIP_UNCONFIRMED` stop releases the claim of its dispatch
//! generation at once; a later claim of the row is not touched).

use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::broker::terminalization_matrix::migrated_pool;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::worker::executor::protocol::{RunEnd, StopReason};
use crate::worker::retry::read_stored_options;

const WORKER: &str = "outcome-stop-worker";

/// A CLAIMED plain row of `WORKER` whose claim lease runs one more minute.
/// Every baseline column is named. Returns the claim generation.
async fn seed_claimed(pool: &PgPool, task_id: Uuid) -> DateTime<Utc> {
    sqlx::query_scalar(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
        ) VALUES (
            $1, 'outcome_stop_task', 'default', 100, '{}', NULL, 'SHARED_THREAD',
            'forever', NOW(), NOW(),
            1, decode(repeat('00', 32), 'hex'), NULL,
            'CLAIMED', NOW(), NOW(), $2, NOW() + interval '1 minute',
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )
        RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(WORKER)
    .fetch_one(pool)
    .await
    .expect("seed a claimed row")
}

/// The claimed task of the dispatch at `generation`.
fn dispatched(task_id: Uuid, generation: DateTime<Utc>) -> ClaimedTask {
    ClaimedTask {
        task_id,
        task_name: "outcome_stop_task".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: generation,
        execution: ExecutionClass::SharedThread,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(None),
    }
}

fn writer(pool: &PgPool) -> OutcomeWriter {
    OutcomeWriter::new(
        Arc::new(PostgresBroker::from_pool(pool.clone())),
        Arc::new(WorkflowSpecRegistry::new()),
        WORKER.to_owned(),
        PayloadPolicy::default(),
        RetentionConfig::default(),
    )
}

fn stopped(reason: StopReason) -> AttemptEnd {
    AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(reason))
}

async fn snapshot(pool: &PgPool, task_id: Uuid) -> serde_json::Value {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the row")
}

async fn attempts(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the attempts")
}

async fn remove(pool: &PgPool, task_id: Uuid) {
    sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("remove the row");
}

/// An unconfirmed start returns the row of its dispatch generation to the
/// queue at once, with its class and every claim and run field cleared, and
/// writes no attempt.
#[tokio::test]
#[serial]
async fn an_unconfirmed_start_releases_the_claim_at_once() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let generation = seed_claimed(&pool, task_id).await;

    let settled = writer(&pool)
        .settle(
            &dispatched(task_id, generation),
            stopped(StopReason::OwnershipUnconfirmed),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let row = snapshot(&pool, task_id).await;
    assert_eq!(row["status"], "PENDING");
    assert_eq!(row["execution"], "SHARED_THREAD");
    for cleared in [
        "claimed_at",
        "claimed_by_worker_id",
        "claim_expires_at",
        "started_at",
        "worker_pid",
        "worker_hostname",
        "worker_process_name",
        "finalizing_at",
        "finalizing_by_worker_id",
    ] {
        assert!(row[cleared].is_null(), "{cleared} is not cleared: {row}");
    }
    assert_eq!(attempts(&pool, task_id).await, 0);
    remove(&pool, task_id).await;
}

/// The release is fenced on the dispatch generation: a row that the same
/// worker claimed again is not touched.
#[tokio::test]
#[serial]
async fn an_unconfirmed_start_does_not_touch_a_later_claim() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let later = seed_claimed(&pool, task_id).await;
    let before = snapshot(&pool, task_id).await;

    let settled = writer(&pool)
        .settle(
            &dispatched(task_id, later - ChronoDuration::seconds(1)),
            stopped(StopReason::OwnershipUnconfirmed),
        )
        .await;

    assert_eq!(settled, Settlement::NotHeld);
    assert_eq!(snapshot(&pool, task_id).await, before);
    remove(&pool, task_id).await;
}

/// Every other stop of a plain task owes nothing: the child's start wrote
/// what its case needs, and the parent leaves the row alone.
#[tokio::test]
#[serial]
async fn other_stops_of_a_plain_task_write_nothing() {
    let pool = migrated_pool().await;
    for reason in [
        StopReason::ClaimLost,
        StopReason::WorkflowStopped,
        StopReason::TaskExpired,
        StopReason::NodeNotLinked,
    ] {
        let task_id = Uuid::new_v4();
        let generation = seed_claimed(&pool, task_id).await;
        let before = snapshot(&pool, task_id).await;

        let settled = writer(&pool)
            .settle(&dispatched(task_id, generation), stopped(reason))
            .await;

        assert_eq!(settled, Settlement::NothingOwed, "{reason:?}");
        assert_eq!(snapshot(&pool, task_id).await, before, "{reason:?}");
        remove(&pool, task_id).await;
    }
}
