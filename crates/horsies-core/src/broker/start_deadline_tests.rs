//! The start write refuses a claim whose deadline passed.
//!
//! Python specification: `tests/integration/test_shared_lifecycle_writes.py::
//! test_a_claim_past_its_deadline_expires_before_start` (the start statement
//! guards `good_until`, so user code never starts after the deadline).

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::claims::{start_attempt, StartAttempt, StartMiss, StartOutcome};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_pool;
use crate::broker::terminalization_matrix::migrated_pool;
use crate::core::lifecycle::OwnedClaim;

const WORKER: &str = "start-deadline-worker";

/// A CLAIMED row of `WORKER` whose `good_until` passed one minute ago and
/// whose claim lease runs one more minute. Every baseline column is named.
async fn seed_claim_past_its_deadline(pool: &PgPool, task_id: Uuid) -> DateTime<Utc> {
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
            $1, 'start_deadline_task', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('00', 32), 'hex'), NULL,
            'CLAIMED', NOW(), NOW(), $2, NOW() + interval '1 minute',
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL, NOW() - interval '1 minute', FALSE,
            NOW(), FALSE, NULL, NULL
        )
        RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(WORKER)
    .fetch_one(pool)
    .await
    .expect("seed a claim past its deadline")
}

/// Replaces `broker::postgres::tests::set_running_guards_good_until_before_start`,
/// which read the statement text. The claim holder's start write
/// (`horsies_start_attempt`) on a row whose deadline passed applies nothing
/// and names the deadline: the row stays CLAIMED, and no runner heartbeat is
/// written.
#[tokio::test]
#[serial]
async fn a_claim_past_its_deadline_does_not_start() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let generation = seed_claim_past_its_deadline(&pool, task_id).await;
    let core = core_pool_for_test_pool(&pool);

    let started = start_attempt(
        &core,
        StatementLog::Off,
        &StartAttempt {
            task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: generation,
            },
            worker_pid: 4242,
            worker_hostname: "start-host".to_owned(),
            worker_process_name: "worker".to_owned(),
            beat_sender_id: WORKER.to_owned(),
        },
        crate::broker::child_writes::WriteDeadline::start_attempt(),
    )
    .await
    .expect("the start write runs");

    assert_eq!(started, StartOutcome::Missed(StartMiss::DeadlinePassed));
    core.close();
    let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("read the row");
    assert_eq!(status, "CLAIMED");
    let beats: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_heartbeats WHERE task_id = $1 AND role = 'runner'",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("count the runner beats");
    assert_eq!(beats, 0);
    sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("remove the row");
}
