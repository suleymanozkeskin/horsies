//! Ports of two move-to-history rows that the lifecycle port does not hold:
//! the recency bound of the stale probe, and the key reservation of a task
//! whose move finds no history leaf.
//!
//! The rest of the move-to-history rows are in
//! `broker/lifecycle_tests/task_history_terminalization_*.rs`.
//! Each test runs in one transaction that it rolls back, on the shared
//! migrated database; a move that must be refused runs in a savepoint, so the
//! test reads the live state after the refusal.

use crate::test_support::sql::{self, TestPool, TestTransaction};
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::broker::terminalization::on_test_pool::{
    terminalize_in_transaction, InTransactionError,
};
use crate::core::history::errors::HISTORY_LEAF_MISSING_SQLSTATE;
use crate::core::history::identity::reservations::{claim_key_reservation, ReservationClaim};
use crate::core::history::periods::RetentionDays;
use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand, TerminalizationOutcome};
use crate::test_support::history::{covered_beat_instant, register_period_class, SeedInstant};
use crate::test_support::terminal_writes::{self, StaleFailure};

const PORT_WORKER: &str = "port-move-worker";
const CLASS_KEY: &str = "standard_30d";
const FAILURE_RESULT: &str = "{\"Err\":{}}";
const SUCCESS_RESULT: &str = "{\"Ok\":true}";

async fn shared_pool() -> TestPool {
    crate::test_support::migrated_pool().await
}

/// A RUNNING plain task claimed by `PORT_WORKER`. Every baseline column is
/// named.
struct RunningTask {
    class_key: &'static str,
    started_minutes_ago: i32,
    idempotency_key_digest: Option<Vec<u8>>,
}

impl RunningTask {
    fn new() -> Self {
        Self {
            class_key: CLASS_KEY,
            started_minutes_ago: 1,
            idempotency_key_digest: None,
        }
    }
}

async fn seed_task(transaction: &mut TestTransaction<'_>, task: &RunningTask) -> Uuid {
    let task_id = Uuid::new_v4();
    sql::query(
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
             $1, 'port.move', 'default', 100, '{}', NULL, 'ISOLATED',
             $2, NOW() - make_interval(mins => $3), NOW() - make_interval(mins => $3),
             1, decode(repeat('0d', 32), 'hex'), $4,
             'RUNNING', NOW() - make_interval(mins => $3),
             NOW() - make_interval(mins => $3), $5, NULL,
             NOW() - make_interval(mins => $3), NULL, NULL,
             13, 'port-host', 'port-process',
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(task.class_key)
    .bind(task.started_minutes_ago)
    .bind(task.idempotency_key_digest.as_deref())
    .bind(PORT_WORKER)
    .execute(&mut *transaction)
    .await
    .expect("insert running task");
    task_id
}

/// The claim generation that the seeded row holds.
async fn claim_generation(transaction: &mut TestTransaction<'_>, task_id: Uuid) -> DateTime<Utc> {
    sql::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&mut *transaction)
        .await
        .expect("read the claim generation")
}

/// The plain success of the worker (`horsies_complete_task_fused`) under the
/// seeded claim.
fn complete(task_id: Uuid, claimed_at: DateTime<Utc>) -> TerminalizationCommand {
    TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence: OwnedClaim {
            worker_id: PORT_WORKER.to_owned(),
            claimed_at,
        },
        result_json: SUCCESS_RESULT.to_owned(),
    }
}

/// Run `command` in a savepoint that the database refuses, roll the
/// savepoint back and return the SQLSTATE.
async fn refused_sqlstate(
    transaction: &mut TestTransaction<'_>,
    command: &TerminalizationCommand,
) -> String {
    let mut savepoint = transaction.begin().await.expect("begin savepoint");
    let refused = terminalize_in_transaction(&mut savepoint, command)
        .await
        .expect_err("the database refuses the move");
    savepoint.rollback().await.expect("roll back the savepoint");
    match refused {
        InTransactionError::Database(error) => error
            .sqlstate()
            .map(str::to_owned)
            .unwrap_or_else(|| panic!("no SQLSTATE: {error}")),
        InTransactionError::Contract(error) => {
            panic!("expected a database refusal, got Contract({error:?})")
        }
    }
}

async fn is_live(transaction: &mut TestTransaction<'_>, task_id: Uuid) -> bool {
    sql::query_scalar("SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)")
        .bind(task_id)
        .fetch_one(&mut *transaction)
        .await
        .expect("read live row")
}

/// `TestStaleProbeOnPartitionedStorage.test_heartbeat_beyond_the_recency_bound_cannot_flip_verdict`:
/// a runner beat older than every threshold gives the verdict of no beat:
/// the stale failure applies.
#[tokio::test]
async fn a_heartbeat_beyond_the_recency_bound_cannot_flip_the_verdict() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let now: DateTime<Utc> = sql::query_scalar("SELECT statement_timestamp()")
        .fetch_one(&mut *transaction)
        .await
        .expect("database now");
    let beat_at = covered_beat_instant(
        &mut *transaction,
        SeedInstant::At(now - Duration::minutes(80)),
    )
    .await;
    let task_id = seed_task(
        &mut transaction,
        &RunningTask {
            started_minutes_ago: 90,
            ..RunningTask::new()
        },
    )
    .await;
    sql::query(
        "INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid)
         VALUES ($1, 'runner', $2, $3, 'port-host', 13)",
    )
    .bind(task_id)
    .bind(beat_at.at())
    .bind(PORT_WORKER)
    .execute(&mut *transaction)
    .await
    .expect("write the old beat");
    let outcomes = terminal_writes::fail_stale_task(
        &mut *transaction,
        task_id,
        &StaleFailure {
            stale_after_ms: 600_000,
            finalizing_stale_after_ms: 600_000,
            result_json: FAILURE_RESULT.to_owned(),
            error_code: "WORKER_CRASHED".to_owned(),
            failed_reason: "port stale runner".to_owned(),
        },
    )
    .await
    .expect("stale failure");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
    transaction.rollback().await.expect("rollback");
}

/// `test_missing_history_leaf_rolls_back_fused_attempt_and_live_delete`
/// (prototype) and `test_missing_history_leaf_rolls_back_location_transition`
/// (identity prototype): a row whose class has no leaf for its terminal time
/// cannot move (55H01, the class partition exists and the leaf does not);
/// the live row stays and its key reservation stays LIVE.
#[tokio::test]
async fn a_move_with_no_history_leaf_keeps_the_task_and_its_reservation_live() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let period = RetentionDays::parse_period(Duration::days(9)).expect("nine days");
    let _registered = register_period_class(&mut *transaction, period).await;
    let key: Vec<u8> = sql::query_scalar("SELECT sha256(convert_to($1, 'UTF8'))")
        .bind(format!("port-key-{}", Uuid::new_v4()))
        .fetch_one(&mut *transaction)
        .await
        .expect("key digest");
    let task_id = seed_task(
        &mut transaction,
        &RunningTask {
            class_key: "standard_9d",
            idempotency_key_digest: Some(key.clone()),
            ..RunningTask::new()
        },
    )
    .await;
    let claimed = claim_key_reservation(
        &mut transaction,
        &key,
        1,
        3_600_000_000,
        1,
        &[13_u8; 32],
        task_id,
    )
    .await
    .expect("reserve the key");
    assert_eq!(claimed, ReservationClaim::Applied { task_id });
    let claimed_at = claim_generation(&mut transaction, task_id).await;
    assert_eq!(
        refused_sqlstate(&mut transaction, &complete(task_id, claimed_at)).await,
        HISTORY_LEAF_MISSING_SQLSTATE
    );
    assert!(is_live(&mut transaction, task_id).await);
    let disposition: String = sql::query_scalar(
        "SELECT disposition FROM horsies_key_reservations WHERE idempotency_key_digest = $1",
    )
    .bind(&key)
    .fetch_one(&mut *transaction)
    .await
    .expect("read the reservation");
    assert_eq!(disposition, "LIVE");
    transaction.rollback().await.expect("rollback");
}
