//! The write of each attempt end on the test database, and on a database
//! that cannot be reached (Python `tests/unit/test_worker_helpers.py`,
//! `TestFinalizeAfterSlotOutcome`, `TestRequeueDbErrorContainment`,
//! `TestHandleFinalizeError`, `TestRetryFinalizeFuture`).
//!
//! - A task that never reached a child goes back to the queue.
//! - A reported success and a lost child each end the row.
//! - A crash write on a row that this claim no longer holds writes nothing.
//! - A write that cannot reach the database is sent again at most three
//!   times (phase 2: five), then the end is left to recovery; it never reads
//!   as written. An error that cannot clear is not sent again.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;

use super::*;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::worker::execution::{phase_retry_delay, run_phase2, Phase2Work, PhaseTwoForm};
use crate::worker::executor::protocol::FailureDetail;
use crate::workflow_engine::steps::ChildBuild;

const WORKER: &str = "worker-1";
const OTHER_WORKER: &str = "worker-2";

fn writer(broker: Arc<PostgresBroker>) -> OutcomeWriter {
    let successes = crate::worker::executor::slots::outcome::test_successes(&broker, WORKER);
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

/// A broker on a port where no server listens: each statement fails after
/// 100 ms with a retryable pool timeout.
fn unreachable_broker() -> Arc<PostgresBroker> {
    let options = PgConnectOptions::new()
        .host("127.0.0.1")
        .port(1)
        .username("horsies")
        .database("horsies");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy_with(options);
    Arc::new(PostgresBroker::from_pool(pool))
}

/// A claimed task with its own id and claim generation, no retry policy.
fn claimed_now() -> ClaimedTask {
    ClaimedTask {
        task_id: Uuid::new_v4(),
        claimed_at: Utc::now(),
        ..task(ExecutionClass::Isolated, "{}")
    }
}

/// A CLAIMED row of `owner` at the claim generation `claimed_at`.
async fn seed_claimed(pool: &PgPool, task_id: Uuid, owner: &str, claimed_at: DateTime<Utc>) {
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
            $1, 'settle_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'standard_30d', NOW(), NOW(), 1,
            decode(repeat('00', 32), 'hex'), NULL, 'CLAIMED', NOW(),
            $2, $3, NOW() + interval '1 minute', NULL,
            NULL, NULL, NULL, NULL,
            NULL, 0, 2, NULL, NULL,
            FALSE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
}

/// A RUNNING row of `owner` at the claim generation `claimed_at`.
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
            $1, 'settle_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'standard_30d', NOW(), NOW(), 1,
            decode(repeat('00', 32), 'hex'), NULL, 'RUNNING', NOW(),
            $2, $3, NULL, NOW(),
            NULL, NULL, 1, 'host',
            'worker', 0, 2, NULL, NULL,
            FALSE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
}

/// The status and owner of a live row.
async fn live_row(pool: &PgPool, task_id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The status and error code of the history row of a task that ended.
async fn history_row(pool: &PgPool, task_id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, error_code FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn live_row_count(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn crashed() -> AttemptEnd {
    AttemptEnd::Crashed {
        reason: FailureDetail::cut("child died: signal 9 (SIGKILL)"),
    }
}

/// Python `TestFinalizeAfterSlotOutcome::test_task_that_never_reached_a_child_is_requeued`
/// and `TestRequeueDbErrorContainment::test_requeue_success_logs_no_critical`:
/// the claim is released at its generation and the row is PENDING again.
#[tokio::test]
#[serial_test::serial]
async fn a_task_that_never_reached_a_child_goes_back_to_the_queue() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let claimed = claimed_now();
    seed_claimed(&pool, claimed.task_id, WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_pool(pool.clone())))
        .settle(
            &claimed,
            AttemptEnd::NotDispatched(NotRunCause::PoolStopping),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    assert_eq!(
        live_row(&pool, claimed.task_id).await,
        ("PENDING".to_owned(), None)
    );
}

/// Python `TestFinalizeAfterSlotOutcome::test_a_failed_requeue_is_retried`
/// and `TestRequeueDbErrorContainment::test_requeue_db_error_logs_critical`:
/// a release that cannot reach the database is tried again, never panics,
/// and does not read as written: the row is still CLAIMED, and its lease
/// returns it.
#[tokio::test]
async fn a_release_that_cannot_reach_the_database_is_not_written() {
    let settled = writer(unreachable_broker())
        .settle(
            &claimed_now(),
            AttemptEnd::NotDispatched(NotRunCause::QueueFull),
        )
        .await;
    assert_eq!(settled, Settlement::RetriesExhausted);
}

/// Python `TestFinalizeAfterSlotOutcome::test_finished_task_goes_to_persist`:
/// a reported value completes the task.
#[tokio::test]
#[serial_test::serial]
async fn a_reported_value_completes_the_task() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_pool(pool.clone())))
        .settle(
            &claimed,
            AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(
                r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
            ))),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    assert_eq!(live_row_count(&pool, claimed.task_id).await, 0);
    assert_eq!(
        history_row(&pool, claimed.task_id).await,
        ("COMPLETED".to_owned(), None)
    );
}

/// Python `TestFinalizeAfterSlotOutcome::test_lost_child_is_recovered_and_retried_only_on_a_db_error`,
/// the write that applies: a lost child fails its RUNNING task with
/// `WORKER_CRASHED`, with no retry when the task has no policy.
#[tokio::test]
#[serial_test::serial]
async fn a_lost_child_fails_its_running_task_as_crashed() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_pool(pool.clone())))
        .settle(&claimed, crashed())
        .await;

    assert_eq!(settled, Settlement::Written);
    assert_eq!(
        history_row(&pool, claimed.task_id).await,
        ("FAILED".to_owned(), Some("WORKER_CRASHED".to_owned()))
    );
}

/// Python `TestFinalizeAfterSlotOutcome::test_lost_child_is_recovered_and_retried_only_on_a_db_error`
/// (`NOT_OWNER_OR_NOT_CLAIMED`) and `TestRetryFinalizeFuture::test_requeue_not_owner_clears_future_attempts`:
/// a row that another worker holds is not this claim's; the write changes
/// nothing and is not tried again.
#[tokio::test]
#[serial_test::serial]
async fn a_crash_write_on_a_row_this_claim_does_not_hold_writes_nothing() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, OTHER_WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_pool(pool.clone())))
        .settle(&claimed, crashed())
        .await;

    assert_eq!(settled, Settlement::NotHeld);
    assert_eq!(
        live_row(&pool, claimed.task_id).await,
        ("RUNNING".to_owned(), Some(OTHER_WORKER.to_owned()))
    );
}

/// Python `TestFinalizeAfterSlotOutcome::test_lost_child_is_recovered_and_retried_only_on_a_db_error`
/// (`DB_ERROR`), `TestHandleFinalizeError::test_retryable_under_max_increments_and_spawns`,
/// `::test_retryable_at_max_attempts_logs_critical_no_spawn`,
/// `::test_future_at_max_attempts_uses_future_limit`,
/// `::test_phase1_retryable_spawns_phase1_retry`,
/// `::test_future_retryable_spawns_future_retry` and
/// `TestRetryFinalizeFuture::test_requeue_db_error_delegates_to_handle_finalize_error`:
/// the write of a reported value and the write of a lost child, when they
/// cannot reach the database, are each sent again three times, after waits
/// of 0.5 s, 1 s and 2 s; then the end is left to recovery.
#[tokio::test]
async fn a_write_that_cannot_reach_the_database_is_sent_three_more_times() {
    let ends = [
        crashed(),
        AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(
            r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
        ))),
    ];
    let three_waits: f64 = (1..=3).map(phase_retry_delay).sum();
    let four_waits = three_waits + phase_retry_delay(4);
    for end in ends {
        let started = std::time::Instant::now();
        let settled = writer(unreachable_broker())
            .settle(&claimed_now(), end.clone())
            .await;
        let elapsed = started.elapsed();
        assert_eq!(settled, Settlement::RetriesExhausted, "{end:?}");
        assert!(
            elapsed >= Duration::from_secs_f64(three_waits),
            "{end:?}: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs_f64(four_waits),
            "{end:?}: {elapsed:?}"
        );
    }
}

/// Python `TestHandleFinalizeError::test_non_retryable_clears_attempts_no_spawn`:
/// a write error that cannot clear (the database does not exist) is not
/// sent again, and the end does not read as written.
#[tokio::test]
#[serial_test::serial]
async fn a_write_error_that_cannot_clear_is_not_sent_again() {
    let url = crate::broker::terminalization_matrix::migrated_database_url().await;
    let missing = PgConnectOptions::from_str(&url)
        .unwrap()
        .database(&format!("horsies_missing_{}", Uuid::new_v4().simple()));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_lazy_with(missing);
    let started = std::time::Instant::now();
    let settled = writer(Arc::new(PostgresBroker::from_pool(pool)))
        .settle(&claimed_now(), crashed())
        .await;
    assert_ne!(settled, Settlement::Written);
    assert!(
        started.elapsed() < Duration::from_secs_f64(phase_retry_delay(1)),
        "a retry wait passed: {:?}",
        started.elapsed()
    );
}

/// Python `TestHandleFinalizeError::test_phase2_at_max_attempts_uses_phase2_limit`
/// and `::test_phase2_retryable_spawns_phase2_retry`: phase 2 of a workflow
/// task that cannot reach the database is tried again five times, after
/// waits of 0.5 s to 8 s.
#[tokio::test]
async fn a_phase_two_that_cannot_reach_the_database_is_tried_five_more_times() {
    let broker = unreachable_broker();
    let started = std::time::Instant::now();
    run_phase2(
        &broker,
        ChildBuild::Registry {
            registry: &WorkflowSpecRegistry::new(
                crate::core::registry::BuilderHookName::SPEC_BUILDER,
            ),
            retention: &RetentionConfig::default(),
        },
        Phase2Work {
            task_id: Uuid::new_v4(),
            queue_name: "default".to_owned(),
            is_workflow_task: true,
            capacity_notified: true,
            phase_two: PhaseTwoForm::NotKnown,
        },
    )
    .await;
    let elapsed = started.elapsed();
    let five_waits: f64 = (1..=5).map(phase_retry_delay).sum();
    let six_waits = five_waits + phase_retry_delay(6);
    assert!(
        elapsed >= Duration::from_secs_f64(five_waits),
        "{elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs_f64(six_waits), "{elapsed:?}");
}

/// Python `TestHandleFinalizeError::test_backoff_delay_doubles_per_attempt`:
/// the wait before retry `n` is 0.5 s doubled per retry, at most 15 s.
#[test]
fn the_retry_wait_doubles_up_to_its_cap() {
    let waits: Vec<f64> = (1..=7).map(phase_retry_delay).collect();
    assert_eq!(waits, vec![0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 15.0]);
    assert_eq!(phase_retry_delay(1_000), 15.0);
}
