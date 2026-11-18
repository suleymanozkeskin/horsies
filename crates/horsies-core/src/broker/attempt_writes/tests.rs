//! Ports of Python `tests/integration/test_failed_attempt_finish.py` and of
//! the fence cases of `tests/integration/test_finalize_status_guard.py` for
//! the two one-statement attempt writers.

use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use uuid::Uuid;

use super::*;
use crate::broker::pools::core_pool_for_test_url;
use crate::core::lifecycle::{AfterFailure, AttemptWriteRefusal};
use crate::db::StatementFailure;
use crate::test_support::sql::{self, TestPool};

const WORKER: &str = "attempt-writer";
const QUEUE: &str = "attempt_writes";

struct Seeded {
    task_id: Uuid,
    claimed_at: DateTime<Utc>,
}

/// A RUNNING task held by `WORKER`, with every column of the baseline task
/// row named.
async fn seed_running(
    pool: &TestPool,
    retry_count: i32,
    max_retries: i32,
    good_until: Option<DateTime<Utc>>,
    is_workflow_task: bool,
) -> Seeded {
    let task_id = Uuid::new_v4();
    let fingerprint: Option<Vec<u8>> = match is_workflow_task {
        true => None,
        false => Some(vec![7_u8; 32]),
    };
    let fingerprint_version: Option<i16> = match is_workflow_task {
        true => None,
        false => Some(1),
    };
    let claimed_at: DateTime<Utc> = sql::query_scalar(
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
            $1, 'attempt_task', $2, 100, '{}', NULL, 'SHARED_ASYNC',
            'forever', NOW() - interval '1 minute', NOW() - interval '1 minute',
            $3, $4, NULL,
            'RUNNING', NOW() - interval '1 minute', NOW() - interval '30 seconds', $5, NULL,
            NOW() - interval '20 seconds', NOW() - interval '1 second', $5,
            4242, 'attempt-host', 'attempt-process',
            $6, $7, NULL, $8, $9,
            NOW(), FALSE, NULL, NULL
         ) RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(QUEUE)
    .bind(fingerprint_version)
    .bind(fingerprint)
    .bind(WORKER)
    .bind(retry_count)
    .bind(max_retries)
    .bind(good_until)
    .bind(is_workflow_task)
    .fetch_one(pool)
    .await
    .expect("seed RUNNING task");
    Seeded {
        task_id,
        claimed_at,
    }
}

fn failure(seeded: &Seeded, retry: RetryRequest) -> FinishFailedAttempt {
    FinishFailedAttempt {
        task_id: seeded.task_id,
        fence: OwnedClaim {
            worker_id: WORKER.to_owned(),
            claimed_at: seeded.claimed_at,
        },
        retry,
        result_json: r#"{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":"BOOM","data":null,"message":"boom"}}"#
            .to_owned(),
        error_code: Some("BOOM".to_owned()),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some("boom".to_owned()),
            failed_reason: None,
        },
    }
}

/// The failed-attempt write (rule `REFUSE`) on a core pool on the test
/// database.
async fn write_failed_attempt(
    pool: &TestPool,
    command: &FinishFailedAttempt,
) -> Result<FailedAttemptOutcome, BrokerError> {
    finish_failed_attempt(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        command,
    )
    .await
}

/// The workflow-success write on a core pool on the test database.
async fn write_workflow_success(
    pool: &TestPool,
    command: &FinishWorkflowSuccess,
) -> Result<WorkflowSuccessOutcome, BrokerError> {
    finish_workflow_success(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        command,
    )
    .await
}

fn retry_after(retry_count: i32, delay_seconds: f64) -> RetryRequest {
    RetryRequest::RetryAfter {
        expected_retry_count: retry_count,
        delay_seconds,
        placement: RetryPlacement::SameClass,
    }
}

async fn live_row(pool: &TestPool, task_id: Uuid) -> Option<serde_json::Value> {
    sql::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read live row")
}

async fn history_status(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar("SELECT status::text FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read history row")
}

async fn attempts(pool: &TestPool, task_id: Uuid) -> Vec<(i32, String, bool)> {
    sql::query_as(
        "SELECT attempt, outcome::text, will_retry FROM horsies_task_attempts
         WHERE task_id = $1 ORDER BY attempt",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read attempts")
}

async fn database_clock(pool: &TestPool) -> DateTime<Utc> {
    sql::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .expect("database clock")
}

async fn cleanup(pool: &TestPool, task_id: Uuid) {
    sql::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("delete attempts");
    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("delete live row");
    sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("delete history row");
}

/// Python `TestRetry::test_a_retry_writes_pending_and_an_attempt_that_will_retry`:
/// the retry time is the database time under the row lock plus the delay,
/// and every claim and run field is cleared.
#[tokio::test]
#[serial]
async fn a_retry_writes_pending_and_an_attempt_that_will_retry() {
    let pool = crate::test_support::migrated_pool().await;
    let seeded = seed_running(&pool, 1, 3, None, false).await;
    let before = database_clock(&pool).await;

    let outcome = write_failed_attempt(&pool, &failure(&seeded, retry_after(1, 30.0)))
        .await
        .expect("finish");

    let after = database_clock(&pool).await;
    let next_retry_at = match outcome {
        FailedAttemptOutcome::RetryScheduled {
            retry_count,
            next_retry_at,
            ref queue_name,
            ..
        } => {
            assert_eq!((retry_count, queue_name.as_str()), (2, QUEUE));
            next_retry_at
        }
        other => panic!("expected RetryScheduled, got {other:?}"),
    };
    assert!(before + Duration::seconds(30) <= next_retry_at);
    assert!(next_retry_at <= after + Duration::seconds(30));

    let row = live_row(&pool, seeded.task_id).await.expect("live row");
    assert_eq!(row["status"], "PENDING");
    assert_eq!(row["retry_count"], 2);
    assert_eq!(row["next_retry_at"], row["enqueued_at"]);
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
        assert!(row[cleared].is_null(), "a retry must clear {cleared}");
    }
    assert_eq!(
        row["execution"], "SHARED_ASYNC",
        "same-class retry keeps the class"
    );
    assert_eq!(
        attempts(&pool, seeded.task_id).await,
        vec![(2, "FAILED".to_owned(), true)]
    );
    assert_eq!(history_status(&pool, seeded.task_id).await, None);
    cleanup(&pool, seeded.task_id).await;
}

/// Python `TestRetry::test_a_retry_at_or_after_good_until_is_the_terminal_failure`:
/// the deadline is judged on the database clock under the lock.
#[tokio::test]
#[serial]
async fn a_retry_at_or_after_good_until_is_the_terminal_failure() {
    let pool = crate::test_support::migrated_pool().await;
    let good_until = Utc::now() + Duration::seconds(10);
    let seeded = seed_running(&pool, 0, 3, Some(good_until), false).await;

    let outcome = write_failed_attempt(&pool, &failure(&seeded, retry_after(0, 60.0)))
        .await
        .expect("finish");

    match outcome {
        FailedAttemptOutcome::FailureApplied {
            retry: RetryRefusal::PastDeadline { .. },
            after: AfterFailure::CapacityWoken,
            ..
        } => {}
        other => panic!("expected a terminal failure past the deadline, got {other:?}"),
    }
    assert!(live_row(&pool, seeded.task_id).await.is_none());
    assert_eq!(
        history_status(&pool, seeded.task_id).await.as_deref(),
        Some("FAILED")
    );
    cleanup(&pool, seeded.task_id).await;
}

/// Python `TestTerminalFailure::test_a_plain_failure_moves_the_row_with_its_attempt_and_wakes_the_queue`.
#[tokio::test]
#[serial]
async fn a_plain_failure_moves_the_row_with_its_attempt() {
    let pool = crate::test_support::migrated_pool().await;
    let seeded = seed_running(&pool, 0, 0, None, false).await;

    let outcome = write_failed_attempt(&pool, &failure(&seeded, RetryRequest::NoRetry))
        .await
        .expect("finish");

    assert!(matches!(
        outcome,
        FailedAttemptOutcome::FailureApplied {
            retry: RetryRefusal::NotRequested,
            after: AfterFailure::CapacityWoken,
            ..
        }
    ));
    assert!(live_row(&pool, seeded.task_id).await.is_none());
    assert_eq!(
        history_status(&pool, seeded.task_id).await.as_deref(),
        Some("FAILED")
    );

    // A second call of the same command is a replay and writes nothing.
    let replay = write_failed_attempt(&pool, &failure(&seeded, RetryRequest::NoRetry))
        .await
        .expect("replay");
    assert!(matches!(
        replay,
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. })
    ));
    cleanup(&pool, seeded.task_id).await;
}

/// Python `TestNoWrite::test_a_changed_retry_count_is_part_of_the_fence`.
#[tokio::test]
#[serial]
async fn a_changed_retry_count_is_part_of_the_fence() {
    let pool = crate::test_support::migrated_pool().await;
    let seeded = seed_running(&pool, 1, 3, None, false).await;
    let before = live_row(&pool, seeded.task_id).await;

    let outcome = write_failed_attempt(&pool, &failure(&seeded, retry_after(0, 1.0)))
        .await
        .expect("finish");

    assert!(matches!(outcome, FailedAttemptOutcome::Refused(_)));
    assert_eq!(live_row(&pool, seeded.task_id).await, before);
    assert!(attempts(&pool, seeded.task_id).await.is_empty());
    cleanup(&pool, seeded.task_id).await;
}

/// Python `test_finalize_status_guard.py::test_fused_finalize_rejects_stale_claim_generation`,
/// for the failed attempt: a stale claim generation of the same worker
/// writes nothing.
#[tokio::test]
#[serial]
async fn a_stale_claim_generation_writes_no_failure() {
    let pool = crate::test_support::migrated_pool().await;
    let seeded = seed_running(&pool, 0, 0, None, false).await;
    let before = live_row(&pool, seeded.task_id).await;
    let stale = Seeded {
        task_id: seeded.task_id,
        claimed_at: seeded.claimed_at - Duration::seconds(1),
    };

    let outcome = write_failed_attempt(&pool, &failure(&stale, RetryRequest::NoRetry))
        .await
        .expect("finish");

    assert!(matches!(
        outcome,
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::LostClaim { .. })
    ));
    assert_eq!(live_row(&pool, seeded.task_id).await, before);
    cleanup(&pool, seeded.task_id).await;
}

/// Python `test_finalize_status_guard.py::test_workflow_success_rejects_stale_claim_generation`.
#[tokio::test]
#[serial]
async fn a_stale_claim_generation_writes_no_workflow_success() {
    let pool = crate::test_support::migrated_pool().await;
    let seeded = seed_running(&pool, 0, 0, None, true).await;
    let before = live_row(&pool, seeded.task_id).await;

    let outcome = write_workflow_success(
        &pool,
        &FinishWorkflowSuccess {
            task_id: seeded.task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: seeded.claimed_at - Duration::seconds(1),
            },
            result_json: r#"{"__h_task_result__":true,"ok":1,"err":null}"#.to_owned(),
        },
    )
    .await
    .expect("finish");

    assert!(matches!(
        outcome,
        WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::LostClaim { .. })
    ));
    assert_eq!(live_row(&pool, seeded.task_id).await, before);
    cleanup(&pool, seeded.task_id).await;
}

/// Python `test_workflow_success.py::TestProgram::test_a_plain_task_is_refused_before_any_write`.
#[tokio::test]
#[serial]
async fn a_plain_task_success_through_the_workflow_writer_is_refused() {
    let pool = crate::test_support::migrated_pool().await;
    let seeded = seed_running(&pool, 0, 0, None, false).await;
    let before = live_row(&pool, seeded.task_id).await;

    let refused = write_workflow_success(
        &pool,
        &FinishWorkflowSuccess {
            task_id: seeded.task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: seeded.claimed_at,
            },
            result_json: r#"{"__h_task_result__":true,"ok":1,"err":null}"#.to_owned(),
        },
    )
    .await;

    match refused {
        Err(BrokerError::Statement(StatementFailure::StatementRefused { cause, .. })) => {
            assert_eq!(cause.server_error().code().code(), "22023");
        }
        other => panic!("expected invalid_parameter_value, got {other:?}"),
    }
    assert_eq!(live_row(&pool, seeded.task_id).await, before);
    cleanup(&pool, seeded.task_id).await;
}
