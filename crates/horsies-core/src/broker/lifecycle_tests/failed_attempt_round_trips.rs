//! Round trips of a failed attempt, counted at the wire.
//!
//! The core reads no row before the statement (differences ledger, row 18),
//! so each failure and timeout write is one statement. The count is taken
//! through the counting proxy on a core pool of one connection, after the
//! statement is prepared on the connection, with no checkout ping (Python's
//! `pre_ping_off` case; the ping rule of a reused connection belongs to the
//! statement layer).

use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    finish_failed_attempt, AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryPlacement,
    RetryRequest,
};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::unpinged_single_connection_core_pool_for_url;
use crate::core::lifecycle::FailedAttemptOutcome;
use crate::test_support::sql::TestPool;

const QUEUE: &str = "lifecycle_round_trips";

/// What the failed attempt reports.
#[derive(Debug, Clone, Copy)]
enum Failure {
    /// A worker-level failure, never retried.
    WorkerLevel,
    /// A task error with this code.
    TaskError {
        error_code: &'static str,
        retry: RetryRequest,
    },
}

fn command(task_id: Uuid, failure: Failure) -> FinishFailedAttempt {
    let (retry, error_code, kind) = match failure {
        Failure::WorkerLevel => (
            RetryRequest::NoRetry,
            "BROKER_ERROR",
            AttemptFailureKind::WorkerFailure,
        ),
        Failure::TaskError { error_code, retry } => (retry, error_code, AttemptFailureKind::Failed),
    };
    FinishFailedAttempt {
        task_id,
        fence: owned_claim(),
        retry,
        result_json: r#"{"err": "flaky"}"#.to_owned(),
        error_code: Some(error_code.to_owned()),
        failed_reason: None,
        attempt: AttemptFailure {
            kind,
            error_message: Some("flaky".to_owned()),
            failed_reason: None,
        },
    }
}

fn retries() -> RetryRequest {
    RetryRequest::RetryAfter {
        expected_retry_count: 0,
        delay_seconds: 600.0,
        placement: RetryPlacement::SameClass,
    }
}

/// The round trips of one failed-attempt write in the steady state of the
/// statement cache, and its outcome.
async fn measure(pool: &TestPool, failure: Failure) -> (u32, FailedAttemptOutcome, Uuid) {
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = unpinged_single_connection_core_pool_for_url(&proxy.url(upstream));
    // Open the connection, run the pool's first-use check and prepare the
    // statement on it, with a task that does not exist. The connection is
    // back in the pool when the write returns.
    let _warm_up: FailedAttemptOutcome = finish_failed_attempt(
        &proxied,
        StatementLog::Off,
        &command(Uuid::new_v4(), failure),
    )
    .await
    .expect("warm up");
    let task_id = seed_task(
        pool,
        &TaskSeed {
            queue_name: QUEUE,
            max_retries: 3,
            ..TaskSeed::running()
        },
    )
    .await;

    proxy.reset_round_trips();
    let outcome = finish_failed_attempt(&proxied, StatementLog::Off, &command(task_id, failure))
        .await
        .expect("failed attempt");
    let round_trips = proxy.round_trips();
    proxied.close();
    (round_trips, outcome, task_id)
}

/// Python `test_a_worker_level_failure_is_one_statement`.
#[tokio::test]
#[serial]
async fn a_worker_level_failure_is_one_statement() {
    let pool = pool().await;
    let (round_trips, outcome, task_id) = measure(&pool, Failure::WorkerLevel).await;
    assert_eq!(round_trips, 1);
    assert!(
        matches!(outcome, FailedAttemptOutcome::FailureApplied { .. }),
        "{outcome:?}"
    );
    assert_eq!(live_row(&pool, task_id).await, None);
    cleanup_tasks(&pool, &[task_id]).await;
}

/// The write of one task error: one statement, whether it fails the task or
/// schedules its retry.
async fn assert_one_statement_per_task_error(error_code: &'static str) {
    let pool = pool().await;
    for retry in [RetryRequest::NoRetry, retries()] {
        let (round_trips, outcome, task_id) =
            measure(&pool, Failure::TaskError { error_code, retry }).await;
        assert_eq!(round_trips, 1, "{retry:?}");
        match retry {
            RetryRequest::NoRetry => {
                assert!(
                    matches!(outcome, FailedAttemptOutcome::FailureApplied { .. }),
                    "{outcome:?}"
                );
                assert_eq!(live_row(&pool, task_id).await, None);
            }
            RetryRequest::RetryAfter { .. } => {
                assert!(
                    matches!(outcome, FailedAttemptOutcome::RetryScheduled { .. }),
                    "{outcome:?}"
                );
                assert_eq!(
                    live_row(&pool, task_id).await.expect("live row")["status"],
                    "PENDING"
                );
            }
        }
        cleanup_tasks(&pool, &[task_id]).await;
    }
}

/// Python `test_a_task_error_is_one_read_and_one_statement`, with the
/// core's design: no read before the statement.
#[tokio::test]
#[serial]
async fn a_task_error_is_one_statement() {
    assert_one_statement_per_task_error("FLAKY").await;
}

/// Python `test_a_timeout_is_one_read_and_one_statement`, with the core's
/// design: no read before the statement.
#[tokio::test]
#[serial]
async fn a_timeout_is_one_statement() {
    assert_one_statement_per_task_error("TASK_TIMEOUT").await;
}
