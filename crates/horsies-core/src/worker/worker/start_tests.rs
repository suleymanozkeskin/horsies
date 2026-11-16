//! The start of the worker with retries, a stop during the start, and the
//! wait of the claim loop (Python `tests/unit/test_worker_helpers.py`,
//! `TestStartWithResilienceConfig`, `TestHandleRetryableStartError`,
//! `TestSleepWithStop`, `TestRunForever`, `TestRequestStop`,
//! `TestWaitForAnyNotify`).
//!
//! - A start error that can clear is tried again with the resilience
//!   backoff until its budget ends; one that cannot clear is returned at
//!   once.
//! - A stop during a retry wait ends the start, and the run, without an
//!   error.
//! - The claim loop waits for a stop or its poll interval.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

use super::{wait_for_claim_wake, ClaimWake, StartOutcome, Worker};
use crate::broker::PostgresBroker;
use crate::core::config::{AppConfig, WorkerResilienceConfig};
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::TaskRegistry;
use crate::worker::config::WorkerConfig;

/// The longest wait for an event a test expects.
const WAIT: Duration = Duration::from_secs(30);

fn resilience(initial_ms: u64, max_attempts: u64) -> WorkerResilienceConfig {
    WorkerResilienceConfig {
        db_retry_initial_ms: initial_ms,
        db_retry_max_ms: initial_ms,
        db_retry_max_attempts: max_attempts,
        ..WorkerResilienceConfig::default()
    }
}

fn worker_on(broker: Arc<PostgresBroker>, resilience: WorkerResilienceConfig) -> Worker {
    let mut app_config = AppConfig::for_database_url("postgresql://horsies@127.0.0.1:1/horsies");
    app_config.resilience = resilience;
    Worker::new(
        broker,
        Arc::new(TaskRegistry::new()),
        Arc::new(WorkflowSpecRegistry::new()),
        app_config,
        WorkerConfig::default(),
    )
    .expect("a valid worker config")
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

/// Python `TestStartWithResilienceConfig::test_success_returns_immediately`.
#[tokio::test]
#[serial_test::serial]
async fn a_start_on_a_ready_database_starts_with_no_retry() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let worker = worker_on(
        Arc::new(PostgresBroker::from_pool(pool)),
        resilience(60_000, 0),
    );
    let started = Instant::now();
    let outcome = worker
        .start_database_with_resilience()
        .await
        .expect("the database steps succeed");
    assert_eq!(outcome, StartOutcome::Started);
    // A retry would wait at least 45 s (60 s less the jitter).
    assert!(started.elapsed() < WAIT, "{:?}", started.elapsed());
}

/// Python `TestStartWithResilienceConfig::test_retryable_connection_error_triggers_retry`,
/// `TestHandleRetryableStartError::test_can_retry_cleans_up_and_sleeps`,
/// `::test_cannot_retry_raises` and `TestSleepWithStop::test_sleeps_full_duration_when_stop_not_set`:
/// a database that cannot be reached is tried again after each wait of the
/// backoff (at least 0.1 s each); after the last retry the error is
/// returned.
#[tokio::test]
async fn a_start_error_that_can_clear_is_retried_until_its_budget_ends() {
    let worker = worker_on(unreachable_broker(), resilience(100, 2));
    let started = Instant::now();
    let error = match worker.start_database_with_resilience().await {
        Ok(outcome) => panic!("an unreachable database cannot start: {outcome:?}"),
        Err(error) => error,
    };
    assert!(error.is_retryable(), "{error}");
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "two retry waits of at least 0.1 s each: {:?}",
        started.elapsed()
    );
    assert!(!worker.cancel_token().is_cancelled());
}

/// Python `TestStartWithResilienceConfig::test_non_retryable_error_raises`:
/// a database that does not exist is no condition that clears; the start
/// returns its error at once, with no retry wait.
#[tokio::test]
#[serial_test::serial]
async fn a_start_error_that_cannot_clear_is_returned_at_once() {
    let url = crate::broker::terminalization_matrix::migrated_database_url().await;
    let missing = PgConnectOptions::from_str(&url)
        .expect("the test database url parses")
        .database(&format!(
            "horsies_missing_{}",
            uuid::Uuid::new_v4().simple()
        ));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_lazy_with(missing);
    let worker = worker_on(
        Arc::new(PostgresBroker::from_pool(pool)),
        resilience(60_000, 0),
    );
    let started = Instant::now();
    let error = match worker.start_database_with_resilience().await {
        Ok(outcome) => panic!("a missing database cannot start: {outcome:?}"),
        Err(error) => error,
    };
    assert!(!error.is_retryable(), "{error}");
    // A retry would wait at least 45 s (60 s less the jitter).
    assert!(started.elapsed() < WAIT, "{:?}", started.elapsed());
}

/// Python `TestSleepWithStop::test_returns_early_when_stop_set` and
/// `TestRunForever::test_stop_set_before_loop_exits_cleanly`: a stop during
/// a retry wait of 60 s ends the run at once, without an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_during_a_start_retry_ends_the_run_without_an_error() {
    let worker = worker_on(unreachable_broker(), resilience(60_000, 0));
    let stop = worker.cancel_token();
    let run = tokio::spawn(async move { worker.run().await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stopped_at = Instant::now();
    stop.cancel();

    let result = tokio::time::timeout(WAIT, run)
        .await
        .expect("the run ends at the stop")
        .expect("the run does not panic");
    assert!(result.is_ok(), "{result:?}");
    assert!(stopped_at.elapsed() < Duration::from_secs(5));
}

/// Python `TestRequestStop::test_request_stop_sets_event`.
#[tokio::test]
async fn request_stop_cancels_the_worker_token() {
    let worker = worker_on(unreachable_broker(), resilience(100, 1));
    let stop = worker.cancel_token();
    assert!(!stop.is_cancelled());
    worker.request_stop();
    assert!(stop.is_cancelled());
}

/// Python `TestWaitForAnyNotify::test_stop_signal_cancels_pending_returns`.
#[tokio::test]
async fn a_stop_ends_the_claim_wait() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (_sender, mut notifications) = mpsc::channel(1);
    let wake = tokio::time::timeout(
        WAIT,
        wait_for_claim_wake(
            &cancel,
            &Notify::new(),
            &mut notifications,
            Duration::from_secs(3_600),
        ),
    )
    .await
    .expect("the wait ends at the stop");
    assert_eq!(wake, ClaimWake::Cancelled);
}

/// Python `TestWaitForAnyNotify::test_timeout_no_done_cancels_and_returns`.
#[tokio::test]
async fn the_claim_wait_ends_at_the_poll_interval() {
    let (_sender, mut notifications) = mpsc::channel(1);
    let started = Instant::now();
    let wake = wait_for_claim_wake(
        &CancellationToken::new(),
        &Notify::new(),
        &mut notifications,
        Duration::from_millis(50),
    )
    .await;
    assert_eq!(wake, ClaimWake::PollElapsed);
    assert!(started.elapsed() >= Duration::from_millis(50));
}
