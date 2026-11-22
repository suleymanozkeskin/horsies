//! The start of the worker with retries, a stop during the start, and the
//! wait of the claim loop (Python `clients/python/tests/unit/test_worker_helpers.py`,
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
//! - A queue notification wakes the claim loop, which then takes at most
//!   `coalesce_notifies` more signals that are already pending.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{
    wait_for_claim_wake, ClaimWake, ClaimWakes, StartOutcome, Worker, WorkerSubscriptions,
};
use crate::broker::PostgresBroker;
use crate::core::config::worker_profile::SelectedProfile;
use crate::core::config::{AppConfig, WorkerResilienceConfig};
use crate::db::{ChannelName, PayloadFilter};
use crate::test_support::sql::{self, TestPoolOptions};
use crate::worker::children::ChildLaunch;
use crate::worker::claim_pass::RegisteredClasses;
use crate::worker::config::WorkerConfig;
use crate::worker::executor::protocol::{
    ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl,
};
use crate::worker::executor::slots::scripted::{idle_child_of_any_kind, idle_child_recording_pid};
use crate::worker::executor::slots::{ChildLayout, PoolBoard};
use crate::worker::executor::supervisor::ChildCommand;

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

/// A worker on `broker` of one isolated child that boots, then exits at the
/// next frame it reads.
fn worker_on(broker: Arc<PostgresBroker>, resilience: WorkerResilienceConfig) -> Worker {
    worker_with(broker, resilience, WorkerConfig::default())
}

/// [`worker_on`] with the worker config `worker_config`.
fn worker_with(
    broker: Arc<PostgresBroker>,
    resilience: WorkerResilienceConfig,
    worker_config: WorkerConfig,
) -> Worker {
    worker_of(broker, resilience, worker_config, idle_child_of_any_kind())
}

/// [`worker_with`] whose child runs `command`.
fn worker_of(
    broker: Arc<PostgresBroker>,
    resilience: WorkerResilienceConfig,
    worker_config: WorkerConfig,
    command: ChildCommand,
) -> Worker {
    let mut app_config = AppConfig::for_database_url("postgresql://horsies@127.0.0.1:1/horsies");
    app_config.resilience = resilience;
    let database_url = DatabaseUrl::new(app_config.broker.database_url.clone());
    let connection = ChildConnection::of_config(&app_config.broker)
        .expect("the config of the test database validates");
    Worker::of_child_specs(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        app_config,
        worker_config,
        SelectedProfile {
            name: "default".to_owned(),
            profile: serde_json::from_value(serde_json::json!({
                "isolated": {"kind": "isolated_children", "count": 1}
            }))
            .expect("a valid test profile"),
        },
        ChildLaunch {
            command,
            preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
            program: ChildProgram::Rust,
            log_level: ChildLogLevel::Info,
            database_url,
            connection,
        },
        RegisteredClasses::default(),
    )
    .expect("a valid worker config")
}

/// Claim wakes with no queue subscription: the wake branch never fires.
fn no_claim_wakes() -> ClaimWakes {
    ClaimWakes {
        subscriptions: Vec::new(),
    }
}

/// A board of one isolated slot whose child never reports ready: its
/// child-ready wake never fires.
fn quiet_board() -> PoolBoard {
    PoolBoard::new(ChildLayout {
        isolated: 1,
        shared_async: 0,
        shared_thread: 0,
    })
}

/// A broker on a port where no server listens: each statement fails before
/// it is sent, with a failure that may clear.
fn unreachable_broker() -> Arc<PostgresBroker> {
    Arc::new(PostgresBroker::from_test_url(
        "postgresql://horsies@127.0.0.1:1/horsies",
        crate::ClientFamily::Rust,
    ))
}

/// Python `TestStartWithResilienceConfig::test_success_returns_immediately`.
#[tokio::test]
async fn a_start_on_a_ready_database_starts_with_no_retry() {
    let pool = crate::test_support::migrated_pool().await;
    let worker = worker_on(
        Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        )),
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
async fn a_start_error_that_cannot_clear_is_returned_at_once() {
    let url = crate::test_support::migrated_database_url().await;
    let missing = crate::test_support::with_database(
        &url,
        &format!("horsies_missing_{}", uuid::Uuid::new_v4().simple()),
    );
    let pool = TestPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_lazy(&missing);
    let worker = worker_on(
        Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        )),
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
    let claim_wakes = no_claim_wakes();
    let wake = tokio::time::timeout(
        WAIT,
        wait_for_claim_wake(
            &cancel,
            &Notify::new(),
            &quiet_board(),
            &claim_wakes,
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
    let claim_wakes = no_claim_wakes();
    let started = Instant::now();
    let wake = wait_for_claim_wake(
        &CancellationToken::new(),
        &Notify::new(),
        &quiet_board(),
        &claim_wakes,
        Duration::from_millis(50),
    )
    .await;
    assert_eq!(wake, ClaimWake::PollElapsed);
    assert!(started.elapsed() >= Duration::from_millis(50));
}

/// The queues of the burst test. A wake subscription holds one signal per
/// queue, so a burst is one notification on each queue.
const BURST_QUEUES: [&str; 4] = ["burst_a", "burst_b", "burst_c", "burst_d"];

/// One notification on each queue of [`BURST_QUEUES`], in one transaction,
/// in list order, then the wait until `witness` (on the last queue) has its
/// signal. The listener delivers the notifications of a transaction in
/// order, each to every subscriber of its channel at once, so then every
/// subscription of the burst holds its signal.
async fn send_burst(witness: &crate::db::WakeSubscription) {
    let notifies: Vec<String> = BURST_QUEUES
        .iter()
        .map(|queue| format!("pg_notify('task_queue_{queue}', 'wake')"))
        .collect();
    sql::query(&format!("SELECT {}", notifies.join(", ")))
        .execute(&crate::test_support::migrated_pool().await)
        .await
        .expect("send the burst");
    let witnessed = tokio::time::timeout(WAIT, witness.recv())
        .await
        .expect("the last notification of the burst arrives");
    assert_eq!(witnessed, crate::db::WakeEvent::Notified);
}

/// Python `TestWaitForAnyNotify::test_notification_received_drains_burst`:
/// a queue notification wakes the claim wait, and the drain then takes the
/// signals of the burst that are already pending, at most
/// `coalesce_notifies` of them.
#[tokio::test]
async fn a_notification_wakes_the_claim_wait_and_the_drain_takes_the_burst() {
    let session_url = crate::test_support::migrated_database_url().await;
    let broker = Arc::new(PostgresBroker::from_test_url_and_session_url(
        crate::test_support::migrated_pool().await.url(),
        &session_url,
        crate::ClientFamily::Rust,
    ));
    let worker = worker_with(
        Arc::clone(&broker),
        resilience(100, 1),
        WorkerConfig {
            queues: BURST_QUEUES
                .iter()
                .map(|queue| (*queue).to_owned())
                .collect(),
            coalesce_notifies: 5,
            ..WorkerConfig::default()
        },
    );
    let WorkerSubscriptions {
        claim_wakes,
        pings: _pings,
    } = worker
        .try_connect_listener()
        .await
        .expect("the session check passes and the subscriptions start");
    let last_queue = BURST_QUEUES[BURST_QUEUES.len() - 1];
    let witness = broker
        .listener()
        .subscribe_wake(
            ChannelName::parse(&format!("task_queue_{last_queue}")).expect("a valid channel"),
            PayloadFilter::AnyPayload,
        )
        .await
        .expect("the witness subscription starts");
    let released = Notify::new();
    let board = quiet_board();
    let claim_wait = || {
        wait_for_claim_wake(
            &worker.cancel,
            &released,
            &board,
            &claim_wakes,
            Duration::from_secs(3_600),
        )
    };

    // The wait takes one signal; the drain (limit 5) takes the other three.
    send_burst(&witness).await;
    let wake = tokio::time::timeout(WAIT, claim_wait())
        .await
        .expect("the burst wakes the claim wait");
    assert_eq!(wake, ClaimWake::DatabaseNotification);
    claim_wakes
        .drain_pending(worker.worker_config.coalesce_notifies)
        .await;
    assert!(
        claim_wait().now_or_never().is_none(),
        "the drain took every signal of the burst"
    );

    // A drain takes at most its limit: with 2, one signal is left, and the
    // next wait returns at once.
    send_burst(&witness).await;
    let wake = tokio::time::timeout(WAIT, claim_wait())
        .await
        .expect("the burst wakes the claim wait");
    assert_eq!(wake, ClaimWake::DatabaseNotification);
    claim_wakes.drain_pending(2).await;
    assert_eq!(
        claim_wait().now_or_never(),
        Some(ClaimWake::DatabaseNotification),
        "one signal of the burst is left after a drain of two"
    );
    assert!(
        claim_wait().now_or_never().is_none(),
        "no signal of the burst is left"
    );
}

/// The cleanup after a failed start. The listener, the service loops and
/// the runner heartbeats start in `Worker::serve`, after the database start.
/// A database start that fails stops the children that started before it,
/// and starts no listener, no
/// service loop and no runner heartbeat: the run ends with the database
/// error, the child's process is gone, and the reaper, the first service
/// loop, never logged its start (`spawn_reaper` logs `reaper started` before
/// its first pass).
#[tokio::test]
async fn a_failed_database_start_stops_the_children_and_starts_no_service_loop() {
    let dir = std::env::temp_dir().join(format!(
        "horsies-failed-start-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("create the test folder");
    let pid_file = dir.join("child.pid");
    let worker = worker_of(
        unreachable_broker(),
        resilience(100, 1),
        WorkerConfig::default(),
        idle_child_recording_pid(&pid_file),
    );
    let capture = crate::test_support::LogCapture::new();
    let guard = capture.set_default();
    let ended = tokio::time::timeout(WAIT, worker.run())
        .await
        .expect("the run ends within the bound");
    drop(guard);

    let error = ended.expect_err("a database that cannot be reached fails the run");
    assert!(error.is_retryable(), "{error}");
    let pid: i32 = std::fs::read_to_string(&pid_file)
        .expect("the child wrote its pid")
        .trim()
        .parse()
        .expect("the pid file holds a pid");
    // SAFETY: signal 0 sends nothing; it only checks that the process exists.
    let exists = unsafe { libc::kill(pid, 0) } == 0;
    assert!(!exists, "the child {pid} still runs after the failed start");
    let reaper_starts: Vec<String> = capture
        .events()
        .into_iter()
        .filter(|event| event.message == "reaper started")
        .map(|event| event.message)
        .collect();
    assert!(reaper_starts.is_empty(), "{reaper_starts:?}");
    std::fs::remove_dir_all(&dir).expect("remove the test folder");
}
