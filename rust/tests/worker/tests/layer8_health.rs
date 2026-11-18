//! Layer 8: health reads of the app against live workers (Python
//! `tests/e2e/test_worker_health.py`).

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use horsies::{BrokerErrorCode, PingTarget, WorkerHistoryLimit};
use horsies_test_support::poll;
use horsies_test_worker::harness::Suite;
use horsies_test_worker::instance::{Instance, Profile};
use serial_test::serial;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for a worker to write a state snapshot. The recovery
/// instance writes one each second.
const SNAPSHOT_WAIT: Duration = Duration::from_secs(20);

/// The ping wait of a test.
const PING_WAIT: Duration = Duration::from_secs(5);

/// Python: `test_worker_health.py::test_ping_database_real`.
#[tokio::test]
#[serial]
async fn test_ping_database_returns_latency() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let ping = suite.app.ping_database().await.expect("ping the database");
    assert!(
        (0.0..10_000.0).contains(&ping.latency_ms),
        "latency {} ms",
        ping.latency_ms
    );
}

/// Python: `test_worker_health.py::test_ping_and_worker_state_correlate`.
/// Two idle workers each have one latest snapshot in the list.
#[tokio::test]
#[serial]
async fn test_list_worker_states_latest_per_worker_includes_idle() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let _first = suite.worker(Profile::Isolated1).await;
    let _second = suite.worker(Profile::Isolated1).await;
    let states = poll::poll_until(SNAPSHOT_WAIT, "a snapshot of each worker", async || {
        let states = suite.app.list_worker_states().await.ok()?;
        (states.len() == 2).then_some(states)
    })
    .await;
    let mut ids: Vec<&str> = states
        .iter()
        .map(|state| state.worker_id.as_str())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 2, "one latest snapshot per worker");
    assert!(states.iter().all(|state| state.tasks_running == 0), "idle");
}

/// Python: `test_worker_health.py::test_ping_and_worker_state_correlate`.
/// The latest snapshot of a live worker reads by its id; an unknown id
/// reads as none.
#[tokio::test]
#[serial]
async fn test_get_worker_state_latest_and_unknown() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let listed = poll::poll_until(SNAPSHOT_WAIT, "a snapshot", async || {
        suite
            .app
            .list_worker_states()
            .await
            .ok()?
            .into_iter()
            .next()
    })
    .await;
    let read = suite
        .app
        .get_worker_state(&listed.worker_id)
        .await
        .expect("read the worker state")
        .expect("the worker reported");
    assert_eq!(read.worker_id, listed.worker_id);
    assert!(read.snapshot_at >= listed.snapshot_at);
    let unknown = suite
        .app
        .get_worker_state("does-not-exist")
        .await
        .expect("read an unknown worker");
    assert!(unknown.is_none(), "an unknown worker has no snapshot");
}

/// Python: `test_worker_health.py::test_ping_and_worker_state_correlate`.
/// A worker that wrote several snapshots reads them newest first, and a
/// limit keeps the newest.
#[tokio::test]
#[serial]
async fn test_get_worker_state_history_newest_first_and_limit() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let listed = poll::poll_until(SNAPSHOT_WAIT, "a snapshot", async || {
        suite
            .app
            .list_worker_states()
            .await
            .ok()?
            .into_iter()
            .next()
    })
    .await;
    let history = poll::poll_until(SNAPSHOT_WAIT, "three snapshots", async || {
        let history = suite
            .app
            .get_worker_state_history(&listed.worker_id, WorkerHistoryLimit::AllRetained)
            .await
            .ok()?;
        (history.len() >= 3).then_some(history)
    })
    .await;
    for pair in history.windows(2) {
        assert!(pair[0].snapshot_at > pair[1].snapshot_at, "newest first");
    }
    let newest = suite
        .app
        .get_worker_state_history(
            &listed.worker_id,
            WorkerHistoryLimit::Newest(NonZeroU32::new(2).expect("2 is not zero")),
        )
        .await
        .expect("read the newest two");
    assert_eq!(newest.len(), 2);
    assert!(newest[0].snapshot_at >= history[0].snapshot_at);
}

/// Python: `tests/unit` ping argument checks; the e2e twin of
/// `horsies-core broker::postgres::broker_operation_error_tests::ping_workers_refuses_a_zero_timeout_as_invalid_input`.
#[tokio::test]
#[serial]
async fn test_ping_workers_rejects_zero_timeout() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let refused = suite
        .app
        .ping_workers(PingTarget::EveryWorker, Duration::ZERO, None)
        .await
        .expect_err("a zero timeout is refused");
    assert_eq!(refused.code(), BrokerErrorCode::InvalidInput);
    assert!(!refused.retryable(), "a bad argument is not retryable");
}

/// Python: `test_worker_health.py::test_ping_workers_reaches_real_worker`.
#[tokio::test]
#[serial]
async fn test_ping_workers_collects_pong_and_targets_worker() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _first = suite.worker(Profile::Isolated1).await;
    let _second = suite.worker(Profile::Isolated1).await;
    let pongs = suite
        .app
        .ping_workers(PingTarget::EveryWorker, PING_WAIT, NonZeroUsize::new(2))
        .await
        .expect("ping every worker");
    assert_eq!(pongs.len(), 2, "both workers reply: {pongs:?}");
    let pong = &pongs[0];
    assert!(!pong.worker_id.is_empty());
    assert!(!pong.hostname.is_empty());
    assert!(pong.pid > 0);
    assert!(pong.round_trip_ms >= 0.0);
    let targeted = suite
        .app
        .ping_workers(PingTarget::Worker(pong.worker_id.clone()), PING_WAIT, None)
        .await
        .expect("ping one worker");
    assert_eq!(targeted.len(), 1, "only the target replies");
    assert_eq!(targeted[0].worker_id, pong.worker_id);
}
