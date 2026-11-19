//! The round trips of the one-statement writes of a worker and of its
//! children, counted at the wire.
//!
//! Each test calls the broker method that the worker or the child calls
//! (the Python child calls the same methods through its core handle,
//! `crates/horsies-python/src/operations/start.rs`, `finalizing.rs`,
//! `stop_before_start.rs`), on a broker whose core pools connect through a
//! counting proxy. A first call of the same statement opens the connection,
//! runs the client family check and prepares the statement, so the counted
//! call is in the steady state of the statement cache and within the idle
//! ping limit of the pool.

use std::time::Duration;

use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::child_writes::WriteDeadline;
use crate::broker::claims::{
    ClaimPassRequest, StartAttempt, StartMiss, StartOutcome, StartWindow, WindowedStart,
};
use crate::broker::core_rows::StatementLog;
use crate::broker::listener::notify_task_queue;
use crate::broker::pools::unpinged_single_connection_core_pool_for_url;
use crate::broker::{OrphanSelfHeal, PostgresBroker, StopBeforeStart, StopMove};
use crate::test_support::sql::{self, TestPool};
use crate::worker::execution::FinalizingMark;

const QUEUE: &str = "lifecycle_one_statement_writes";
/// The window of a start whose run has a deadline a minute away (Python
/// `StartWithin(milliseconds=60_000)`, a seat of a shared thread child).
const A_MINUTE: Duration = Duration::from_secs(60);

/// A broker whose core pools connect through a proxy that counts round
/// trips.
struct Counted {
    proxy: WireProxy,
    proxied: TestPool,
    broker: PostgresBroker,
}

impl Counted {
    async fn open(pool: &TestPool) -> Self {
        let upstream = pool.url();
        let proxy = WireProxy::start(upstream).await;
        let proxied = single_session_pool(&proxy.url(upstream)).await;
        let broker = PostgresBroker::from_test_url(proxied.url(), crate::ClientFamily::Rust);
        Self {
            proxy,
            proxied,
            broker,
        }
    }

    async fn close(self) {
        self.proxied.close().await;
    }
}

fn claimed(origin: Origin) -> TaskSeed {
    TaskSeed {
        queue_name: QUEUE,
        origin,
        ..TaskSeed::claimed()
    }
}

fn start(task_id: Uuid) -> StartAttempt {
    StartAttempt {
        task_id,
        fence: owned_claim(),
        worker_pid: 4242,
        worker_hostname: "lifecycle-host".to_owned(),
        worker_process_name: "lifecycle-process".to_owned(),
        beat_sender_id: WORKER.to_owned(),
    }
}

async fn start_on(broker: &PostgresBroker, task_id: Uuid, window: StartWindow) -> WindowedStart {
    broker
        .start_attempt(&start(task_id), window, WriteDeadline::start_attempt())
        .await
        .expect("the start statement")
}

fn started(outcome: WindowedStart) -> bool {
    matches!(outcome, WindowedStart::Ran(StartOutcome::Started { .. }))
}

async fn finalizing_at_is_set(pool: &TestPool, task_id: Uuid) -> bool {
    sql::query_scalar("SELECT finalizing_at IS NOT NULL FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the marker")
}

/// Python `test_a_sync_running_update_is_one_statement` (cases `isolated`
/// and `shared_thread`) and `test_a_shared_async_running_update_is_one_statement`:
/// the start of a plain task, with its first runner beat, is one round trip,
/// with an open window and with a window of a minute.
#[tokio::test]
#[serial]
async fn a_start_is_one_statement_in_either_window() {
    let pool = pool().await;
    let counted = Counted::open(&pool).await;
    let warm_up = start_on(&counted.broker, Uuid::new_v4(), StartWindow::Open).await;
    assert_eq!(
        warm_up,
        WindowedStart::Ran(StartOutcome::Missed(StartMiss::NotHeld))
    );
    for window in [
        StartWindow::Open,
        StartWindow::Until(std::time::Instant::now() + A_MINUTE),
    ] {
        let task_id = seed_task(&pool, &claimed(Origin::Send)).await;

        counted.proxy.reset_round_trips();
        let outcome = start_on(&counted.broker, task_id, window).await;
        let round_trips = counted.proxy.round_trips();

        assert_eq!(round_trips, 1, "{window:?}");
        assert!(started(outcome), "{outcome:?}");
        assert_eq!(status_of(&pool, task_id).await, "RUNNING");
        cleanup_tasks(&pool, &[task_id]).await;
    }
    counted.close().await;
}

/// Python `test_a_sync_workflow_start_is_one_statement` and
/// `test_a_shared_async_workflow_start_is_one_statement`: the start of a
/// workflow task and the write of its node are one statement.
#[tokio::test]
#[serial]
async fn a_workflow_task_start_is_one_statement() {
    let pool = pool().await;
    let counted = Counted::open(&pool).await;
    let _warm_up = start_on(&counted.broker, Uuid::new_v4(), StartWindow::Open).await;
    let task_id = seed_task(&pool, &claimed(Origin::WorkflowNode)).await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "RUNNING").await;

    counted.proxy.reset_round_trips();
    let outcome = start_on(&counted.broker, task_id, StartWindow::Open).await;
    let round_trips = counted.proxy.round_trips();

    assert_eq!(round_trips, 1);
    assert!(started(outcome), "{outcome:?}");
    assert_eq!(status_of(&pool, task_id).await, "RUNNING");
    let (node_status, linked) = node_state(&pool, workflow_id).await;
    assert_eq!((node_status.as_str(), linked), ("RUNNING", Some(task_id)));
    counted.close().await;
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_a_stop_before_start_is_one_statement_after_the_start`: a
/// claimed workflow task with no node misses its start with
/// `NODE_NOT_LINKED`, and the stop before start cancels the orphan (the
/// orphan self-heal on): two statements in all, and the task is CANCELLED.
#[tokio::test]
#[serial]
async fn a_stop_before_start_is_one_statement_after_the_start() {
    let pool = pool().await;
    let counted = Counted::open(&pool).await;
    let warm_up_task = seed_task(&pool, &claimed(Origin::WorkflowNode)).await;
    let _warm_up_start = start_on(&counted.broker, warm_up_task, StartWindow::Open).await;
    let _warm_up_stop = counted
        .broker
        .stop_workflow_task_before_start(
            warm_up_task,
            &owned_claim(),
            OrphanSelfHeal::On,
            WriteDeadline::start_attempt(),
        )
        .await
        .expect("warm up the stop");
    let task_id = seed_task(&pool, &claimed(Origin::WorkflowNode)).await;

    counted.proxy.reset_round_trips();
    let outcome = start_on(&counted.broker, task_id, StartWindow::Open).await;
    let stopped = counted
        .broker
        .stop_workflow_task_before_start(
            task_id,
            &owned_claim(),
            OrphanSelfHeal::On,
            WriteDeadline::start_attempt(),
        )
        .await
        .expect("the stop before start");
    let round_trips = counted.proxy.round_trips();

    assert_eq!(
        outcome,
        WindowedStart::Ran(StartOutcome::Missed(StartMiss::NodeNotLinked))
    );
    assert_eq!(stopped, StopBeforeStart::OrphanCancelled(StopMove::Applied));
    assert_eq!(round_trips, 2);
    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(image.status, "CANCELLED");
    counted.close().await;
    cleanup_tasks(&pool, &[warm_up_task, task_id]).await;
}

/// Python `test_a_sync_finalizing_marker_is_one_statement` and
/// `test_a_shared_async_finalizing_marker_is_one_statement`: the finalizing
/// marker of a run is one round trip.
#[tokio::test]
#[serial]
async fn a_finalizing_marker_is_one_statement() {
    let pool = pool().await;
    let counted = Counted::open(&pool).await;
    let warm_up = counted
        .broker
        .mark_finalizing(Uuid::new_v4(), &owned_claim())
        .await
        .expect("warm up");
    assert_eq!(warm_up, FinalizingMark::NotHeld);
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            queue_name: QUEUE,
            ..TaskSeed::running()
        },
    )
    .await;

    counted.proxy.reset_round_trips();
    let marked = counted
        .broker
        .mark_finalizing(task_id, &owned_claim())
        .await
        .expect("the marker");
    let round_trips = counted.proxy.round_trips();

    assert_eq!(round_trips, 1);
    assert_eq!(marked, FinalizingMark::Marked);
    assert!(finalizing_at_is_set(&pool, task_id).await);
    counted.close().await;
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_claim_pass_is_one_statement`: `horsies_claim` claims and
/// commits in one round trip.
#[tokio::test]
#[serial]
async fn a_claim_pass_is_one_statement() {
    let pool = pool().await;
    let counted = Counted::open(&pool).await;
    let request = claim_request(QUEUE);
    let warm_up = counted.broker.claim_pass(&request).await.expect("warm up");
    let warmed: Vec<Uuid> = warm_up.claimed.iter().map(|row| row.id).collect();
    cleanup_tasks(&pool, &warmed).await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            queue_name: QUEUE,
            ..TaskSeed::pending()
        },
    )
    .await;

    counted.proxy.reset_round_trips();
    let pass = counted
        .broker
        .claim_pass(&request)
        .await
        .expect("the claim");
    let round_trips = counted.proxy.round_trips();

    assert_eq!(round_trips, 1);
    assert_eq!(
        pass.claimed.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![task_id]
    );
    assert_eq!(status_of(&pool, task_id).await, "CLAIMED");
    counted.close().await;
    cleanup_tasks(&pool, &[task_id]).await;
}

/// A claim pass over three capped queues with nothing to claim takes its
/// queue locks, counts the rows in flight and claims in one statement.
#[tokio::test]
#[serial]
async fn an_empty_capped_claim_pass_is_one_statement() {
    let pool = pool().await;
    let counted = Counted::open(&pool).await;
    let queues: Vec<String> = (0..3)
        .map(|index| format!("lifecycle_capped_empty_{index}"))
        .collect();
    let request = ClaimPassRequest {
        queues: queues.clone(),
        queue_priority: queues.iter().cloned().zip(0..).collect(),
        queue_max_concurrency: queues.iter().map(|queue| (queue.clone(), 4)).collect(),
        lock_keys: vec![7_340_001, 7_340_002, 7_340_003],
        ..claim_request(QUEUE)
    };
    let _warm_up = counted.broker.claim_pass(&request).await.expect("warm up");

    counted.proxy.reset_round_trips();
    let pass = counted
        .broker
        .claim_pass(&request)
        .await
        .expect("the claim");
    let round_trips = counted.proxy.round_trips();

    assert_eq!(round_trips, 1);
    assert!(pass.claimed.is_empty(), "{:?}", pass.take);
    counted.close().await;
}

/// Python `test_the_delayed_retry_wake_is_one_statement`: the wake of a
/// queue when a retry comes due is one statement in autocommit.
#[tokio::test]
#[serial]
async fn the_delayed_retry_wake_is_one_statement() {
    let pool = pool().await;
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = unpinged_single_connection_core_pool_for_url(&proxy.url(upstream));
    notify_task_queue(&proxied, StatementLog::Off, QUEUE, Uuid::new_v4())
        .await
        .expect("warm up");

    proxy.reset_round_trips();
    notify_task_queue(&proxied, StatementLog::Off, QUEUE, Uuid::new_v4())
        .await
        .expect("the wake");
    let round_trips = proxy.round_trips();

    assert_eq!(round_trips, 1);
    proxied.close();
}
