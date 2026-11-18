//! Ports of Python `tests/unit/test_broker_postgres.py` rows of the worker
//! ping and of the result wait: the pong decode, the early return at
//! `min_responses`, and the end of a result wait that its caller drops.
//!
//! The database tests run on the database of their test runtime
//! (`terminalization_matrix::migrated_pool`), with the broker's listener on
//! the same database under an application name of the test's own, so the
//! listener's backend is the only one with that name.

use std::time::{Duration, Instant};

use serial_test::serial;
use uuid::Uuid;

use super::{decode_pong, PostgresBroker};
use crate::broker::health::{WorkerPingRequest, WorkerPongPayload, WORKER_PING_CHANNEL};
use crate::broker::SendKind;
use crate::core::config::EnqueueRetention;
use crate::test_support::sql::{self, TestListener, TestPool};
use crate::test_support::{migrated_database_url, migrated_pool};

/// The polls of [`wait_for_listener_statement`], 20 ms apart: at most 5 s.
const LISTENER_STATEMENT_POLLS: u32 = 250;

/// The pause between two polls of [`wait_for_listener_statement`].
const LISTENER_STATEMENT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The LISTEN and UNLISTEN of the result wait's channel, as the listener
/// sends them (a quoted identifier).
const LISTEN_TASK_DONE: &str = "LISTEN \"task_done\"";
const UNLISTEN_TASK_DONE: &str = "UNLISTEN \"task_done\"";

fn pong_payload(correlation_id: &str) -> String {
    serde_json::to_string(&WorkerPongPayload {
        correlation_id: correlation_id.to_owned(),
        worker_id: "w1".to_owned(),
        hostname: "h".to_owned(),
        pid: 7,
    })
    .expect("a pong payload encodes")
}

/// A broker on the database of this test whose listener connects with
/// `application_name`.
async fn listener_broker(application_name: &str) -> (PostgresBroker, TestPool) {
    let url = migrated_database_url().await;
    let separator = if url.contains('?') { '&' } else { '?' };
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_test_url_and_session_url(
        pool.url(),
        &format!("{url}{separator}application_name={application_name}"),
        crate::ClientFamily::Rust,
    );
    (broker, pool)
}

/// Whether the last statement of the backend of `application_name` became
/// `statement` within [`LISTENER_STATEMENT_POLLS`] polls.
async fn wait_for_listener_statement(
    pool: &TestPool,
    application_name: &str,
    statement: &str,
) -> bool {
    for _ in 0..LISTENER_STATEMENT_POLLS {
        let last: Option<String> =
            sql::query_scalar("SELECT query FROM pg_stat_activity WHERE application_name = $1")
                .bind(application_name)
                .fetch_optional(pool)
                .await
                .expect("read the listener backend");
        if last.as_deref() == Some(statement) {
            return true;
        }
        tokio::time::sleep(LISTENER_STATEMENT_POLL_INTERVAL).await;
    }
    false
}

/// Python `TestPingWorkers::test_decode_pong_accepts_matching_correlation`:
/// a pong of this round reads with its worker and the round trip.
#[test]
fn a_pong_of_this_round_reads_with_its_worker_and_round_trip() {
    let pong = decode_pong(&pong_payload("corr-1"), "corr-1", Duration::from_millis(50))
        .expect("a pong of this round reads");

    assert_eq!(pong.worker_id, "w1");
    assert_eq!(pong.hostname, "h");
    assert_eq!(pong.pid, 7);
    assert!(
        (pong.round_trip_ms - 50.0).abs() < 1.0,
        "{}",
        pong.round_trip_ms
    );
}

/// Python `TestPingWorkers::test_decode_pong_drops_mismatched_correlation`:
/// a pong of another round is dropped.
#[test]
fn a_pong_of_another_round_is_dropped() {
    let pong = decode_pong(&pong_payload("other"), "corr-1", Duration::from_millis(50));

    assert_eq!(pong, None);
}

/// Python `TestPingWorkers::test_min_responses_returns_early`: with
/// `min_responses` of 1 the ping returns at the first pong, not at the end of
/// its 5 s window. A responder on the ping channel answers each request on
/// its reply channel, as a worker does.
#[tokio::test]
#[serial]
async fn the_ping_returns_at_the_first_pong_when_one_is_enough() {
    let (broker, pool) = listener_broker("ping_min_responses").await;
    let mut requests = TestListener::connect_with(&pool)
        .await
        .expect("connect the responder");
    requests
        .listen(WORKER_PING_CHANNEL)
        .await
        .expect("listen on the ping channel");
    let replies = pool.clone();
    let responder = tokio::spawn(async move {
        let notification = requests.recv().await.expect("a ping request");
        let request: WorkerPingRequest =
            serde_json::from_str(notification.payload()).expect("a ping request decodes");
        sql::query("SELECT pg_notify($1, $2)")
            .bind(&request.reply_channel)
            .bind(pong_payload(&request.correlation_id))
            .execute(&replies)
            .await
            .expect("send the pong");
    });

    let started = Instant::now();
    let pongs = broker
        .ping_workers(None, Duration::from_secs(5), Some(1))
        .await
        .expect("the ping answers");
    let elapsed = started.elapsed();
    responder.await.expect("the responder ends");

    let workers: Vec<&str> = pongs.iter().map(|pong| pong.worker_id.as_str()).collect();
    assert_eq!(workers, ["w1"]);
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}

/// Python `TestGetRawResultRecordAsync::test_cancellation_still_completes_unsubscribe_cleanup`:
/// a result wait that its caller drops while it waits leaves no LISTEN
/// behind. The drop of the subscription is synchronous, so no second cancel
/// can interrupt it; the listener then UNLISTENs the channel of the wait.
#[tokio::test]
#[serial]
async fn a_result_wait_dropped_while_it_waits_unlistens_its_channel() {
    let application_name = format!("result_wait_drop_{}", Uuid::new_v4().simple());
    let (broker, pool) = listener_broker(&application_name).await;
    let task_id = broker
        .enqueue(
            "result_wait_drop",
            "{}",
            "default",
            100,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            EnqueueRetention::Forever,
            None,
            SendKind::First,
        )
        .await
        .expect("enqueue a task that stays PENDING");
    let mut wait = Box::pin(broker.get_raw_result_record(task_id, Some(Duration::from_secs(60))));

    let listened = tokio::select! {
        _ended = &mut wait => panic!("the wait for a PENDING task ended before its limit"),
        listened = wait_for_listener_statement(&pool, &application_name, LISTEN_TASK_DONE) => listened,
    };
    assert!(listened, "the wait subscribed on task_done");
    drop(wait);

    assert!(
        wait_for_listener_statement(&pool, &application_name, UNLISTEN_TASK_DONE).await,
        "the dropped wait left its LISTEN behind"
    );
}
