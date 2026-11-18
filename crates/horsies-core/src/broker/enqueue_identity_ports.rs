//! Ports of the core rows of the identity and enqueue prototypes (#880).
//!
//! Python specification: `tests/integration/test_task_history_identity_prototypes.py`
//! and `tests/integration/test_enqueue_cutover_conversion.py`. The identity
//! prototypes call candidate SQL functions; these ports send through
//! `PostgresBroker::enqueue`, the production path. Each test runs on the P6
//! enqueue database (`enqueue_history_tests::migrated_pool`) and clears its
//! task and reservation rows first.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serial_test::serial;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::broker::enqueue_history_tests::migrated_pool;
use crate::broker::{BrokerError, PostgresBroker, SendKind};
use crate::core::config::retention::{EnqueueRetention, RetentionConfig};
use crate::core::history::identity::uuid7::mint_task_id;
use crate::core::history::maintenance::coverage::CoverageOutcome;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::test_support::terminal_writes;

const TASK_NAME: &str = "port.identity";
const INPUT: &str = "{\"order_id\":\"order-1\"}";
const PRIORITY: i32 = 10;
const OTHER_PRIORITY: i32 = 11;
const PORT_WORKER: &str = "port-identity-worker";
/// Sends of one key in the concurrency tests, as Python's 64.
const CONCURRENT_SENDS: usize = 64;
/// Connections that the concurrent sends share.
const CONCURRENT_CONNECTIONS: u32 = 16;

/// The fields of one send that the tests vary. The rest of the command is
/// fixed: `TASK_NAME`, `INPUT`, queue `default`, the default class, no task
/// options, no rerun input.
#[derive(Debug, Clone, Copy)]
struct Send {
    task_id: Uuid,
    priority: i32,
    key: Option<&'static str>,
    sent_at: DateTime<Utc>,
}

impl Send {
    fn new(key: Option<&'static str>, sent_at: DateTime<Utc>) -> Self {
        Self {
            task_id: mint_task_id().expect("mint a task id"),
            priority: PRIORITY,
            key,
            sent_at,
        }
    }

    /// The same command and call time under a new task id.
    fn with_new_id(self) -> Self {
        Self {
            task_id: mint_task_id().expect("mint a task id"),
            ..self
        }
    }
}

async fn send(broker: &PostgresBroker, send: Send) -> Result<Uuid, BrokerError> {
    send_as(broker, send, SendKind::First).await
}

async fn send_as(
    broker: &PostgresBroker,
    send: Send,
    send_kind: SendKind,
) -> Result<Uuid, BrokerError> {
    broker
        .enqueue(
            TASK_NAME,
            INPUT,
            "default",
            send.priority,
            Some(send.sent_at),
            None,
            None,
            None,
            Some(send.task_id),
            None,
            send.key,
            EnqueueRetention::Default,
            Some(false),
            send_kind,
        )
        .await
}

async fn clear_enqueue_state(pool: &TestPool) {
    sql::query("DELETE FROM horsies_key_reservations")
        .execute(pool)
        .await
        .expect("clear reservations");
    sql::query("DELETE FROM horsies_tasks")
        .execute(pool)
        .await
        .expect("clear tasks");
}

/// Creates the history leaves of the default class and publishes the
/// readers, so a move finds its leaf.
async fn ensure_history_coverage(pool: &TestPool) {
    let config = RetentionConfig::default();
    let core = crate::broker::pools::core_pool_for_test_url(pool.url());
    let coverage = crate::core::history::maintenance::coverage::ensure_partition_coverage_in_pool(
        &core,
        crate::broker::core_rows::StatementLog::Off,
        config.history_leaf_horizon_days,
        config.heartbeat_leaf_horizon_hours,
        &config.registrable_periods(),
    )
    .await
    .expect("ensure coverage");
    core.close();
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
}

/// Moves a PENDING task to history: the row becomes RUNNING under
/// `PORT_WORKER`, with the process facts of its start, then completes
/// (`horsies_complete_task_fused`). The move terminalizes the task's key
/// reservation.
async fn move_to_history(pool: &TestPool, task_id: Uuid) {
    let started = sql::query(
        "UPDATE horsies_tasks
         SET status = 'RUNNING', claimed_at = NOW(), claimed_by_worker_id = $2,
             started_at = NOW(), worker_pid = 4242, worker_hostname = 'port-host',
             worker_process_name = 'port-process', updated_at = NOW()
         WHERE id = $1 AND status = 'PENDING'",
    )
    .bind(task_id)
    .bind(PORT_WORKER)
    .execute(pool)
    .await
    .expect("start the task");
    assert_eq!(started.rows_affected(), 1);
    let result_json = serde_json::to_string(&crate::core::TaskResult::Ok(
        serde_json::json!({"value": 7}),
    ))
    .expect("encode the result");
    let outcomes = terminal_writes::complete_plain_task(pool, task_id, PORT_WORKER, &result_json)
        .await
        .expect("complete the task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
}

async fn count(pool: &TestPool, sql: &str, task_id: Uuid) -> i64 {
    sql::query_scalar(sql)
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count rows")
}

async fn live_rows(pool: &TestPool, task_id: Uuid) -> i64 {
    count(
        pool,
        "SELECT count(*) FROM horsies_tasks WHERE id = $1",
        task_id,
    )
    .await
}

async fn history_rows(pool: &TestPool, task_id: Uuid) -> i64 {
    count(
        pool,
        "SELECT count(*) FROM horsies_task_history WHERE task_id = $1",
        task_id,
    )
    .await
}

async fn reservations_of(pool: &TestPool, task_id: Uuid) -> i64 {
    count(
        pool,
        "SELECT count(*) FROM horsies_key_reservations WHERE task_id = $1",
        task_id,
    )
    .await
}

async fn all_reservations(pool: &TestPool) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_key_reservations")
        .fetch_one(pool)
        .await
        .expect("count reservations")
}

/// One concurrent send: its candidate id, its priority and its outcome.
type ConcurrentOutcome = (Uuid, i32, Result<Uuid, BrokerError>);

/// Sends `CONCURRENT_SENDS` commands under `key` at once, each with its own
/// task id and the priority that `priority_of(index)` gives.
async fn concurrent_sends(
    pool: &TestPool,
    key: &'static str,
    priority_of: fn(usize) -> i32,
) -> Vec<ConcurrentOutcome> {
    let wide = TestPoolOptions::new()
        .max_connections(CONCURRENT_CONNECTIONS)
        .connect(pool.url())
        .await
        .expect("connect the concurrent pool");
    let broker = Arc::new(PostgresBroker::from_test_url(
        wide.url(),
        crate::ClientFamily::Rust,
    ));
    let sent_at = Utc::now();
    let mut sends = tokio::task::JoinSet::new();
    for index in 0..CONCURRENT_SENDS {
        let broker = Arc::clone(&broker);
        let one = Send {
            priority: priority_of(index),
            ..Send::new(Some(key), sent_at)
        };
        sends.spawn(async move {
            let outcome = send(&broker, one).await;
            (one.task_id, one.priority, outcome)
        });
    }
    let mut outcomes = Vec::with_capacity(CONCURRENT_SENDS);
    while let Some(joined) = sends.join_next().await {
        outcomes.push(joined.expect("join a concurrent send"));
    }
    wide.close().await;
    outcomes
}

/// The one send whose own task id came back: the applied send.
fn the_applied_send(outcomes: &[ConcurrentOutcome]) -> (Uuid, i32) {
    let applied: Vec<(Uuid, i32)> = outcomes
        .iter()
        .filter(|(task_id, _, outcome)| matches!(outcome, Ok(id) if id == task_id))
        .map(|(task_id, priority, _)| (*task_id, *priority))
        .collect();
    assert_eq!(applied.len(), 1, "{outcomes:?}");
    applied[0]
}

/// `test_concurrent_same_key_commits_exactly_one_request`: 64 concurrent
/// sends of one command under one key commit one task; the other 63 replay
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_sends_of_one_key_apply_once_and_replay_the_rest() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;

    let outcomes = concurrent_sends(&pool, "concurrent-order", |_| PRIORITY).await;

    let (winner, _) = the_applied_send(&outcomes);
    assert!(
        outcomes
            .iter()
            .all(|(_, _, outcome)| matches!(outcome, Ok(id) if *id == winner)),
        "{outcomes:?}"
    );
    let tasks: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks")
        .fetch_one(&pool)
        .await
        .expect("count tasks");
    assert_eq!(tasks, 1);
    assert_eq!(all_reservations(&pool).await, 1);
}

/// `test_concurrent_same_key_different_fingerprints_classify_by_winner`:
/// concurrent sends of two commands under one key; each send is classified
/// against the one that applied: the same command replays it, the other
/// command is a key conflict that names it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_sends_of_two_commands_are_classified_by_the_winner() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;

    let outcomes = concurrent_sends(&pool, "concurrent-conflict", |index| match index % 2 {
        0 => PRIORITY,
        _ => OTHER_PRIORITY,
    })
    .await;

    let (winner, winning_priority) = the_applied_send(&outcomes);
    for (task_id, priority, outcome) in &outcomes {
        match outcome {
            Ok(id) if id == task_id => assert_eq!(*task_id, winner),
            Ok(id) => {
                assert_eq!(*id, winner, "{outcomes:?}");
                assert_eq!(*priority, winning_priority, "{outcomes:?}");
            }
            Err(BrokerError::IdempotencyKeyConflict { task_id: owner, .. }) => {
                assert_eq!(*owner, winner, "{outcomes:?}");
                assert_ne!(*priority, winning_priority, "{outcomes:?}");
            }
            Err(other) => panic!("send {task_id}: unexpected outcome {other:?}"),
        }
    }
    assert_eq!(all_reservations(&pool).await, 1);
}

/// `test_post_history_removal_follows_reservation_storage_lifetime`
/// (key-registry candidate, the production design): after the task moved to
/// history and its history row is gone, the key still replays the task until
/// the reservation expires.
#[tokio::test]
#[serial]
async fn a_key_replays_its_task_after_the_history_row_is_gone() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    ensure_history_coverage(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let first = Send::new(Some("history-removed"), Utc::now());
    assert_eq!(
        send(&broker, first).await.expect("first send"),
        first.task_id
    );
    move_to_history(&pool, first.task_id).await;
    sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(first.task_id)
        .execute(&pool)
        .await
        .expect("remove the history row");

    let resend = first.with_new_id();
    let replayed = send(&broker, resend).await.expect("resend");

    assert_eq!(replayed, first.task_id);
    assert_eq!(live_rows(&pool, resend.task_id).await, 0);
}

/// `test_expired_key_can_be_reused_without_removing_old_task_identity`: once
/// the terminal reservation has expired, the key applies to a new task, and
/// the old task's history row stays.
#[tokio::test]
#[serial]
async fn an_expired_key_applies_to_a_new_task_and_the_old_task_stays() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    ensure_history_coverage(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let first = Send::new(Some("reusable-order"), Utc::now());
    assert_eq!(
        send(&broker, first).await.expect("first send"),
        first.task_id
    );
    move_to_history(&pool, first.task_id).await;
    let expired = sql::query(
        "UPDATE horsies_key_reservations
         SET expires_at = statement_timestamp() - interval '1 second'
         WHERE task_id = $1 AND disposition = 'TERMINAL'",
    )
    .bind(first.task_id)
    .execute(&pool)
    .await
    .expect("expire the reservation");
    assert_eq!(expired.rows_affected(), 1);

    let second = first.with_new_id();
    let applied = send(&broker, second).await.expect("send after expiry");

    assert_eq!(applied, second.task_id);
    assert_eq!(history_rows(&pool, first.task_id).await, 1);
    assert_eq!(live_rows(&pool, second.task_id).await, 1);
    assert_eq!(reservations_of(&pool, first.task_id).await, 0);
    assert_eq!(reservations_of(&pool, second.task_id).await, 1);
}

/// `test_same_task_identity_requires_same_key_and_fingerprint`. The keyed
/// halves are covered by `enqueue_history_tests` (replay, second key). The
/// prototype refused a resend of a keyed task id with no key; Python's
/// production exact-id check compares the version-1 fingerprint and
/// `sent_at` only (`python/horsies/core/brokers/postgres.py:2071-2131`), and the
/// fingerprint has no key field (`python/horsies/core/history/identity/fingerprint.py:31-50`).
/// The core keeps the production rule: the resend returns the same task, and
/// the key binding stays.
#[tokio::test]
#[serial]
async fn an_unkeyed_resend_of_a_keyed_task_returns_it_and_keeps_its_key() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let keyed = Send::new(Some("order-1"), Utc::now());
    assert_eq!(
        send(&broker, keyed).await.expect("keyed send"),
        keyed.task_id
    );

    let unkeyed = Send { key: None, ..keyed };
    let resent = send(&broker, unkeyed).await.expect("unkeyed resend");

    assert_eq!(resent, keyed.task_id);
    assert_eq!(live_rows(&pool, keyed.task_id).await, 1);
    let bound: Option<i32> = sql::query_scalar(
        "SELECT octet_length(idempotency_key_digest) FROM horsies_tasks WHERE id = $1",
    )
    .bind(keyed.task_id)
    .fetch_one(&pool)
    .await
    .expect("read the bound key");
    assert_eq!(bound, Some(32));
    assert_eq!(reservations_of(&pool, keyed.task_id).await, 1);
}

/// `test_replay_survives_terminal_move_for_declared_window`: inside the
/// reservation window, a send of the same command under the same key after
/// the move returns the moved task and writes no row.
#[tokio::test]
#[serial]
async fn a_replay_after_the_move_returns_the_moved_task() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    ensure_history_coverage(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let first = Send::new(Some("order-1"), Utc::now());
    assert_eq!(
        send(&broker, first).await.expect("first send"),
        first.task_id
    );
    move_to_history(&pool, first.task_id).await;

    let resend = first.with_new_id();
    let replayed = send(&broker, resend).await.expect("resend after the move");

    assert_eq!(replayed, first.task_id);
    assert_eq!(live_rows(&pool, resend.task_id).await, 0);
    assert_eq!(history_rows(&pool, first.task_id).await, 1);
}

/// `tests/dualrun/test_enqueue_task.py`, cases `history.first`,
/// `history.repeat` and `history.repeat_other`: after a task moved to
/// history, a `REPEAT` send of the same command answers with its id and
/// writes nothing; a `REPEAT` send of another command under the id is a
/// payload mismatch and writes nothing; a `FIRST` send does not read history
/// and writes a new live row.
#[tokio::test]
#[serial]
async fn a_repeat_send_of_a_finished_task_answers_from_history() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    ensure_history_coverage(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let repeated = Send::new(None, Utc::now());
    let other = Send::new(None, Utc::now());
    let first = Send::new(None, Utc::now());
    for finished in [repeated, other, first] {
        assert_eq!(
            send(&broker, finished).await.expect("the first send"),
            finished.task_id
        );
        move_to_history(&pool, finished.task_id).await;
    }

    let same = send_as(&broker, repeated, SendKind::Repeat).await;
    let changed = Send {
        priority: OTHER_PRIORITY,
        ..other
    };
    let mismatch = send_as(&broker, changed, SendKind::Repeat).await;
    let again = send_as(&broker, first, SendKind::First).await;

    assert_eq!(
        same.expect("a repeat of the same command"),
        repeated.task_id
    );
    assert_eq!(live_rows(&pool, repeated.task_id).await, 0);
    assert!(
        matches!(mismatch, Err(BrokerError::PayloadMismatch { task_id }) if task_id == other.task_id),
        "{mismatch:?}"
    );
    assert_eq!(live_rows(&pool, other.task_id).await, 0);
    assert_eq!(again.expect("a first send writes again"), first.task_id);
    assert_eq!(live_rows(&pool, first.task_id).await, 1);
}

/// `test_unkeyed_enqueue_creates_no_key_only_registry_row`: unkeyed sends
/// write no reservation.
#[tokio::test]
#[serial]
async fn unkeyed_sends_write_no_reservation() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let sent_at = Utc::now();
    for one in [Send::new(None, sent_at), Send::new(None, sent_at)] {
        assert_eq!(send(&broker, one).await.expect("unkeyed send"), one.task_id);
    }
    assert_eq!(all_reservations(&pool).await, 0);
}

/// `test_enqueue_cutover_conversion.py::TestConvertedStatement::test_new_row_carries_real_cutover_values`:
/// the stored fingerprint is the SHA-256 of Python's version-1 array for the
/// send (`python/horsies/core/history/identity/fingerprint.py:31-50`; the input
/// takes the keyword slot, the positional slot is null). The expected bytes
/// come from the literal array, not from the core's fingerprint code.
#[tokio::test]
#[serial]
async fn a_new_row_stores_the_exact_version_one_fingerprint() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_id = mint_task_id().expect("mint a task id");
    let applied = broker
        .enqueue(
            "cutover.enqueue",
            "{\"x\":1}",
            "default",
            100,
            Some(Utc::now()),
            None,
            None,
            None,
            Some(task_id),
            None,
            None,
            EnqueueRetention::Default,
            None,
            SendKind::First,
        )
        .await
        .expect("send");
    assert_eq!(applied, task_id);
    let expected: [u8; 32] = Sha256::digest(
        br#"[1,"cutover.enqueue","default",100,null,"{\"x\":1}",null,null,null,"standard_30d",false,null,null]"#,
    )
    .into();

    let (input, version, fingerprint, class_key, retain): (
        String,
        Option<i16>,
        Option<Vec<u8>>,
        String,
        bool,
    ) = sql::query_as(
        "SELECT input, command_fingerprint_version, command_fingerprint,
                    retention_class_key, retain_rerun_input
             FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the row");
    assert_eq!(input, "{\"x\":1}");
    assert_eq!(version, Some(1));
    assert_eq!(fingerprint.as_deref(), Some(expected.as_slice()));
    assert_eq!(class_key, "standard_30d");
    assert!(!retain);
}

/// `TestKeyedEnqueue::test_task_id_conflict_binds_the_verified_task_to_the_key`:
/// a keyed resend of an unkeyed task binds the key to it; after the bind, a
/// send of the same command under the key with a new id replays the task.
#[tokio::test]
#[serial]
async fn after_the_bind_a_send_with_a_new_id_replays_the_bound_task() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let existing = Send::new(None, Utc::now());
    assert_eq!(
        send(&broker, existing).await.expect("unkeyed send"),
        existing.task_id
    );
    let keyed = Send {
        key: Some("late-key"),
        ..existing
    };
    assert_eq!(send(&broker, keyed).await.expect("bind"), existing.task_id);
    let (disposition, digests_match): (String, bool) = sql::query_as(
        "SELECT r.disposition, r.idempotency_key_digest = t.idempotency_key_digest
         FROM horsies_key_reservations AS r
         JOIN horsies_tasks AS t ON t.id = r.task_id
         WHERE r.task_id = $1",
    )
    .bind(existing.task_id)
    .fetch_one(&pool)
    .await
    .expect("read the binding");
    assert_eq!(disposition, "LIVE");
    assert!(digests_match);

    let fresh = keyed.with_new_id();
    let replayed = send(&broker, fresh).await.expect("send with a new id");

    assert_eq!(replayed, existing.task_id);
    assert_eq!(live_rows(&pool, fresh.task_id).await, 0);
    assert_eq!(live_rows(&pool, existing.task_id).await, 1);
}

/// `TestKeyedEnqueue::test_task_id_cannot_be_bound_to_a_second_key`: a task
/// sent under one key, resent with the same command under a second key, is a
/// payload mismatch, not a key conflict; the first reservation is the only
/// one.
#[tokio::test]
#[serial]
async fn a_keyed_task_resent_under_a_second_key_is_a_payload_mismatch() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let first = Send::new(Some("first-key"), Utc::now());
    assert_eq!(
        send(&broker, first).await.expect("keyed send"),
        first.task_id
    );

    let second_key = Send {
        key: Some("second-key"),
        ..first
    };
    let refused = send(&broker, second_key)
        .await
        .expect_err("a second key is refused");

    assert!(
        matches!(refused, BrokerError::PayloadMismatch { task_id } if task_id == first.task_id),
        "{refused:?}"
    );
    assert_eq!(reservations_of(&pool, first.task_id).await, 1);
    assert_eq!(all_reservations(&pool).await, 1);
}

/// `TestKeyedEnqueue::test_registry_indexes_are_present_since_emission`: the
/// reservation table has exactly its primary key and the expiry index.
#[tokio::test]
#[serial]
async fn the_reservation_table_has_exactly_its_key_and_expiry_index() {
    let pool = migrated_pool().await;
    let names: Vec<String> = sql::query_scalar(
        "SELECT indexname::text FROM pg_indexes WHERE tablename = 'horsies_key_reservations'",
    )
    .fetch_all(&pool)
    .await
    .expect("read the indexes");
    assert_eq!(
        names.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "horsies_key_reservations_expiry_idx".to_owned(),
            "horsies_key_reservations_pkey".to_owned(),
        ])
    );
}
