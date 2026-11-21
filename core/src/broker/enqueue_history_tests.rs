//! P6 enqueue and retention persistence gate.

use chrono::{Duration, Utc};
use uuid::Uuid;

use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_url;
use crate::broker::{BrokerError, PostgresBroker, SendKind};
use crate::core::config::retention::{EnqueueRetention, RetentionConfig};
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage_in_pool, CoverageOutcome,
};
use crate::core::history::periods::RetentionDays;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};

/// The sessions of the pool of [`migrated_pool`].
const POOL_CONNECTIONS: u32 = 5;

/// The database of the running test (`crate::test_support::database_of_this_test`),
/// a copy of the installed test template, with no coverage pass.
pub(crate) async fn migrated_pool() -> TestPool {
    let database = crate::test_support::database_of_this_test().await;
    TestPoolOptions::new()
        .max_connections(POOL_CONNECTIONS)
        .connect(database.url())
        .await
        .expect("connect the database of this test")
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

#[derive(Debug)]
struct StoredFacts {
    task_id: Uuid,
    input: String,
    execution: String,
    rerun_of_task_id: Option<Uuid>,
    rerun_root_task_id: Option<Uuid>,
    command_fingerprint_version: Option<i16>,
    command_fingerprint_length: Option<i32>,
    retention_class_key: Option<String>,
    idempotency_key_digest_length: Option<i32>,
    retain_rerun_input: Option<bool>,
}

impl sql::FromTestRow for StoredFacts {
    fn from_row(row: &tokio_postgres::Row) -> Result<Self, sql::Error> {
        Ok(Self {
            task_id: sql::named(row, "task_id")?,
            input: sql::named(row, "input")?,
            execution: sql::named(row, "execution")?,
            rerun_of_task_id: sql::named(row, "rerun_of_task_id")?,
            rerun_root_task_id: sql::named(row, "rerun_root_task_id")?,
            command_fingerprint_version: sql::named(row, "command_fingerprint_version")?,
            command_fingerprint_length: sql::named(row, "command_fingerprint_length")?,
            retention_class_key: sql::named(row, "retention_class_key")?,
            idempotency_key_digest_length: sql::named(row, "idempotency_key_digest_length")?,
            retain_rerun_input: sql::named(row, "retain_rerun_input")?,
        })
    }
}

async fn stored_facts(pool: &TestPool, task_id: Uuid) -> StoredFacts {
    sql::query_as(
        "SELECT id AS task_id, input, execution, rerun_of_task_id, rerun_root_task_id,
                command_fingerprint_version,
                octet_length(command_fingerprint) AS command_fingerprint_length,
                retention_class_key,
                octet_length(idempotency_key_digest) AS idempotency_key_digest_length,
                retain_rerun_input
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read P6 enqueue facts")
}

/// The input of the keyed sends: `value` varies the command, `named` stays.
fn keyed_input(value: &str) -> String {
    format!("{{\"value\":{value},\"named\":true}}")
}

fn period_days(days: i64) -> EnqueueRetention {
    EnqueueRetention::Period(RetentionDays::parse_period(Duration::days(days)).unwrap())
}

async fn enqueue_keyed(
    broker: &PostgresBroker,
    task_id: Uuid,
    sent_at: chrono::DateTime<Utc>,
    value: &str,
    key: Option<&str>,
    retention: EnqueueRetention,
) -> Result<Uuid, BrokerError> {
    broker
        .enqueue(
            "p6_direct",
            &keyed_input(value),
            "default",
            50,
            Some(sent_at),
            None,
            None,
            Some("{}"),
            Some(task_id),
            None,
            key,
            retention,
            Some(true),
            SendKind::First,
        )
        .await
}

#[tokio::test]
async fn direct_enqueue_persists_v27_facts_and_key_outcomes() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let sent_at = Utc::now();
    let first_id = crate::core::history::identity::uuid7::mint_task_id().unwrap();
    let applied = enqueue_keyed(
        &broker,
        first_id,
        sent_at,
        "[1]",
        Some("request-1"),
        period_days(7),
    )
    .await
    .expect("first keyed enqueue applies");
    assert_eq!(applied, first_id);
    assert_eq!(applied.get_version_num(), 7);
    let facts = stored_facts(&pool, applied).await;
    assert_eq!(facts.task_id, applied);
    assert_eq!(facts.input, keyed_input("[1]"));
    assert_eq!(facts.execution, "ISOLATED");
    assert!(facts.rerun_of_task_id.is_none());
    assert!(facts.rerun_root_task_id.is_none());
    assert_eq!(facts.command_fingerprint_version, Some(1));
    assert_eq!(facts.command_fingerprint_length, Some(32));
    assert_eq!(facts.retention_class_key.as_deref(), Some("standard_7d"));
    assert_eq!(facts.idempotency_key_digest_length, Some(32));
    assert_eq!(facts.retain_rerun_input, Some(true));
    let reservation_window_seconds: i64 = sql::query_scalar(
        "SELECT EXTRACT(EPOCH FROM reservation_window)::bigint
         FROM horsies_key_reservations WHERE task_id = $1::uuid",
    )
    .bind(applied)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reservation_window_seconds, 86_400);

    let replay_candidate = crate::core::history::identity::uuid7::mint_task_id().unwrap();
    let replay = enqueue_keyed(
        &broker,
        replay_candidate,
        sent_at,
        "[1]",
        Some("request-1"),
        period_days(7),
    )
    .await
    .expect("same command replays");
    assert_eq!(replay, applied);

    let conflict_candidate = crate::core::history::identity::uuid7::mint_task_id().unwrap();
    let conflict = enqueue_keyed(
        &broker,
        conflict_candidate,
        sent_at,
        "[2]",
        Some("request-1"),
        period_days(7),
    )
    .await
    .expect_err("different command conflicts");
    assert!(matches!(
        conflict,
        BrokerError::IdempotencyKeyConflict { .. }
    ));
    let count: i64 = sql::query_scalar("SELECT COUNT(*) FROM horsies_tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "replay and conflict cannot write another row");

    let forever_id = crate::core::history::identity::uuid7::mint_task_id().unwrap();
    let forever = broker
        .enqueue(
            "p6_forever",
            "null",
            "default",
            50,
            Some(sent_at),
            None,
            None,
            None,
            Some(forever_id),
            None,
            None,
            EnqueueRetention::Forever,
            Some(false),
            SendKind::First,
        )
        .await
        .expect("explicit forever enqueue");
    let forever_facts = stored_facts(&pool, forever).await;
    assert_eq!(
        forever_facts.retention_class_key.as_deref(),
        Some("forever")
    );
    assert_eq!(forever_facts.input, "null");
    assert_eq!(forever_facts.retain_rerun_input, Some(false));
}

#[tokio::test]
async fn task_id_conflict_binds_applied_key_and_queue_classes_register() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url_with_idempotency_reservation_window(
        pool.url(),
        Some(Duration::hours(2)),
        crate::ClientFamily::Rust,
    )
    .unwrap();
    let task_id = crate::core::history::identity::uuid7::mint_task_id().unwrap();
    let sent_at = Utc::now();

    broker
        .enqueue(
            "p6_bind",
            "[1]",
            "bulk",
            50,
            Some(sent_at),
            None,
            None,
            None,
            Some(task_id),
            None,
            None,
            period_days(2),
            Some(false),
            SendKind::First,
        )
        .await
        .expect("seed unkeyed task");
    let rebound = broker
        .enqueue(
            "p6_bind",
            "[1]",
            "bulk",
            50,
            Some(sent_at),
            None,
            None,
            None,
            Some(task_id),
            None,
            Some("bind-key"),
            period_days(2),
            Some(false),
            SendKind::First,
        )
        .await
        .expect("matching task-ID conflict binds key");
    assert_eq!(rebound, task_id);
    let facts = stored_facts(&pool, task_id).await;
    assert_eq!(facts.idempotency_key_digest_length, Some(32));
    let reservation_count: i64 = sql::query_scalar(
        "SELECT COUNT(*) FROM horsies_key_reservations
         WHERE task_id = $1::uuid AND disposition = 'LIVE'",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reservation_count, 1);
    let configured_window_seconds: i64 = sql::query_scalar(
        "SELECT EXTRACT(EPOCH FROM reservation_window)::bigint
         FROM horsies_key_reservations WHERE task_id = $1::uuid",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(configured_window_seconds, 7_200);

    let different_key = broker
        .enqueue(
            "p6_bind",
            "[1]",
            "bulk",
            50,
            Some(sent_at),
            None,
            None,
            None,
            Some(task_id),
            None,
            Some("different-bind-key"),
            period_days(2),
            Some(false),
            SendKind::First,
        )
        .await
        .expect_err("matching command cannot replace its bound idempotency key");
    // Python `test_send_idempotency_conflict.py::TestTaskIdBoundToAnotherKey::
    // test_changed_key_on_retry_is_payload_mismatch`.
    assert!(matches!(
        different_key,
        BrokerError::PayloadMismatch {
            task_id: conflicting_task_id,
        } if conflicting_task_id == task_id
    ));
    let reservations_after_key_conflict: i64 =
        sql::query_scalar("SELECT COUNT(*) FROM horsies_key_reservations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        reservations_after_key_conflict, 1,
        "key-binding conflict must roll back the second reservation",
    );

    let mismatch = broker
        .enqueue(
            "p6_bind",
            "[2]",
            "bulk",
            50,
            Some(sent_at),
            None,
            None,
            None,
            Some(task_id),
            None,
            Some("rejected-bind-key"),
            period_days(2),
            Some(false),
            SendKind::First,
        )
        .await
        .expect_err("mismatched task-ID binding must fail closed");
    assert!(matches!(mismatch, BrokerError::PayloadMismatch { .. }));
    let reservations_after_mismatch: i64 =
        sql::query_scalar("SELECT COUNT(*) FROM horsies_key_reservations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        reservations_after_mismatch, 1,
        "failed task-ID binding must roll back the new reservation",
    );

    let config = RetentionConfig {
        retention_periods: vec![Duration::days(7)],
        queue_retention: std::collections::HashMap::from([(
            "bulk".to_owned(),
            Some(Duration::days(2)),
        )]),
        ..Default::default()
    };
    let declared = config.registrable_periods();
    let core = core_pool_for_test_url(pool.url());
    let coverage = ensure_partition_coverage_in_pool(
        &core,
        StatementLog::Off,
        config.history_leaf_horizon_days,
        config.heartbeat_leaf_horizon_hours,
        &declared,
    )
    .await
    .expect("register configured classes");
    core.close();
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );

    let registered: Vec<(String, String, Option<i16>)> = sql::query_as(
        "SELECT class_key, class_kind::text, retention_days
         FROM horsies_retention_classes
         WHERE class_key IN ('standard_2d', 'standard_7d')
         ORDER BY class_key",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        registered,
        vec![
            ("standard_2d".to_owned(), "PERIOD".to_owned(), Some(2)),
            ("standard_7d".to_owned(), "PERIOD".to_owned(), Some(7)),
        ],
    );
}

/// The broker binds its reservation window exact to the microsecond: a
/// window of 1.5 s is stored as 1.5 s, and a window of 0.5 s is admitted and
/// stored as 0.5 s (whole seconds stored 1 s and 0 s, and the key claim
/// refused the 0 s window).
#[tokio::test]
async fn a_keyed_send_stores_its_window_exact_to_the_microsecond() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    for (window, key) in [
        (Duration::milliseconds(1_500), "window-1500ms"),
        (Duration::milliseconds(500), "window-500ms"),
    ] {
        let broker = PostgresBroker::from_test_url_with_idempotency_reservation_window(
            pool.url(),
            Some(window),
            crate::ClientFamily::Rust,
        )
        .expect("a window from 1 microsecond to 30 days");
        let task_id = crate::core::history::identity::uuid7::mint_task_id().unwrap();
        let sent = broker
            .enqueue(
                "p6_window",
                "[1]",
                "default",
                50,
                Some(Utc::now()),
                None,
                None,
                None,
                Some(task_id),
                None,
                Some(key),
                EnqueueRetention::Default,
                Some(false),
                SendKind::First,
            )
            .await
            .expect("the keyed send is admitted");
        assert_eq!(sent, task_id, "{key}");
        let stored_micros: i64 = sql::query_scalar(
            "SELECT (EXTRACT(EPOCH FROM reservation_window) * 1000000)::bigint
             FROM horsies_key_reservations WHERE task_id = $1::uuid",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(Some(stored_micros), window.num_microseconds(), "{key}");
    }
}

#[tokio::test]
async fn broker_retain_default_is_inherited_and_explicit_false_wins() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_test_url_with_enqueue_policy(
        pool.url(),
        true,
        None,
        crate::ClientFamily::Rust,
    )
    .unwrap();
    let sent_at = Utc::now();

    let inherited = broker
        .enqueue(
            "p6_retain_default",
            "[1]",
            "default",
            50,
            Some(sent_at),
            None,
            None,
            None,
            None,
            None,
            None,
            EnqueueRetention::Default,
            None,
            SendKind::First,
        )
        .await
        .unwrap();
    let declined = broker
        .enqueue(
            "p6_retain_default",
            "[2]",
            "default",
            50,
            Some(sent_at),
            None,
            None,
            None,
            None,
            None,
            None,
            EnqueueRetention::Default,
            Some(false),
            SendKind::First,
        )
        .await
        .unwrap();

    let inherited_facts = stored_facts(&pool, inherited).await;
    assert_eq!(inherited_facts.retain_rerun_input, Some(true));
    // A send that names no class stores the default class.
    assert_eq!(
        inherited_facts.retention_class_key.as_deref(),
        Some("standard_30d")
    );
    let declined_facts = stored_facts(&pool, declined).await;
    assert_eq!(declined_facts.retain_rerun_input, Some(false));
}

/// Python `test_broker_postgres.py::TestEnqueueCatch::test_an_outcome_outside_the_contract_crashes_loud`:
/// an outcome outside the contract of `horsies_enqueue_task` is a defect of
/// the function, not a refusal of the send. The enqueue panics with the
/// outcome and the task id, and answers no catalog error. The function is
/// replaced, in this test's database, by one that reports `SOMETHING_ELSE`.
#[tokio::test]
async fn an_enqueue_outcome_outside_the_contract_panics_with_the_outcome_and_the_task() {
    let pool = migrated_pool().await;
    sql::raw_sql(
        "CREATE OR REPLACE FUNCTION horsies_enqueue_task(
             p_task_id uuid, p_task_name text, p_queue_name text, p_priority integer,
             p_input text, p_task_options text, p_execution text, p_retention_class_key text,
             p_retain_rerun_input boolean, p_max_retries integer,
             p_good_until timestamp with time zone, p_sent_at timestamp with time zone,
             p_start_kind text, p_start_at timestamp with time zone, p_start_delay_s integer,
             p_fingerprint_version smallint, p_fingerprint bytea, p_lineage_kind text,
             p_rerun_of_task_id uuid, p_rerun_root_task_id uuid, p_key_kind text,
             p_key_digest bytea, p_key_scope_version smallint, p_key_window interval,
             p_send_kind text, p_trace_kind text, p_trace_carrier text, p_trace_id bytea
         ) RETURNS TABLE(outcome text, task_id uuid)
         LANGUAGE sql AS $$ SELECT 'SOMETHING_ELSE'::text, p_task_id $$;",
    )
    .execute(&pool)
    .await
    .expect("replace the enqueue function");
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_id = Uuid::from_u128(0x1232);

    let sent = tokio::spawn(async move {
        broker
            .enqueue(
                "p6_contract",
                "{}",
                "default",
                50,
                Some(Utc::now()),
                None,
                None,
                None,
                Some(task_id),
                None,
                None,
                EnqueueRetention::Default,
                Some(false),
                SendKind::First,
            )
            .await
    })
    .await;

    let panic = match sent {
        Ok(answer) => panic!("the enqueue answered {answer:?} instead of a panic"),
        Err(joined) => joined.into_panic(),
    };
    let message = panic
        .downcast_ref::<String>()
        .expect("the panic carries its formatted message");
    assert!(message.contains("\"SOMETHING_ELSE\""), "{message}");
    assert!(message.contains(&task_id.to_string()), "{message}");
}
