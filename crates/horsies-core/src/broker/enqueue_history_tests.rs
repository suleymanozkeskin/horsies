//! P6 enqueue and retention persistence gate.

use chrono::{Duration, Utc};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::retention::{EnqueueRetention, RetentionConfig};
use crate::core::history::maintenance::coverage::{ensure_partition_coverage, CoverageOutcome};
use crate::core::history::periods::RetentionDays;
use crate::core::history::reads::publisher::StagedLoaderPublisher;

/// The database of the running test (`crate::test_support::database_of_this_test`),
/// a copy of the installed test template, as a sqlx pool.
pub(crate) async fn migrated_pool() -> PgPool {
    let database = crate::test_support::database_of_this_test().await;
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database.url())
        .await
        .expect("connect the database of this test")
}

async fn clear_enqueue_state(pool: &PgPool) {
    sqlx::query("DELETE FROM horsies_key_reservations")
        .execute(pool)
        .await
        .expect("clear reservations");
    sqlx::query("DELETE FROM horsies_tasks")
        .execute(pool)
        .await
        .expect("clear tasks");
}

#[derive(Debug, sqlx::FromRow)]
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

async fn stored_facts(pool: &PgPool, task_id: Uuid) -> StoredFacts {
    sqlx::query_as(
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
        )
        .await
}

#[tokio::test]
#[serial]
async fn direct_enqueue_persists_v27_facts_and_key_outcomes() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_pool(pool.clone());
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
    let reservation_window_seconds: i64 = sqlx::query_scalar(
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
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM horsies_tasks")
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
#[serial]
async fn task_id_conflict_binds_applied_key_and_queue_classes_register() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_pool_with_idempotency_reservation_window(
        pool.clone(),
        Some(Duration::hours(2)),
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
        )
        .await
        .expect("matching task-ID conflict binds key");
    assert_eq!(rebound, task_id);
    let facts = stored_facts(&pool, task_id).await;
    assert_eq!(facts.idempotency_key_digest_length, Some(32));
    let reservation_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_key_reservations
         WHERE task_id = $1::uuid AND disposition = 'LIVE'",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reservation_count, 1);
    let configured_window_seconds: i64 = sqlx::query_scalar(
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
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_key_reservations")
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
        )
        .await
        .expect_err("mismatched task-ID binding must fail closed");
    assert!(matches!(mismatch, BrokerError::PayloadMismatch { .. }));
    let reservations_after_mismatch: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_key_reservations")
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
    let mut transaction = pool.begin().await.unwrap();
    let coverage = ensure_partition_coverage(
        &mut transaction,
        config.history_leaf_horizon_days,
        config.heartbeat_leaf_horizon_hours,
        &declared,
        &StagedLoaderPublisher,
    )
    .await
    .expect("register configured classes");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    transaction.commit().await.unwrap();

    let registered: Vec<(String, String, Option<i16>)> = sqlx::query_as(
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

#[tokio::test]
#[serial]
async fn broker_retain_default_is_inherited_and_explicit_false_wins() {
    let pool = migrated_pool().await;
    clear_enqueue_state(&pool).await;
    let broker = PostgresBroker::from_pool_with_enqueue_policy(pool.clone(), true, None).unwrap();
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
