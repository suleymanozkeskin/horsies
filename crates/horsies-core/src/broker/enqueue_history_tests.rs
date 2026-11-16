//! P6 enqueue and retention persistence gate.

use chrono::{Duration, Utc};
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use std::str::FromStr;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::broker::migrations::run_horsies_migrations;
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::retention::{RetentionClassConfig, RetentionConfig};
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage, CoverageOutcome, DeclaredRetentionClass,
};
use crate::core::history::reads::publisher::StagedLoaderPublisher;

fn test_db_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let root = std::path::Path::new(manifest_dir)
        .ancestors()
        .find(|path| path.join(".env").exists());
    let password = root
        .and_then(|path| std::fs::read_to_string(path.join(".env")).ok())
        .and_then(|contents| {
            contents
                .lines()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(key, _)| key.trim() == "DB_PASSWORD")
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{password}@localhost:5432/horsies-core")
}

struct P6TestDatabase {
    name: String,
    _anchor: PgPool,
}

static P6_DATABASE: OnceCell<P6TestDatabase> = OnceCell::const_new();

pub(crate) async fn migrated_pool() -> PgPool {
    let database = P6_DATABASE
        .get_or_init(|| async {
            let base = PgConnectOptions::from_str(&test_db_url()).expect("invalid P6 database URL");
            let database_name = format!("core_p6_enqueue_{}", Uuid::new_v4().simple());
            let mut admin = PgConnection::connect_with(&base.clone().database("postgres"))
                .await
                .expect("connect P6 admin database");
            sqlx::query("SELECT pg_advisory_lock(hashtext('core_p6_enqueue_setup'))")
                .execute(&mut admin)
                .await
                .expect("lock P6 database setup");
            let stale: Vec<String> = sqlx::query_scalar(
                "SELECT d.datname
                 FROM pg_database d
                 WHERE left(d.datname, length('core_p6_enqueue_')) =
                       'core_p6_enqueue_'
                   AND NOT EXISTS (
                       SELECT 1 FROM pg_stat_activity a WHERE a.datname = d.datname
                   )
                 ORDER BY d.datname",
            )
            .fetch_all(&mut admin)
            .await
            .expect("list inactive P6 databases");
            for stale_database in stale {
                let suffix = stale_database
                    .strip_prefix("core_p6_enqueue_")
                    .expect("query enforces P6 database prefix");
                assert!(
                    suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "refuse to drop non-generated P6 database {stale_database:?}",
                );
                sqlx::query(&format!("DROP DATABASE \"{stale_database}\""))
                    .execute(&mut admin)
                    .await
                    .expect("drop inactive P6 database");
            }
            sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
                .execute(&mut admin)
                .await
                .expect("create P6 database");
            let anchor = PgPoolOptions::new()
                .min_connections(1)
                .max_connections(1)
                .max_lifetime(None)
                .idle_timeout(None)
                .connect_with(base.database(&database_name))
                .await
                .expect("connect P6 database");
            let unlocked: bool =
                sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext('core_p6_enqueue_setup'))")
                    .fetch_one(&mut admin)
                    .await
                    .expect("unlock P6 database setup");
            assert!(unlocked, "P6 database setup lock was held");
            run_horsies_migrations(&anchor)
                .await
                .expect("migrate P6 database");
            P6TestDatabase {
                name: database_name,
                _anchor: anchor,
            }
        })
        .await;
    PgPoolOptions::new()
        .max_connections(5)
        .connect_with(
            PgConnectOptions::from_str(&test_db_url())
                .expect("invalid P6 database URL")
                .database(&database.name),
        )
        .await
        .expect("connect current P6 test runtime")
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

async fn enqueue_keyed(
    broker: &PostgresBroker,
    task_id: Uuid,
    sent_at: chrono::DateTime<Utc>,
    value: &str,
    key: Option<&str>,
    class: Option<&str>,
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
            class,
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
        Some("audit_7d"),
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
    assert_eq!(facts.retention_class_key.as_deref(), Some("audit_7d"));
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
        Some("audit_7d"),
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
        Some("audit_7d"),
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
            None,
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
            Some("q_bulk_36h"),
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
            Some("q_bulk_36h"),
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
            Some("q_bulk_36h"),
            Some(false),
        )
        .await
        .expect_err("matching command cannot replace its bound idempotency key");
    assert!(matches!(
        different_key,
        BrokerError::IdempotencyKeyConflict {
            task_id: conflicting_task_id,
            ..
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
            Some("q_bulk_36h"),
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
        retention_classes: vec![RetentionClassConfig {
            key: "audit_7d".to_owned(),
            duration: Duration::days(7),
        }],
        queue_retention: std::collections::HashMap::from([(
            "bulk".to_owned(),
            Some(Duration::hours(36)),
        )]),
        ..Default::default()
    };
    let declared: Vec<_> = config
        .registrable_classes()
        .into_iter()
        .map(|class| DeclaredRetentionClass {
            class_key: class.key,
            duration: class.duration,
        })
        .collect();
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

    let registered: Vec<(String, i64)> = sqlx::query_as(
        "SELECT class_key, EXTRACT(EPOCH FROM duration)::bigint
         FROM horsies_retention_classes
         WHERE class_key IN ('audit_7d', 'q_bulk_36h')
         ORDER BY class_key",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        registered,
        vec![
            ("audit_7d".to_owned(), Duration::days(7).num_seconds()),
            ("q_bulk_36h".to_owned(), Duration::hours(36).num_seconds()),
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
            Some("standard_30d"),
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
            Some("standard_30d"),
            Some(false),
        )
        .await
        .unwrap();

    let inherited_facts = stored_facts(&pool, inherited).await;
    assert_eq!(inherited_facts.retain_rerun_input, Some(true));
    let declined_facts = stored_facts(&pool, declined).await;
    assert_eq!(declined_facts.retain_rerun_input, Some(false));
}

#[test]
fn every_core_task_insert_names_the_baseline_columns() {
    // Gate rule: any change that adds, removes, or relocates a task INSERT in
    // the core must rerun this inventory and update the exact count. The
    // baseline schema has no column defaults, so every insert names these.
    const SOURCES: &[(&str, &str)] = &[
        ("broker/migrations.rs", include_str!("migrations.rs")),
        (
            "broker/monitoring_schema_tests.rs",
            include_str!("monitoring_schema_tests.rs"),
        ),
        ("broker/postgres.rs", include_str!("postgres.rs")),
        (
            "broker/terminalization_matrix.rs",
            include_str!("terminalization_matrix.rs"),
        ),
        (
            "core/history/reads/tests.rs",
            include_str!("../core/history/reads/tests.rs"),
        ),
        (
            "core/history/rerun/tests.rs",
            include_str!("../core/history/rerun/tests.rs"),
        ),
        (
            "monitoring/action_tests.rs",
            include_str!("../monitoring/action_tests.rs"),
        ),
        (
            "monitoring/tests.rs",
            include_str!("../monitoring/tests.rs"),
        ),
        ("web/tests.rs", include_str!("../web/tests.rs")),
        (
            "worker/execution.rs",
            include_str!("../worker/execution.rs"),
        ),
        (
            "worker/finalizing_tests.rs",
            include_str!("../worker/finalizing_tests.rs"),
        ),
        ("worker/recovery.rs", include_str!("../worker/recovery.rs")),
        ("worker/worker.rs", include_str!("../worker/worker.rs")),
        (
            "workflow_engine/engine.rs",
            include_str!("../workflow_engine/engine.rs"),
        ),
        (
            "workflow_engine/lifecycle.rs",
            include_str!("../workflow_engine/lifecycle.rs"),
        ),
        (
            "workflow_engine/phase2_recovery.rs",
            include_str!("../workflow_engine/phase2_recovery.rs"),
        ),
        (
            "workflow_engine/recovery.rs",
            include_str!("../workflow_engine/recovery.rs"),
        ),
        (
            "workflow_engine/start.rs",
            include_str!("../workflow_engine/start.rs"),
        ),
    ];
    const REQUIRED: &[&str] = &[
        "input",
        "execution",
        "command_fingerprint_version",
        "command_fingerprint",
        "retention_class_key",
        "retain_rerun_input",
    ];
    let needle = ["INSERT INTO horsies_", "tasks"].concat();
    let mut insert_count = 0;

    for (path, source) in SOURCES {
        for suffix in source.split(&needle).skip(1) {
            insert_count += 1;
            let boundary = [
                suffix.find("VALUES"),
                suffix.find("push_values"),
                suffix.find("SELECT"),
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or_else(|| panic!("{path}: task insert has no values boundary"));
            let columns = &suffix[..boundary];
            let missing: Vec<_> = REQUIRED
                .iter()
                .copied()
                .filter(|column| !columns.contains(column))
                .collect();
            assert!(
                missing.is_empty(),
                "{path}: task insert omits baseline columns {missing:?}"
            );
        }
    }

    assert_eq!(insert_count, 38, "classify every core task insert");
}

#[test]
fn workflow_and_worker_database_identities_stay_uuid_typed() {
    const SOURCES: &[(&str, &str)] = &[
        ("worker/worker.rs", include_str!("../worker/worker.rs")),
        (
            "workflow_engine/engine.rs",
            include_str!("../workflow_engine/engine.rs"),
        ),
        (
            "workflow_engine/lifecycle.rs",
            include_str!("../workflow_engine/lifecycle.rs"),
        ),
        (
            "workflow_engine/start.rs",
            include_str!("../workflow_engine/start.rs"),
        ),
    ];
    const FORBIDDEN: &[&str] = &[
        "mint_task_id()\n                .map(|task_id| task_id.to_string())",
        "UNNEST($1::text[])::uuid",
        "HashSet<String>>",
        ".contains(&row.id.to_string())",
        "let task_id = row.id.to_string()",
    ];

    for (path, source) in SOURCES {
        for forbidden in FORBIDDEN {
            assert!(
                !source.contains(forbidden),
                "{path}: database identity crossed a String adapter: {forbidden}",
            );
        }
    }

    assert!(
        SOURCES[1].1.contains("UNNEST($1::uuid[]) AS tid"),
        "batch workflow promotion must bind its task identities as uuid[]",
    );
    assert!(
        SOURCES[0].1.contains("HashSet<Uuid>>"),
        "in-dispatch task identities must remain UUID-typed",
    );
}
