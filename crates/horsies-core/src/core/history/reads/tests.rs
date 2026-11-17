//! P4 staged-read parity and disposable-database tests.

use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::broker::core_rows::StatementLog;
use crate::broker::error::BrokerError;
use crate::core::history::archive::attempts::{
    encode_attempt_snapshot, AttemptRecord, StoredAttemptSnapshot,
};
use crate::core::history::archive::versions::{archive_digest, ArchiveDecodeError, ArchiveDomain};
use crate::core::history::commands::{
    CreateDailyHistoryLeaf, DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf, LeafBounds, LeafRef,
};
use crate::core::history::ddl::classes::{
    finite_class_parent_name, register_period_class, ClassRegistration,
};
use crate::core::history::ddl::runtime_names::daily_leaf_name;
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::coverage::{ensure_partition_coverage, CoverageOutcome};
use crate::core::history::names::{
    LEAF_CATALOG, TASK_DETAIL_FUNCTION, TASK_HISTORY_PARENT, TASK_RESULT_FUNCTION,
};
use crate::core::history::outcomes::{LeafCreation, LeafDrop};
use crate::core::history::partitions::catalog::database_now;
use crate::core::history::partitions::manager::{
    create_daily_leaf, detach_expired_leaf, drop_detached_leaf,
};
use crate::core::history::partitions::publication::LoaderPublication;
use crate::core::history::periods::RetentionDays;
use crate::db::StatementFailure;

use super::aggregates::plan_rows_from_explain_text;
use super::pages::HistoryWindow;
use super::publisher::{published_manifest_absent_leaves, StagedLoaderPublisher};
use super::result::{
    decode_result_row, HistoryResultPayload, LiveTaskResult, ResultWireRow, TaskResultRead,
};
use super::{detail::read_task_detail, detail::TaskAbsenceReason, detail::TaskDetailResult};

mod ports;
mod reader_locks;

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("fixture timestamp")
        .with_timezone(&Utc)
}

#[test]
fn history_window_is_increasing_and_at_most_thirty_days() {
    let lower = timestamp("2026-08-01T00:00:00Z");
    assert!(HistoryWindow::new(lower, lower + Duration::days(30)).is_ok());
    assert!(HistoryWindow::new(lower, lower + Duration::days(30) + Duration::seconds(1)).is_err());
    assert!(HistoryWindow::new(lower, lower).is_err());
    assert!(HistoryWindow::new(lower + Duration::days(1), lower).is_err());
}

/// The row estimate of an `EXPLAIN (FORMAT JSON)` text is the top plan's
/// `Plan Rows`, an integer or a float cut to an integer; any other text is a
/// contract error.
#[test]
fn plan_rows_from_explain_text_reads_the_top_plan_rows_and_fails_closed() {
    assert_eq!(
        plan_rows_from_explain_text("[{\"Plan\": {\"Plan Rows\": 42}}]").unwrap(),
        42
    );
    assert_eq!(
        plan_rows_from_explain_text("[{\"Plan\":{\"Plan Rows\":7.0}}]").unwrap(),
        7
    );
    for payload in [
        "null",
        "[]",
        "{}",
        "[{\"Plan\": {}}]",
        "[{\"Plan\": {\"Plan Rows\": \"many\"}}]",
        "[{\"Plan\": {\"Plan Rows\": true}}]",
        "not json",
    ] {
        assert!(plan_rows_from_explain_text(payload).is_err(), "{payload}");
    }
}

/// A copy of the installed test template (`crate::test_support`) with a
/// sqlx pool for the history reads that still take a sqlx connection.
struct TestDatabase {
    pool: PgPool,
    database: crate::test_support::TestDatabase,
}

impl TestDatabase {
    async fn create() -> Self {
        let database = crate::test_support::TestDatabase::create().await;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database.url())
            .await
            .expect("connect to P4 test database");
        Self { pool, database }
    }

    async fn drop(self) {
        self.pool.close().await;
        self.database.drop().await;
    }
}

fn utc_day(value: DateTime<Utc>) -> DateTime<Utc> {
    value.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc()
}

fn v7_with_birth(birth: DateTime<Utc>) -> Uuid {
    let milliseconds = birth.timestamp_millis() as u128;
    Uuid::from_u128((milliseconds << 80) | (0x7_u128 << 76) | (0b10_u128 << 62) | 1)
}

async fn register_test_class(
    connection: &mut PgConnection,
    class_key: &str,
    duration_days: i64,
) -> String {
    let days =
        RetentionDays::parse_period(Duration::days(duration_days)).expect("P4 retention period");
    assert_eq!(
        days.class_key(),
        class_key,
        "a class key is derived from its period"
    );
    let outcome = register_period_class(connection, days)
        .await
        .expect("register P4 retention class");
    assert!(matches!(
        outcome,
        ClassRegistration::Registered { .. } | ClassRegistration::AlreadyRegistered { .. }
    ));
    finite_class_parent_name(class_key).expect("finite parent name")
}

async fn create_test_leaf(
    connection: &mut PgConnection,
    parent: &str,
    class_key: &str,
    lower: DateTime<Utc>,
) -> LeafRef {
    let leaf = LeafRef::new(
        daily_leaf_name(parent, lower).expect("daily leaf name"),
        class_key,
        LeafBounds::new(lower, lower + Duration::days(1)).expect("daily leaf bounds"),
    )
    .expect("daily leaf ref");
    let outcome = create_daily_leaf(
        connection,
        &CreateDailyHistoryLeaf::new(leaf.clone()).expect("create daily command"),
        &StagedLoaderPublisher,
    )
    .await
    .expect("create P4 leaf");
    assert!(matches!(
        outcome,
        LeafCreation::Created { .. } | LeafCreation::AlreadyConformant { .. }
    ));
    leaf
}

fn attempt_snapshot(anchor: DateTime<Utc>) -> StoredAttemptSnapshot {
    encode_attempt_snapshot(&[AttemptRecord::new(
        1,
        "FAILED",
        true,
        anchor - Duration::minutes(2),
        anchor - Duration::minutes(1),
        Some("BOOM".to_owned()),
        Some("exploded once".to_owned()),
        Some("TASK_EXCEPTION".to_owned()),
        Some("worker-p4".to_owned()),
        Some("host-p4".to_owned()),
        Some(42),
        Some("process-p4".to_owned()),
    )
    .expect("attempt record")])
    .expect("encode attempt snapshot")
}

async fn seed_history_row(
    connection: &mut PgConnection,
    task_id: Uuid,
    class_key: &str,
    anchor: DateTime<Utc>,
    status: &str,
    kind: &str,
) -> StoredAttemptSnapshot {
    seed_history_row_with_payload(
        connection,
        task_id,
        class_key,
        anchor,
        status,
        kind,
        br#"{"ok":true}"#,
    )
    .await
}

async fn seed_history_row_with_payload(
    connection: &mut PgConnection,
    task_id: Uuid,
    class_key: &str,
    anchor: DateTime<Utc>,
    status: &str,
    kind: &str,
    result_payload: &[u8],
) -> StoredAttemptSnapshot {
    let attempts = attempt_snapshot(anchor);
    let sql = format!(
        "INSERT INTO {TASK_HISTORY_PARENT} (
             task_id, task_name, queue_name, priority,
             command_fingerprint_version, command_fingerprint,
             status, terminalization_kind, terminal_at, retention_class_key,
             sent_at, enqueued_at, started_at, created_at,
             retry_count, max_retries, last_claimed_worker_id,
             last_worker_hostname, last_worker_pid,
             result_envelope_version, result_payload, result_digest,
             error_code, final_failed_reason,
             is_workflow_task, history_schema_version,
             attempt_archive_version, attempt_snapshot,
             attempt_snapshot_digest, rerun_input_disposition
         ) VALUES (
             $1, 'p4.history', 'default', 100,
             1, $2, $3, $4, $5, $6,
             $5, $5, $5, $5, 1, 3, 'worker-p4', 'host-p4', 42,
             1, $7, $8, 'BOOM', 'failed',
             FALSE, 1, $9, $10, $11, 'NEVER_ELIGIBLE'
         )"
    );
    sqlx::query(&sql)
        .bind(task_id)
        .bind(vec![7_u8; 32])
        .bind(status)
        .bind(kind)
        .bind(anchor)
        .bind(class_key)
        .bind(result_payload)
        .bind(archive_digest(result_payload).to_vec())
        .bind(attempts.version)
        .bind(&attempts.payload)
        .bind(attempts.digest.to_vec())
        .execute(connection)
        .await
        .expect("seed P4 history row");
    attempts
}

async fn seed_live_row(
    connection: &mut PgConnection,
    task_id: Uuid,
    class_key: &str,
    anchor: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at,
             command_fingerprint_version, command_fingerprint, idempotency_key_digest,
             status, enqueued_at, retry_count, max_retries, is_workflow_task, updated_at,
             retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
             $1, 'p4.live', 'default', 100, '{}', NULL, 'ISOLATED',
             $4, $2, $2,
             1, $3, NULL,
             'PENDING', $2, 0, 0, FALSE, $2,
             FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(anchor)
    .bind(vec![9_u8; 32])
    .bind(class_key)
    .execute(connection)
    .await
    .expect("seed P4 live row");
}

/// One row of the provenance reader for `task_id`: history holds the id. The
/// reader has no live probe.
async fn provenance_found(connection: &mut PgConnection, task_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM horsies_task_provenance_staged($1))")
        .bind(task_id)
        .fetch_one(connection)
        .await
        .expect("call the provenance reader")
}

/// One publication installs the four readers of
/// `horsies_history_reader_signatures()`, each with one argument, and they
/// resolve history ids of two classes (a birth-pruned id, a fast-clock id, a
/// non-UUIDv7 id), a live id and an unknown id.
#[tokio::test]
#[serial]
async fn publisher_installs_the_four_readers_and_resolves_live_history_and_absent_ids() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let mut transaction = database.pool.begin().await.expect("begin P4 lookup setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let other_class_key = "standard_31d";
    let other_parent = register_test_class(&mut transaction, other_class_key, 31).await;
    let other_leaf =
        create_test_leaf(&mut transaction, &other_parent, other_class_key, today).await;
    let normal_id = v7_with_birth(today + Duration::hours(1));
    let future_id = v7_with_birth(today + Duration::days(365));
    let legacy_id = Uuid::new_v4();
    let other_class_id = v7_with_birth(today + Duration::hours(6));
    seed_history_row(
        &mut transaction,
        normal_id,
        class_key,
        today + Duration::hours(2),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    seed_history_row(
        &mut transaction,
        future_id,
        class_key,
        today + Duration::hours(3),
        "COMPLETED",
        "COMPLETE_FUSED",
    )
    .await;
    seed_history_row(
        &mut transaction,
        legacy_id,
        class_key,
        today + Duration::hours(4),
        "COMPLETED",
        "COMPLETE_LOCKED",
    )
    .await;
    seed_history_row(
        &mut transaction,
        other_class_id,
        other_class_key,
        today + Duration::hours(7),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    let live_id = v7_with_birth(today + Duration::hours(5));
    seed_live_row(&mut transaction, live_id, class_key, now).await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish the readers");
    transaction.commit().await.expect("commit P4 lookup setup");

    let mut connection = database.pool.acquire().await.expect("acquire P4 lookup");
    for task_id in [normal_id, future_id, legacy_id, other_class_id] {
        assert!(
            provenance_found(&mut connection, task_id).await,
            "retained history id {task_id}"
        );
    }
    assert!(!provenance_found(&mut connection, live_id).await);
    assert!(!provenance_found(&mut connection, Uuid::new_v4()).await);
    assert!(matches!(
        read_task_detail(&mut connection, live_id)
            .await
            .expect("read live detail"),
        TaskDetailResult::Live { task_id } if task_id == live_id
    ));
    let TaskDetailResult::History(detail) = read_task_detail(&mut connection, normal_id)
        .await
        .expect("read history detail")
    else {
        panic!("retained task did not decode as history detail");
    };
    assert_eq!(detail.task_id, normal_id);
    assert_eq!(
        detail.status,
        crate::core::types::status::TaskStatus::Failed
    );
    assert_eq!(detail.attempts.len(), 1);
    assert_eq!(detail.attempts[0].outcome(), "FAILED");
    assert!(StagedLoaderPublisher
        .references_leaf(&mut connection, leaf.leaf_name())
        .await
        .expect("published leaf reference"));
    assert!(StagedLoaderPublisher
        .references_leaf(&mut connection, other_leaf.leaf_name())
        .await
        .expect("other-class published leaf reference"));
    let manifest_positions: Vec<(String, i32)> = sqlx::query_as(
        "SELECT leaf_name, probe_position FROM horsies_task_lookup_manifest
         ORDER BY probe_position",
    )
    .fetch_all(&mut *connection)
    .await
    .expect("read published manifest positions");
    assert!(manifest_positions
        .iter()
        .any(|(name, _)| name == leaf.leaf_name()));
    assert!(manifest_positions
        .iter()
        .any(|(name, _)| name == other_leaf.leaf_name()));
    assert!(manifest_positions
        .iter()
        .enumerate()
        .all(|(position, (_, stored))| *stored == position as i32));

    let signatures: Vec<(String, String)> = sqlx::query_as(
        "SELECT procedure.proname,
                pg_get_function_identity_arguments(procedure.oid)
         FROM pg_proc AS procedure
         JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
         WHERE namespace.nspname = current_schema()
           AND procedure.proname = ANY($1::text[])
         ORDER BY procedure.proname, 2",
    )
    .bind(vec![
        "horsies_task_history_duplicate_staged",
        "horsies_task_provenance_staged",
        "horsies_task_detail_staged",
        "horsies_task_result_staged",
    ])
    .fetch_all(&mut *connection)
    .await
    .expect("read reader signatures");
    assert_eq!(
        signatures,
        vec![
            (
                "horsies_task_detail_staged".to_owned(),
                "p_task_id uuid".to_owned(),
            ),
            (
                "horsies_task_history_duplicate_staged".to_owned(),
                "p_task_id uuid".to_owned(),
            ),
            (
                "horsies_task_provenance_staged".to_owned(),
                "p_task_id uuid".to_owned(),
            ),
            (
                "horsies_task_result_staged".to_owned(),
                "p_task_id uuid".to_owned(),
            ),
        ]
    );
    drop(connection);
    database.drop().await;
}

/// Whether the detail reader `horsies_task_detail_staged(uuid)` exists.
async fn detail_reader_exists(connection: &mut PgConnection) -> bool {
    sqlx::query_scalar("SELECT to_regprocedure($1) IS NOT NULL")
        .bind(format!("{TASK_DETAIL_FUNCTION}(uuid)"))
        .fetch_one(connection)
        .await
        .expect("look up the detail reader")
}

#[tokio::test]
#[serial]
async fn coverage_republishes_the_triple_when_only_the_detail_function_is_missing() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin initial coverage");
    let initial = ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .expect("establish steady coverage");
    assert!(matches!(initial, CoverageOutcome::Ensured(_)));
    transaction.commit().await.expect("commit initial coverage");

    sqlx::query("DROP FUNCTION horsies_task_detail_staged(uuid)")
        .execute(&database.pool)
        .await
        .expect("drop only staged detail function");
    let mut transaction = database.pool.begin().await.expect("begin healing coverage");
    assert!(!detail_reader_exists(&mut transaction).await);
    assert!(published_manifest_absent_leaves(&mut transaction)
        .await
        .expect("healthy manifest probe")
        .is_empty());
    let healed = ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .expect("heal missing staged detail");
    let CoverageOutcome::Ensured(healed) = healed else {
        panic!("missing detail function was not healed by coverage");
    };
    assert_eq!(healed.created_history_leaves, 0);
    assert_eq!(healed.created_heartbeat_leaves, 0);
    assert!(healed.republished);
    assert!(healed.absent_leaves.is_empty());
    assert!(detail_reader_exists(&mut transaction).await);
    transaction.commit().await.expect("commit healing coverage");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn missing_relation_is_detected_excluded_reported_and_not_misclassified_as_ageing() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let mut transaction = database.pool.begin().await.expect("begin missing setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let task_id = v7_with_birth(today + Duration::hours(2));
    seed_history_row(
        &mut transaction,
        task_id,
        class_key,
        today + Duration::hours(3),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish before missing relation");
    transaction.commit().await.expect("commit missing setup");

    sqlx::query(&format!("DROP TABLE {}", leaf.leaf_name()))
        .execute(&database.pool)
        .await
        .expect("drop leaf out of band");
    let mut connection = database
        .pool
        .acquire()
        .await
        .expect("acquire missing probe");
    let broken = read_task_detail(&mut connection, task_id)
        .await
        .expect_err("published missing leaf must break execution");
    assert!(matches!(
        broken,
        HistoryError::Database(sqlx::Error::Database(ref error)) if error.code().as_deref() == Some("42P01")
    ));
    assert_eq!(
        published_manifest_absent_leaves(&mut connection)
            .await
            .expect("published absent leaves"),
        vec![leaf.leaf_name().to_owned()]
    );
    let republished = StagedLoaderPublisher
        .republish(&mut connection)
        .await
        .expect("heal staged readers");
    assert_eq!(republished.absent_leaves, vec![leaf.leaf_name().to_owned()]);
    assert!(published_manifest_absent_leaves(&mut connection)
        .await
        .expect("healed manifest divergence")
        .is_empty());
    assert!(!StagedLoaderPublisher
        .references_leaf(&mut connection, leaf.leaf_name())
        .await
        .expect("excluded manifest reference"));
    assert!(matches!(
        read_task_detail(&mut connection, task_id)
            .await
            .expect("classify destroyed-leaf detail"),
        TaskDetailResult::Absent {
            task_id: found,
            reason: TaskAbsenceReason::WithinRetainedHistory,
        } if found == task_id
    ));
    let catalog_still_attached: bool = sqlx::query_scalar(&format!(
        "SELECT detached_at IS NULL AND dropped_at IS NULL FROM {LEAF_CATALOG}
         WHERE leaf_name = $1"
    ))
    .bind(leaf.leaf_name())
    .fetch_one(&mut *connection)
    .await
    .expect("read missing leaf catalog evidence");
    assert!(catalog_still_attached);
    drop(connection);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn detail_refuses_a_stored_status_that_is_not_a_task_status() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let mut transaction = database.pool.begin().await.expect("begin status setup");
    let today = utc_day(database_now(&mut transaction).await.expect("database now"));
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let _leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let task_id = v7_with_birth(today + Duration::hours(1));
    let _snapshot = seed_history_row(
        &mut transaction,
        task_id,
        class_key,
        today + Duration::hours(2),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish status setup");
    sqlx::query(&format!(
        "UPDATE {TASK_HISTORY_PARENT} SET status = 'BOGUS' WHERE task_id = $1"
    ))
    .bind(task_id)
    .execute(&mut *transaction)
    .await
    .expect("store a status that is not a task status");
    assert!(matches!(
        read_task_detail(&mut transaction, task_id)
            .await
            .expect_err("an unknown status must refuse detail"),
        HistoryError::Contract(ref detail) if detail.contains("'BOGUS'")
    ));
    transaction
        .rollback()
        .await
        .expect("roll back status setup");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn detail_refuses_attempt_digest_corruption_and_purged_floor_is_typed() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let mut transaction = database.pool.begin().await.expect("begin purged setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let old_day = today - Duration::days(40);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let old_leaf = create_test_leaf(&mut transaction, &parent, class_key, old_day).await;
    let _current_leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let old_id = v7_with_birth(old_day + Duration::hours(1));
    let current_id = v7_with_birth(today + Duration::hours(1));
    let old_snapshot = seed_history_row(
        &mut transaction,
        old_id,
        class_key,
        old_day + Duration::hours(2),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    seed_history_row(
        &mut transaction,
        current_id,
        class_key,
        today + Duration::hours(2),
        "COMPLETED",
        "COMPLETE_FUSED",
    )
    .await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish purged setup");
    assert!(matches!(
        read_task_detail(&mut transaction, old_id)
            .await
            .expect("read old detail"),
        TaskDetailResult::History(_)
    ));
    sqlx::query(&format!(
        "UPDATE {TASK_HISTORY_PARENT} SET attempt_snapshot_digest = $1 WHERE task_id = $2"
    ))
    .bind(vec![0_u8; 32])
    .bind(old_id)
    .execute(&mut *transaction)
    .await
    .expect("corrupt attempt snapshot digest");
    assert!(matches!(
        read_task_detail(&mut transaction, old_id)
            .await
            .expect_err("digest corruption must refuse detail"),
        HistoryError::ArchiveDecode(ArchiveDecodeError::DigestMismatch {
            domain: ArchiveDomain::Attempts
        })
    ));
    sqlx::query(&format!(
        "UPDATE {TASK_HISTORY_PARENT} SET attempt_snapshot_digest = $1 WHERE task_id = $2"
    ))
    .bind(old_snapshot.digest.to_vec())
    .bind(old_id)
    .execute(&mut *transaction)
    .await
    .expect("restore attempt digest");
    transaction.commit().await.expect("commit purged setup");

    detach_expired_leaf(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        StatementLog::Off,
        &DetachExpiredHistoryLeaf::new(old_leaf.clone(), Some(5_000)).expect("detach command"),
    )
    .await
    .expect("detach old P4 leaf");
    let mut transaction = database.pool.begin().await.expect("begin old leaf drop");
    assert!(matches!(
        drop_detached_leaf(
            &mut transaction,
            &DropDetachedHistoryLeaf::new(old_leaf.clone()),
        )
        .await
        .expect("drop old P4 leaf"),
        LeafDrop::Dropped { .. }
    ));
    assert!(matches!(
        read_task_detail(&mut transaction, old_id)
            .await
            .expect("classify purged detail"),
        TaskDetailResult::Absent {
            task_id,
            reason: TaskAbsenceReason::PredatesRetainedHistory,
        } if task_id == old_id
    ));
    transaction.commit().await.expect("commit old leaf drop");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn duplicate_guard_finds_a_recorded_clock_exception() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin exception setup");
    let coverage = ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .expect("publish the readers");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    let now = database_now(&mut transaction).await.expect("database now");
    // Born an hour after its terminal time: a client clock ahead of the
    // database. The move records such an id; the guard finds it there.
    let recorded = v7_with_birth(now + Duration::hours(1));
    let unrecorded = v7_with_birth(now + Duration::hours(2));
    sqlx::query(
        "INSERT INTO horsies_task_history_clock_exceptions (
             task_id, retention_class_key, terminal_at
         ) VALUES ($1, 'standard_30d', $2)",
    )
    .bind(recorded)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .expect("record a clock exception");
    assert!(duplicate_guard(&mut transaction, recorded).await);
    assert!(!duplicate_guard(&mut transaction, unrecorded).await);
    transaction
        .rollback()
        .await
        .expect("rollback exception setup");
    database.drop().await;
}

/// The stamp comment of every published reader, in signature order.
async fn reader_stamps(connection: &mut PgConnection) -> Vec<Option<String>> {
    let mut stamps = Vec::new();
    for signature in super::publisher::rendered_reader_signatures() {
        let stamp: Option<String> =
            sqlx::query_scalar("SELECT obj_description(to_regprocedure($1), 'pg_proc')")
                .bind(&signature)
                .fetch_one(&mut *connection)
                .await
                .expect("read reader stamp");
        stamps.push(stamp);
    }
    stamps
}

/// The publication stamps each of the four readers with
/// `horsies_history_reader_stamp()`, which does not depend on the session
/// time zone; the publisher reads them as current, and one reader with
/// another stamp makes them need a publication.
#[tokio::test]
#[serial]
async fn publication_stamps_every_rendered_reader_independent_of_the_time_zone() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin stamp setup");
    let coverage = ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .expect("publish the readers");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    let stamps = reader_stamps(&mut transaction).await;
    let expected: String = sqlx::query_scalar("SELECT horsies_history_reader_stamp()")
        .fetch_one(&mut *transaction)
        .await
        .expect("compute manifest stamp");
    assert!(expected.starts_with("horsies:manifest:"));
    assert_eq!(stamps, vec![Some(expected.clone()); 4]);
    for timezone in ["Etc/GMT+12", "Etc/GMT-12"] {
        sqlx::query("SELECT set_config('timezone', $1, true)")
            .bind(timezone)
            .execute(&mut *transaction)
            .await
            .expect("set session time zone");
        let stamp: String = sqlx::query_scalar("SELECT horsies_history_reader_stamp()")
            .fetch_one(&mut *transaction)
            .await
            .expect("compute manifest stamp");
        assert_eq!(stamp, expected, "{timezone}");
    }
    assert!(!StagedLoaderPublisher
        .needs_republication(&mut transaction)
        .await
        .expect("stamped readers are current"));
    // A reader without the stamp of the published manifest needs a
    // republication, whichever reader it is.
    sqlx::query("COMMENT ON FUNCTION horsies_task_detail_staged(uuid) IS 'stale'")
        .execute(&mut *transaction)
        .await
        .expect("stale detail stamp");
    assert!(StagedLoaderPublisher
        .needs_republication(&mut transaction)
        .await
        .expect("stale stamp probe"));
    transaction.rollback().await.expect("rollback stamp setup");
    database.drop().await;
}

async fn duplicate_guard(connection: &mut PgConnection, task_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT horsies_task_history_duplicate_staged($1)")
        .bind(task_id)
        .fetch_one(connection)
        .await
        .expect("call duplicate guard")
}

async fn guard_marker(connection: &mut PgConnection) -> Option<String> {
    sqlx::query_scalar(
        "SELECT obj_description(to_regprocedure('horsies_task_history_duplicate_staged(uuid)'), 'pg_proc')",
    )
    .fetch_one(connection)
    .await
    .expect("read duplicate guard marker")
}

/// The published guard probes only the birth-pruned leaves. The baseline
/// defines no guard of its own: migration 1000 publishes the readers before
/// the install returns, so a fresh database already holds a stamped guard,
/// and a guard dropped out of band comes back with the next publication.
#[tokio::test]
#[serial]
async fn duplicate_guard_probes_the_pruned_set_and_a_dropped_guard_is_published_again() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let mut transaction = database.pool.begin().await.expect("begin guard setup");
    assert!(guard_marker(&mut transaction)
        .await
        .is_some_and(|stamp| stamp.starts_with("horsies:manifest:")));
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let _today_leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let _older_leaf = create_test_leaf(
        &mut transaction,
        &parent,
        class_key,
        today - Duration::days(1),
    )
    .await;
    let normal_id = v7_with_birth(today + Duration::hours(2));
    let legacy_id = Uuid::new_v4();
    // Born today, stored in yesterday's leaf: a wrong client clock.
    let clock_violation_id = v7_with_birth(today + Duration::hours(3));
    let absent_id = v7_with_birth(today + Duration::hours(4));
    for (task_id, anchor) in [
        (normal_id, today + Duration::hours(2)),
        (legacy_id, today + Duration::hours(2)),
        (clock_violation_id, today - Duration::hours(12)),
    ] {
        seed_history_row(
            &mut transaction,
            task_id,
            class_key,
            anchor,
            "FAILED",
            "FAIL_RUNNING",
        )
        .await;
    }
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish staged readers");
    transaction.commit().await.expect("commit guard setup");

    let mut connection = database.pool.acquire().await.expect("acquire guard reads");
    assert!(guard_marker(&mut connection)
        .await
        .is_some_and(|stamp| stamp.starts_with("horsies:manifest:")));
    assert!(duplicate_guard(&mut connection, normal_id).await);
    assert!(duplicate_guard(&mut connection, legacy_id).await);
    assert!(!duplicate_guard(&mut connection, absent_id).await);
    // The stated loss: the guard skips the older leaf; the reader still finds the row.
    assert!(!duplicate_guard(&mut connection, clock_violation_id).await);
    assert!(provenance_found(&mut connection, clock_violation_id).await);

    assert!(!StagedLoaderPublisher
        .needs_republication(&mut connection)
        .await
        .expect("republication probe after publish"));
    sqlx::query("DROP FUNCTION horsies_task_history_duplicate_staged(uuid)")
        .execute(&mut *connection)
        .await
        .expect("drop rendered guard");
    assert!(!crate::broker::migrations::BASELINE_INTERIM_FUNCTIONS_SQL
        .contains("FUNCTION horsies_task_history_duplicate_staged("));
    assert_eq!(guard_marker(&mut connection).await, None);
    assert!(StagedLoaderPublisher
        .needs_republication(&mut connection)
        .await
        .expect("republication probe with the guard dropped"));
    StagedLoaderPublisher
        .republish(&mut connection)
        .await
        .expect("publish the guard again");
    assert!(guard_marker(&mut connection)
        .await
        .is_some_and(|stamp| stamp.starts_with("horsies:manifest:")));
    assert!(!duplicate_guard(&mut connection, clock_violation_id).await);
    drop(connection);
    database.drop().await;
}

fn result_wire_row(location: &str) -> ResultWireRow {
    ResultWireRow {
        location: location.to_owned(),
        status: Some("COMPLETED".to_owned()),
        failed_reason: None,
        result_envelope_version: Some(1),
        result_payload: Some(b"1".to_vec()),
        result_digest: Some(vec![3; 32]),
        task_name: Some("p4.history".to_owned()),
    }
}

#[test]
fn result_wire_decode_is_typed_and_fail_closed() {
    let task_id = Uuid::parse_str("0198c0de-0000-7000-8000-000000000002").unwrap();
    assert_eq!(
        decode_result_row(
            task_id,
            ResultWireRow {
                status: Some("RUNNING".to_owned()),
                failed_reason: None,
                result_envelope_version: None,
                result_payload: None,
                result_digest: None,
                ..result_wire_row("LIVE")
            }
        )
        .unwrap(),
        TaskResultRead::Live(LiveTaskResult {
            task_name: "p4.history".to_owned(),
            status: "RUNNING".to_owned(),
        })
    );
    let TaskResultRead::History(stored) =
        decode_result_row(task_id, result_wire_row("HISTORY")).unwrap()
    else {
        panic!("history row did not decode as history");
    };
    assert_eq!(stored.task_name, "p4.history");
    assert_eq!(
        stored.payload,
        HistoryResultPayload::Stored {
            payload: b"1".to_vec(),
            digest: vec![3; 32],
        }
    );
    let TaskResultRead::History(prior_only) = decode_result_row(
        task_id,
        ResultWireRow {
            result_payload: None,
            ..result_wire_row("HISTORY")
        },
    )
    .unwrap() else {
        panic!("history row without payload did not decode as history");
    };
    assert_eq!(prior_only.payload, HistoryResultPayload::Absent);
    for broken in [
        result_wire_row("QUARANTINE"),
        ResultWireRow {
            status: None,
            ..result_wire_row("LIVE")
        },
        ResultWireRow {
            result_envelope_version: None,
            ..result_wire_row("HISTORY")
        },
        ResultWireRow {
            result_digest: None,
            ..result_wire_row("HISTORY")
        },
        ResultWireRow {
            task_name: None,
            ..result_wire_row("HISTORY")
        },
        ResultWireRow {
            task_name: None,
            ..result_wire_row("LIVE")
        },
    ] {
        assert!(matches!(
            decode_result_row(task_id, broken),
            Err(HistoryError::Contract(_))
        ));
    }
}

/// Reads one task's result with the broker's statement-layer reader
/// (`broker::history_reads::read_task_result`) on a core handle of the test
/// database, then closes the handle. Each call opens new connections and
/// starts from a reader state that is not seen, as the first call of a new
/// broker does, so no statement prepared before a reader change is reused.
async fn core_result_read(
    database: &TestDatabase,
    task_id: Uuid,
) -> Result<TaskResultRead, BrokerError> {
    let handle = crate::test_support::core_handle(database.database.url());
    let open = handle
        .pools()
        .expect("the statement layer accepts the test URL");
    let read = crate::broker::history_reads::read_task_result(
        open.main(),
        StatementLog::Off,
        &crate::broker::history_reads::ResultReaderSeen::default(),
        task_id,
    )
    .await;
    drop(open);
    let _closed = handle.close().await;
    read
}

/// The `ok` value of the stored result envelope of `record`.
fn stored_ok(record: &crate::broker::RawResultRecord) -> serde_json::Value {
    let envelope: serde_json::Value = serde_json::from_str(
        record
            .raw_result
            .as_ref()
            .expect("the record stores a result")
            .as_str(),
    )
    .expect("the stored envelope is JSON");
    envelope["ok"].clone()
}

#[tokio::test]
#[serial]
async fn result_absence_from_a_stale_reader_is_confirmed_by_detail() {
    use crate::broker::postgres::PostgresBroker;
    use crate::core::TaskResult;

    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin stale reader setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let _leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let completed_id = v7_with_birth(today + Duration::hours(2));
    let completed_json =
        serde_json::to_string(&TaskResult::<i32>::Ok(7)).expect("encode completed result");
    seed_history_row_with_payload(
        &mut transaction,
        completed_id,
        class_key,
        today + Duration::hours(3),
        "COMPLETED",
        "COMPLETE_FUSED",
        completed_json.as_bytes(),
    )
    .await;
    // A result reader of a publication that ran before the leaf existed: it
    // probes the live table and no leaf (Python `_STALE_RESULT_READER_SQL`,
    // `tests/integration/test_task_result_history_fallback.py`).
    sqlx::query(
        "CREATE OR REPLACE FUNCTION horsies_task_result_staged(p_task_id uuid)
         RETURNS TABLE(location text, status text, failed_reason text,
                       result_envelope_version smallint, result_payload bytea,
                       result_digest bytea, task_name text)
         LANGUAGE sql STABLE STRICT AS $$
             SELECT 'LIVE'::text, t.status::text, NULL::text, NULL::smallint, NULL::bytea,
                 NULL::bytea, t.task_name::text
             FROM horsies_tasks AS t WHERE t.id = p_task_id
         $$",
    )
    .execute(&mut *transaction)
    .await
    .expect("install a stale result reader");
    transaction
        .commit()
        .await
        .expect("commit stale reader setup");

    assert_eq!(
        core_result_read(&database, completed_id)
            .await
            .expect("stale result read"),
        TaskResultRead::Absent
    );
    let broker = PostgresBroker::from_pool(database.pool.clone());
    let confirmed = broker
        .get_raw_result_record(completed_id, None)
        .await
        .expect("the detail reader confirms the history row")
        .expect_found();
    assert_eq!(stored_ok(&confirmed), serde_json::json!(7));
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn result_reader_resolves_each_location_and_falls_back_while_unpublished() {
    use crate::broker::postgres::PostgresBroker;
    use crate::core::TaskResult;

    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let mut transaction = database.pool.begin().await.expect("begin result setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let _leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let failed_id = v7_with_birth(today + Duration::hours(2));
    let completed_id = Uuid::from_u128(v7_with_birth(today + Duration::hours(2)).as_u128() + 1);
    let live_id = v7_with_birth(today + Duration::hours(4));
    let completed_json =
        serde_json::to_string(&TaskResult::<i32>::Ok(7)).expect("encode completed result");
    seed_history_row(
        &mut transaction,
        failed_id,
        class_key,
        today + Duration::hours(3),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    seed_history_row_with_payload(
        &mut transaction,
        completed_id,
        class_key,
        today + Duration::hours(3),
        "COMPLETED",
        "COMPLETE_FUSED",
        completed_json.as_bytes(),
    )
    .await;
    seed_live_row(&mut transaction, live_id, class_key, now).await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish staged readers");
    transaction.commit().await.expect("commit result setup");

    let mut connection = database.pool.acquire().await.expect("acquire result reads");
    assert_eq!(
        core_result_read(&database, live_id)
            .await
            .expect("read live result"),
        TaskResultRead::Live(LiveTaskResult {
            task_name: "p4.live".to_owned(),
            status: "PENDING".to_owned(),
        })
    );
    let TaskResultRead::History(failed) = core_result_read(&database, failed_id)
        .await
        .expect("read history result")
    else {
        panic!("retained task did not read as history");
    };
    assert_eq!(failed.task_name, "p4.history");
    assert_eq!(failed.status, "FAILED");
    assert_eq!(failed.failed_reason.as_deref(), Some("failed"));
    assert_eq!(failed.envelope_version, 1);
    assert_eq!(failed.codec, "json-utf8");
    assert_eq!(failed.content_type, "application/json");
    assert_eq!(
        failed.payload,
        HistoryResultPayload::Stored {
            payload: br#"{"ok":true}"#.to_vec(),
            digest: archive_digest(br#"{"ok":true}"#).to_vec(),
        }
    );
    for absent_id in [v7_with_birth(today + Duration::hours(5)), Uuid::new_v4()] {
        assert_eq!(
            core_result_read(&database, absent_id)
                .await
                .expect("read absent result"),
            TaskResultRead::Absent
        );
    }

    let broker = PostgresBroker::from_pool(database.pool.clone());
    let staged = broker
        .get_raw_result_record(completed_id, None)
        .await
        .expect("staged history result read")
        .expect_found();
    assert_eq!(stored_ok(&staged), serde_json::json!(7));

    sqlx::query("DROP FUNCTION horsies_task_result_staged(uuid)")
        .execute(&mut *connection)
        .await
        .expect("drop only staged result function");
    assert_eq!(
        core_result_read(&database, failed_id)
            .await
            .expect("read with unpublished result function"),
        TaskResultRead::NotPublished
    );
    // The broker saw the function, so its first call after the drop fails
    // once with the refusal that names the function (Python's rule); the
    // next call falls back to the live and detail reads.
    let refused = broker
        .get_raw_result_record(completed_id, None)
        .await
        .expect_err("the first broker call after the drop fails");
    assert_eq!(
        refused.code,
        crate::broker::BrokerErrorCode::TaskInfoQueryFailed,
        "{refused:?}"
    );
    assert!(refused.message.contains(TASK_RESULT_FUNCTION), "{refused}");
    let fallback = broker
        .get_raw_result_record(completed_id, None)
        .await
        .expect("fallback history result read")
        .expect_found();
    assert_eq!(stored_ok(&fallback), serde_json::json!(7));

    assert!(StagedLoaderPublisher
        .needs_republication(&mut connection)
        .await
        .expect("republication probe"));
    StagedLoaderPublisher
        .republish(&mut connection)
        .await
        .expect("republish staged readers");
    assert!(matches!(
        core_result_read(&database, failed_id)
            .await
            .expect("read with the republished result function"),
        TaskResultRead::History(_)
    ));

    sqlx::query(
        "CREATE OR REPLACE FUNCTION horsies_task_result_staged(p_task_id uuid)
         RETURNS TABLE (location text, status text, failed_reason text,
                        result_envelope_version smallint, result_payload bytea,
                        result_digest bytea, task_name text)
         LANGUAGE plpgsql STABLE AS
         $$ BEGIN PERFORM core_p4_function_that_does_not_exist(); END $$",
    )
    .execute(&mut *connection)
    .await
    .expect("plant result function with an undefined inner call");
    assert!(matches!(
        core_result_read(&database, failed_id).await,
        Err(BrokerError::Statement(StatementFailure::StatementRefused { ref cause, .. }))
            if cause.server_error().code().code() == "42883"
    ));
    drop(connection);
    database.drop().await;
}
