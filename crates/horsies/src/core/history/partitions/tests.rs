use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{Duration, Timelike, Utc};
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use crate::broker::migrations::run_horsies_migrations;
use crate::core::history::commands::{
    CollectPartitionHealth, CreateDailyHistoryLeaf, DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf, EnsureLeafCoverage, InspectHistoryLeaf, LeafBounds, LeafRef,
};
use crate::core::history::ddl::classes::{
    finite_class_parent_name, register_finite_retention_class, ClassRegistration,
};
use crate::core::history::ddl::runtime_names::{
    daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name,
};
use crate::core::history::heartbeats::partitioning::{
    create_hourly_heartbeat_leaf, ensure_heartbeat_coverage, heartbeat_horizon, hourly_leaf_name,
    hourly_leaf_ref, probe_index_name, register_heartbeat_class, sweep_expired_heartbeat_leaves,
    CreateHourlyHeartbeatLeaf, EnsureHeartbeatCoverage, HeartbeatClassRegistration,
};
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage, ensure_startup_coverage, CoverageOutcome, StartupCoverageOutcome,
};
use crate::core::history::maintenance::gate::{
    active_maintenance_session, begin_archive_maintenance, finish_archive_maintenance,
    MaintenanceSessionError, ARCHIVE_AVAILABILITY_FUNCTION,
};
use crate::core::history::maintenance::pruning::prune_expired_partitions;
use crate::core::history::names::{
    LEAF_CATALOG, RETENTION_CLASSES, TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT,
};
use crate::core::history::outcomes::{HealthFault, LeafCreation, LeafDrop, LeafInspection};

use super::catalog::{
    capture_partition_bound_utc, database_now, read_attached_birth_floor, read_leaf_catalog_row,
    read_leaf_physical_state, read_manifest_leaf_rows,
};
use super::forever::{ensure_forever_range_partitioning, FOREVER_LEGACY_LEAF};
use super::health::collect_partition_health;
use super::manager::{
    create_daily_leaf, detach_expired_leaf, drop_detached_leaf, ensure_leaf_coverage, inspect_leaf,
    DetachExpiredLeafOutcome, LeafBlockerQuarantine, NoQuarantine, QuarantineRefusalVerdict,
    QuarantineRefused, QuarantineResult, TaskQuarantineRefusal,
};
use super::publication::{LoaderPublication, LoaderRepublished, UnpublishedLoader};

#[derive(Debug, Default)]
struct CatalogPublisher;

impl LoaderPublication for CatalogPublisher {
    async fn republish(
        &self,
        connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        let selection = read_manifest_leaf_rows(connection).await?;
        Ok(LoaderRepublished {
            absent_leaves: selection.absent_relations,
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(false)
    }

    async fn needs_republication(
        &self,
        connection: &mut PgConnection,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(!read_manifest_leaf_rows(connection)
            .await?
            .absent_relations
            .is_empty())
    }
}

#[derive(Debug, Default)]
struct ReferencingPublisher;

impl LoaderPublication for ReferencingPublisher {
    async fn republish(
        &self,
        _connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        Ok(LoaderRepublished {
            absent_leaves: Vec::new(),
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(true)
    }
}

#[derive(Debug, Default)]
struct FailFirstRepublish {
    calls: AtomicUsize,
    reference_calls: AtomicUsize,
}

impl LoaderPublication for FailFirstRepublish {
    async fn republish(
        &self,
        _connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(crate::core::history::errors::HistoryError::contract(
                "injected first-leaf publication failure",
            ));
        }
        Ok(LoaderRepublished {
            absent_leaves: Vec::new(),
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(self.reference_calls.fetch_add(1, Ordering::SeqCst) == 0)
    }
}

#[derive(Debug, Clone)]
struct RefusingQuarantine {
    task_id: Uuid,
}

impl LeafBlockerQuarantine for RefusingQuarantine {
    async fn quarantine(
        &self,
        _connection: &mut PgConnection,
        leaf: &LeafRef,
        _horizon: Duration,
    ) -> Result<QuarantineResult, crate::core::history::errors::HistoryError> {
        Ok(QuarantineResult::Refused(QuarantineRefused {
            leaf_name: leaf.leaf_name().to_owned(),
            repointed: 0,
            refusals: vec![TaskQuarantineRefusal {
                task_id: self.task_id,
                verdict: QuarantineRefusalVerdict::SourceAbsent,
                detail: Some("history row absent at locator".to_owned()),
            }],
        }))
    }
}

struct TestDatabase {
    pool: PgPool,
    database_name: String,
    admin_options: PgConnectOptions,
}

impl TestDatabase {
    async fn create() -> Self {
        Self::create_with_connections(1).await
    }

    async fn create_with_connections(max_connections: u32) -> Self {
        let base = database_url();
        let base_options = PgConnectOptions::from_str(&base).expect("invalid test database URL");
        let admin_options = base_options.clone().database("postgres");
        let database_name = format!("horsies_p3_{}", Uuid::new_v4().simple());
        let mut admin = PgConnection::connect_with(&admin_options)
            .await
            .expect("connect to postgres admin database");
        sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
            .execute(&mut admin)
            .await
            .expect("create P3 test database");
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(base_options.database(&database_name))
            .await
            .expect("connect to P3 test database");
        run_horsies_migrations(&pool)
            .await
            .expect("migrate P3 test database");
        Self {
            pool,
            database_name,
            admin_options,
        }
    }

    async fn drop(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.admin_options)
            .await
            .expect("connect for P3 database cleanup");
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = $1 AND pid <> pg_backend_pid()",
        )
        .bind(&self.database_name)
        .execute(&mut admin)
        .await
        .expect("terminate P3 test database sessions");
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\"",
            self.database_name
        ))
        .execute(&mut admin)
        .await
        .expect("drop P3 test database");
    }
}

fn database_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let contents = std::fs::read_to_string(root.join(".env")).expect("read workspace .env");
    let password = contents
        .lines()
        .filter_map(|line| line.split_once('='))
        .find_map(|(key, value)| (key.trim() == "DB_PASSWORD").then(|| value.trim()))
        .expect("DB_PASSWORD in workspace .env");
    format!("postgresql://postgres:{password}@localhost:5432/horsies-rust-port")
}

async fn register_class(pool: &PgPool, class_key: &str, days: i64) -> String {
    let mut transaction = pool.begin().await.expect("begin class registration");
    let outcome =
        register_finite_retention_class(&mut transaction, class_key, Duration::days(days))
            .await
            .expect("register finite class");
    assert!(matches!(
        outcome,
        ClassRegistration::Registered { .. } | ClassRegistration::AlreadyRegistered { .. }
    ));
    transaction
        .commit()
        .await
        .expect("commit class registration");
    finite_class_parent_name(class_key).expect("finite parent name")
}

fn leaf_ref(parent: &str, class_key: &str, lower: chrono::DateTime<Utc>) -> LeafRef {
    LeafRef::new(
        daily_leaf_name(parent, lower).expect("daily leaf name"),
        class_key,
        LeafBounds::new(lower, lower + Duration::days(1)).expect("daily bounds"),
    )
    .expect("daily leaf ref")
}

async fn create_leaf(pool: &PgPool, leaf: &LeafRef) -> LeafCreation {
    let mut transaction = pool.begin().await.expect("begin leaf creation");
    let outcome = create_daily_leaf(
        &mut transaction,
        &CreateDailyHistoryLeaf::new(leaf.clone()).expect("daily command"),
        &UnpublishedLoader,
    )
    .await
    .expect("create leaf");
    transaction.commit().await.expect("commit leaf creation");
    outcome
}

#[test]
fn heartbeat_derivation_and_command_contracts_match_the_authority() {
    assert_eq!(
        heartbeat_horizon(Duration::minutes(10), Duration::minutes(45), 4),
        Ok(Duration::hours(3))
    );
    assert_eq!(
        heartbeat_horizon(Duration::seconds(30), Duration::seconds(45), 2),
        Ok(Duration::hours(1))
    );
    assert!(heartbeat_horizon(Duration::zero(), Duration::minutes(1), 2).is_err());
    assert!(heartbeat_horizon(Duration::minutes(1), Duration::minutes(1), 0).is_err());
    assert!(EnsureHeartbeatCoverage::new(1).is_err());
    assert!(EnsureHeartbeatCoverage::new(2).is_ok());
    let hour = chrono::DateTime::parse_from_rfc3339("2026-08-07T13:00:00Z")
        .expect("fixed heartbeat hour")
        .with_timezone(&Utc);
    assert_eq!(
        hourly_leaf_name(hour).expect("hourly leaf name"),
        "horsies_heartbeats_2026_08_07_13"
    );
    assert_eq!(
        probe_index_name("horsies_heartbeats_2026_08_07_13").expect("heartbeat probe index name"),
        "horsies_heartbeats_2026_08_07_13_probe_idx"
    );
    assert!(probe_index_name(&format!("horsies_heartbeats_{}", "x".repeat(50))).is_err());
}

#[tokio::test]
#[serial]
async fn lifecycle_is_idempotent_repairs_index_property_and_is_timezone_independent() {
    let database = TestDatabase::create().await;
    let class_key = "p3_lifecycle_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let mut transaction = database.pool.begin().await.expect("begin coverage");
    sqlx::query("SELECT set_config('timezone', 'Etc/GMT+12', false)")
        .execute(&mut *transaction)
        .await
        .expect("set creating timezone");
    let command = EnsureLeafCoverage::new(class_key, 3).expect("coverage command");
    let first = ensure_leaf_coverage(&mut transaction, &command, &UnpublishedLoader)
        .await
        .expect("first coverage");
    assert_eq!(first.len(), 4);
    assert!(first
        .iter()
        .all(|item| matches!(item, LeafCreation::Created { .. })));
    transaction.commit().await.expect("commit coverage");

    let mut transaction = database.pool.begin().await.expect("begin verification");
    sqlx::query("SELECT set_config('timezone', 'Etc/GMT-12', false)")
        .execute(&mut *transaction)
        .await
        .expect("set inspecting timezone");
    let second = ensure_leaf_coverage(&mut transaction, &command, &UnpublishedLoader)
        .await
        .expect("second coverage");
    assert!(second
        .iter()
        .all(|item| matches!(item, LeafCreation::AlreadyConformant { .. })));
    let now = database_now(&mut transaction).await.expect("database now");
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate today");
    let leaf = leaf_ref(&parent, class_key, today);
    let cataloged_bound = read_leaf_catalog_row(&mut transaction, leaf.leaf_name())
        .await
        .expect("read cataloged leaf")
        .expect("cataloged current leaf")
        .partition_bound;
    for timezone in ["UTC", "Etc/GMT+12", "Etc/GMT-12"] {
        sqlx::query("SELECT set_config('timezone', $1, false)")
            .bind(timezone)
            .execute(&mut *transaction)
            .await
            .expect("set bound probe timezone");
        assert_eq!(
            capture_partition_bound_utc(&mut transaction, leaf.leaf_name())
                .await
                .expect("capture UTC partition bound")
                .as_deref(),
            Some(cataloged_bound.as_str())
        );
    }
    let id_index = leaf_id_index_name(leaf.leaf_name());
    sqlx::query(&format!("DROP INDEX {id_index}"))
        .execute(&mut *transaction)
        .await
        .expect("drop task-ID index");
    assert!(matches!(
        create_daily_leaf(
            &mut transaction,
            &CreateDailyHistoryLeaf::new(leaf.clone()).expect("repair command"),
            &UnpublishedLoader,
        )
        .await
        .expect("repair task-ID index"),
        LeafCreation::IndexRepaired { .. }
    ));
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET partition_bound = 'wrong bound' WHERE leaf_name = $1"
    ))
    .bind(leaf.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("corrupt cataloged partition bound");
    assert!(matches!(
        create_daily_leaf(
            &mut transaction,
            &CreateDailyHistoryLeaf::new(leaf.clone()).expect("conflict command"),
            &UnpublishedLoader,
        )
        .await
        .expect("classify physical mismatch"),
        LeafCreation::CatalogConflict {
            kind: crate::core::history::outcomes::CatalogConflictKind::PhysicalNonconformant,
            ..
        }
    ));
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET partition_bound = $1 WHERE leaf_name = $2"
    ))
    .bind(&cataloged_bound)
    .bind(leaf.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("restore cataloged partition bound");
    let canonical_order = leaf_enqueued_index_name(leaf.leaf_name());
    sqlx::query(&format!("DROP INDEX {canonical_order}"))
        .execute(&mut *transaction)
        .await
        .expect("drop canonical ordering index");
    sqlx::query(&format!(
        "CREATE INDEX p3_property_named_index ON {} (enqueued_at)",
        leaf.leaf_name()
    ))
    .execute(&mut *transaction)
    .await
    .expect("create property-equivalent index");
    let verified = create_daily_leaf(
        &mut transaction,
        &CreateDailyHistoryLeaf::new(leaf).expect("daily command"),
        &UnpublishedLoader,
    )
    .await
    .expect("verify property index");
    assert!(matches!(verified, LeafCreation::AlreadyConformant { .. }));
    let report = collect_partition_health(
        &mut transaction,
        &CollectPartitionHealth::new(class_key, true).expect("health command"),
    )
    .await
    .expect("collect health");
    assert!(report.is_healthy(), "health faults: {:?}", report.faults);
    assert!(
        report
            .coverage
            .expect("class coverage")
            .complete_future_intervals
            >= 2
    );
    let squatter = leaf_ref(&parent, class_key, today + Duration::days(10));
    sqlx::query(&format!(
        "CREATE TABLE {} (x integer)",
        squatter.leaf_name()
    ))
    .execute(&mut *transaction)
    .await
    .expect("create uncataloged squatter relation");
    let conflict = create_daily_leaf(
        &mut transaction,
        &CreateDailyHistoryLeaf::new(squatter).expect("squatter command"),
        &UnpublishedLoader,
    )
    .await
    .expect("classify uncataloged relation");
    assert!(matches!(
        conflict,
        LeafCreation::CatalogConflict {
            kind: crate::core::history::outcomes::CatalogConflictKind::RelationWithoutCatalog,
            ..
        }
    ));
    transaction.commit().await.expect("commit verification");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pending_blocker_refuses_then_detach_restores_timeout_and_drop_reconciles_catalog() {
    let database = TestDatabase::create().await;
    let class_key = "p3_retire_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut connection = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut connection).await.expect("database now");
    drop(connection);
    let lower = now - Duration::days(5);
    let lower = lower
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate expired day");
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let mut transaction = database.pool.begin().await.expect("begin blocker insert");
    let workflow_id = Uuid::new_v4();
    let node_row_id = Uuid::new_v4();
    let pending_task_id = Uuid::new_v4();
    sqlx::query("INSERT INTO horsies_workflows (id, name) VALUES ($1, 'p3 blocker')")
        .bind(workflow_id)
        .execute(&mut *transaction)
        .await
        .expect("insert blocker workflow");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks
             (id, workflow_id, task_index, task_name)
         VALUES ($1, $2, 0, 'p3 blocker node')",
    )
    .bind(node_row_id)
    .bind(workflow_id)
    .execute(&mut *transaction)
    .await
    .expect("insert blocker workflow node");
    sqlx::query(
        "INSERT INTO horsies_workflow_phase2_pending (
             task_id, workflow_id, workflow_node_row_id, terminal_status, terminal_at,
             terminalization_kind, recovery_source, history_class, history_anchor,
             history_schema_version, result_digest, phase2_generation, created_at,
             attempt_count
         ) VALUES ($1, $2, $3, 'COMPLETED', $4, 'COMPLETE_FUSED', 'HISTORY',
                   $5, $4, 1, $6, $7, statement_timestamp(), 0)",
    )
    .bind(pending_task_id)
    .bind(workflow_id)
    .bind(node_row_id)
    .bind(lower + Duration::hours(1))
    .bind(class_key)
    .bind(vec![7_u8; 32])
    .bind(Uuid::new_v4())
    .execute(&mut *transaction)
    .await
    .expect("insert pending blocker");
    transaction.commit().await.expect("commit blocker");
    let quarantine_detach =
        DetachExpiredHistoryLeaf::new(leaf.clone(), Some(Duration::hours(1)), Some(5_000))
            .expect("quarantine detach command");
    let quarantine_refusal = detach_expired_leaf(
        &database.pool,
        &quarantine_detach,
        &UnpublishedLoader,
        &RefusingQuarantine {
            task_id: pending_task_id,
        },
    )
    .await
    .expect("typed quarantine refusal");
    match quarantine_refusal {
        DetachExpiredLeafOutcome::QuarantineRefused(refusal) => {
            assert_eq!(refusal.leaf_name, leaf.leaf_name());
            assert_eq!(refusal.repointed, 0);
            assert_eq!(refusal.refusals.len(), 1);
            assert_eq!(refusal.refusals[0].task_id, pending_task_id);
            assert_eq!(
                refusal.refusals[0].verdict,
                QuarantineRefusalVerdict::SourceAbsent
            );
            assert!(refusal.refusals[0].detail.is_some());
        }
        other => panic!("expected task-level quarantine refusal, got {other:?}"),
    }
    let detach =
        DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(5_000)).expect("detach command");
    let blocked = detach_expired_leaf(&database.pool, &detach, &UnpublishedLoader, &NoQuarantine)
        .await
        .expect("blocked detach");
    assert!(matches!(
        blocked,
        DetachExpiredLeafOutcome::Inspection(LeafInspection::PendingBlocked {
            blocker_count: 1,
            ..
        })
    ));
    sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
        .execute(&database.pool)
        .await
        .expect("clear pending blocker");
    sqlx::query("SELECT set_config('statement_timeout', '17s', false)")
        .execute(&database.pool)
        .await
        .expect("set prior timeout");
    let detached = detach_expired_leaf(&database.pool, &detach, &UnpublishedLoader, &NoQuarantine)
        .await
        .expect("detach expired leaf");
    assert!(matches!(
        detached,
        DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
    ));
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&database.pool)
        .await
        .expect("read restored timeout");
    assert_eq!(timeout, "17s");
    let mut transaction = database.pool.begin().await.expect("begin drop");
    let dropped = drop_detached_leaf(
        &mut transaction,
        &DropDetachedHistoryLeaf::new(leaf.clone()),
        &UnpublishedLoader,
    )
    .await
    .expect("drop detached leaf");
    assert!(matches!(dropped, LeafDrop::Dropped { .. }));
    assert!(matches!(
        inspect_leaf(&mut transaction, &InspectHistoryLeaf::new(leaf))
            .await
            .expect("inspect dropped leaf"),
        LeafInspection::Dropped { .. }
    ));
    transaction.commit().await.expect("commit drop");

    let referenced_leaf = leaf_ref(&parent, class_key, lower - Duration::days(2));
    assert!(matches!(
        create_leaf(&database.pool, &referenced_leaf).await,
        LeafCreation::Created { .. }
    ));
    let referenced_detach =
        DetachExpiredHistoryLeaf::new(referenced_leaf.clone(), None, Some(5_000))
            .expect("referenced detach command");
    assert!(matches!(
        detach_expired_leaf(
            &database.pool,
            &referenced_detach,
            &UnpublishedLoader,
            &NoQuarantine,
        )
        .await
        .expect("detach referenced leaf"),
        DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
    ));
    let mut transaction = database.pool.begin().await.expect("begin refused drop");
    let refused = drop_detached_leaf(
        &mut transaction,
        &DropDetachedHistoryLeaf::new(referenced_leaf.clone()),
        &ReferencingPublisher,
    )
    .await
    .expect("loader-reference drop refusal");
    assert!(matches!(refused, LeafDrop::RefusedLoaderReferences { .. }));
    transaction.commit().await.expect("commit refused drop");
    sqlx::query(&format!("DROP TABLE {parent} CASCADE"))
        .execute(&database.pool)
        .await
        .expect("drop parent while detached relation survives");
    let mut connection = database
        .pool
        .acquire()
        .await
        .expect("inspect missing parent");
    let missing_parent = inspect_leaf(&mut connection, &InspectHistoryLeaf::new(referenced_leaf))
        .await
        .expect_err("detached leaf with missing parent must fail closed");
    assert!(matches!(
        missing_parent,
        crate::core::history::errors::HistoryError::HistoryParentAbsent(_)
    ));
    drop(connection);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn heartbeat_registration_coverage_and_expiry_sweep_share_the_catalog() {
    let database = TestDatabase::create().await;
    let horizon = Duration::hours(2);
    let mut transaction = database.pool.begin().await.expect("begin heartbeat setup");
    let first = register_heartbeat_class(&mut transaction, horizon)
        .await
        .expect("register heartbeat class");
    assert!(matches!(
        first,
        HeartbeatClassRegistration::Registered { .. }
            | HeartbeatClassRegistration::HorizonUpdated { .. }
            | HeartbeatClassRegistration::Verified { .. }
    ));
    assert!(matches!(
        register_heartbeat_class(&mut transaction, horizon)
            .await
            .expect("verify heartbeat class"),
        HeartbeatClassRegistration::Verified { .. }
    ));
    assert!(matches!(
        register_heartbeat_class(&mut transaction, Duration::hours(3))
            .await
            .expect("update heartbeat horizon"),
        HeartbeatClassRegistration::HorizonUpdated {
            previous_horizon,
            horizon: updated,
        } if previous_horizon == horizon && updated == Duration::hours(3)
    ));
    let coverage = ensure_heartbeat_coverage(
        &mut transaction,
        &EnsureHeartbeatCoverage::new(2).expect("heartbeat coverage command"),
    )
    .await
    .expect("heartbeat coverage");
    assert_eq!(coverage.len(), 3);
    let now = database_now(&mut transaction).await.expect("database now");
    let expired_lower = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate hour")
        - Duration::hours(6);
    let expired = hourly_leaf_ref(expired_lower).expect("expired heartbeat ref");
    assert!(matches!(
        create_hourly_heartbeat_leaf(
            &mut transaction,
            &CreateHourlyHeartbeatLeaf::new(expired.clone()).expect("hourly command"),
        )
        .await
        .expect("create expired heartbeat leaf"),
        LeafCreation::Created { .. }
    ));
    let index_ddl: String = sqlx::query_scalar(
        "SELECT pg_get_indexdef(i.indexrelid) FROM pg_index AS i
         WHERE i.indrelid = to_regclass($1) AND NOT i.indisprimary",
    )
    .bind(expired.leaf_name())
    .fetch_one(&mut *transaction)
    .await
    .expect("heartbeat probe index");
    assert!(index_ddl.contains("(task_id, role, sent_at DESC)"));
    transaction.commit().await.expect("commit heartbeat setup");
    let swept = sweep_expired_heartbeat_leaves(&database.pool, &UnpublishedLoader)
        .await
        .expect("sweep expired heartbeats");
    assert_eq!(swept.len(), 1);
    assert_eq!(swept[0].leaf_name, expired.leaf_name());
    assert!(matches!(swept[0].drop, Some(LeafDrop::Dropped { .. })));
    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin unpartitioned posture");
    sqlx::query("DROP TABLE horsies_heartbeats CASCADE")
        .execute(&mut *transaction)
        .await
        .expect("drop heartbeat partitioned parent");
    sqlx::query("CREATE TABLE horsies_heartbeats (sent_at timestamptz NOT NULL)")
        .execute(&mut *transaction)
        .await
        .expect("create unpartitioned heartbeat parent");
    assert_eq!(
        register_heartbeat_class(&mut transaction, Duration::hours(2))
            .await
            .expect("classify unpartitioned heartbeat parent"),
        HeartbeatClassRegistration::ParentUnpartitioned
    );
    let startup = ensure_startup_coverage(&mut transaction, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("startup coverage posture");
    assert!(matches!(startup, StartupCoverageOutcome::Refused(_)));
    transaction
        .commit()
        .await
        .expect("commit unpartitioned posture");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn manifest_excludes_absent_probes_but_birth_floor_keeps_the_complete_attached_set() {
    let database = TestDatabase::create().await;
    let class_key = "p3_manifest_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let mut transaction = database.pool.begin().await.expect("begin manifest setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate day");
    let gone = leaf_ref(&parent, class_key, today);
    let kept = leaf_ref(&parent, class_key, today + Duration::days(1));
    for leaf in [&gone, &kept] {
        assert!(matches!(
            create_daily_leaf(
                &mut transaction,
                &CreateDailyHistoryLeaf::new(leaf.clone()).expect("daily command"),
                &UnpublishedLoader,
            )
            .await
            .expect("create manifest leaf"),
            LeafCreation::Created { .. }
        ));
    }
    let gone_birth = now - Duration::hours(6);
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET min_birth_at = $1 WHERE leaf_name = $2"
    ))
    .bind(gone_birth)
    .bind(gone.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("set missing leaf birth");
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET min_birth_at = $1 WHERE leaf_name = $2"
    ))
    .bind(now + Duration::hours(6))
    .bind(kept.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("set kept leaf birth");
    sqlx::query(&format!("DROP TABLE {}", gone.leaf_name()))
        .execute(&mut *transaction)
        .await
        .expect("drop history leaf out of band");
    let heartbeat = hourly_leaf_ref(
        now.with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .expect("truncate hour")
            - Duration::hours(10),
    )
    .expect("heartbeat ref");
    let _ = register_heartbeat_class(&mut transaction, Duration::hours(2))
        .await
        .expect("register heartbeat class");
    let _ = create_hourly_heartbeat_leaf(
        &mut transaction,
        &CreateHourlyHeartbeatLeaf::new(heartbeat.clone()).expect("hourly command"),
    )
    .await
    .expect("create heartbeat leaf");
    sqlx::query(&format!("DROP TABLE {}", heartbeat.leaf_name()))
        .execute(&mut *transaction)
        .await
        .expect("drop heartbeat leaf out of band");
    let selection = read_manifest_leaf_rows(&mut transaction)
        .await
        .expect("read manifest leaves");
    assert!(selection
        .attached
        .iter()
        .any(|row| row.leaf_name == gone.leaf_name()));
    assert_eq!(
        selection.absent_relations,
        vec![gone.leaf_name().to_owned()]
    );
    assert_eq!(
        read_attached_birth_floor(&mut transaction)
            .await
            .expect("read attached birth floor"),
        Some(gone_birth)
    );
    let coverage = ensure_partition_coverage(&mut transaction, 2, 2, &[], &CatalogPublisher)
        .await
        .expect("self-correcting coverage pass");
    match coverage {
        CoverageOutcome::Failed(failure) => {
            assert!(failure.refusal.contains(class_key));
            assert_eq!(failure.absent_leaves, vec![gone.leaf_name().to_owned()]);
        }
        other => panic!("missing in-horizon relation must freeze its class: {other:?}"),
    }
    transaction.commit().await.expect("commit manifest setup");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn coverage_savepoint_contains_database_failure_and_still_serves_other_classes() {
    let database = TestDatabase::create().await;
    let survivor = "z_p3_survivor_7d";
    register_class(&database.pool, survivor, 7).await;
    let mut transaction = database.pool.begin().await.expect("begin bad class insert");
    let sql = format!(
        "INSERT INTO {RETENTION_CLASSES} (
             class_key, duration, partition_interval, finite_parent_name, created_at
         ) VALUES ('a_p3_broken_7d', interval '7 days', interval '1 day',
                   'horsies_task_history_a_p3_broken_7d', statement_timestamp())"
    );
    sqlx::query(&sql)
        .execute(&mut *transaction)
        .await
        .expect("insert class with missing parent");
    let outcome = ensure_partition_coverage(&mut transaction, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("contained coverage pass");
    let failure = match outcome {
        CoverageOutcome::Failed(failure) => failure,
        other => panic!("expected contained failure, got {other:?}"),
    };
    assert!(failure.refusal.contains("a_p3_broken_7d"));
    assert!(failure.heartbeat_covered_now);
    let survivor_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {LEAF_CATALOG} WHERE class_key = $1"
    ))
    .bind(survivor)
    .fetch_one(&mut *transaction)
    .await
    .expect("connection remains usable after rollback to savepoint");
    assert_eq!(survivor_count, 3);
    transaction
        .rollback()
        .await
        .expect("rollback containment test");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn maintenance_session_exclusively_gates_archive_transitions_until_finished() {
    let database = TestDatabase::create().await;
    let session_id = Uuid::new_v4();
    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin maintenance session");
    let session = begin_archive_maintenance(&mut transaction, session_id)
        .await
        .expect("open maintenance session");
    assert_eq!(session.session_id, session_id);
    assert_eq!(
        active_maintenance_session(&mut transaction)
            .await
            .expect("read active maintenance session"),
        Some(session_id)
    );
    assert!(matches!(
        begin_archive_maintenance(&mut transaction, Uuid::new_v4()).await,
        Err(MaintenanceSessionError::AlreadyActive)
    ));
    transaction
        .commit()
        .await
        .expect("commit maintenance session");

    let unavailable = sqlx::query(&format!("SELECT {ARCHIVE_AVAILABILITY_FUNCTION}()"))
        .execute(&database.pool)
        .await
        .expect_err("archive transition must fail while maintenance is active");
    let sqlstate = match unavailable {
        sqlx::Error::Database(error) => error.code().map(|code| code.into_owned()),
        other => panic!("availability gate returned a non-database error: {other}"),
    };
    assert_eq!(sqlstate.as_deref(), Some("55006"));

    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin maintenance finish");
    finish_archive_maintenance(&mut transaction, session_id)
        .await
        .expect("finish maintenance session");
    assert_eq!(
        active_maintenance_session(&mut transaction)
            .await
            .expect("read closed maintenance session"),
        None
    );
    transaction
        .commit()
        .await
        .expect("commit maintenance finish");
    sqlx::query(&format!("SELECT {ARCHIVE_AVAILABILITY_FUNCTION}()"))
        .execute(&database.pool)
        .await
        .expect("archive transition resumes after maintenance");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pruning_finalizes_a_timeout_interrupted_detach_before_sweeping_it() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "p3_finalize_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database
        .pool
        .acquire()
        .await
        .expect("acquire finalization clock");
    let now = database_now(&mut clock).await.expect("database now");
    drop(clock);
    let lower = (now - Duration::days(6))
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate interrupted leaf day");
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));

    let mut blocker = database.pool.acquire().await.expect("acquire old snapshot");
    sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *blocker)
        .await
        .expect("begin old snapshot");
    sqlx::query(&format!("SELECT count(*) FROM {}", leaf.leaf_name()))
        .execute(&mut *blocker)
        .await
        .expect("establish old snapshot on leaf");
    let command =
        DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(100)).expect("short detach command");
    let timed_out =
        detach_expired_leaf(&database.pool, &command, &UnpublishedLoader, &NoQuarantine)
            .await
            .expect_err("old snapshot must interrupt concurrent detach at the timeout");
    assert!(timed_out.to_string().contains("statement timeout"));

    let mut inspector = database
        .pool
        .acquire()
        .await
        .expect("inspect interrupted detach");
    assert!(matches!(
        inspect_leaf(&mut inspector, &InspectHistoryLeaf::new(leaf.clone()))
            .await
            .expect("classify interrupted detach"),
        LeafInspection::DetachInterrupted { .. }
    ));
    drop(inspector);
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .expect("release old snapshot");
    drop(blocker);

    let pass = prune_expired_partitions(&database.pool, &UnpublishedLoader).await;
    assert_eq!(pass.finalized_leaves, vec![leaf.leaf_name().to_owned()]);
    assert_eq!(pass.dropped_count(), 1);
    assert!(pass.errors.is_empty(), "prune errors: {:?}", pass.errors);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pruning_contains_one_leaf_error_and_one_drop_refusal_then_keeps_going() {
    let database = TestDatabase::create().await;
    let class_key = "p3_prune_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database
        .pool
        .acquire()
        .await
        .expect("acquire pruning clock");
    let now = database_now(&mut clock).await.expect("database now");
    drop(clock);
    let base = (now - Duration::days(8))
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate prune base day");
    for offset in 0..3 {
        let leaf = leaf_ref(&parent, class_key, base + Duration::days(offset));
        assert!(matches!(
            create_leaf(&database.pool, &leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    let publisher = FailFirstRepublish::default();
    let pass = prune_expired_partitions(&database.pool, &publisher).await;
    assert_eq!(pass.history_swept.len(), 2);
    assert_eq!(pass.errors.len(), 1);
    assert!(pass.errors[0].contains("injected first-leaf publication failure"));
    assert_eq!(pass.refusals.len(), 1);
    assert!(pass.refusals[0].contains("RefusedLoaderReferences"));
    assert_eq!(pass.detached_count(), 2);
    assert_eq!(pass.dropped_count(), 1);
    assert!(pass.acted());
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn forever_conversion_is_idempotent_and_reports_daily_coverage_health() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin forever check");
    assert_eq!(
        ensure_forever_range_partitioning(&mut transaction)
            .await
            .expect("ensure forever range partitioning"),
        0
    );
    let creations = ensure_leaf_coverage(
        &mut transaction,
        &EnsureLeafCoverage::new("forever", 3).expect("forever coverage command"),
        &UnpublishedLoader,
    )
    .await
    .expect("ensure forever coverage");
    assert!(!creations
        .iter()
        .any(|item| matches!(item, LeafCreation::ForeverClassLeaf { .. })));
    let report = collect_partition_health(
        &mut transaction,
        &CollectPartitionHealth::new("forever", true).expect("forever health command"),
    )
    .await
    .expect("forever health");
    assert!(report.is_healthy(), "forever faults: {:?}", report.faults);
    assert!(!report
        .faults
        .iter()
        .any(|fault| matches!(fault, HealthFault::CoverageBelowFloor { .. })));
    transaction.commit().await.expect("commit forever check");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn populated_v34_forever_leaf_converts_without_rewriting_old_rows() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin v34 restoration");
    sqlx::query(&format!(
        "DELETE FROM {LEAF_CATALOG} WHERE class_key = 'forever'"
    ))
    .execute(&mut *transaction)
    .await
    .expect("remove v35 forever catalog rows");
    sqlx::query(&format!(
        "ALTER TABLE {TASK_HISTORY_PARENT} DETACH PARTITION {TASK_HISTORY_FOREVER}"
    ))
    .execute(&mut *transaction)
    .await
    .expect("detach v35 forever range parent");
    sqlx::query(&format!("DROP TABLE {TASK_HISTORY_FOREVER} CASCADE"))
        .execute(&mut *transaction)
        .await
        .expect("drop v35 forever range parent");
    sqlx::query(&format!(
        "CREATE TABLE {TASK_HISTORY_FOREVER}
         PARTITION OF {TASK_HISTORY_PARENT} FOR VALUES IN ('forever')"
    ))
    .execute(&mut *transaction)
    .await
    .expect("restore v34 unbounded forever leaf");
    sqlx::query(&format!(
        "CREATE INDEX {TASK_HISTORY_FOREVER}_task_idx
         ON {TASK_HISTORY_FOREVER} (task_id)"
    ))
    .execute(&mut *transaction)
    .await
    .expect("restore v34 forever task index");
    sqlx::query(&format!(
        "CREATE INDEX {TASK_HISTORY_FOREVER}_enqueued_idx
         ON {TASK_HISTORY_FOREVER} (enqueued_at)"
    ))
    .execute(&mut *transaction)
    .await
    .expect("restore v34 forever ordering index");

    let now = database_now(&mut transaction).await.expect("database now");
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate UTC day");
    let old_anchor = today - Duration::days(30);
    let current_anchor = today + Duration::hours(1);
    let old_id = Uuid::new_v4();
    let current_id = Uuid::new_v4();
    let insert = format!(
        "INSERT INTO {TASK_HISTORY_PARENT} (
             task_id, task_name, queue_name, priority,
             command_fingerprint_version, command_fingerprint,
             status, terminalization_kind, terminal_at,
             retention_anchor_at, retention_class_key,
             enqueued_at, created_at, retry_count, max_retries,
             result_envelope_version, result_codec, result_content_type,
             is_workflow_task, history_schema_version,
             attempt_archive_version, attempt_snapshot_codec,
             attempt_snapshot_content_type, attempt_snapshot,
             attempt_snapshot_digest, rerun_input_disposition
         ) VALUES (
             $1, 'p3 v34 forever', 'default', 100,
             1, $2, 'COMPLETED', 'LEGACY_TERMINAL', $3,
             $3, 'forever', $3, $3, 0, 0,
             1, 'json-utf8', 'application/json', FALSE, 1,
             1, 'json-utf8', 'application/json', $4, $5,
             'NEVER_ELIGIBLE'
         )"
    );
    for (task_id, anchor) in [(old_id, old_anchor), (current_id, current_anchor)] {
        sqlx::query(&insert)
            .bind(task_id)
            .bind(vec![3_u8; 32])
            .bind(anchor)
            .bind(b"[]".as_slice())
            .bind(vec![5_u8; 32])
            .execute(&mut *transaction)
            .await
            .expect("seed populated v34 forever row");
    }
    let old_relation_oid: i64 = sqlx::query_scalar(&format!(
        "SELECT tableoid::oid::bigint FROM {TASK_HISTORY_PARENT} WHERE task_id = $1"
    ))
    .bind(old_id)
    .fetch_one(&mut *transaction)
    .await
    .expect("capture pre-conversion old-row relation");

    assert_eq!(
        ensure_forever_range_partitioning(&mut transaction)
            .await
            .expect("convert populated v34 forever leaf"),
        1
    );
    let relkind: String =
        sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
            .bind(TASK_HISTORY_FOREVER)
            .fetch_one(&mut *transaction)
            .await
            .expect("read converted forever relkind");
    assert_eq!(relkind, "p");
    let current_leaf =
        daily_leaf_name(TASK_HISTORY_FOREVER, today).expect("current forever daily leaf name");
    let locations: Vec<(Uuid, String)> = sqlx::query_as(&format!(
        "SELECT task_id, tableoid::regclass::text
         FROM {TASK_HISTORY_PARENT} WHERE task_id = ANY($1) ORDER BY task_id"
    ))
    .bind(vec![old_id, current_id])
    .fetch_all(&mut *transaction)
    .await
    .expect("read converted row locations");
    assert_eq!(locations.len(), 2);
    for (task_id, relation) in locations {
        if task_id == old_id {
            assert_eq!(relation, FOREVER_LEGACY_LEAF);
        } else if task_id == current_id {
            assert_eq!(relation, current_leaf);
        } else {
            panic!("unexpected converted task ID {task_id}");
        }
    }
    let legacy_oid: i64 = sqlx::query_scalar("SELECT to_regclass($1)::oid::bigint")
        .bind(FOREVER_LEGACY_LEAF)
        .fetch_one(&mut *transaction)
        .await
        .expect("read legacy forever OID");
    assert_eq!(legacy_oid, old_relation_oid);
    let legacy = read_leaf_catalog_row(&mut transaction, FOREVER_LEGACY_LEAF)
        .await
        .expect("read legacy forever catalog row")
        .expect("legacy forever catalog row exists");
    assert_eq!(legacy.parent_name, TASK_HISTORY_FOREVER);
    assert_eq!(legacy.class_key, "forever");
    assert_eq!(
        legacy.lower_anchor,
        chrono::DateTime::from_timestamp(0, 0).unwrap()
    );
    assert_eq!(legacy.upper_anchor, today);
    assert!(!legacy.min_birth_verified);
    assert_eq!(
        legacy.id_index_name,
        leaf_id_index_name(FOREVER_LEGACY_LEAF)
    );
    assert!(
        read_leaf_physical_state(
            &mut transaction,
            FOREVER_LEGACY_LEAF,
            TASK_HISTORY_FOREVER,
            &legacy.id_index_name,
        )
        .await
        .expect("read legacy physical state")
        .id_index_exists
    );
    assert_eq!(
        capture_partition_bound_utc(&mut transaction, FOREVER_LEGACY_LEAF)
            .await
            .expect("capture legacy bound")
            .as_deref(),
        Some(legacy.partition_bound.as_str())
    );
    assert!(
        super::catalog::read_leaf_ordering_index_exists(&mut transaction, FOREVER_LEGACY_LEAF,)
            .await
            .expect("read legacy ordering-index property")
    );
    assert_eq!(
        ensure_forever_range_partitioning(&mut transaction)
            .await
            .expect("rerun forever conversion"),
        0
    );
    let row_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {TASK_HISTORY_PARENT} WHERE task_id = ANY($1)"
    ))
    .bind(vec![old_id, current_id])
    .fetch_one(&mut *transaction)
    .await
    .expect("verify rows survive idempotent rerun");
    assert_eq!(row_count, 2);
    transaction
        .commit()
        .await
        .expect("commit v34 conversion test");
    database.drop().await;
}
