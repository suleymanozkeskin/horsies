//! Horsies-owned migration runner.
//!
//! sqlx's `Migrator::run()` writes its bookkeeping into a fixed
//! `_sqlx_migrations` table (hardcoded in `sqlx-postgres`). When horsies is
//! embedded in an application that also uses `sqlx::migrate!()` against the
//! same database, both writers collide on that table.
//!
//! This module re-implements the migrator using the same embedded migration
//! set (`sqlx::migrate!()` at build time) but tracks applied versions in a
//! horsies-owned table named [`MIGRATIONS_TABLE`], leaving `_sqlx_migrations`
//! untouched for the host application.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rand::Rng;
use sqlx::migrate::{MigrateError, Migration, Migrator};
use sqlx::{Acquire, Executor, PgConnection, PgPool};
use tokio::time::{sleep, Instant};

use crate::broker::error::BrokerError;

/// Name of the horsies-owned migrations bookkeeping table.
pub const MIGRATIONS_TABLE: &str = "horsies_migrations";

/// Highest embedded migration version expected by this binary.
pub fn expected_schema_version() -> i64 {
    sqlx::migrate!()
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| migration.version)
        .max()
        .expect("the embedded horsies migration chain is non-empty")
}

/// Highest successfully applied horsies migration, or `None` when the ledger
/// does not exist or has no successful rows.
pub(crate) async fn successful_schema_version(pool: &PgPool) -> Result<Option<i64>, BrokerError> {
    let table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(MIGRATIONS_TABLE)
        .fetch_one(pool)
        .await?;
    if !table_exists {
        return Ok(None);
    }
    sqlx::query_scalar(&format!(
        "SELECT max(version) FROM {MIGRATIONS_TABLE} WHERE success"
    ))
    .fetch_one(pool)
    .await
    .map_err(BrokerError::from)
}

/// Postgres advisory-lock key used to serialise concurrent horsies migrators.
///
/// Disjoint from sqlx's own lock id (`0x3d32ad9e * crc32(db_name)`), so it
/// never blocks or is blocked by an application's `sqlx::migrate!().run()`.
const ADVISORY_LOCK_KEY: i64 = 0x484F_5253_4945_5300;

const MIGRATION_MAX_ATTEMPTS: usize = 5;
const DEADLOCK_SQLSTATE: &str = "40P01";

#[derive(Clone, Copy)]
struct ConcurrentRecoveryIndex {
    version: i64,
    name: &'static str,
    table: &'static str,
}

const CONCURRENT_RECOVERY_INDEXES: [ConcurrentRecoveryIndex; 2] = [
    ConcurrentRecoveryIndex {
        version: 46,
        name: "idx_horsies_workflows_running_recovery_scan",
        table: "horsies_workflows",
    },
    ConcurrentRecoveryIndex {
        version: 47,
        name: "idx_horsies_tasks_orphan_recovery_scan",
        table: "horsies_tasks",
    },
];

enum RecoveryIndexRelationState {
    Absent,
    ExpectedTable,
    Conflict,
}

struct RecoveryIndexClaim {
    name: String,
}

#[cfg(test)]
#[derive(Clone)]
struct RecoveryIndexClaimPause {
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static RECOVERY_INDEX_CLAIM_PAUSE: std::sync::OnceLock<
    std::sync::Mutex<Option<RecoveryIndexClaimPause>>,
> = std::sync::OnceLock::new();

/// Run all embedded horsies migrations against `pool`.
///
/// Bookkeeps in [`MIGRATIONS_TABLE`] rather than `_sqlx_migrations`.
/// On first run, copies any matching horsies rows out of an existing
/// `_sqlx_migrations` table so prior alpha installs upgrade cleanly.
pub async fn run_horsies_migrations(pool: &PgPool) -> Result<(), BrokerError> {
    let migrator = sqlx::migrate!();
    if migrations_are_current(pool, &migrator).await? {
        return Ok(());
    }

    let mut attempt = 1;
    loop {
        match run_horsies_migrations_locked(pool, &migrator).await {
            Ok(()) => return Ok(()),
            Err(err) if is_deadlock_error(&err) && attempt < MIGRATION_MAX_ATTEMPTS => {
                let scale = 1_u64 << (attempt - 1);
                let jitter_ms = rand::thread_rng().gen_range(0..=200_u64) * scale;
                let backoff = Duration::from_millis(50 + jitter_ms);
                tracing::warn!(
                    attempt,
                    backoff_ms = backoff.as_millis(),
                    "horsies migration hit deadlock, retrying"
                );
                sleep(backoff).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn run_horsies_migrations_locked(
    pool: &PgPool,
    migrator: &Migrator,
) -> Result<(), BrokerError> {
    let mut conn = pool.acquire().await?;

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .execute(&mut *conn)
        .await?;

    let outcome = run_inner(&mut conn, migrator).await;

    // Always release the session-level lock, even on error, so the connection
    // can return to the pool clean. Swallow unlock errors so we surface the
    // real failure to the caller.
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .execute(&mut *conn)
        .await;

    outcome
}

async fn run_inner(conn: &mut PgConnection, migrator: &Migrator) -> Result<(), BrokerError> {
    if migrations_are_current_on_conn(conn, migrator).await? {
        return Ok(());
    }

    run_inner_through(conn, migrator, i64::MAX).await
}

async fn run_inner_through(
    conn: &mut PgConnection,
    migrator: &Migrator,
    maximum_version: i64,
) -> Result<(), BrokerError> {
    ensure_migrations_table(conn).await?;
    backfill_from_sqlx_migrations(conn, migrator).await?;

    let dirty: Option<(i64,)> = sqlx::query_as(&format!(
        "SELECT version FROM {MIGRATIONS_TABLE} \
         WHERE success = false ORDER BY version LIMIT 1"
    ))
    .fetch_optional(&mut *conn)
    .await?;
    if let Some((version,)) = dirty {
        return Err(BrokerError::Migration(MigrateError::Dirty(version)));
    }

    let applied_rows: Vec<(i64, Vec<u8>)> = sqlx::query_as(&format!(
        "SELECT version, checksum FROM {MIGRATIONS_TABLE} ORDER BY version"
    ))
    .fetch_all(&mut *conn)
    .await?;
    let applied: HashMap<i64, Vec<u8>> = applied_rows.into_iter().collect();

    let embedded_versions: HashSet<i64> = migrator.iter().map(|m| m.version).collect();
    for &version in applied.keys() {
        if !embedded_versions.contains(&version) {
            // Match sqlx migrator rollback behavior: a database with a future
            // horsies migration should fail hard under an older binary.
            return Err(BrokerError::Migration(MigrateError::VersionMissing(
                version,
            )));
        }
    }

    for migration in migrator.iter() {
        if migration.migration_type.is_down_migration() || migration.version > maximum_version {
            continue;
        }
        match applied.get(&migration.version) {
            Some(applied_checksum) => {
                if applied_checksum.as_slice() != migration.checksum.as_ref() {
                    return Err(BrokerError::Migration(MigrateError::VersionMismatch(
                        migration.version,
                    )));
                }
            }
            None => apply_migration(conn, migration).await?,
        }
    }

    Ok(())
}

/// Apply the embedded journal only through `maximum_version`.
///
/// This exists solely for the cutover pipeline, which must construct the
/// exact pre-emission database before seeding legacy rows. Production callers
/// can only use [`run_horsies_migrations`], which always applies the full
/// append-only journal.
#[cfg(test)]
pub(crate) async fn run_horsies_migrations_through(
    pool: &PgPool,
    maximum_version: i64,
) -> Result<(), BrokerError> {
    let migrator = sqlx::migrate!();
    let mut conn = pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .execute(&mut *conn)
        .await?;
    let outcome = run_inner_through(&mut conn, &migrator, maximum_version).await;
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .execute(&mut *conn)
        .await;
    outcome
}

async fn migrations_are_current(pool: &PgPool, migrator: &Migrator) -> Result<bool, BrokerError> {
    let mut conn = pool.acquire().await?;
    migrations_are_current_on_conn(&mut conn, migrator).await
}

async fn migrations_are_current_on_conn(
    conn: &mut PgConnection,
    migrator: &Migrator,
) -> Result<bool, BrokerError> {
    let table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(MIGRATIONS_TABLE)
        .fetch_one(&mut *conn)
        .await?;
    if !table_exists {
        return Ok(false);
    }

    let applied_rows: Vec<(i64, bool, Vec<u8>)> = sqlx::query_as(&format!(
        "SELECT version, success, checksum FROM {MIGRATIONS_TABLE} ORDER BY version"
    ))
    .fetch_all(&mut *conn)
    .await?;

    let mut applied: HashMap<i64, (bool, Vec<u8>)> = HashMap::new();
    for (version, success, checksum) in applied_rows {
        if !success {
            return Ok(false);
        }
        applied.insert(version, (success, checksum));
    }

    let embedded_versions: HashSet<i64> = migrator.iter().map(|m| m.version).collect();
    for &version in applied.keys() {
        if !embedded_versions.contains(&version) {
            // Force the locked path, which reports VersionMissing for future
            // migration rows instead of silently treating the schema as current.
            return Ok(false);
        }
    }

    for migration in migrator.iter() {
        if migration.migration_type.is_down_migration() {
            continue;
        }
        match applied.get(&migration.version) {
            Some((true, checksum)) if checksum.as_slice() == migration.checksum.as_ref() => {}
            _ => return Ok(false),
        }
    }

    Ok(true)
}

fn is_deadlock_error(err: &BrokerError) -> bool {
    broker_error_sqlstate(err).as_deref() == Some(DEADLOCK_SQLSTATE)
}

fn broker_error_sqlstate(err: &BrokerError) -> Option<String> {
    match err {
        BrokerError::Database(err) => sqlx_error_sqlstate(err),
        BrokerError::Migration(MigrateError::Execute(err))
        | BrokerError::Migration(MigrateError::ExecuteMigration(err, _)) => {
            sqlx_error_sqlstate(err)
        }
        _ => None,
    }
}

fn sqlx_error_sqlstate(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().map(|code| code.into_owned()),
        _ => None,
    }
}

async fn ensure_migrations_table(conn: &mut PgConnection) -> Result<(), BrokerError> {
    conn.execute(
        format!(
            "CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (\n\
                version BIGINT PRIMARY KEY,\n\
                description TEXT NOT NULL,\n\
                installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),\n\
                success BOOLEAN NOT NULL,\n\
                checksum BYTEA NOT NULL,\n\
                execution_time BIGINT NOT NULL\n\
            )"
        )
        .as_str(),
    )
    .await?;
    Ok(())
}

/// One-time backfill from a pre-existing `_sqlx_migrations` table.
///
/// If `horsies_migrations` is empty and `_sqlx_migrations` exists in the
/// current search path, copies rows whose `(version, checksum)` pair matches
/// one of our embedded migrations. The checksum gate is essential: a host
/// application's `sqlx::migrate!()` may have populated `_sqlx_migrations`
/// with rows at the same version numbers as horsies (1, 2, 3…), and those
/// rows belong to the application, not to a prior horsies install. Matching
/// on checksum (SHA-384 of the migration SQL) reliably distinguishes our
/// rows from anyone else's. Foreign rows are left untouched.
async fn backfill_from_sqlx_migrations(
    conn: &mut PgConnection,
    migrator: &Migrator,
) -> Result<(), BrokerError> {
    let (already_has_rows,): (bool,) = sqlx::query_as(&format!(
        "SELECT EXISTS (SELECT 1 FROM {MIGRATIONS_TABLE} LIMIT 1)"
    ))
    .fetch_one(&mut *conn)
    .await?;
    if already_has_rows {
        return Ok(());
    }

    let (sqlx_table_exists,): (bool,) =
        sqlx::query_as("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    if !sqlx_table_exists {
        return Ok(());
    }

    let mut versions: Vec<i64> = Vec::with_capacity(migrator.iter().len());
    let mut checksums: Vec<Vec<u8>> = Vec::with_capacity(migrator.iter().len());
    for m in migrator.iter() {
        if m.migration_type.is_down_migration() {
            continue;
        }
        versions.push(m.version);
        checksums.push(m.checksum.to_vec());
    }
    if versions.is_empty() {
        return Ok(());
    }

    sqlx::query(&format!(
        "INSERT INTO {MIGRATIONS_TABLE} \
            (version, description, installed_on, success, checksum, execution_time) \
         SELECT s.version, s.description, s.installed_on, s.success, s.checksum, s.execution_time \
         FROM _sqlx_migrations s \
         JOIN unnest($1::BIGINT[], $2::BYTEA[]) AS e(version, checksum) \
           ON s.version = e.version AND s.checksum = e.checksum \
         ON CONFLICT (version) DO NOTHING"
    ))
    .bind(&versions[..])
    .bind(&checksums[..])
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn apply_migration(
    conn: &mut PgConnection,
    migration: &Migration,
) -> Result<(), BrokerError> {
    let start = Instant::now();

    if migration.no_tx {
        let concurrent_index = CONCURRENT_RECOVERY_INDEXES
            .iter()
            .find(|index| index.version == migration.version)
            .copied();
        let claim = match concurrent_index {
            Some(index) => prepare_concurrent_recovery_index(conn, migration, index).await?,
            None => None,
        };
        execute_sql(&mut *conn, migration).await?;
        if let Some(claim) = claim {
            remove_concurrent_recovery_index_claim(conn, migration, &claim).await?;
        }
        if let Some(index) = concurrent_index {
            remove_abandoned_recovery_index_claims(conn, migration, index).await?;
        }
        record_applied(&mut *conn, migration).await?;
    } else {
        let mut tx = conn.begin().await?;
        execute_sql(&mut *tx, migration).await?;
        record_applied(&mut *tx, migration).await?;
        tx.commit().await?;
    }

    // execution_time is best-effort; if the process dies here the row already
    // exists with execution_time = -1 (matching sqlx's own behaviour).
    let elapsed_ns: i64 = start.elapsed().as_nanos().try_into().unwrap_or(i64::MAX);
    sqlx::query(&format!(
        "UPDATE {MIGRATIONS_TABLE} SET execution_time = $1 WHERE version = $2"
    ))
    .bind(elapsed_ns)
    .bind(migration.version)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn prepare_concurrent_recovery_index(
    conn: &mut PgConnection,
    migration: &Migration,
    index: ConcurrentRecoveryIndex,
) -> Result<Option<RecoveryIndexClaim>, BrokerError> {
    let state = recovery_index_relation_state(conn, index.name, index.table).await?;
    pause_after_recovery_index_inspection().await;
    match state {
        RecoveryIndexRelationState::Absent => return Ok(None),
        RecoveryIndexRelationState::Conflict => {
            return Err(recovery_index_conflict(migration, index));
        }
        RecoveryIndexRelationState::ExpectedTable => {}
    }

    let claim_name = format!(
        "horsies_recovery_index_claim_{}",
        uuid::Uuid::new_v4().simple()
    );
    // The first ownership read can become stale. Rename the current canonical
    // name and recheck its owner in one transaction. Rollback restores a
    // foreign relation that reused the canonical name.
    let mut transaction = conn.begin().await?;
    sqlx::query(&format!(
        "ALTER INDEX IF EXISTS {} RENAME TO {claim_name}",
        index.name
    ))
    .execute(transaction.as_mut())
    .await
    .map_err(|error| {
        BrokerError::Migration(MigrateError::ExecuteMigration(error, migration.version))
    })?;
    let claim_state =
        recovery_index_relation_state(transaction.as_mut(), &claim_name, index.table).await?;
    match claim_state {
        RecoveryIndexRelationState::ExpectedTable => {
            transaction.commit().await?;
            Ok(Some(RecoveryIndexClaim { name: claim_name }))
        }
        RecoveryIndexRelationState::Absent | RecoveryIndexRelationState::Conflict => {
            transaction.rollback().await?;
            Err(recovery_index_conflict(migration, index))
        }
    }
}

async fn recovery_index_relation_state(
    conn: &mut PgConnection,
    index_name: &str,
    table_name: &str,
) -> Result<RecoveryIndexRelationState, BrokerError> {
    let relation_owner: Option<(Option<bool>,)> = sqlx::query_as(
        "SELECT i.indrelid = to_regclass($2)
         FROM pg_class AS c
         LEFT JOIN pg_index AS i ON i.indexrelid = c.oid
         WHERE c.oid = to_regclass($1)",
    )
    .bind(index_name)
    .bind(table_name)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(match relation_owner {
        None => RecoveryIndexRelationState::Absent,
        Some((Some(true),)) => RecoveryIndexRelationState::ExpectedTable,
        Some((Some(false) | None,)) => RecoveryIndexRelationState::Conflict,
    })
}

async fn remove_concurrent_recovery_index_claim(
    conn: &mut PgConnection,
    migration: &Migration,
    claim: &RecoveryIndexClaim,
) -> Result<(), BrokerError> {
    // DROP INDEX CONCURRENTLY cannot run in the claim transaction. The UUID
    // name prevents canonical-name reuse from changing the drop target.
    sqlx::query(&format!("DROP INDEX CONCURRENTLY {}", claim.name))
        .execute(&mut *conn)
        .await
        .map_err(|error| {
            BrokerError::Migration(MigrateError::ExecuteMigration(error, migration.version))
        })?;
    Ok(())
}

async fn remove_abandoned_recovery_index_claims(
    conn: &mut PgConnection,
    migration: &Migration,
    index: ConcurrentRecoveryIndex,
) -> Result<(), BrokerError> {
    let claim_names: Vec<String> = sqlx::query_scalar(
        "SELECT relation.relname
         FROM pg_class AS relation
         JOIN pg_index AS index_state ON index_state.indexrelid = relation.oid
         WHERE index_state.indrelid = to_regclass($1)
           AND relation.relname LIKE 'horsies_recovery_index_claim\\_%' ESCAPE '\\'",
    )
    .bind(index.table)
    .fetch_all(&mut *conn)
    .await?;
    for claim_name in claim_names {
        if !claim_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(recovery_index_conflict(migration, index));
        }
        remove_concurrent_recovery_index_claim(
            conn,
            migration,
            &RecoveryIndexClaim { name: claim_name },
        )
        .await?;
    }
    Ok(())
}

fn recovery_index_conflict(migration: &Migration, index: ConcurrentRecoveryIndex) -> BrokerError {
    let error = sqlx::Error::Protocol(format!(
        "migration {} requires index {} on {}, but that name belongs to another relation",
        migration.version, index.name, index.table,
    ));
    BrokerError::Migration(MigrateError::ExecuteMigration(error, migration.version))
}

#[cfg(test)]
async fn pause_after_recovery_index_inspection() {
    let pause = RECOVERY_INDEX_CLAIM_PAUSE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("recovery index claim pause lock")
        .clone();
    if let Some(pause) = pause {
        pause.entered.notify_one();
        pause.release.notified().await;
    }
}

#[cfg(not(test))]
async fn pause_after_recovery_index_inspection() {}

async fn execute_sql<'c, E>(executor: E, migration: &Migration) -> Result<(), BrokerError>
where
    E: Executor<'c, Database = sqlx::Postgres>,
{
    executor
        .execute(migration.sql.as_ref())
        .await
        .map_err(|e| {
            BrokerError::Migration(MigrateError::ExecuteMigration(e, migration.version))
        })?;
    Ok(())
}

async fn record_applied<'c, E>(executor: E, migration: &Migration) -> Result<(), BrokerError>
where
    E: Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query(&format!(
        "INSERT INTO {MIGRATIONS_TABLE} \
            (version, description, success, checksum, execution_time) \
         VALUES ($1, $2, TRUE, $3, -1)"
    ))
    .bind(migration.version)
    .bind(migration.description.as_ref())
    .bind(migration.checksum.as_ref())
    .execute(executor)
    .await?;
    Ok(())
}

#[cfg(test)]
mod recovery_index_migration_tests {
    use std::str::FromStr;

    use serial_test::serial;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::{Connection, PgConnection};
    use uuid::Uuid;

    use super::*;

    const WORKFLOW_INDEX: &str =
        include_str!("../../migrations/0046_running_workflow_recovery_index.sql");
    const TASK_INDEX: &str = include_str!("../../migrations/0047_orphan_task_recovery_index.sql");
    const VALIDATION_AND_FUNCTION: &str =
        include_str!("../../migrations/0048_bounded_recovery_function.sql");

    #[test]
    fn recovery_indexes_are_concurrent_and_validated_before_function_install() {
        for migration in [WORKFLOW_INDEX, TASK_INDEX] {
            assert!(migration.starts_with("-- no-transaction\n"));
            assert!(migration.contains("CREATE INDEX CONCURRENTLY"));
            assert!(!migration.contains("IF NOT EXISTS"));
        }
        let validation = VALIDATION_AND_FUNCTION
            .find("DO $migration$")
            .expect("index validation block");
        let function = VALIDATION_AND_FUNCTION
            .find("CREATE OR REPLACE FUNCTION horsies_cancel_orphaned_tasks")
            .expect("bounded orphan function");
        assert!(validation < function);
        for required_shape_check in [
            "'valid', i.indisvalid",
            "'ready', i.indisready",
            "'live', i.indislive",
            "'operator_classes', to_jsonb(i.indclass::oid[])",
            "'collations', to_jsonb(i.indcollation::oid[])",
            "'predicate', pg_get_expr(i.indpred, i.indrelid)",
        ] {
            assert!(VALIDATION_AND_FUNCTION.contains(required_shape_check));
        }
    }

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
        format!("postgresql://postgres:{password}@localhost:5432/horsies-rust-port")
    }

    struct MigrationTestDatabase {
        admin: PgConnection,
        pool: PgPool,
        name: String,
    }

    impl MigrationTestDatabase {
        async fn create() -> Self {
            let base_options = PgConnectOptions::from_str(&test_db_url()).unwrap();
            let mut admin = PgConnection::connect_with(&base_options.clone().database("postgres"))
                .await
                .unwrap();
            let name = format!("horsies_migration_retry_{}", Uuid::new_v4().simple());
            sqlx::query(&format!("CREATE DATABASE \"{name}\""))
                .execute(&mut admin)
                .await
                .unwrap();
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect_with(base_options.database(&name))
                .await
                .unwrap();
            Self { admin, pool, name }
        }

        async fn drop(mut self) {
            self.pool.close().await;
            sqlx::query(&format!("DROP DATABASE \"{}\"", self.name))
                .execute(&mut self.admin)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    #[serial]
    async fn invalid_concurrent_recovery_index_is_rebuilt_on_migration_retry() {
        let database = MigrationTestDatabase::create().await;
        let pool = &database.pool;

        run_horsies_migrations_through(pool, 45).await.unwrap();
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES
                 ($1, 'migration_retry_a', 'RUNNING', 'fail', $3, 0,
                  $1, NOW(), NOW(), NOW(), NOW()),
                 ($2, 'migration_retry_b', 'RUNNING', 'fail', $4, 0,
                  $2, NOW(), NOW(), NOW(), NOW())",
        )
        .bind(first_id)
        .bind(second_id)
        .bind(format!("test.migration-retry.{first_id}"))
        .bind(format!("test.migration-retry.{second_id}"))
        .execute(pool)
        .await
        .unwrap();

        let build_error = sqlx::query(
            "CREATE UNIQUE INDEX CONCURRENTLY
                 idx_horsies_workflows_running_recovery_scan
             ON horsies_workflows (status)",
        )
        .execute(pool)
        .await
        .unwrap_err();
        assert_eq!(
            build_error
                .as_database_error()
                .and_then(|error| error.code().map(|code| code.into_owned()))
                .as_deref(),
            Some("23505"),
        );
        let invalid: bool = sqlx::query_scalar(
            "SELECT NOT i.indisvalid
             FROM pg_index AS i
             WHERE i.indexrelid =
                   'idx_horsies_workflows_running_recovery_scan'::regclass",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(
            invalid,
            "failed concurrent build must leave an invalid index"
        );

        run_horsies_migrations_through(pool, 48).await.unwrap();
        let valid: bool = sqlx::query_scalar(
            "SELECT i.indisvalid AND i.indisready AND i.indislive
             FROM pg_index AS i
             WHERE i.indexrelid =
                   'idx_horsies_workflows_running_recovery_scan'::regclass
               AND i.indrelid = 'horsies_workflows'::regclass",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(valid, "migration retry must install a valid recovery index");
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM horsies_migrations
                 WHERE version = 46 AND success
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(applied, "repaired index migration must be recorded");

        database.drop().await;
    }

    #[tokio::test]
    #[serial]
    async fn recovery_index_claim_refuses_a_canonical_name_reused_after_inspection() {
        let database = MigrationTestDatabase::create().await;
        let pool = &database.pool;
        run_horsies_migrations_through(pool, 45).await.unwrap();
        sqlx::query(
            "CREATE INDEX idx_horsies_workflows_running_recovery_scan
             ON horsies_workflows (status)",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("CREATE TABLE migration_recovery_index_foreign (value integer)")
            .execute(pool)
            .await
            .unwrap();

        let pause = RecoveryIndexClaimPause {
            entered: std::sync::Arc::new(tokio::sync::Notify::new()),
            release: std::sync::Arc::new(tokio::sync::Notify::new()),
        };
        *RECOVERY_INDEX_CLAIM_PAUSE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap() = Some(pause.clone());
        let migration_pool = pool.clone();
        let migration =
            tokio::spawn(async move { run_horsies_migrations_through(&migration_pool, 48).await });
        pause.entered.notified().await;

        sqlx::query(
            "ALTER INDEX idx_horsies_workflows_running_recovery_scan
             RENAME TO migration_recovery_index_moved",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE INDEX idx_horsies_workflows_running_recovery_scan
             ON migration_recovery_index_foreign (value)",
        )
        .execute(pool)
        .await
        .unwrap();
        pause.release.notify_one();

        let error = migration.await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("belongs to another relation"),
            "migration must refuse the reused canonical name: {error}",
        );
        *RECOVERY_INDEX_CLAIM_PAUSE.get().unwrap().lock().unwrap() = None;
        let owner: String = sqlx::query_scalar(
            "SELECT index_state.indrelid::regclass::text
             FROM pg_index AS index_state
             WHERE index_state.indexrelid =
                   'idx_horsies_workflows_running_recovery_scan'::regclass",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(owner, "migration_recovery_index_foreign");
        let moved_owner: String = sqlx::query_scalar(
            "SELECT index_state.indrelid::regclass::text
             FROM pg_index AS index_state
             WHERE index_state.indexrelid =
                   'migration_recovery_index_moved'::regclass",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(moved_owner, "horsies_workflows");
        let recorded: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM horsies_migrations
                 WHERE version = 46
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(!recorded, "refused claim must not record migration 46");

        database.drop().await;
    }
}
