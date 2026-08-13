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
        execute_sql(&mut *conn, migration).await?;
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
