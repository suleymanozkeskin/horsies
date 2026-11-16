//! Horsies-owned migration runner.
//!
//! sqlx's `Migrator::run()` writes its bookkeeping into a fixed
//! `_sqlx_migrations` table (hardcoded in `sqlx-postgres`). When horsies is
//! embedded in an application that also uses `sqlx::migrate!()` against the
//! same database, both writers collide on that table. This runner tracks
//! applied versions in a horsies-owned table named [`MIGRATIONS_TABLE`],
//! leaving `_sqlx_migrations` untouched for the host application.
//!
//! The embedded chain:
//! - Version 1000 is the baseline schema: every package file of
//!   `horsies/core/baseline/sql/`, in the order of Python's
//!   `BASELINE_SQL_FILES` (`horsies/core/baseline/migration.py`), concatenated
//!   with no separator, with one SHA-384 checksum over the UTF-8 bytes of the
//!   result. It creates [`MIGRATIONS_TABLE`] itself. The Python package
//!   installs the same files as its migration 1000 with the same checksum
//!   rule, so both write the same bookkeeping row.
//! - Version 1001 holds the claim functions of the copied Rust worker, which
//!   have no counterpart in migration 1000. It goes when the worker claims
//!   through the functions of migration 1000.
//!
//! Each migration runs in one transaction. The runner writes its bookkeeping
//! row after the migration SQL, in the same transaction, and names every value.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use rand::Rng;
use sha2::{Digest, Sha256};
use sqlx::migrate::{MigrateError, Migration, MigrationType};
use sqlx::ConnectOptions;
use sqlx::{Acquire, Executor, PgConnection, PgPool};
use tokio::time::{sleep, Instant};

use crate::broker::error::{BrokerError, InstallLockFailure};

/// Name of the horsies-owned migrations bookkeeping table.
pub const MIGRATIONS_TABLE: &str = "horsies_migrations";

/// Version of the baseline schema migration.
pub const BASELINE_MIGRATION_VERSION: i64 = 1000;

/// Declares the baseline file list once. From one list of file names it
/// builds `BASELINE_SQL_FILES` (the names) and `BASELINE_SQL` (the texts,
/// concatenated with no separator), so the names and the migration text
/// cannot differ. The files of `horsies/core/baseline/sql/` are the single
/// source; the core includes them by relative path and keeps no copy.
macro_rules! baseline_sql_files {
    ($($file:literal),+ $(,)?) => {
        /// The file names of migration 1000, in the order they run.
        #[cfg(test)]
        pub(crate) const BASELINE_SQL_FILES: &[&str] = &[$($file),+];

        /// Migration 1000: the files of `BASELINE_SQL_FILES`, in that
        /// order, concatenated with no separator.
        const BASELINE_SQL: &str = concat!(
            $(include_str!(concat!("../../../../horsies/core/baseline/sql/", $file))),+
        );
    };
}

// Python's `BASELINE_SQL_FILES` (`horsies/core/baseline/migration.py`), in
// the same order. A file added to the folder or to Python's list without a
// line here fails `baseline_file_list_is_python_s_list_and_the_whole_folder`
// and Python's `tests/unit/test_baseline_migration.py`.
baseline_sql_files!(
    "0001_tables.sql",
    "0002_indexes.sql",
    "0003_interim_functions.sql",
    "0006_history_functions.sql",
    "0007_scheduler_monitoring_functions.sql",
    "0008_history_maintenance_functions.sql",
);

/// The tables file of the baseline.
#[cfg(test)]
pub(crate) const BASELINE_TABLES_SQL: &str =
    include_str!("../../../../horsies/core/baseline/sql/0001_tables.sql");
/// The interim functions file of the baseline.
#[cfg(test)]
pub(crate) const BASELINE_INTERIM_FUNCTIONS_SQL: &str =
    include_str!("../../../../horsies/core/baseline/sql/0003_interim_functions.sql");

/// Version, description and SQL of each embedded migration, in apply order.
const MIGRATION_SOURCES: [(i64, &str, &str); 2] = [
    (BASELINE_MIGRATION_VERSION, "baseline", BASELINE_SQL),
    (
        1001,
        "interim rust claim functions",
        include_str!("../../migrations/1001_interim_rust_claim_functions.sql"),
    ),
];

/// The embedded migrations with their SHA-384 checksums.
fn embedded_migrations() -> &'static [Migration] {
    static MIGRATIONS: OnceLock<Vec<Migration>> = OnceLock::new();
    MIGRATIONS.get_or_init(|| {
        MIGRATION_SOURCES
            .iter()
            .map(|(version, description, sql)| {
                Migration::new(
                    *version,
                    Cow::Borrowed(description),
                    MigrationType::Simple,
                    Cow::Borrowed(sql),
                    false,
                )
            })
            .collect()
    })
}

/// Highest embedded migration version expected by this binary.
pub fn expected_schema_version() -> i64 {
    embedded_migrations()
        .iter()
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

/// The name whose digest is the install lock key (Python
/// `horsies/core/brokers/postgres.py:1346-1348`).
const INSTALL_LOCK_NAME: &str = "horsies:schema:v1";

/// The longest an installer waits for another installer's lock, then
/// refuses with a retryable [`InstallLockFailure::WaitBoundReached`].
pub const INSTALL_LOCK_WAIT: Duration = Duration::from_secs(300);

/// The pause between two tries of the install lock. Each try is a
/// `pg_try_advisory_lock`, so a waiting installer holds no snapshot.
const INSTALL_LOCK_POLL: Duration = Duration::from_millis(25);

/// The session advisory lock key of every installer, Python's and the
/// core's: the first 8 bytes of SHA-256 of [`INSTALL_LOCK_NAME`], read as a
/// big-endian signed integer.
fn install_lock_key() -> i64 {
    let digest = Sha256::digest(INSTALL_LOCK_NAME.as_bytes());
    let mut first = [0_u8; 8];
    first.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(first)
}

const MIGRATION_MAX_ATTEMPTS: usize = 5;
const DEADLOCK_SQLSTATE: &str = "40P01";

/// Install the schema for a client of `family`: run the migrations, then
/// record the family, or check the family that the database records
/// (`crate::broker::client_family`). A database of the other family, or one
/// that keeps no family row after the insert, is refused with
/// `BrokerError::ClientFamilyMismatch`.
pub async fn install_schema(
    pool: &PgPool,
    family: crate::broker::client_family::ClientFamily,
) -> Result<(), BrokerError> {
    run_horsies_migrations(pool).await?;
    crate::broker::client_family::record_client_family(pool, family).await
}

/// Run all embedded horsies migrations against `pool`.
///
/// Bookkeeps in [`MIGRATIONS_TABLE`] rather than `_sqlx_migrations`. A
/// database whose ledger holds a version that this binary does not embed is
/// refused with `VersionMissing`; a changed migration with `VersionMismatch`;
/// a failed earlier run with `Dirty`.
pub async fn run_horsies_migrations(pool: &PgPool) -> Result<(), BrokerError> {
    if migrations_are_current(pool).await? {
        return Ok(());
    }

    let mut attempt = 1;
    loop {
        match run_horsies_migrations_locked(pool).await {
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

/// Run the migrations under the install lock.
///
/// The lock is a session advisory lock on a dedicated connection opened
/// outside the pool, so no pool can hand that connection to another caller
/// while it holds the lock (Python `:1683-1688`). The wait is bounded by
/// [`INSTALL_LOCK_WAIT`]. The unlock is checked: an unlock that releases
/// nothing means the session lost the lock, and the install reports
/// [`InstallLockFailure::OwnershipLost`] (Python `:1508-1525`). The
/// dedicated connection is closed in every case, which also ends any lock it
/// still holds.
async fn run_horsies_migrations_locked(pool: &PgPool) -> Result<(), BrokerError> {
    let mut conn = pool.connect_options().connect().await?;
    let key = install_lock_key();
    match acquire_install_lock(&mut conn, key, INSTALL_LOCK_WAIT).await {
        Ok(()) => {}
        Err(error) => {
            close_install_connection(conn).await;
            return Err(error);
        }
    }

    let outcome = run_inner(&mut conn).await;
    let unlocked = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(&mut conn)
        .await;
    close_install_connection(conn).await;
    match (outcome, unlocked) {
        (Err(error), _) => Err(error),
        (Ok(()), Ok(true)) => Ok(()),
        (Ok(()), Ok(false) | Err(_)) => {
            Err(BrokerError::InstallLock(InstallLockFailure::OwnershipLost))
        }
    }
}

/// Take the install lock with `pg_try_advisory_lock` every
/// [`INSTALL_LOCK_POLL`], for at most `wait` ([`INSTALL_LOCK_WAIT`] for an
/// install).
async fn acquire_install_lock(
    conn: &mut PgConnection,
    key: i64,
    wait: Duration,
) -> Result<(), BrokerError> {
    let started = Instant::now();
    loop {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *conn)
            .await?;
        if acquired {
            return Ok(());
        }
        if started.elapsed() >= wait {
            return Err(BrokerError::InstallLock(
                InstallLockFailure::WaitBoundReached { waited: wait },
            ));
        }
        sleep(INSTALL_LOCK_POLL).await;
    }
}

/// Close the dedicated install connection. A close that fails still drops
/// the connection, which ends its session and any lock it holds; the
/// failure is logged and not returned, because the install's own outcome is
/// the answer.
async fn close_install_connection(conn: PgConnection) {
    if let Err(error) = sqlx::Connection::close(conn).await {
        tracing::warn!(error = %error, "closing the schema install connection failed");
    }
}

async fn run_inner(conn: &mut PgConnection) -> Result<(), BrokerError> {
    if migrations_are_current_on_conn(conn).await? {
        return Ok(());
    }

    // Migration 1000 creates the ledger. Without it the database has no
    // horsies schema, and every embedded migration is applied in order.
    let applied = if ledger_exists(conn).await? {
        applied_checksums(conn).await?
    } else {
        HashMap::new()
    };

    for migration in embedded_migrations() {
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

async fn ledger_exists(conn: &mut PgConnection) -> Result<bool, BrokerError> {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(MIGRATIONS_TABLE)
        .fetch_one(&mut *conn)
        .await
        .map_err(BrokerError::from)
}

/// Checksums of the applied versions. Refuses a failed earlier run and a
/// version that this binary does not embed.
async fn applied_checksums(conn: &mut PgConnection) -> Result<HashMap<i64, Vec<u8>>, BrokerError> {
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

    let embedded_versions: HashSet<i64> = embedded_migrations().iter().map(|m| m.version).collect();
    if let Some(&version) = applied
        .keys()
        .filter(|version| !embedded_versions.contains(version))
        .min()
    {
        // A database with a migration that this binary does not know (a
        // newer horsies, or a 0.x chain) fails hard.
        return Err(BrokerError::Migration(MigrateError::VersionMissing(
            version,
        )));
    }
    Ok(applied)
}

async fn migrations_are_current(pool: &PgPool) -> Result<bool, BrokerError> {
    let mut conn = pool.acquire().await?;
    migrations_are_current_on_conn(&mut conn).await
}

async fn migrations_are_current_on_conn(conn: &mut PgConnection) -> Result<bool, BrokerError> {
    if !ledger_exists(conn).await? {
        return Ok(false);
    }

    let applied_rows: Vec<(i64, bool, Vec<u8>)> = sqlx::query_as(&format!(
        "SELECT version, success, checksum FROM {MIGRATIONS_TABLE} ORDER BY version"
    ))
    .fetch_all(&mut *conn)
    .await?;

    let mut applied: HashMap<i64, Vec<u8>> = HashMap::new();
    for (version, success, checksum) in applied_rows {
        if !success {
            return Ok(false);
        }
        applied.insert(version, checksum);
    }

    let embedded_versions: HashSet<i64> = embedded_migrations().iter().map(|m| m.version).collect();
    if applied
        .keys()
        .any(|version| !embedded_versions.contains(version))
    {
        // Force the locked path, which reports VersionMissing for unknown
        // migration rows instead of treating the schema as current.
        return Ok(false);
    }

    Ok(embedded_migrations().iter().all(|migration| {
        applied
            .get(&migration.version)
            .is_some_and(|checksum| checksum.as_slice() == migration.checksum.as_ref())
    }))
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

/// Apply one migration and write its bookkeeping row in one transaction.
async fn apply_migration(
    conn: &mut PgConnection,
    migration: &Migration,
) -> Result<(), BrokerError> {
    let start = Instant::now();
    let mut tx = conn.begin().await?;
    tx.execute(migration.sql.as_ref()).await.map_err(|error| {
        BrokerError::Migration(MigrateError::ExecuteMigration(error, migration.version))
    })?;
    let execution_time_ns: i64 = start.elapsed().as_nanos().try_into().unwrap_or(i64::MAX);
    sqlx::query(&format!(
        "INSERT INTO {MIGRATIONS_TABLE} \
            (version, description, installed_on, success, checksum, execution_time) \
         VALUES ($1, $2, statement_timestamp(), TRUE, $3, $4)"
    ))
    .bind(migration.version)
    .bind(migration.description.as_ref())
    .bind(migration.checksum.as_ref())
    .bind(execution_time_ns)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serial_test::serial;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::{Connection, PgConnection};
    use uuid::Uuid;

    use super::*;

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
            let name = format!("core_migration_retry_{}", Uuid::new_v4().simple());
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

    /// Python's key: the first 8 bytes of SHA-256 of `horsies:schema:v1`,
    /// big-endian, signed (`horsies/core/brokers/postgres.py:1346-1348`).
    #[test]
    fn the_install_lock_key_is_python_s() {
        assert_eq!(install_lock_key(), 6_194_151_126_714_960_738);
    }

    #[test]
    fn a_lost_lock_is_not_retryable_and_a_busy_lock_is() {
        assert!(!BrokerError::InstallLock(InstallLockFailure::OwnershipLost).is_retryable());
        assert!(
            BrokerError::InstallLock(InstallLockFailure::WaitBoundReached {
                waited: INSTALL_LOCK_WAIT,
            })
            .is_retryable()
        );
        assert_eq!(INSTALL_LOCK_WAIT, Duration::from_secs(300));
    }

    /// While another session holds the lock, a waiter gives up at its bound
    /// with a retryable refusal, and takes the lock once it is free. The
    /// test uses its own key, so no installer waits on it.
    #[tokio::test]
    async fn a_waiter_stops_at_its_bound_and_takes_a_free_lock() {
        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let key = rand::thread_rng().gen_range(1..i64::MAX);
        let mut holder = PgConnection::connect(&url).await.unwrap();
        let mut waiter = PgConnection::connect(&url).await.unwrap();
        let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut holder)
            .await
            .unwrap();
        assert!(held);

        let refused = acquire_install_lock(&mut waiter, key, Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(matches!(
            refused,
            BrokerError::InstallLock(InstallLockFailure::WaitBoundReached { .. })
        ));
        assert!(refused.is_retryable());

        sqlx::Connection::close(holder).await.unwrap();
        acquire_install_lock(&mut waiter, key, Duration::from_secs(5))
            .await
            .unwrap();
        sqlx::Connection::close(waiter).await.unwrap();
    }

    /// The package folder of the baseline SQL files.
    fn baseline_sql_folder() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../horsies/core/baseline/sql")
    }

    /// Python's `BASELINE_SQL_FILES`, read from
    /// `horsies/core/baseline/migration.py`: one quoted name per line between
    /// the line that opens the tuple and the line that closes it.
    fn python_baseline_sql_files() -> Vec<String> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../horsies/core/baseline/migration.py");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let mut lines = source
            .lines()
            .skip_while(|line| !line.starts_with("BASELINE_SQL_FILES: Final = ("));
        assert!(
            lines.next().is_some(),
            "no BASELINE_SQL_FILES tuple in {}",
            path.display()
        );
        lines
            .take_while(|line| line.trim() != ")")
            .map(|line| {
                line.trim()
                    .trim_end_matches(',')
                    .trim_matches('\'')
                    .to_owned()
            })
            .collect()
    }

    /// The core's list is Python's list in Python's order, and it holds every
    /// SQL file of the package folder. A file that one side adds fails here
    /// and in Python's `tests/unit/test_baseline_migration.py`.
    #[test]
    fn baseline_file_list_is_python_s_list_and_the_whole_folder() {
        let python = python_baseline_sql_files();
        assert!(!python.is_empty(), "Python's list is empty");
        assert_eq!(BASELINE_SQL_FILES, python.as_slice());

        let mut folder: Vec<String> = std::fs::read_dir(baseline_sql_folder())
            .expect("read the baseline SQL folder")
            .map(|entry| {
                entry
                    .expect("baseline SQL folder entry")
                    .file_name()
                    .into_string()
                    .expect("UTF-8 file name")
            })
            .filter(|name| name.ends_with(".sql"))
            .collect();
        folder.sort();
        assert_eq!(BASELINE_SQL_FILES, folder.as_slice());
    }

    /// Migration 1000 is the listed files as they are on disk, concatenated
    /// in list order, and its checksum is Python's rule
    /// (`horsies/core/baseline/migration.py`, `baseline_checksum`): SHA-384
    /// of the UTF-8 bytes of that text.
    #[test]
    fn baseline_is_one_migration_built_from_every_listed_file() {
        use sha2::Sha384;

        let baseline = &embedded_migrations()[0];
        assert_eq!(baseline.version, BASELINE_MIGRATION_VERSION);
        assert_eq!(baseline.description.as_ref(), "baseline");
        let on_disk: String = BASELINE_SQL_FILES
            .iter()
            .map(|name| {
                let path = baseline_sql_folder().join(name);
                std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
            })
            .collect();
        assert_eq!(baseline.sql.as_ref(), on_disk);
        assert_eq!(
            baseline.checksum.as_ref(),
            Sha384::digest(on_disk.as_bytes()).as_slice()
        );
        assert!(BASELINE_TABLES_SQL.contains(&format!("CREATE TABLE {MIGRATIONS_TABLE} (")));
        let versions: Vec<i64> = embedded_migrations().iter().map(|m| m.version).collect();
        let mut sorted = versions.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(versions, sorted, "apply order is ascending and unique");
        assert_eq!(expected_schema_version(), *versions.last().unwrap());
    }

    #[tokio::test]
    #[serial]
    async fn fresh_database_gets_every_migration_with_named_bookkeeping() {
        let database = MigrationTestDatabase::create().await;
        run_horsies_migrations(&database.pool).await.unwrap();

        let rows: Vec<(i64, String, bool, Vec<u8>, i64, bool)> = sqlx::query_as(&format!(
            "SELECT version, description, success, checksum, execution_time, \
                    installed_on <= statement_timestamp() \
             FROM {MIGRATIONS_TABLE} ORDER BY version"
        ))
        .fetch_all(&database.pool)
        .await
        .unwrap();
        let expected: Vec<(i64, String, bool, Vec<u8>)> = embedded_migrations()
            .iter()
            .map(|m| {
                (
                    m.version,
                    m.description.to_string(),
                    true,
                    m.checksum.to_vec(),
                )
            })
            .collect();
        let observed: Vec<(i64, String, bool, Vec<u8>)> = rows
            .iter()
            .map(|(version, description, success, checksum, _, _)| {
                (*version, description.clone(), *success, checksum.clone())
            })
            .collect();
        assert_eq!(observed, expected);
        assert!(rows
            .iter()
            .all(|(_, _, _, _, execution_time, installed)| *execution_time >= 0 && *installed));
        assert_eq!(
            successful_schema_version(&database.pool).await.unwrap(),
            Some(expected_schema_version())
        );
        let guard: i32 = sqlx::query_scalar("SELECT max(version) FROM horsies_schema_version")
            .fetch_one(&database.pool)
            .await
            .unwrap();
        assert_eq!(i64::from(guard), BASELINE_MIGRATION_VERSION);

        // A second run is a no-op.
        run_horsies_migrations(&database.pool).await.unwrap();
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {MIGRATIONS_TABLE}"))
            .fetch_one(&database.pool)
            .await
            .unwrap();
        assert_eq!(count, i64::try_from(embedded_migrations().len()).unwrap());
        database.drop().await;
    }

    #[tokio::test]
    #[serial]
    async fn changed_checksum_is_refused_as_version_mismatch() {
        let database = MigrationTestDatabase::create().await;
        run_horsies_migrations(&database.pool).await.unwrap();
        sqlx::query(&format!(
            "UPDATE {MIGRATIONS_TABLE} SET checksum = '\\x00'::bytea WHERE version = $1"
        ))
        .bind(BASELINE_MIGRATION_VERSION)
        .execute(&database.pool)
        .await
        .unwrap();
        match run_horsies_migrations(&database.pool).await {
            Err(BrokerError::Migration(MigrateError::VersionMismatch(version))) => {
                assert_eq!(version, BASELINE_MIGRATION_VERSION);
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
        database.drop().await;
    }

    #[tokio::test]
    #[serial]
    async fn unknown_applied_version_is_refused_as_version_missing() {
        let database = MigrationTestDatabase::create().await;
        run_horsies_migrations(&database.pool).await.unwrap();
        let unknown = expected_schema_version() + 1;
        sqlx::query(&format!(
            "INSERT INTO {MIGRATIONS_TABLE} \
                (version, description, installed_on, success, checksum, execution_time) \
             VALUES ($1, 'from a newer binary', statement_timestamp(), TRUE, '\\x00'::bytea, 0)"
        ))
        .bind(unknown)
        .execute(&database.pool)
        .await
        .unwrap();
        match run_horsies_migrations(&database.pool).await {
            Err(BrokerError::Migration(MigrateError::VersionMissing(version))) => {
                assert_eq!(version, unknown);
            }
            other => panic!("expected VersionMissing, got {other:?}"),
        }
        database.drop().await;
    }

    #[tokio::test]
    #[serial]
    async fn failed_earlier_run_is_refused_as_dirty() {
        let database = MigrationTestDatabase::create().await;
        run_horsies_migrations(&database.pool).await.unwrap();
        let last = expected_schema_version();
        sqlx::query(&format!(
            "UPDATE {MIGRATIONS_TABLE} SET success = FALSE WHERE version = $1"
        ))
        .bind(last)
        .execute(&database.pool)
        .await
        .unwrap();
        match run_horsies_migrations(&database.pool).await {
            Err(BrokerError::Migration(MigrateError::Dirty(version))) => {
                assert_eq!(version, last);
            }
            other => panic!("expected Dirty, got {other:?}"),
        }
        database.drop().await;
    }

    #[tokio::test]
    #[serial]
    async fn failing_migration_leaves_no_ledger_row() {
        let database = MigrationTestDatabase::create().await;
        // An object that the baseline creates already exists: migration 1000
        // fails and its transaction, ledger included, rolls back.
        sqlx::query("CREATE TABLE horsies_tasks (id uuid)")
            .execute(&database.pool)
            .await
            .unwrap();
        match run_horsies_migrations(&database.pool).await {
            Err(BrokerError::Migration(MigrateError::ExecuteMigration(_, version))) => {
                assert_eq!(version, BASELINE_MIGRATION_VERSION);
            }
            other => panic!("expected ExecuteMigration, got {other:?}"),
        }
        let ledger: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(MIGRATIONS_TABLE)
            .fetch_one(&database.pool)
            .await
            .unwrap();
        assert!(!ledger);
        database.drop().await;
    }

    async fn claim_planning_database() -> MigrationTestDatabase {
        let database = MigrationTestDatabase::create().await;
        let pool = &database.pool;
        sqlx::query("CREATE EXTENSION pg_stat_statements")
            .execute(pool)
            .await
            .unwrap();
        run_horsies_migrations(pool).await.unwrap();
        sqlx::query("ALTER TABLE horsies_tasks SET (autovacuum_enabled = false)")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at, command_fingerprint_version,
                command_fingerprint, status, enqueued_at, retry_count, max_retries,
                is_workflow_task, updated_at, retain_rerun_input
             ) SELECT md5(n::text)::uuid, 'plan_test', 'plan_test', n % 5 + 1, 'null', NULL,
                'ISOLATED', 'forever', NOW() - interval '1 hour', NOW() - interval '1 hour', 1,
                decode(repeat('00', 32), 'hex'), 'PENDING', NOW() - interval '1 hour', 0, 0,
                FALSE, NOW() - interval '1 hour', FALSE
             FROM generate_series(1, 50000) n",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("VACUUM ANALYZE horsies_tasks")
            .execute(pool)
            .await
            .unwrap();
        database
    }

    async fn candidate_planning_counts(connection: &mut PgConnection) -> (i64, i64) {
        sqlx::query_as(
            "SELECT COALESCE(sum(plans), 0)::bigint, COALESCE(sum(calls), 0)::bigint
             FROM pg_stat_statements
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND NOT toplevel AND ltrim(query) LIKE 'WITH cand AS (%'",
        )
        .fetch_one(connection)
        .await
        .unwrap()
    }

    async fn rolled_back_claim(connection: &mut PgConnection) {
        let mut transaction = sqlx::Acquire::begin(&mut *connection).await.unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM horsies_claim(
                'plan_worker', '[\"plan_test\"]'::jsonb, '{}'::jsonb, '{}'::jsonb,
                TRUE, 32, 0, 32, 8, NULL, 60000, '[]'::jsonb
             )",
        )
        .fetch_one(transaction.as_mut())
        .await
        .unwrap();
        assert_eq!(count, 8);
        transaction.rollback().await.unwrap();
        let mode: String = sqlx::query_scalar("SHOW plan_cache_mode")
            .fetch_one(connection)
            .await
            .unwrap();
        assert_eq!(mode, "auto");
    }

    async fn configure_planning_counters(connection: &mut PgConnection) {
        for statement in [
            "SET plan_cache_mode = auto",
            "SET pg_stat_statements.track = 'all'",
            "SET pg_stat_statements.track_planning = on",
        ] {
            sqlx::query(statement)
                .execute(&mut *connection)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    #[serial]
    #[ignore = "requires preloaded pg_stat_statements; release CI runs this test"]
    async fn claim_candidate_plan_is_reused_across_calls() {
        let database = claim_planning_database().await;
        let mut connection = database.pool.acquire().await.unwrap();
        configure_planning_counters(&mut connection).await;
        rolled_back_claim(&mut connection).await;
        let first = candidate_planning_counts(&mut connection).await;
        assert_eq!(first, (1, 1));
        for _ in 0..30 {
            rolled_back_claim(&mut connection).await;
        }
        let repeated = candidate_planning_counts(&mut connection).await;
        assert_eq!(repeated, (1, 31));
        drop(connection);
        database.drop().await;
    }

    #[tokio::test]
    #[serial]
    #[ignore = "requires preloaded pg_stat_statements; release CI runs this test"]
    async fn claim_candidate_plan_rebuilds_once_after_analyze() {
        let database = claim_planning_database().await;
        let mut connection = database.pool.acquire().await.unwrap();
        configure_planning_counters(&mut connection).await;
        for _ in 0..8 {
            rolled_back_claim(&mut connection).await;
        }
        let before = candidate_planning_counts(&mut connection).await;
        assert_eq!(before, (1, 8));
        sqlx::query("ANALYZE horsies_tasks")
            .execute(&mut *connection)
            .await
            .unwrap();
        rolled_back_claim(&mut connection).await;
        let rebuilt = candidate_planning_counts(&mut connection).await;
        assert_eq!(rebuilt, (before.0 + 1, before.1 + 1));
        for _ in 0..30 {
            rolled_back_claim(&mut connection).await;
        }
        let repeated = candidate_planning_counts(&mut connection).await;
        assert_eq!(repeated, (rebuilt.0, rebuilt.1 + 30));
        drop(connection);
        database.drop().await;
    }
}
