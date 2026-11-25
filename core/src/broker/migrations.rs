//! The schema install: the embedded migration chain, its bookkeeping in
//! [`MIGRATIONS_TABLE`], and the install lock, on the statement layer
//! ([`crate::db`]).
//!
//! The bookkeeping table is horsies' own, so the install never collides with
//! the migration runner of an application on the same database.
//!
//! The embedded chain:
//! - Version 1000 is the baseline schema: every file of
//!   `core/baseline/`, in the order of `baseline_sql_files!`
//!   below, concatenated with no separator, with one SHA-384 checksum over the
//!   UTF-8 bytes of the result. It creates [`MIGRATIONS_TABLE`], the 0.x guard
//!   row of `horsies_schema_version` and the empty `horsies_client_family`
//!   table itself. The Python package reads the same files through
//!   `horsies._core` ([`BASELINE_SQL_FILE_TEXTS`]).
//! - Each later version is one file of `core/migrations/`
//!   ([`LATER_MIGRATION_FILES`]), with the SHA-384 checksum of its text. Its
//!   text writes no family row and does not publish the history readers.
//!   Version 1001 (`1001_lifecycle_record.sql`) adds the phase record of each attempt
//!   and the event table of the workflow runs (`horsies_workflow_events`).
//!   Version 1002 (`1002_workflow_start_command_fingerprint.sql`) adds the
//!   start command fingerprint of each top-level workflow run, which a start
//!   with an existing id compares. Version 1003 (`1003_attempt_cancelled.sql`)
//!   adds the attempt outcome `CANCELLED`: the cancel of a RUNNING task
//!   records its attempt and sends the cancel notice to its worker. Version
//!   1004 (`1004_worker_profile_name.sql`) adds the name of the worker
//!   profile to each worker snapshot. Version 1005
//!   (`1005_claim_generation_by_status.sql`) adds the CHECK that a CLAIMED or
//!   RUNNING task row holds its claim generation (`claimed_at`). Version 1006
//!   (`1006_typescript_client_family.sql`) adds the client family
//!   `TYPESCRIPT` to the CHECK of the family row.
//!
//! The released text of a migration never changes: a database that applied
//! another text of it is refused ([`SchemaRefusal::MigrationEdited`]), so a
//! schema change is a new migration. [`migration_sql_file_texts`] gives every
//! file of the chain, in run order, to the test harnesses of both clients.
//!
//! The install ([`install_schema_on`]):
//! 1. Read the state of the current schema on a session-work connection, with
//!    no lock: one statement on an empty database, two otherwise, and a third
//!    on a database that holds this build's chain (whether its history
//!    readers exist).
//! 2. Decide. A database that holds this build's chain and this client's
//!    family gets no lock and no write. A database that this build does not
//!    know is refused with a [`SchemaRefusal`], and a database of another
//!    client family is refused; neither is changed.
//! 3. Otherwise take the install lock on a session-work connection, read the
//!    state again on it (another installer may have finished during the
//!    wait), and act until the state is current or refused:
//!    - each pending migration runs in its own READ COMMITTED transaction
//!      with its bookkeeping row; the transaction of migration 1000 also
//!      publishes the history readers (`horsies_publish_history_readers`),
//!      and the transaction of the last migration of the chain also writes
//!      this client's family row when the database has none
//!      ([`family_row_at`]). A migration that declares
//!      index steps first gives its exact indexes their form outside a
//!      transaction (concurrent builds), and after its transaction commits
//!      the retired indexes are dropped concurrently ([`apply_chain`]);
//!    - a database that holds this build's chain with a history reader
//!      missing or no family row (an install that stopped before the
//!      publication, or a reader or the family row removed out of band)
//!      gets the readers published in one READ COMMITTED transaction, then
//!      this client's family row (Python `CompleteInstall`).
//! 4. Release the lock and check the release.
//!
//! A deadlock (SQLSTATE 40P01) runs the whole install again, at most
//! [`INSTALL_MAX_ATTEMPTS`] runs in all.

use std::sync::OnceLock;
use std::time::Duration;

use rand::Rng;
use sha2::{Digest, Sha256, Sha384};
use tokio::time::{sleep, Instant};
use tokio_postgres::error::SqlState;

use crate::broker::client_family::{
    recorded_family_from, ClientFamily, RecordedFamily, UnknownStoredFamily, RECORD_CLIENT_FAMILY,
};
use crate::broker::core_rows::{
    column, execute_in, one_row, query_in, query_on, RowDecodeError, StatementLog,
};
use crate::broker::error::{BrokerError, InstallLockFailure};
use crate::db::{
    CheckoutClass, CorePool, IsolationLevel, NotSentCause, ParameterType, PooledConnection,
    RefusalCause, RollbackOutcome, Row, SqlScript, StatementBound, StatementFailure, Transaction,
    TypedStatement,
};

/// Name of the horsies-owned migrations bookkeeping table.
pub const MIGRATIONS_TABLE: &str = "horsies_migrations";

/// Version of the baseline schema migration.
pub const BASELINE_MIGRATION_VERSION: i64 = 1000;

/// Declares the baseline file list once. From one list of file names it
/// builds `BASELINE_SQL_FILES` (the names), `BASELINE_SQL_FILE_TEXTS` (each
/// name with its text) and `BASELINE_SQL` (the texts, concatenated with no
/// separator), so the names and the migration text cannot differ. The files
/// of `core/baseline/` are the single source; the core includes
/// them by relative path and keeps no copy.
macro_rules! baseline_sql_files {
    ($($file:literal),+ $(,)?) => {
        /// The file names of migration 1000, in the order they run.
        #[cfg(test)]
        pub(crate) const BASELINE_SQL_FILES: &[&str] = &[$($file),+];

        /// The files of migration 1000 as `(file name, text)` pairs, in the
        /// order they run. The Python package reads its migration 1000
        /// through these (`horsies._core.baseline_sql_files`).
        pub const BASELINE_SQL_FILE_TEXTS: &[(&str, &str)] = &[
            $(($file, include_str!(concat!("../../baseline/", $file)))),+
        ];

        /// Migration 1000: the files of `BASELINE_SQL_FILES`, in that
        /// order, concatenated with no separator.
        const BASELINE_SQL: &str = concat!(
            $(include_str!(concat!("../../baseline/", $file))),+
        );
    };
}

// The files of `core/baseline/`, in the order they run. A file
// added to the folder without a line here fails
// `baseline_file_list_is_the_whole_folder`.
baseline_sql_files!(
    "0001_tables.sql",
    "0002_indexes.sql",
    "0003_interim_functions.sql",
    "0004_lifecycle_functions.sql",
    "0005_workflow_functions.sql",
    "0006_history_functions.sql",
    "0007_scheduler_monitoring_functions.sql",
    "0008_history_maintenance_functions.sql",
);

/// Version of the migration that adds the phase record of each attempt and
/// the events of each workflow run
/// (`core/migrations/1001_lifecycle_record.sql`).
pub(crate) const LIFECYCLE_RECORD_MIGRATION_VERSION: i64 = 1001;

/// Version of the migration that adds the start command fingerprint of each
/// top-level workflow run
/// (`core/migrations/1002_workflow_start_command_fingerprint.sql`).
pub(crate) const START_COMMAND_FINGERPRINT_MIGRATION_VERSION: i64 = 1002;

/// Version of the migration that adds the cancelled attempt of a running task
/// and the cancel notice to its worker
/// (`core/migrations/1003_attempt_cancelled.sql`).
pub(crate) const ATTEMPT_CANCELLED_MIGRATION_VERSION: i64 = 1003;

/// Version of the migration that adds the name of the worker profile to
/// each worker snapshot (`core/migrations/1004_worker_profile_name.sql`).
pub(crate) const WORKER_PROFILE_NAME_MIGRATION_VERSION: i64 = 1004;

/// Version of the migration that adds the CHECK that a CLAIMED or RUNNING
/// task row holds its claim generation
/// (`core/migrations/1005_claim_generation_by_status.sql`).
pub(crate) const CLAIM_GENERATION_BY_STATUS_MIGRATION_VERSION: i64 = 1005;

/// Version of the migration that adds the client family `TYPESCRIPT` to the
/// CHECK of the family row
/// (`core/migrations/1006_typescript_client_family.sql`).
pub(crate) const TYPESCRIPT_CLIENT_FAMILY_MIGRATION_VERSION: i64 = 1006;

/// The files of `core/migrations/`, one per migration after the baseline,
/// in apply order: the version of the migration, the file name and the text.
/// The core includes them by relative path and keeps no copy. A file added to
/// the folder without a line here fails
/// `later_migration_list_is_the_whole_folder`.
pub(crate) const LATER_MIGRATION_FILES: [(i64, &str, &str); 6] = [
    (
        LIFECYCLE_RECORD_MIGRATION_VERSION,
        "1001_lifecycle_record.sql",
        include_str!("../../migrations/1001_lifecycle_record.sql"),
    ),
    (
        START_COMMAND_FINGERPRINT_MIGRATION_VERSION,
        "1002_workflow_start_command_fingerprint.sql",
        include_str!("../../migrations/1002_workflow_start_command_fingerprint.sql"),
    ),
    (
        ATTEMPT_CANCELLED_MIGRATION_VERSION,
        "1003_attempt_cancelled.sql",
        include_str!("../../migrations/1003_attempt_cancelled.sql"),
    ),
    (
        WORKER_PROFILE_NAME_MIGRATION_VERSION,
        "1004_worker_profile_name.sql",
        include_str!("../../migrations/1004_worker_profile_name.sql"),
    ),
    (
        CLAIM_GENERATION_BY_STATUS_MIGRATION_VERSION,
        "1005_claim_generation_by_status.sql",
        include_str!("../../migrations/1005_claim_generation_by_status.sql"),
    ),
    (
        TYPESCRIPT_CLIENT_FAMILY_MIGRATION_VERSION,
        "1006_typescript_client_family.sql",
        include_str!("../../migrations/1006_typescript_client_family.sql"),
    ),
];

/// Every SQL file of the embedded chain, in run order, with the version of
/// its migration: the files of migration 1000
/// ([`BASELINE_SQL_FILE_TEXTS`]), then the file of each later migration
/// ([`LATER_MIGRATION_FILES`]). The text of one migration is its files,
/// concatenated in this order with no separator. A test harness installs
/// the schema of this build by running every text in this order; the
/// Python package reads the list through `horsies._core`
/// (`migration_sql_files`). Pure.
pub fn migration_sql_file_texts() -> Vec<(i64, &'static str, &'static str)> {
    BASELINE_SQL_FILE_TEXTS
        .iter()
        .map(|(name, text)| (BASELINE_MIGRATION_VERSION, *name, *text))
        .chain(LATER_MIGRATION_FILES.iter().copied())
        .collect()
}

/// The tables file of the baseline.
#[cfg(test)]
pub(crate) const BASELINE_TABLES_SQL: &str = include_str!("../../baseline/0001_tables.sql");
/// The interim functions file of the baseline.
#[cfg(test)]
pub(crate) const BASELINE_INTERIM_FUNCTIONS_SQL: &str =
    include_str!("../../baseline/0003_interim_functions.sql");

/// Bytes of a SHA-384 checksum.
const CHECKSUM_BYTES: usize = 48;

/// Whether the transaction of a migration also writes this client's family
/// row. The install decides it from the position of the migration in the
/// chain it applies ([`family_row_at`]), not from the migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FamilyRow {
    /// After the text and the bookkeeping row, the transaction writes the
    /// row of this family when the table has none ([`RECORD_CLIENT_FAMILY`]).
    Written(ClientFamily),
    /// The transaction writes no family row.
    NotWritten,
}

/// The family row of the transaction of the migration at `position` of
/// `chain`, in the install of a client of family `own`. Pure.
///
/// One rule for every client family: the transaction of the last migration
/// of the chain writes the row, and no other transaction does. The CHECK
/// `horsies_client_family_family_known` admits a family only from the
/// migration that adds it on (`TYPESCRIPT` from migration 1006 on), and
/// after the last migration it admits every family of this build. So the
/// row is written once, after the migration that admits its value, and
/// commits with the last migration.
fn family_row_at(chain: &[EmbeddedMigration], position: usize, own: ClientFamily) -> FamilyRow {
    match position + 1 == chain.len() {
        true => FamilyRow::Written(own),
        false => FamilyRow::NotWritten,
    }
}

/// Whether the transaction of a migration publishes the history readers
/// (`horsies_publish_history_readers`, baseline `0008`) after its text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReaderPublication {
    /// Migration 1000 creates the leaf catalog, the static forever leaf and
    /// the publication function; its transaction publishes the readers, so a
    /// recorded version 1000 always has them.
    PublishedWithMigration,
    /// The migration does not change what the readers read: every
    /// migration after the baseline so far.
    NotPublished,
}

/// One migration of the embedded chain, as written in the source.
struct MigrationSource {
    version: i64,
    description: &'static str,
    script: SqlScript,
    readers: ReaderPublication,
    /// The indexes that the migration gives their declared form outside a
    /// transaction, before its transaction ([`install_exact_indexes`]). The
    /// columns they read exist before the migration: a migration that adds
    /// a column and indexes it is two migrations.
    exact_indexes: &'static [ExactIndex],
    /// The indexes that the migration retires, dropped outside a transaction
    /// after its bookkeeping row commits ([`remove_retired_indexes`]).
    retired_indexes: &'static [RetiredIndex],
}

/// The embedded chain, in apply order: the baseline, then the migrations of
/// [`LATER_MIGRATION_FILES`]. The Python client installs through the core,
/// so both clients apply the same chain.
const MIGRATION_SOURCES: [MigrationSource; 7] = [
    MigrationSource {
        version: BASELINE_MIGRATION_VERSION,
        description: "baseline",
        script: SqlScript::new("migration_1000_baseline", BASELINE_SQL),
        readers: ReaderPublication::PublishedWithMigration,
        exact_indexes: &[],
        retired_indexes: &[],
    },
    // The new columns are nullable and the new constraints on the existing
    // tables are CHECKs. The indexes that the migration builds are on its new
    // table, which no other session can write before the commit, so they are
    // built in the migration transaction: no exact or retired index step.
    MigrationSource {
        version: LATER_MIGRATION_FILES[0].0,
        description: "lifecycle record",
        script: SqlScript::new(
            "migration_1001_lifecycle_record",
            LATER_MIGRATION_FILES[0].2,
        ),
        readers: ReaderPublication::NotPublished,
        exact_indexes: &[],
        retired_indexes: &[],
    },
    // The new columns are nullable and the new constraint is a CHECK. The
    // migration builds no index: the start reads the workflow row by its
    // primary key.
    MigrationSource {
        version: LATER_MIGRATION_FILES[1].0,
        description: "workflow start command fingerprint",
        script: SqlScript::new(
            "migration_1002_workflow_start_command_fingerprint",
            LATER_MIGRATION_FILES[1].2,
        ),
        readers: ReaderPublication::NotPublished,
        exact_indexes: &[],
        retired_indexes: &[],
    },
    // The migration adds no column and builds no index: it replaces two CHECKs
    // of horsies_task_attempts and the function horsies_cancel_task, and adds
    // the read horsies_cancelled_runs.
    MigrationSource {
        version: LATER_MIGRATION_FILES[2].0,
        description: "attempt cancelled",
        script: SqlScript::new(
            "migration_1003_attempt_cancelled",
            LATER_MIGRATION_FILES[2].2,
        ),
        readers: ReaderPublication::NotPublished,
        exact_indexes: &[],
        retired_indexes: &[],
    },
    // The new column is nullable and the new constraint is a CHECK. The
    // migration builds no index: the reads take a snapshot by its worker and
    // its time.
    MigrationSource {
        version: LATER_MIGRATION_FILES[3].0,
        description: "worker profile name",
        script: SqlScript::new(
            "migration_1004_worker_profile_name",
            LATER_MIGRATION_FILES[3].2,
        ),
        readers: ReaderPublication::NotPublished,
        exact_indexes: &[],
        retired_indexes: &[],
    },
    // The migration adds no column and builds no index: it adds one CHECK to
    // horsies_tasks. A CLAIMED or RUNNING row with no claim generation stops
    // it with SQLSTATE 23514, and nothing of it stays.
    MigrationSource {
        version: LATER_MIGRATION_FILES[4].0,
        description: "claim generation by status",
        script: SqlScript::new(
            "migration_1005_claim_generation_by_status",
            LATER_MIGRATION_FILES[4].2,
        ),
        readers: ReaderPublication::NotPublished,
        exact_indexes: &[],
        retired_indexes: &[],
    },
    // The migration adds no column and builds no index: it replaces the CHECK
    // of horsies_client_family, and it does not write or change the family
    // row.
    MigrationSource {
        version: LATER_MIGRATION_FILES[5].0,
        description: "typescript client family",
        script: SqlScript::new(
            "migration_1006_typescript_client_family",
            LATER_MIGRATION_FILES[5].2,
        ),
        readers: ReaderPublication::NotPublished,
        exact_indexes: &[],
        retired_indexes: &[],
    },
];

/// The most rows that one state read returns: `horsies_` table names, and
/// bookkeeping rows (Python `CATALOG_NAME_LIMIT`).
const CATALOG_ROW_LIMIT: i64 = 64;

// A full read of CATALOG_ROW_LIMIT bookkeeping rows holds more rows than the
// chain, so a truncated read is never taken for a start of the chain.
const _: () = assert!((MIGRATION_SOURCES.len() as i64) < CATALOG_ROW_LIMIT);

/// One migration of the embedded chain with its checksum: SHA-384 of the
/// UTF-8 bytes of its text, the form the bookkeeping row stores.
#[derive(Debug)]
pub(crate) struct EmbeddedMigration {
    version: i64,
    description: &'static str,
    script: SqlScript,
    readers: ReaderPublication,
    exact_indexes: &'static [ExactIndex],
    retired_indexes: &'static [RetiredIndex],
    checksum: [u8; CHECKSUM_BYTES],
}

impl EmbeddedMigration {
    fn from_source(source: &MigrationSource) -> EmbeddedMigration {
        let mut checksum = [0_u8; CHECKSUM_BYTES];
        checksum.copy_from_slice(Sha384::digest(source.script.sql().as_bytes()).as_slice());
        EmbeddedMigration {
            version: source.version,
            description: source.description,
            script: source.script,
            readers: source.readers,
            exact_indexes: source.exact_indexes,
            retired_indexes: source.retired_indexes,
            checksum,
        }
    }

    pub(crate) fn version(&self) -> i64 {
        self.version
    }
}

/// The embedded chain with its checksums, in apply order.
pub(crate) fn embedded_migrations() -> &'static [EmbeddedMigration] {
    static MIGRATIONS: OnceLock<Vec<EmbeddedMigration>> = OnceLock::new();
    MIGRATIONS.get_or_init(|| {
        MIGRATION_SOURCES
            .iter()
            .map(EmbeddedMigration::from_source)
            .collect()
    })
}

/// Highest embedded migration version expected by this binary.
pub fn expected_schema_version() -> i64 {
    newest_version(embedded_migrations())
}

fn newest_version(chain: &[EmbeddedMigration]) -> i64 {
    chain
        .last()
        .map(EmbeddedMigration::version)
        .expect("the embedded horsies migration chain is non-empty")
}

/// The versions of the embedded chain, in apply order.
fn shipped_versions() -> Vec<i64> {
    embedded_migrations()
        .iter()
        .map(EmbeddedMigration::version)
        .collect()
}

// ---- The install lock ----

/// The name whose digest is the install lock key (Python
/// `PostgresBroker._schema_advisory_key`, `clients/python/horsies/core/brokers/postgres.py`).
const INSTALL_LOCK_NAME: &str = "horsies:schema:v1";

/// The longest an installer waits for another installer's lock, then
/// refuses with a retryable [`InstallLockFailure::WaitBoundReached`].
pub const INSTALL_LOCK_WAIT: Duration = Duration::from_secs(300);

/// The pause between two tries of the install lock. Each try is a
/// `pg_try_advisory_lock`, so a waiting installer holds no snapshot.
const INSTALL_LOCK_POLL: Duration = Duration::from_millis(25);

/// The most runs of the whole install: the first run, and one more after
/// each deadlock (Python `_SCHEMA_INIT_MAX_ATTEMPTS`).
pub const INSTALL_MAX_ATTEMPTS: u32 = 5;

/// The most actions under the lock: apply the pending migrations, then
/// complete the install (publish the readers, write the family row). A state
/// read follows each action.
const LOCKED_ACTION_BOUND: usize = 2;

/// The session advisory lock key of every installer, Python's and the
/// core's: the first 8 bytes of SHA-256 of [`INSTALL_LOCK_NAME`], read as a
/// big-endian signed integer.
fn install_lock_key() -> i64 {
    let digest = Sha256::digest(INSTALL_LOCK_NAME.as_bytes());
    let mut first = [0_u8; 8];
    first.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(first)
}

/// The key and the wait bound of the install lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InstallLockPlan {
    key: i64,
    wait: Duration,
}

impl InstallLockPlan {
    /// The lock of every installer of a database, Python's and the core's,
    /// waited for at most [`INSTALL_LOCK_WAIT`].
    pub(crate) fn fleet() -> InstallLockPlan {
        InstallLockPlan {
            key: install_lock_key(),
            wait: INSTALL_LOCK_WAIT,
        }
    }
}

// ---- Statements ----

/// The catalog facts of the current schema (Python `_CATALOG_SQL`): which
/// bookkeeping tables exist, and the names of the `horsies_` tables, at most
/// `$1` of them.
static READ_SCHEMA_CATALOG: TypedStatement = TypedStatement::new(
    "read_schema_catalog",
    "SELECT
         to_regclass('horsies_migrations') IS NOT NULL AS migrations_table,
         to_regclass('horsies_schema_version') IS NOT NULL AS schema_version_table,
         to_regclass('horsies_client_family') IS NOT NULL AS family_table,
         ARRAY(
             SELECT c.relname::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = current_schema()
               AND c.relkind IN ('r', 'p')
               AND c.relname LIKE 'horsies\\_%'
             ORDER BY 1
             LIMIT $1
         ) AS horsies_tables",
    &[ParameterType::Int8],
);

/// The bookkeeping rows, newest first, at most `$1`, each with the family
/// row (Python `_MIGRATIONS_WITH_FAMILY_SQL`).
static READ_BOOKKEEPING_WITH_FAMILY: TypedStatement = TypedStatement::new(
    "read_bookkeeping_with_family",
    "SELECT m.version, m.success, m.checksum,
            (SELECT f.family FROM horsies_client_family f) AS family
     FROM horsies_migrations m
     ORDER BY m.version DESC
     LIMIT $1",
    &[ParameterType::Int8],
);

/// The bookkeeping rows of a database with no family table (Python
/// `_MIGRATIONS_SQL`).
static READ_BOOKKEEPING: TypedStatement = TypedStatement::new(
    "read_bookkeeping",
    "SELECT m.version, m.success, m.checksum, NULL::text AS family
     FROM horsies_migrations m
     ORDER BY m.version DESC
     LIMIT $1",
    &[ParameterType::Int8],
);

/// The bookkeeping row of one migration, written in the migration's
/// transaction (Python `_INSERT_MIGRATION_SQL`): version, description,
/// checksum, and the run time of the migration text in nanoseconds.
static RECORD_MIGRATION: TypedStatement = TypedStatement::new(
    "record_migration",
    "INSERT INTO horsies_migrations
         (version, description, installed_on, success, checksum, execution_time)
     VALUES ($1, $2, now(), TRUE, $3, $4)",
    &[
        ParameterType::Int8,
        ParameterType::Text,
        ParameterType::Bytea,
        ParameterType::Int8,
    ],
);

/// The publication of the history readers for the current leaf catalog
/// (`horsies_publish_history_readers`, baseline `0008`), in the caller's
/// READ COMMITTED transaction. One row: `PUBLISHED` or `ALREADY_CURRENT`.
static PUBLISH_HISTORY_READERS: TypedStatement = TypedStatement::new(
    "publish_history_readers",
    "SELECT outcome FROM horsies_publish_history_readers()",
    &[],
);

/// Whether every history reader that `horsies_publish_history_readers`
/// publishes exists (Python `read_database_state`, the reader column of
/// `_CATALOG_SQL`). The list is the baseline's own
/// (`horsies_history_reader_signatures()`, baseline `0008`), so the
/// statement runs only on a database whose bookkeeping holds this build's
/// whole chain.
static READ_READERS_PUBLISHED: TypedStatement = TypedStatement::new(
    "read_readers_published",
    "SELECT NOT EXISTS (
         SELECT 1
         FROM horsies_history_reader_signatures() AS s
         WHERE to_regprocedure(s.signature) IS NULL
     ) AS readers_published",
    &[],
);

/// One try of the install lock; it never waits.
static TRY_INSTALL_LOCK: TypedStatement = TypedStatement::new(
    "try_install_lock",
    "SELECT pg_try_advisory_lock($1) AS acquired",
    &[ParameterType::Int8],
);

/// The release of the install lock: false when the session held no lock.
static RELEASE_INSTALL_LOCK: TypedStatement = TypedStatement::new(
    "release_install_lock",
    "SELECT pg_advisory_unlock($1) AS released",
    &[ParameterType::Int8],
);

// ---- States ----

/// Whether a bookkeeping row records a run that succeeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordedRun {
    Succeeded,
    Failed,
}

/// One row of [`MIGRATIONS_TABLE`].
#[derive(Clone, Debug, PartialEq, Eq)]
struct AppliedMigration {
    version: i64,
    run: RecordedRun,
    checksum: Vec<u8>,
}

/// The bookkeeping of a database whose ledger exists: at most
/// [`CATALOG_ROW_LIMIT`] of its newest rows, in ascending version order, and
/// its family row.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Bookkeeping {
    applied: Vec<AppliedMigration>,
    family: RecordedFamily,
}

/// The row of [`READ_SCHEMA_CATALOG`].
struct CatalogRow {
    migrations_table: bool,
    schema_version_table: bool,
    family_table: bool,
    horsies_tables: Vec<String>,
}

/// The facts of one database that the classification reads.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SchemaFacts {
    /// No `horsies_migrations` table.
    NoLedger {
        schema_version_table: bool,
        horsies_tables: Vec<String>,
    },
    /// `horsies_migrations` exists.
    Ledger(Bookkeeping),
}

/// Whether the history readers of a database exist (Python
/// `ReaderPresence`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReaderPresence {
    /// Every reader of `horsies_history_reader_signatures()` exists.
    Published,
    /// A reader is missing: an install stopped between migration 1000 and
    /// the publication (Python's install published in a transaction of its
    /// own), or a reader was dropped out of band.
    Missing,
}

/// The classification of the facts of a database.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Classification {
    /// The state needs no other fact.
    Settled(DatabaseState),
    /// The bookkeeping holds this build's whole chain, each migration
    /// recorded as succeeded with this build's checksum. The presence of the
    /// history readers is read next ([`READ_READERS_PUBLISHED`]).
    ChainCurrent { family: RecordedFamily },
}

/// What a database holds, as the install sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DatabaseState {
    /// No `horsies_` table in the current schema.
    Empty,
    /// The bookkeeping holds the first `applied` migrations of this build's
    /// chain, each recorded as succeeded with this build's checksum; the rest
    /// of the chain is pending.
    Behind {
        applied: usize,
        family: RecordedFamily,
    },
    /// The bookkeeping holds this build's whole chain, each recorded as
    /// succeeded with this build's checksum.
    Current {
        family: RecordedFamily,
        readers: ReaderPresence,
    },
    /// A database this build does not install into.
    Unsupported(SchemaRefusal),
}

/// A database that this build does not install into. The install changes
/// nothing, except before `IndexNameHeldElsewhere`: the migrations before
/// the refused one stay committed. Not retryable: the same database is
/// refused again.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SchemaRefusal {
    /// `horsies_schema_version` without `horsies_migrations`: a horsies 0.x
    /// Python schema.
    #[error(
        "the database holds a horsies 0.x schema (horsies_schema_version without \
         horsies_migrations; tables: {}); this build does not upgrade a 0.x database: drain it \
         and point the fleet at a new database",
        joined(.tables)
    )]
    ZeroXPython { tables: Vec<String> },
    /// `horsies_` tables with neither bookkeeping table.
    #[error(
        "the database holds horsies tables with no schema bookkeeping (tables: {})",
        joined(.tables)
    )]
    UnknownHorsiesObjects { tables: Vec<String> },
    /// The bookkeeping holds a version above the newest of this build.
    #[error("database schema version {stored} is newer than this build supports ({own})")]
    SchemaNewer { stored: i64, own: i64 },
    /// The bookkeeping is not a start of this build's chain: it has no
    /// baseline row, a gap, or a version this build does not ship (a
    /// horsies-rust 0.x database: its versions end at 65).
    #[error(
        "the schema bookkeeping holds migration versions this build does not ship (found: {}; \
         this build ships {})",
        listed(.versions),
        listed(&shipped_versions())
    )]
    ForeignMigrations { versions: Vec<i64> },
    /// A bookkeeping row is recorded as failed.
    #[error("schema migration {version} is recorded as failed")]
    MigrationIncomplete { version: i64 },
    /// The name of an index that migration `version` declares (exact or
    /// retired) is held by a relation other than an index of its table, or
    /// changed holder while the install renamed it. This migration changed
    /// nothing that its transaction holds; the migrations before it stay
    /// committed. Not retryable: the name needs an operator.
    #[error(
        "index {index} of schema migration {version} is held by a relation other than an index \
         of {table}"
    )]
    IndexNameHeldElsewhere {
        version: i64,
        index: &'static str,
        table: &'static str,
    },
    /// A bookkeeping row's checksum differs from this build's text.
    #[error(
        "schema migration {version} was applied from another text (stored checksum {}, this \
         build {})",
        hex(.stored_checksum),
        hex(.own_checksum)
    )]
    MigrationEdited {
        version: i64,
        stored_checksum: Vec<u8>,
        own_checksum: Vec<u8>,
    },
}

fn joined(names: &[String]) -> String {
    names.join(", ")
}

fn listed(versions: &[i64]) -> String {
    match versions.is_empty() {
        true => "none".to_owned(),
        false => versions
            .iter()
            .map(i64::to_string)
            .collect::<Vec<String>>()
            .join(", "),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// What the install does for one state.
#[derive(Clone, Debug, PartialEq, Eq)]
enum InstallAction {
    /// Nothing: the database holds this build's chain and this client's
    /// family.
    Ready,
    /// Apply the migrations of the chain from this index on.
    ApplyFrom(usize),
    /// Publish the history readers, then write this client's family row
    /// when the database has none (Python `CompleteInstall`). Both steps are
    /// idempotent.
    CompleteInstall,
    /// Change nothing and refuse.
    Refuse(InstallRefusal),
}

/// Why the install changes nothing and refuses.
#[derive(Clone, Debug, PartialEq, Eq)]
enum InstallRefusal {
    Schema(SchemaRefusal),
    OtherFamily {
        stored: ClientFamily,
        own: ClientFamily,
    },
}

// ---- Errors ----

/// Why a schema install failed.
#[derive(Debug, thiserror::Error)]
pub enum SchemaInstallError {
    /// The database holds something this build does not install into.
    /// Nothing was changed, except before `IndexNameHeldElsewhere` (the
    /// migrations before the refused one stay committed). Not retryable.
    #[error("schema install refused: {0}")]
    Refused(SchemaRefusal),
    /// The database was installed by another client family. Nothing was
    /// changed. Not retryable.
    #[error(
        "this database was installed by the {} client; this is the {} client, which installs \
         only into a database of its own family",
        family_name(.stored),
        family_name(.own)
    )]
    OtherFamily {
        stored: ClientFamily,
        own: ClientFamily,
    },
    /// The family row holds a value that is not a client family: corrupt
    /// stored data. The state read found it before any write of this
    /// install. Not retryable.
    #[error("schema install refused: {0}")]
    UnknownStoredFamily(#[from] UnknownStoredFamily),
    /// The install lock could not be used ([`InstallLockFailure`]).
    #[error("schema install lock: {0}")]
    Lock(InstallLockFailure),
    /// A statement of the install failed with one of the statement layer's
    /// three outcomes; the statement name says which step. A failure inside
    /// a migration's transaction rolls that migration back; migrations
    /// committed before it stay, and the next install applies the rest.
    /// `OutcomeUnknown` at a `COMMIT` means that migration may have
    /// committed.
    #[error("schema install: {0}")]
    Statement(#[from] StatementFailure),
    /// A row of the state read does not have its shape. Not retryable.
    #[error("schema install: {0}")]
    RowDecode(#[from] RowDecodeError),
}

fn family_name(family: &ClientFamily) -> &'static str {
    family.as_str()
}

impl SchemaInstallError {
    /// Whether the install met a deadlock (40P01), so the whole install runs
    /// again.
    fn is_deadlock(&self) -> bool {
        match self {
            SchemaInstallError::Statement(StatementFailure::StatementRefused { cause, .. }) => {
                is_deadlock_refusal(cause)
            }
            SchemaInstallError::Statement(
                StatementFailure::NotSent { .. } | StatementFailure::OutcomeUnknown { .. },
            )
            | SchemaInstallError::Refused(_)
            | SchemaInstallError::OtherFamily { .. }
            | SchemaInstallError::UnknownStoredFamily(_)
            | SchemaInstallError::Lock(_)
            | SchemaInstallError::RowDecode(_) => false,
        }
    }
}

fn is_deadlock_refusal(cause: &RefusalCause) -> bool {
    is_deadlock_code(cause.server_error().code())
}

fn is_deadlock_code(code: &SqlState) -> bool {
    *code == SqlState::T_R_DEADLOCK_DETECTED
}

impl From<InstallRefusal> for SchemaInstallError {
    fn from(refusal: InstallRefusal) -> Self {
        match refusal {
            InstallRefusal::Schema(refusal) => SchemaInstallError::Refused(refusal),
            InstallRefusal::OtherFamily { stored, own } => {
                SchemaInstallError::OtherFamily { stored, own }
            }
        }
    }
}

/// The broker's form of each install error: the same refusal, lock failure,
/// statement failure or row error.
impl From<SchemaInstallError> for BrokerError {
    fn from(error: SchemaInstallError) -> Self {
        match error {
            SchemaInstallError::Refused(refusal) => BrokerError::SchemaRefused(refusal),
            SchemaInstallError::OtherFamily { stored, own } => BrokerError::ClientFamilyMismatch {
                stored: RecordedFamily::Recorded(stored),
                own,
            },
            SchemaInstallError::UnknownStoredFamily(unknown) => {
                BrokerError::UnknownStoredFamily(unknown)
            }
            SchemaInstallError::Lock(failure) => BrokerError::InstallLock(failure),
            SchemaInstallError::Statement(failure) => BrokerError::Statement(failure),
            SchemaInstallError::RowDecode(error) => BrokerError::RowDecode(error),
        }
    }
}

// ---- Classification and decision (pure) ----

/// Classifies the facts of one database against `chain` (Python
/// `classify_database`, with a chain of migrations in place of one).
fn classify(facts: SchemaFacts, chain: &[EmbeddedMigration]) -> Classification {
    match facts {
        SchemaFacts::NoLedger { horsies_tables, .. } if horsies_tables.is_empty() => {
            Classification::Settled(DatabaseState::Empty)
        }
        SchemaFacts::NoLedger {
            schema_version_table: true,
            horsies_tables,
        } => Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::ZeroXPython {
            tables: horsies_tables,
        })),
        SchemaFacts::NoLedger {
            schema_version_table: false,
            horsies_tables,
        } => Classification::Settled(DatabaseState::Unsupported(
            SchemaRefusal::UnknownHorsiesObjects {
                tables: horsies_tables,
            },
        )),
        SchemaFacts::Ledger(bookkeeping) => classify_bookkeeping(bookkeeping, chain),
    }
}

/// Classifies the bookkeeping rows: a version above the chain, then rows
/// that do not start the chain, then a failed or changed row.
fn classify_bookkeeping(bookkeeping: Bookkeeping, chain: &[EmbeddedMigration]) -> Classification {
    let Bookkeeping { applied, family } = bookkeeping;
    let own = newest_version(chain);
    let versions: Vec<i64> = applied.iter().map(|row| row.version).collect();
    if let Some(&stored) = versions.last().filter(|stored| **stored > own) {
        return Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored,
            own,
        }));
    }
    if !starts_the_chain(&versions, chain) {
        return Classification::Settled(DatabaseState::Unsupported(
            SchemaRefusal::ForeignMigrations { versions },
        ));
    }
    match (
        first_broken_row(&applied, chain),
        applied.len() == chain.len(),
    ) {
        (Some(refusal), _) => Classification::Settled(DatabaseState::Unsupported(refusal)),
        (None, true) => Classification::ChainCurrent { family },
        (None, false) => Classification::Settled(DatabaseState::Behind {
            applied: applied.len(),
            family,
        }),
    }
}

/// Whether `versions` (ascending) are the first versions of `chain`: at
/// least the baseline, in chain order, with no gap and nothing else.
fn starts_the_chain(versions: &[i64], chain: &[EmbeddedMigration]) -> bool {
    !versions.is_empty()
        && versions.len() <= chain.len()
        && versions
            .iter()
            .zip(chain)
            .all(|(version, migration)| *version == migration.version)
}

/// The refusal of the first bookkeeping row that is recorded as failed or
/// holds another checksum than this build's.
fn first_broken_row(
    applied: &[AppliedMigration],
    chain: &[EmbeddedMigration],
) -> Option<SchemaRefusal> {
    applied.iter().zip(chain).find_map(|(row, migration)| {
        match (
            row.run,
            row.checksum.as_slice() == migration.checksum.as_slice(),
        ) {
            (RecordedRun::Failed, _) => Some(SchemaRefusal::MigrationIncomplete {
                version: row.version,
            }),
            (RecordedRun::Succeeded, false) => Some(SchemaRefusal::MigrationEdited {
                version: row.version,
                stored_checksum: row.checksum.clone(),
                own_checksum: migration.checksum.to_vec(),
            }),
            (RecordedRun::Succeeded, true) => None,
        }
    })
}

/// What the install does for `state`, for a client of family `own`
/// (Python `decide_install`).
fn decide(state: &DatabaseState, own: ClientFamily) -> InstallAction {
    match state {
        DatabaseState::Empty => InstallAction::ApplyFrom(0),
        DatabaseState::Behind { applied, family } => match family {
            RecordedFamily::Recorded(stored) if *stored != own => other_family(*stored, own),
            RecordedFamily::Recorded(_) | RecordedFamily::NotRecorded => {
                InstallAction::ApplyFrom(*applied)
            }
        },
        DatabaseState::Current { family, readers } => match (family, readers) {
            (RecordedFamily::Recorded(stored), _) if *stored != own => other_family(*stored, own),
            (RecordedFamily::Recorded(_), ReaderPresence::Published) => InstallAction::Ready,
            (RecordedFamily::Recorded(_), ReaderPresence::Missing)
            | (RecordedFamily::NotRecorded, ReaderPresence::Published | ReaderPresence::Missing) => {
                InstallAction::CompleteInstall
            }
        },
        DatabaseState::Unsupported(refusal) => {
            InstallAction::Refuse(InstallRefusal::Schema(refusal.clone()))
        }
    }
}

fn other_family(stored: ClientFamily, own: ClientFamily) -> InstallAction {
    InstallAction::Refuse(InstallRefusal::OtherFamily { stored, own })
}

// ---- Index steps of a migration ----

/// The suffix of the name under which a replacement index is built.
const BUILD_SUFFIX: &str = "_build";

/// The suffix of the name to which an index on its way out is renamed
/// before its concurrent drop.
const CLAIM_SUFFIX: &str = "_claim";

/// The longest declared index name: the build and claim names stay inside
/// PostgreSQL's 63-byte identifier limit. A test of the chain checks every
/// declared name against it.
#[cfg_attr(not(test), allow(dead_code))]
const MAX_STEP_INDEX_NAME_BYTES: usize = crate::db::MAX_IDENTIFIER_BYTES - BUILD_SUFFIX.len();

const _: () = assert!(BUILD_SUFFIX.len() == CLAIM_SUFFIX.len());

/// One index that a migration gives its declared form outside a transaction
/// (Python 0.7.0 `RecoveryIndex`, in `core/schemas/recovery.py` of that
/// release's package). Built
/// by [`exact_index!`], which writes every statement text at compile time
/// from the name, the table and the definition, so no text is built at run
/// time. Each name is an unquoted identifier of at most
/// [`MAX_STEP_INDEX_NAME_BYTES`] bytes (checked by a test of the chain).
#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ExactIndex {
    name: &'static str,
    table: &'static str,
    build_name: &'static str,
    claim_name: &'static str,
    /// `CREATE INDEX CONCURRENTLY <name> ON <table> <definition>`.
    create: SqlScript,
    /// The same index under the build name.
    build: SqlScript,
    /// The declared form on a temporary copy of the table's columns
    /// (`horsies_expected_index_idx`), dropped with the transaction.
    expected: SqlScript,
    /// `ALTER INDEX <name> RENAME TO <claim name>`.
    claim: SqlScript,
    /// `ALTER INDEX <build name> RENAME TO <name>`.
    promote: SqlScript,
    /// `DROP INDEX CONCURRENTLY <build name>`.
    drop_build: SqlScript,
    /// `DROP INDEX CONCURRENTLY <claim name>`.
    drop_claim: SqlScript,
}

/// One index that a migration retires (Python 0.7.0 `RetiredIndex`). Built
/// by [`retired_index!`].
#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct RetiredIndex {
    name: &'static str,
    table: &'static str,
    claim_name: &'static str,
    /// `ALTER INDEX <name> RENAME TO <claim name>`.
    claim: SqlScript,
    /// `DROP INDEX CONCURRENTLY <claim name>`.
    drop_claim: SqlScript,
}

/// An [`ExactIndex`] from its name, its table and its definition (the text
/// after `ON <table>`: key, `INCLUDE` columns, predicate).
#[cfg_attr(not(test), allow(unused_macros))]
macro_rules! exact_index {
    ($name:literal, $table:literal, $definition:literal) => {
        ExactIndex {
            name: $name,
            table: $table,
            build_name: concat!($name, "_build"),
            claim_name: concat!($name, "_claim"),
            create: SqlScript::new(
                concat!("create_index_", $name),
                concat!(
                    "CREATE INDEX CONCURRENTLY ",
                    $name,
                    " ON ",
                    $table,
                    " ",
                    $definition
                ),
            ),
            build: SqlScript::new(
                concat!("build_index_", $name),
                concat!(
                    "CREATE INDEX CONCURRENTLY ",
                    $name,
                    "_build ON ",
                    $table,
                    " ",
                    $definition
                ),
            ),
            expected: SqlScript::new(
                concat!("expected_index_", $name),
                concat!(
                    "CREATE TEMP TABLE horsies_expected_index (LIKE ",
                    $table,
                    ") ON COMMIT DROP; ",
                    "CREATE INDEX horsies_expected_index_idx ON horsies_expected_index ",
                    $definition
                ),
            ),
            claim: SqlScript::new(
                concat!("claim_index_", $name),
                concat!("ALTER INDEX ", $name, " RENAME TO ", $name, "_claim"),
            ),
            promote: SqlScript::new(
                concat!("promote_index_", $name),
                concat!("ALTER INDEX ", $name, "_build RENAME TO ", $name),
            ),
            drop_build: SqlScript::new(
                concat!("drop_build_", $name),
                concat!("DROP INDEX CONCURRENTLY ", $name, "_build"),
            ),
            drop_claim: SqlScript::new(
                concat!("drop_claim_", $name),
                concat!("DROP INDEX CONCURRENTLY ", $name, "_claim"),
            ),
        }
    };
}

/// A [`RetiredIndex`] from its name and its table.
#[cfg_attr(not(test), allow(unused_macros))]
macro_rules! retired_index {
    ($name:literal, $table:literal) => {
        RetiredIndex {
            name: $name,
            table: $table,
            claim_name: concat!($name, "_claim"),
            claim: SqlScript::new(
                concat!("claim_retired_index_", $name),
                concat!("ALTER INDEX ", $name, " RENAME TO ", $name, "_claim"),
            ),
            drop_claim: SqlScript::new(
                concat!("drop_retired_index_", $name),
                concat!("DROP INDEX CONCURRENTLY ", $name, "_claim"),
            ),
        }
    };
}

/// Who holds the relation name `$1`: `ABSENT` (no relation), `TABLE` (an
/// index of the table `$2`), `OTHER` (another relation, or an index of
/// another table). Python 0.7.0 `_relation_owner`.
static READ_INDEX_HOLDER: TypedStatement = TypedStatement::new(
    "read_index_holder",
    "SELECT CASE
         WHEN to_regclass($1) IS NULL THEN 'ABSENT'
         WHEN EXISTS (
             SELECT 1 FROM pg_index AS i
             WHERE i.indexrelid = to_regclass($1) AND i.indrelid = to_regclass($2)
         ) THEN 'TABLE'
         ELSE 'OTHER'
     END AS holder",
    &[ParameterType::Text, ParameterType::Text],
);

/// Whether the index `$1` of the table `$2` has the form of
/// `horsies_expected_index_idx`, which the same transaction built from the
/// declared definition (Python 0.7.0 `_INDEX_FORMS_MATCH_SQL`): access
/// method, validity, readiness, liveness, uniqueness, exclusion, the key and
/// INCLUDE columns as `pg_get_indexdef` writes each, operator classes,
/// collations, options and predicate. An invalid index (an interrupted
/// concurrent build) does not match.
static READ_INDEX_FORM_MATCHES: TypedStatement = TypedStatement::new(
    "read_index_form_matches",
    "WITH shape AS (
         SELECT ic.oid AS index_oid,
                jsonb_build_object(
                    'method', am.amname, 'valid', i.indisvalid,
                    'ready', i.indisready, 'live', i.indislive,
                    'unique', i.indisunique, 'exclusion', i.indisexclusion,
                    'immediate', i.indimmediate, 'key_count', i.indnkeyatts,
                    'attribute_count', i.indnatts,
                    'has_expressions', i.indexprs IS NOT NULL,
                    'columns', (
                        SELECT jsonb_agg(pg_get_indexdef(i.indexrelid, n, FALSE) ORDER BY n)
                        FROM generate_series(1, i.indnatts) AS n
                    ),
                    'operator_classes', to_jsonb(i.indclass::oid[]),
                    'collations', to_jsonb(i.indcollation::oid[]),
                    'options', to_jsonb(i.indoption::smallint[]),
                    'predicate', pg_get_expr(i.indpred, i.indrelid)
                ) AS form
         FROM pg_index AS i
         JOIN pg_class AS ic ON ic.oid = i.indexrelid
         JOIN pg_am AS am ON am.oid = ic.relam
     )
     SELECT (
         SELECT s.form FROM shape AS s
         WHERE s.index_oid = to_regclass('horsies_expected_index_idx')
     ) IS NOT DISTINCT FROM (
         SELECT s.form FROM shape AS s
         JOIN pg_index AS i ON i.indexrelid = s.index_oid
         WHERE s.index_oid = to_regclass($1) AND i.indrelid = to_regclass($2)
     ) AS matches",
    &[ParameterType::Text, ParameterType::Text],
);

/// Who holds the name of an index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexHolder {
    /// No relation has the name.
    Absent,
    /// An index of the declared table has the name.
    Table,
    /// Another relation has the name.
    Other,
}

/// The state of an exact index under its name (Python 0.7.0 `IndexState`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExactIndexState {
    /// No relation has the name.
    Absent,
    /// The table holds an index of the name, valid and of the declared form.
    Matches,
    /// The table holds an index of the name of another form, or an invalid
    /// one.
    Differs,
}

/// Reads who holds `name`, against `table` ([`READ_INDEX_HOLDER`]).
async fn index_holder_on(
    connection: &mut PooledConnection,
    name: &'static str,
    table: &'static str,
    log: StatementLog,
) -> Result<IndexHolder, SchemaInstallError> {
    let statement = &READ_INDEX_HOLDER;
    let rows = query_on(
        connection,
        log,
        &statement.bind().value(&name).value(&table).finish(),
    )
    .await?;
    index_holder_of(&one_row(rows, statement)?)
}

/// [`index_holder_on`] inside `transaction`.
async fn index_holder_in(
    transaction: &mut Transaction<'_>,
    name: &'static str,
    table: &'static str,
    log: StatementLog,
) -> Result<IndexHolder, SchemaInstallError> {
    let statement = &READ_INDEX_HOLDER;
    let rows = query_in(
        transaction,
        log,
        &statement.bind().value(&name).value(&table).finish(),
    )
    .await?;
    index_holder_of(&one_row(rows, statement)?)
}

fn index_holder_of(row: &Row) -> Result<IndexHolder, SchemaInstallError> {
    let statement = &READ_INDEX_HOLDER;
    let holder: String = column(row, statement, "holder")?;
    match holder.as_str() {
        "ABSENT" => Ok(IndexHolder::Absent),
        "TABLE" => Ok(IndexHolder::Table),
        "OTHER" => Ok(IndexHolder::Other),
        other => Err(SchemaInstallError::RowDecode(RowDecodeError::Shape {
            statement: statement.name(),
            reason: format!("unknown index holder {other:?}"),
        })),
    }
}

/// The refusal for a name of migration `version` that another relation
/// holds.
fn name_held_elsewhere(
    version: i64,
    index: &'static str,
    table: &'static str,
) -> SchemaInstallError {
    SchemaInstallError::Refused(SchemaRefusal::IndexNameHeldElsewhere {
        version,
        index,
        table,
    })
}

/// Gives each exact index of `migration` its declared form under its name,
/// outside a transaction (Python 0.7.0 `_install_exact_indexes`):
/// 1. the build and claim indexes of the name that an interrupted install
///    left on the table are dropped concurrently;
/// 2. an index of the declared form is kept; an absent one is built
///    concurrently under its name; one of another form, or an invalid one,
///    is replaced ([`replace_exact_index`]), so the table keeps an index of
///    the name throughout.
///
/// Errors: `Refused(IndexNameHeldElsewhere)` when a relation other than an
/// index of the table holds the name, the build name or the claim name;
/// `Statement` or `RowDecode` when a step fails. A concurrent build that
/// fails leaves an invalid index, which the next install replaces or drops.
async fn install_exact_indexes(
    connection: &mut PooledConnection,
    migration: &EmbeddedMigration,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    for index in migration.exact_indexes {
        drop_leftover(
            connection,
            migration.version,
            index.build_name,
            index.table,
            &index.drop_build,
            log,
        )
        .await?;
        drop_leftover(
            connection,
            migration.version,
            index.claim_name,
            index.table,
            &index.drop_claim,
            log,
        )
        .await?;
        match inspect_exact_index(connection, migration.version, index, log).await? {
            ExactIndexState::Matches => {}
            ExactIndexState::Absent => {
                tracing::info!(
                    index = index.name,
                    "building an index of a schema migration"
                );
                connection
                    .run_script(&index.create, StatementBound::ConnectionTimeouts)
                    .await?;
            }
            ExactIndexState::Differs => {
                tracing::info!(
                    index = index.name,
                    "replacing an index of a schema migration"
                );
                replace_exact_index(connection, migration.version, index, log).await?;
            }
        }
    }
    Ok(())
}

/// Drops the index `name` with `drop` when an index of `table` holds the
/// name (a build or claim index that an interrupted install left).
///
/// Errors: `Refused(IndexNameHeldElsewhere)` when another relation holds the
/// name; `Statement` or `RowDecode` when a step fails.
async fn drop_leftover(
    connection: &mut PooledConnection,
    version: i64,
    name: &'static str,
    table: &'static str,
    drop: &SqlScript,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    match index_holder_on(connection, name, table, log).await? {
        IndexHolder::Absent => Ok(()),
        IndexHolder::Other => Err(name_held_elsewhere(version, name, table)),
        IndexHolder::Table => {
            tracing::info!(
                index = name,
                "dropping an index that an interrupted install left"
            );
            Ok(connection
                .run_script(drop, StatementBound::ConnectionTimeouts)
                .await?)
        }
    }
}

/// Reads the state of `index` in a transaction that is rolled back: the
/// declared form is built on a temporary table and compared
/// ([`READ_INDEX_FORM_MATCHES`]). Changes nothing.
///
/// Errors: `Refused(IndexNameHeldElsewhere)` when another relation holds the
/// name; `Statement` or `RowDecode` when a step fails.
async fn inspect_exact_index(
    connection: &mut PooledConnection,
    version: i64,
    index: &ExactIndex,
    log: StatementLog,
) -> Result<ExactIndexState, SchemaInstallError> {
    let mut transaction = connection.begin(IsolationLevel::ReadCommitted).await?;
    let state = inspect_in(&mut transaction, version, index, log).await;
    // The transaction read the catalog and built the temporary form only.
    let _rolled_back: RollbackOutcome = transaction.rollback().await;
    state
}

async fn inspect_in(
    transaction: &mut Transaction<'_>,
    version: i64,
    index: &ExactIndex,
    log: StatementLog,
) -> Result<ExactIndexState, SchemaInstallError> {
    match index_holder_in(transaction, index.name, index.table, log).await? {
        IndexHolder::Absent => return Ok(ExactIndexState::Absent),
        IndexHolder::Other => return Err(name_held_elsewhere(version, index.name, index.table)),
        IndexHolder::Table => {}
    }
    transaction
        .run_script(&index.expected, StatementBound::ConnectionTimeouts)
        .await?;
    let statement = &READ_INDEX_FORM_MATCHES;
    let rows = query_in(
        transaction,
        log,
        &statement
            .bind()
            .value(&index.name)
            .value(&index.table)
            .finish(),
    )
    .await?;
    let matches: bool = column(&one_row(rows, statement)?, statement, "matches")?;
    Ok(match matches {
        true => ExactIndexState::Matches,
        false => ExactIndexState::Differs,
    })
}

/// Replaces the index of the name by the declared form (Python 0.7.0
/// `_replace_index`): builds it concurrently under the build name, then in
/// one transaction renames the old index to the claim name and the built
/// one to the name ([`swap_in_built_index`]), then drops the claim index
/// concurrently. Readers of the table see the old index or the new one under
/// the name, never neither.
///
/// Errors: those of the steps. A failure leaves the old index under the
/// name, or under the claim name next to the new one; the next install
/// drops the leftover.
async fn replace_exact_index(
    connection: &mut PooledConnection,
    version: i64,
    index: &ExactIndex,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    connection
        .run_script(&index.build, StatementBound::ConnectionTimeouts)
        .await?;
    swap_in_built_index(connection, version, index, log).await?;
    Ok(connection
        .run_script(&index.drop_claim, StatementBound::ConnectionTimeouts)
        .await?)
}

/// The rename pair of [`replace_exact_index`], in one READ COMMITTED
/// transaction (Python 0.7.0 `_swap_in_built_index`): the name must still
/// be an index of the table, and after the rename the claim name must be
/// that index.
///
/// Errors: `Refused(IndexNameHeldElsewhere)` when the name is no longer an
/// index of the table, or the claim name is another relation after the
/// rename; nothing is renamed then. `Statement` or `RowDecode` when a step
/// fails; the transaction is rolled back.
async fn swap_in_built_index(
    connection: &mut PooledConnection,
    version: i64,
    index: &ExactIndex,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let mut transaction = connection.begin(IsolationLevel::ReadCommitted).await?;
    match swap_in(&mut transaction, version, index, log).await {
        Ok(()) => Ok(transaction.commit().await?),
        Err(failure) => {
            let _rolled_back: RollbackOutcome = transaction.rollback().await;
            Err(failure)
        }
    }
}

async fn swap_in(
    transaction: &mut Transaction<'_>,
    version: i64,
    index: &ExactIndex,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    match index_holder_in(transaction, index.name, index.table, log).await? {
        IndexHolder::Table => {}
        IndexHolder::Absent | IndexHolder::Other => {
            return Err(name_held_elsewhere(version, index.name, index.table))
        }
    }
    transaction
        .run_script(&index.claim, StatementBound::ConnectionTimeouts)
        .await?;
    match index_holder_in(transaction, index.claim_name, index.table, log).await? {
        IndexHolder::Table => {}
        IndexHolder::Absent | IndexHolder::Other => {
            return Err(name_held_elsewhere(version, index.name, index.table))
        }
    }
    Ok(transaction
        .run_script(&index.promote, StatementBound::ConnectionTimeouts)
        .await?)
}

/// Drops each retired index of `migrations` concurrently, outside a
/// transaction, after the bookkeeping row of the last of them committed
/// (Python 0.7.0 `remove_retired_indexes`): the claim index that an
/// interrupted removal left is dropped; an index of the name on its table is
/// renamed to the claim name (in a transaction that checks the holder before
/// and after) and dropped. An absent name is skipped.
///
/// Errors: `Refused(IndexNameHeldElsewhere)` when another relation holds the
/// name or the claim name; `Statement` or `RowDecode` when a step fails. A
/// failed removal leaves the index, or its claim index, for the next
/// migration that an install applies.
async fn remove_retired_indexes(
    connection: &mut PooledConnection,
    migrations: &[EmbeddedMigration],
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    for migration in migrations {
        for index in migration.retired_indexes {
            drop_leftover(
                connection,
                migration.version,
                index.claim_name,
                index.table,
                &index.drop_claim,
                log,
            )
            .await?;
            match claim_retired_index(connection, migration.version, index, log).await? {
                RetiredClaim::Claimed => {
                    tracing::info!(index = index.name, "dropping a retired index");
                    connection
                        .run_script(&index.drop_claim, StatementBound::ConnectionTimeouts)
                        .await?;
                }
                RetiredClaim::Absent => {}
            }
        }
    }
    Ok(())
}

/// What [`claim_retired_index`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetiredClaim {
    /// No relation has the name: nothing to remove.
    Absent,
    /// The index was renamed to its claim name.
    Claimed,
}

/// Renames the retired index to its claim name in one READ COMMITTED
/// transaction when an index of its table holds the name (Python 0.7.0
/// `_claim_retired_index`).
///
/// Errors: `Refused(IndexNameHeldElsewhere)` when another relation holds the
/// name, or the claim name after the rename; the transaction is rolled back.
async fn claim_retired_index(
    connection: &mut PooledConnection,
    version: i64,
    index: &RetiredIndex,
    log: StatementLog,
) -> Result<RetiredClaim, SchemaInstallError> {
    let mut transaction = connection.begin(IsolationLevel::ReadCommitted).await?;
    match claim_in(&mut transaction, version, index, log).await {
        Ok(claim) => {
            transaction.commit().await?;
            Ok(claim)
        }
        Err(failure) => {
            let _rolled_back: RollbackOutcome = transaction.rollback().await;
            Err(failure)
        }
    }
}

async fn claim_in(
    transaction: &mut Transaction<'_>,
    version: i64,
    index: &RetiredIndex,
    log: StatementLog,
) -> Result<RetiredClaim, SchemaInstallError> {
    match index_holder_in(transaction, index.name, index.table, log).await? {
        IndexHolder::Absent => return Ok(RetiredClaim::Absent),
        IndexHolder::Other => return Err(name_held_elsewhere(version, index.name, index.table)),
        IndexHolder::Table => {}
    }
    transaction
        .run_script(&index.claim, StatementBound::ConnectionTimeouts)
        .await?;
    match index_holder_in(transaction, index.claim_name, index.table, log).await? {
        IndexHolder::Table => Ok(RetiredClaim::Claimed),
        IndexHolder::Absent | IndexHolder::Other => {
            Err(name_held_elsewhere(version, index.name, index.table))
        }
    }
}

// ---- The install ----

/// Installs the schema for a client of family `own` through `pool`, the
/// session-work pool ([`crate::broker::pools::BrokerPools::session_work`]):
/// the steps of the module documentation.
///
/// Writes only under the install lock; each migration commits with its
/// bookkeeping row, and the last migration of the chain also with this
/// client's family row ([`family_row_at`]). Does not check the family before
/// the writes of other operations
/// (`PostgresBroker::ensure_client_family_for_write` does).
///
/// Errors ([`SchemaInstallError`]):
/// - `Refused`, `OtherFamily`: nothing was changed; not retryable.
/// - `Lock(WaitBoundReached)`: another installer held the lock for the
///   whole wait; this call changed nothing; retryable.
/// - `Lock(OwnershipLost)`: the install completed and the release found no
///   lock; not retryable.
/// - `Statement`: a statement failed, a deadlock on the last run included.
///   Migrations committed before the failure stay. On a database with no
///   family row, a failure before the last migration commits leaves none;
///   the next install, of any client family, applies the rest of the chain
///   and writes its own row.
/// - `RowDecode`: a row of the state read has another shape; not retryable.
pub(crate) async fn install_schema_on(
    pool: &CorePool,
    own: ClientFamily,
    lock: InstallLockPlan,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let mut attempt: u32 = 1;
    loop {
        match install_once(pool, own, lock, log).await {
            Err(error) if error.is_deadlock() && attempt < INSTALL_MAX_ATTEMPTS => {
                let backoff = deadlock_backoff(attempt);
                tracing::warn!(
                    attempt,
                    backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                    "the schema install hit a deadlock; it runs again",
                );
                sleep(backoff).await;
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

/// The pause before the next run after a deadlock: 50 ms plus a jitter of
/// up to 200 ms times 2^(attempt - 1) (Python `_run_fleet_schema_init`).
fn deadlock_backoff(attempt: u32) -> Duration {
    let scale = 1_u64 << attempt.saturating_sub(1).min(INSTALL_MAX_ATTEMPTS);
    let jitter_ms = rand::thread_rng().gen_range(0..=200_u64) * scale;
    Duration::from_millis(50 + jitter_ms)
}

/// One run: read the state with no lock, and only when a write is due take
/// the lock and act (Python `_install_once`).
async fn install_once(
    pool: &CorePool,
    own: ClientFamily,
    lock: InstallLockPlan,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let state = read_state_unlocked(pool, log).await?;
    match decide(&state, own) {
        InstallAction::Ready => return Ok(()),
        InstallAction::Refuse(refusal) => return Err(refusal.into()),
        InstallAction::ApplyFrom(_) | InstallAction::CompleteInstall => {}
    }
    let mut session = LockSession::acquire(pool, lock, log).await?;
    let installed = install_under_lock(&mut session.connection, own, log).await;
    let released = session.release(lock.key, log).await;
    match (installed, released) {
        (Ok(()), released) => released,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(lost)) => {
            tracing::warn!(
                error = %lost,
                "the schema install lock was not released cleanly after a failed install: {lost}",
            );
            Err(error)
        }
    }
}

/// Reads the state on a connection that is returned to the pool after the
/// read.
async fn read_state_unlocked(
    pool: &CorePool,
    log: StatementLog,
) -> Result<DatabaseState, SchemaInstallError> {
    let mut connection = checkout(pool, &READ_SCHEMA_CATALOG).await?;
    read_state(&mut connection, log).await
}

/// Checks out a session-work connection whose first statement is `first`.
/// Install work passes the reserve gate ([`CheckoutClass::Batch`]).
async fn checkout(
    pool: &CorePool,
    first: &TypedStatement,
) -> Result<PooledConnection, SchemaInstallError> {
    pool.checkout(CheckoutClass::Batch).await.map_err(|cause| {
        SchemaInstallError::Statement(StatementFailure::NotSent {
            statement: first.name(),
            cause: NotSentCause::Checkout(cause),
        })
    })
}

/// Reads the state on the connection that holds the lock and acts until the
/// state is current or refused (Python `_install_under_lock`).
///
/// # Panics
///
/// When [`LOCKED_ACTION_BOUND`] actions did not reach a final state: each
/// action commits and the lock keeps other installers out, so the state
/// after them is current or refused.
async fn install_under_lock(
    connection: &mut PooledConnection,
    own: ClientFamily,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let mut state = read_state(connection, log).await?;
    for _action in 0..LOCKED_ACTION_BOUND {
        match decide(&state, own) {
            InstallAction::Ready => return Ok(()),
            InstallAction::Refuse(refusal) => return Err(refusal.into()),
            InstallAction::ApplyFrom(first) => apply_pending(connection, first, own, log).await?,
            InstallAction::CompleteInstall => complete_install(connection, own, log).await?,
        }
        state = read_state(connection, log).await?;
    }
    match decide(&state, own) {
        InstallAction::Ready => Ok(()),
        InstallAction::Refuse(refusal) => Err(refusal.into()),
        action @ (InstallAction::ApplyFrom(_) | InstallAction::CompleteInstall) => panic!(
            "the schema install still decides {action:?} for {state:?} after \
             {LOCKED_ACTION_BOUND} actions under the install lock"
        ),
    }
}

/// Reads and classifies the horsies objects of the current schema (Python
/// `read_database_state`). One statement on an empty database, two
/// otherwise, three on a database that holds this build's chain. Changes
/// nothing.
async fn read_state(
    connection: &mut PooledConnection,
    log: StatementLog,
) -> Result<DatabaseState, SchemaInstallError> {
    let limit = CATALOG_ROW_LIMIT;
    let rows = query_on(
        connection,
        log,
        &READ_SCHEMA_CATALOG.bind().value(&limit).finish(),
    )
    .await?;
    let catalog = catalog_row(&one_row(rows, &READ_SCHEMA_CATALOG)?)?;
    let facts = match (catalog.migrations_table, catalog.family_table) {
        (false, _) => SchemaFacts::NoLedger {
            schema_version_table: catalog.schema_version_table,
            horsies_tables: catalog.horsies_tables,
        },
        (true, true) => SchemaFacts::Ledger(
            read_bookkeeping(connection, log, &READ_BOOKKEEPING_WITH_FAMILY).await?,
        ),
        (true, false) => {
            SchemaFacts::Ledger(read_bookkeeping(connection, log, &READ_BOOKKEEPING).await?)
        }
    };
    match classify(facts, embedded_migrations()) {
        Classification::Settled(state) => Ok(state),
        Classification::ChainCurrent { family } => Ok(DatabaseState::Current {
            family,
            readers: read_reader_presence(connection, log).await?,
        }),
    }
}

/// Reads whether every history reader exists ([`READ_READERS_PUBLISHED`]).
/// Changes nothing.
async fn read_reader_presence(
    connection: &mut PooledConnection,
    log: StatementLog,
) -> Result<ReaderPresence, SchemaInstallError> {
    let statement = &READ_READERS_PUBLISHED;
    let rows = query_on(connection, log, &statement.bind().finish()).await?;
    let published: bool = column(&one_row(rows, statement)?, statement, "readers_published")?;
    Ok(match published {
        true => ReaderPresence::Published,
        false => ReaderPresence::Missing,
    })
}

fn catalog_row(row: &Row) -> Result<CatalogRow, RowDecodeError> {
    let statement = &READ_SCHEMA_CATALOG;
    Ok(CatalogRow {
        migrations_table: column(row, statement, "migrations_table")?,
        schema_version_table: column(row, statement, "schema_version_table")?,
        family_table: column(row, statement, "family_table")?,
        horsies_tables: column(row, statement, "horsies_tables")?,
    })
}

/// Reads the bookkeeping rows with `statement` (with or without the family
/// row) and returns them in ascending version order.
async fn read_bookkeeping(
    connection: &mut PooledConnection,
    log: StatementLog,
    statement: &'static TypedStatement,
) -> Result<Bookkeeping, SchemaInstallError> {
    let limit = CATALOG_ROW_LIMIT;
    let rows = query_on(connection, log, &statement.bind().value(&limit).finish()).await?;
    let mut applied = rows
        .iter()
        .map(|row| applied_migration(row, statement))
        .collect::<Result<Vec<AppliedMigration>, RowDecodeError>>()?;
    let family = match rows.first() {
        Some(row) => recorded_family_from(column(row, statement, "family")?)?,
        None => RecordedFamily::NotRecorded,
    };
    // The read is newest first; the classification walks the chain in apply
    // order.
    applied.reverse();
    Ok(Bookkeeping { applied, family })
}

fn applied_migration(
    row: &Row,
    statement: &TypedStatement,
) -> Result<AppliedMigration, RowDecodeError> {
    let succeeded: bool = column(row, statement, "success")?;
    Ok(AppliedMigration {
        version: column(row, statement, "version")?,
        run: match succeeded {
            true => RecordedRun::Succeeded,
            false => RecordedRun::Failed,
        },
        checksum: column(row, statement, "checksum")?,
    })
}

/// Applies the migrations of the embedded chain from index `first` on
/// ([`apply_chain`]).
async fn apply_pending(
    connection: &mut PooledConnection,
    first: usize,
    own: ClientFamily,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    apply_chain(connection, embedded_migrations(), first, own, log).await
}

/// Applies the migrations of `chain` from index `first` on, in order
/// (Python 0.7.0 `_run_schema_upgrade`). For each migration:
/// 1. its exact indexes, outside a transaction ([`install_exact_indexes`]);
/// 2. its transaction with its bookkeeping row, and for the last migration
///    of `chain` this client's family row ([`apply_migration`],
///    [`family_row_at`]);
/// 3. after that commit, the retired indexes of every migration of `chain`
///    up to it, outside a transaction ([`remove_retired_indexes`]). A
///    removal that a failure stopped runs again with the next migration that
///    an install applies.
///
/// Errors: the first error of a step; the migrations before it stay
/// committed. A failure in step 1 or 2 leaves the bookkeeping row of the
/// migration unwritten, so the next install applies it again: an exact
/// index that matches is kept, and the build and claim indexes that the
/// failure left are dropped. A failure before the transaction of the last
/// migration commits writes no family row.
async fn apply_chain(
    connection: &mut PooledConnection,
    chain: &[EmbeddedMigration],
    first: usize,
    own: ClientFamily,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    for (position, migration) in chain.iter().enumerate().skip(first) {
        tracing::info!(
            version = migration.version,
            description = migration.description,
            "installing schema migration",
        );
        install_exact_indexes(connection, migration, log).await?;
        let family_row = family_row_at(chain, position, own);
        apply_migration(connection, migration, family_row, log).await?;
        remove_retired_indexes(connection, &chain[..=position], log).await?;
    }
    Ok(())
}

/// Runs one migration in one READ COMMITTED transaction (Python
/// `install_baseline`): its text, then for migration 1000 the publication of
/// the history readers, then its bookkeeping row, then, for
/// `FamilyRow::Written`, the family row when the table has none. READ
/// COMMITTED is named, not taken from the server default, because the
/// publication refuses any other isolation. On a failure nothing of the
/// migration stays; the failure is returned.
async fn apply_migration(
    connection: &mut PooledConnection,
    migration: &EmbeddedMigration,
    family_row: FamilyRow,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let mut transaction = connection.begin(IsolationLevel::ReadCommitted).await?;
    match migration_statements(&mut transaction, migration, family_row, log).await {
        Ok(()) => Ok(transaction.commit().await?),
        Err(failure) => {
            // The failure is the answer. A failed ROLLBACK discards the
            // connection, and the server rolls the transaction back when the
            // socket closes.
            let _rolled_back: RollbackOutcome = transaction.rollback().await;
            Err(failure)
        }
    }
}

async fn migration_statements(
    transaction: &mut Transaction<'_>,
    migration: &EmbeddedMigration,
    family_row: FamilyRow,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let started = Instant::now();
    transaction
        .run_script(&migration.script, StatementBound::ConnectionTimeouts)
        .await?;
    let execution_time = i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX);
    match migration.readers {
        ReaderPublication::PublishedWithMigration => {
            let published = publish_history_readers(transaction, log).await?;
            tracing::info!(
                version = migration.version,
                outcome = published.as_str(),
                "history readers published with the schema migration",
            );
        }
        ReaderPublication::NotPublished => {}
    }
    let checksum = migration.checksum.to_vec();
    let _recorded = execute_in(
        transaction,
        log,
        &RECORD_MIGRATION
            .bind()
            .value(&migration.version)
            .value(&migration.description)
            .value(&checksum)
            .value(&execution_time)
            .finish(),
    )
    .await?;
    match family_row {
        FamilyRow::Written(own) => {
            let family = own.as_str();
            let _recorded = execute_in(
                transaction,
                log,
                &RECORD_CLIENT_FAMILY.bind().value(&family).finish(),
            )
            .await?;
        }
        FamilyRow::NotWritten => {}
    }
    Ok(())
}

/// What one reader publication did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublishedReaders {
    /// The manifest was rewritten and the four readers were replaced and
    /// stamped.
    Published,
    /// The manifest and every stamp were current; nothing changed.
    AlreadyCurrent,
}

impl PublishedReaders {
    fn as_str(self) -> &'static str {
        match self {
            PublishedReaders::Published => "PUBLISHED",
            PublishedReaders::AlreadyCurrent => "ALREADY_CURRENT",
        }
    }
}

/// Publishes the history readers in `transaction`.
///
/// Errors: `Statement` when the function refuses (25000 outside READ
/// COMMITTED, 42P01 when no attached leaf resolves its parent) or the call
/// fails; `RowDecode` when it returns no row or an outcome outside its two.
async fn publish_history_readers(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
) -> Result<PublishedReaders, SchemaInstallError> {
    let statement = &PUBLISH_HISTORY_READERS;
    let rows = query_in(transaction, log, &statement.bind().finish()).await?;
    let outcome: String = column(&one_row(rows, statement)?, statement, "outcome")?;
    match outcome.as_str() {
        "PUBLISHED" => Ok(PublishedReaders::Published),
        "ALREADY_CURRENT" => Ok(PublishedReaders::AlreadyCurrent),
        other => Err(SchemaInstallError::RowDecode(RowDecodeError::Shape {
            statement: statement.name(),
            reason: format!("unknown publication outcome {other:?}"),
        })),
    }
}

/// Completes the install of a database that holds this build's chain
/// (Python `CompleteInstall`): publishes the history readers in one READ
/// COMMITTED transaction (`horsies_publish_history_readers` writes the
/// lookup manifest and the four readers, or answers `ALREADY_CURRENT` and
/// changes nothing), then writes this client's family row when the database
/// has none ([`record_family`]).
///
/// Errors: `Statement` or `RowDecode` from the publication (its transaction
/// is rolled back, so nothing changed) or from the family write (the
/// publication stays committed; the next install writes the row).
async fn complete_install(
    connection: &mut PooledConnection,
    own: ClientFamily,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let mut transaction = connection.begin(IsolationLevel::ReadCommitted).await?;
    match publish_history_readers(&mut transaction, log).await {
        Ok(published) => {
            transaction.commit().await?;
            tracing::info!(
                outcome = published.as_str(),
                "history readers published by the install of a current schema",
            );
        }
        Err(failure) => {
            // The failure is the answer, as in `apply_migration`.
            let _rolled_back: RollbackOutcome = transaction.rollback().await;
            return Err(failure);
        }
    }
    record_family(connection, own, log).await?;
    Ok(())
}

/// Writes this client's family row when the database has none (Python
/// `record_client_family`), outside a transaction.
async fn record_family(
    connection: &mut PooledConnection,
    own: ClientFamily,
    log: StatementLog,
) -> Result<(), StatementFailure> {
    let family = own.as_str();
    log.log(&RECORD_CLIENT_FAMILY);
    let _recorded = connection
        .execute(
            &RECORD_CLIENT_FAMILY.bind().value(&family).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    Ok(())
}

/// A session-work connection on which the install lock was tried. Until a
/// release succeeds the session may hold the lock, so dropping it discards
/// the connection: the server ends the session and its locks, and no
/// connection that may hold the lock goes back to the pool.
struct LockSession {
    connection: PooledConnection,
    hold: LockHold,
}

/// Whether the session of a [`LockSession`] may hold the install lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LockHold {
    /// A try was sent: the session may hold the lock.
    MayHold,
    /// The last try was refused, or the release succeeded.
    NotHeld,
}

impl LockSession {
    /// Checks out a session-work connection and takes the lock on it: one
    /// `pg_try_advisory_lock` every [`INSTALL_LOCK_POLL`], for at most
    /// `lock.wait` (Python `_acquire_session_advisory_lock`, with the bound
    /// of the core). The loop ends when the lock is taken or the wait has
    /// passed.
    ///
    /// Errors: `Lock(WaitBoundReached)` when another session held the lock
    /// for the whole wait; `Statement` or `RowDecode` when a try failed (the
    /// connection is then discarded).
    async fn acquire(
        pool: &CorePool,
        lock: InstallLockPlan,
        log: StatementLog,
    ) -> Result<LockSession, SchemaInstallError> {
        let connection = checkout(pool, &TRY_INSTALL_LOCK).await?;
        let mut session = LockSession {
            connection,
            hold: LockHold::MayHold,
        };
        let started = Instant::now();
        loop {
            let rows = query_on(
                &mut session.connection,
                log,
                &TRY_INSTALL_LOCK.bind().value(&lock.key).finish(),
            )
            .await?;
            let acquired: bool = column(
                &one_row(rows, &TRY_INSTALL_LOCK)?,
                &TRY_INSTALL_LOCK,
                "acquired",
            )?;
            match (acquired, started.elapsed() >= lock.wait) {
                (true, _) => return Ok(session),
                (false, true) => {
                    session.hold = LockHold::NotHeld;
                    return Err(SchemaInstallError::Lock(
                        InstallLockFailure::WaitBoundReached { waited: lock.wait },
                    ));
                }
                (false, false) => sleep(INSTALL_LOCK_POLL).await,
            }
        }
    }

    /// Releases the lock and checks the release (Python
    /// `_schema_session_lock`). A release that frees no lock means the
    /// session lost the lock: `Lock(OwnershipLost)`. A release that fails is
    /// reported the same way, after a warning with its cause. On both the
    /// connection is discarded, which ends the session and any lock it still
    /// holds.
    async fn release(mut self, key: i64, log: StatementLog) -> Result<(), SchemaInstallError> {
        match release_lock(&mut self.connection, key, log).await {
            Ok(true) => {
                self.hold = LockHold::NotHeld;
                Ok(())
            }
            Ok(false) => Err(SchemaInstallError::Lock(InstallLockFailure::OwnershipLost)),
            Err(failure) => {
                tracing::warn!(
                    error = %failure,
                    "releasing the schema install lock failed: {failure}; the connection is \
                     discarded",
                );
                Err(SchemaInstallError::Lock(InstallLockFailure::OwnershipLost))
            }
        }
    }
}

impl Drop for LockSession {
    fn drop(&mut self) {
        match self.hold {
            LockHold::MayHold => self.connection.discard(),
            LockHold::NotHeld => {}
        }
    }
}

async fn release_lock(
    connection: &mut PooledConnection,
    key: i64,
    log: StatementLog,
) -> Result<bool, SchemaInstallError> {
    let rows = query_on(
        connection,
        log,
        &RELEASE_INSTALL_LOCK.bind().value(&key).finish(),
    )
    .await?;
    column(
        &one_row(rows, &RELEASE_INSTALL_LOCK)?,
        &RELEASE_INSTALL_LOCK,
        "released",
    )
    .map_err(SchemaInstallError::from)
}

#[cfg(test)]
mod tests;
