//! The schema install: the embedded migration chain, its bookkeeping in
//! [`MIGRATIONS_TABLE`], and the install lock, on the statement layer
//! ([`crate::db`]).
//!
//! The bookkeeping table is horsies' own, so the install never collides with
//! the migration runner of an application on the same database.
//!
//! The embedded chain:
//! - Version 1000 is the baseline schema: every file of
//!   `crates/horsies-core/baseline/`, in the order of `baseline_sql_files!`
//!   below, concatenated with no separator, with one SHA-384 checksum over the
//!   UTF-8 bytes of the result. It creates [`MIGRATIONS_TABLE`], the 0.x guard
//!   row of `horsies_schema_version` and the empty `horsies_client_family`
//!   table itself. The Python package reads the same files through
//!   `horsies._core` ([`BASELINE_SQL_FILE_TEXTS`]) and installs them as its
//!   migration 1000 with the same checksum rule, so both write the same
//!   bookkeeping row.
//!
//! The install ([`install_schema_on`]; Python reference:
//! `python/horsies/core/baseline/install.py`, and `_run_fleet_schema_init` in
//! `python/horsies/core/brokers/postgres.py`):
//! 1. Read the state of the current schema on a session-work connection, with
//!    no lock: one statement on an empty database, two otherwise.
//! 2. Decide. A database that holds this build's chain and this client's
//!    family gets no lock and no write. A database that this build does not
//!    know is refused with a [`SchemaRefusal`], and a database of the other
//!    client family is refused; neither is changed.
//! 3. Otherwise take the install lock on a session-work connection, read the
//!    state again on it (another installer may have finished during the
//!    wait), and act until the state is current or refused:
//!    - each pending migration runs in its own READ COMMITTED transaction
//!      with its bookkeeping row; the transaction of migration 1000 also
//!      publishes the history readers (`horsies_publish_history_readers`)
//!      and writes this client's family row;
//!    - a database with no family row gets this client's row.
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
    recorded_family_from, ClientFamily, RecordedFamily, RECORD_CLIENT_FAMILY,
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
/// of `crates/horsies-core/baseline/` are the single source; the core includes
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

// The files of `crates/horsies-core/baseline/`, in the order they run. A file
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
/// row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FamilyRow {
    /// Migration 1000 creates the family table; its transaction writes the
    /// row (Python `install_baseline`).
    WrittenWithMigration,
    /// The migration writes no family row: every migration after the
    /// baseline. The chain holds the baseline only until the first migration
    /// after it, so only tests construct this variant now.
    #[cfg_attr(not(test), allow(dead_code))]
    NotWritten,
}

/// Whether the transaction of a migration publishes the history readers
/// (`horsies_publish_history_readers`, baseline `0008`) after its text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReaderPublication {
    /// Migration 1000 creates the leaf catalog, the static forever leaf and
    /// the publication function; its transaction publishes the readers, so a
    /// recorded version 1000 always has them.
    PublishedWithMigration,
    /// The migration does not change what the readers read. As
    /// [`FamilyRow::NotWritten`], only tests construct it while the chain
    /// holds the baseline only.
    #[cfg_attr(not(test), allow(dead_code))]
    NotPublished,
}

/// One migration of the embedded chain, as written in the source.
struct MigrationSource {
    version: i64,
    description: &'static str,
    script: SqlScript,
    family_row: FamilyRow,
    readers: ReaderPublication,
}

/// The embedded chain, in apply order: the baseline only, the chain that
/// the Python client installs too.
const MIGRATION_SOURCES: [MigrationSource; 1] = [MigrationSource {
    version: BASELINE_MIGRATION_VERSION,
    description: "baseline",
    script: SqlScript::new("migration_1000_baseline", BASELINE_SQL),
    family_row: FamilyRow::WrittenWithMigration,
    readers: ReaderPublication::PublishedWithMigration,
}];

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
    family_row: FamilyRow,
    readers: ReaderPublication,
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
            family_row: source.family_row,
            readers: source.readers,
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
/// `PostgresBroker._schema_advisory_key`, `python/horsies/core/brokers/postgres.py`).
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

/// The most actions under the lock: apply the pending migrations, then write
/// the family row. A state read follows each action.
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
    Current { family: RecordedFamily },
    /// A database this build does not install into.
    Unsupported(SchemaRefusal),
}

/// A database that this build does not install into. The install changes
/// nothing. Not retryable: the same database is refused again.
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
    /// Write this client's family row.
    RecordFamily,
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
    /// Nothing was changed. Not retryable.
    #[error("schema install refused: {0}")]
    Refused(SchemaRefusal),
    /// The database was installed by the other client family. Nothing was
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
            SchemaInstallError::Lock(failure) => BrokerError::InstallLock(failure),
            SchemaInstallError::Statement(failure) => BrokerError::Statement(failure),
            SchemaInstallError::RowDecode(error) => BrokerError::RowDecode(error),
        }
    }
}

// ---- Classification and decision (pure) ----

/// Classifies the facts of one database against `chain` (Python
/// `classify_database`, with a chain of migrations in place of one).
fn classify(facts: SchemaFacts, chain: &[EmbeddedMigration]) -> DatabaseState {
    match facts {
        SchemaFacts::NoLedger { horsies_tables, .. } if horsies_tables.is_empty() => {
            DatabaseState::Empty
        }
        SchemaFacts::NoLedger {
            schema_version_table: true,
            horsies_tables,
        } => DatabaseState::Unsupported(SchemaRefusal::ZeroXPython {
            tables: horsies_tables,
        }),
        SchemaFacts::NoLedger {
            schema_version_table: false,
            horsies_tables,
        } => DatabaseState::Unsupported(SchemaRefusal::UnknownHorsiesObjects {
            tables: horsies_tables,
        }),
        SchemaFacts::Ledger(bookkeeping) => classify_bookkeeping(bookkeeping, chain),
    }
}

/// Classifies the bookkeeping rows: a version above the chain, then rows
/// that do not start the chain, then a failed or changed row.
fn classify_bookkeeping(bookkeeping: Bookkeeping, chain: &[EmbeddedMigration]) -> DatabaseState {
    let Bookkeeping { applied, family } = bookkeeping;
    let own = newest_version(chain);
    let versions: Vec<i64> = applied.iter().map(|row| row.version).collect();
    if let Some(&stored) = versions.last().filter(|stored| **stored > own) {
        return DatabaseState::Unsupported(SchemaRefusal::SchemaNewer { stored, own });
    }
    if !starts_the_chain(&versions, chain) {
        return DatabaseState::Unsupported(SchemaRefusal::ForeignMigrations { versions });
    }
    match (
        first_broken_row(&applied, chain),
        applied.len() == chain.len(),
    ) {
        (Some(refusal), _) => DatabaseState::Unsupported(refusal),
        (None, true) => DatabaseState::Current { family },
        (None, false) => DatabaseState::Behind {
            applied: applied.len(),
            family,
        },
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
        DatabaseState::Current { family } => match family {
            RecordedFamily::Recorded(stored) if *stored != own => other_family(*stored, own),
            RecordedFamily::Recorded(_) => InstallAction::Ready,
            RecordedFamily::NotRecorded => InstallAction::RecordFamily,
        },
        DatabaseState::Unsupported(refusal) => {
            InstallAction::Refuse(InstallRefusal::Schema(refusal.clone()))
        }
    }
}

fn other_family(stored: ClientFamily, own: ClientFamily) -> InstallAction {
    InstallAction::Refuse(InstallRefusal::OtherFamily { stored, own })
}

// ---- The install ----

/// Installs the schema for a client of family `own` through `pool`, the
/// session-work pool ([`crate::broker::pools::BrokerPools::session_work`]):
/// the steps of the module documentation.
///
/// Writes only under the install lock; each migration commits with its
/// bookkeeping row. Does not check the family before the writes of other
/// operations (`PostgresBroker::ensure_client_family_for_write` does).
///
/// Errors ([`SchemaInstallError`]):
/// - `Refused`, `OtherFamily`: nothing was changed; not retryable.
/// - `Lock(WaitBoundReached)`: another installer held the lock for the
///   whole wait; this call changed nothing; retryable.
/// - `Lock(OwnershipLost)`: the install completed and the release found no
///   lock; not retryable.
/// - `Statement`: a statement failed, a deadlock on the last run included.
///   Migrations committed before the failure stay.
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
        InstallAction::ApplyFrom(_) | InstallAction::RecordFamily => {}
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
            InstallAction::RecordFamily => record_family(connection, own, log).await?,
        }
        state = read_state(connection, log).await?;
    }
    match decide(&state, own) {
        InstallAction::Ready => Ok(()),
        InstallAction::Refuse(refusal) => Err(refusal.into()),
        action @ (InstallAction::ApplyFrom(_) | InstallAction::RecordFamily) => panic!(
            "the schema install still decides {action:?} for {state:?} after \
             {LOCKED_ACTION_BOUND} actions under the install lock"
        ),
    }
}

/// Reads and classifies the horsies objects of the current schema (Python
/// `read_database_state`). One statement on an empty database, two
/// otherwise. Changes nothing.
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
    Ok(classify(facts, embedded_migrations()))
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
        Some(row) => recorded_family_from(column(row, statement, "family")?),
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

/// Applies the migrations of the chain from index `first` on, each in its
/// own transaction.
async fn apply_pending(
    connection: &mut PooledConnection,
    first: usize,
    own: ClientFamily,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    for migration in embedded_migrations().iter().skip(first) {
        tracing::info!(
            version = migration.version,
            description = migration.description,
            "installing schema migration",
        );
        apply_migration(connection, migration, own, log).await?;
    }
    Ok(())
}

/// Runs one migration in one READ COMMITTED transaction (Python
/// `install_baseline`): its text, then for migration 1000 the publication of
/// the history readers, then its bookkeeping row, then for migration 1000
/// this client's family row. READ COMMITTED is named, not taken from the
/// server default, because the publication refuses any other isolation. On a
/// failure nothing of the migration stays; the failure is returned.
async fn apply_migration(
    connection: &mut PooledConnection,
    migration: &EmbeddedMigration,
    own: ClientFamily,
    log: StatementLog,
) -> Result<(), SchemaInstallError> {
    let mut transaction = connection.begin(IsolationLevel::ReadCommitted).await?;
    match migration_statements(&mut transaction, migration, own, log).await {
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
    own: ClientFamily,
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
    match migration.family_row {
        FamilyRow::WrittenWithMigration => {
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
