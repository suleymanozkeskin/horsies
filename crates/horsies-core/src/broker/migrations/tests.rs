//! Tests of the schema install.
//!
//! The pure tests cover the chain, the classification, the decision and the
//! error forms. Each database test runs the install on an empty database of
//! its own (`crate::test_support::TestDatabase::create_empty`) through a core
//! handle, and reads the result through a plain driver connection; one test
//! per install outcome.

use std::time::Duration;

use rand::Rng;
use serial_test::serial;
use sha2::{Digest, Sha384};
use tokio_postgres::error::SqlState;
use tokio_postgres::Client;

use super::*;
use crate::db::{CheckoutFailure, PoolRole};
use crate::test_support::{core_handle, TestDatabase};

/// An empty database of one test, with the install helpers.
struct MigrationTestDatabase {
    database: TestDatabase,
}

impl MigrationTestDatabase {
    async fn create() -> Self {
        Self {
            database: TestDatabase::create_empty().await,
        }
    }

    fn client(&self) -> &Client {
        self.database.client()
    }

    /// Runs the install for `own` with `lock` through a new core handle, and
    /// closes the handle.
    async fn install(
        &self,
        own: ClientFamily,
        lock: InstallLockPlan,
    ) -> Result<(), SchemaInstallError> {
        let handle = core_handle(self.database.url());
        let open = handle
            .pools()
            .expect("the statement layer accepts the test URL");
        let installed =
            install_schema_on(open.pools().session_work(), own, lock, StatementLog::Off).await;
        drop(open);
        let _closed = handle.close().await;
        installed
    }

    /// Applies migration 1000 alone, with `own` as the family, as Python's
    /// install leaves a database.
    async fn install_baseline_only(&self, own: ClientFamily) {
        let handle = core_handle(self.database.url());
        let open = handle
            .pools()
            .expect("the statement layer accepts the test URL");
        let mut connection = open
            .pools()
            .session_work()
            .checkout(CheckoutClass::Batch)
            .await
            .expect("check out a connection");
        apply_migration(
            &mut connection,
            &embedded_migrations()[0],
            own,
            StatementLog::Off,
        )
        .await
        .expect("apply migration 1000");
        drop(connection);
        drop(open);
        let _closed = handle.close().await;
    }

    /// A session of its own that holds the advisory lock `key` until it is
    /// dropped.
    async fn hold_lock(&self, key: i64) -> Client {
        let holder = self.database.connect().await;
        let row = holder
            .query_one("SELECT pg_try_advisory_lock($1)", &[&key])
            .await
            .expect("try the lock");
        let held: bool = row.get(0);
        assert!(held, "the lock {key} is free");
        holder
    }

    async fn execute(&self, statement: &str) {
        self.client()
            .batch_execute(statement)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }

    async fn drop(self) {
        self.database.drop().await;
    }
}

/// A lock with a key of the test's own and a short wait.
fn test_lock() -> InstallLockPlan {
    InstallLockPlan {
        key: rand::thread_rng().gen_range(1..i64::MAX),
        wait: Duration::from_millis(200),
    }
}

/// The bookkeeping rows: version, description, success, checksum.
async fn bookkeeping_rows(database: &MigrationTestDatabase) -> Vec<(i64, String, bool, Vec<u8>)> {
    database
        .client()
        .query(
            "SELECT version, description, success, checksum \
             FROM horsies_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("read the bookkeeping")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect()
}

/// The bookkeeping rows of the first `count` migrations of the chain.
fn chain_rows(count: usize) -> Vec<(i64, String, bool, Vec<u8>)> {
    embedded_migrations()
        .iter()
        .take(count)
        .map(|migration| {
            (
                migration.version,
                migration.description.to_owned(),
                true,
                migration.checksum.to_vec(),
            )
        })
        .collect()
}

async fn installed_on(database: &MigrationTestDatabase) -> Vec<(i64, String)> {
    database
        .client()
        .query(
            "SELECT version, installed_on::text FROM horsies_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("read the install times")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

async fn family_row(database: &MigrationTestDatabase) -> Option<String> {
    database
        .client()
        .query_opt("SELECT family FROM horsies_client_family", &[])
        .await
        .expect("read the family row")
        .map(|row| row.get(0))
}

async fn relation_exists(database: &MigrationTestDatabase, name: &str) -> bool {
    database
        .client()
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&name])
        .await
        .expect("read the catalog")
        .get(0)
}

/// Makes the first `failing` DDL commands of the database fail with SQLSTATE
/// 40P01 (`deadlock_detected`), through an event trigger. A sequence counts
/// the commands: `nextval` is not rolled back with the failed transaction.
async fn inject_deadlocks(database: &MigrationTestDatabase, failing: i64) {
    database
        .execute("CREATE SEQUENCE install_deadlock_probe")
        .await;
    database
        .execute(&format!(
            "CREATE FUNCTION install_deadlock_probe() RETURNS event_trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF nextval('install_deadlock_probe') <= {failing} THEN
                     RAISE EXCEPTION 'injected deadlock' USING ERRCODE = 'deadlock_detected';
                 END IF;
             END
             $$"
        ))
        .await;
    database
        .execute(
            "CREATE EVENT TRIGGER install_deadlock_probe ON ddl_command_start
             EXECUTE FUNCTION install_deadlock_probe()",
        )
        .await;
}

// ---- The chain ----

/// Python's key: the first 8 bytes of SHA-256 of `horsies:schema:v1`,
/// big-endian, signed (Python `PostgresBroker._schema_advisory_key`).
#[test]
fn the_install_lock_key_is_python_s() {
    assert_eq!(install_lock_key(), 6_194_151_126_714_960_738);
    assert_eq!(InstallLockPlan::fleet().key, install_lock_key());
    assert_eq!(InstallLockPlan::fleet().wait, INSTALL_LOCK_WAIT);
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

/// The folder of the baseline SQL files.
fn baseline_sql_folder() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("baseline")
}

/// The list holds every SQL file of the baseline folder, in name order. A
/// file added to the folder without a line in `baseline_sql_files!` fails
/// here.
#[test]
fn baseline_file_list_is_the_whole_folder() {
    assert!(!BASELINE_SQL_FILES.is_empty(), "the baseline list is empty");

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

/// Migration 1000 is the listed files as they are on disk, concatenated in
/// list order, and its checksum is Python's rule
/// (`python/horsies/core/baseline/migration.py`, `baseline_checksum`):
/// SHA-384 of the UTF-8 bytes of that text. `BASELINE_SQL_FILE_TEXTS`, which
/// Python reads, holds the same names and texts in the same order.
#[test]
fn baseline_is_one_migration_built_from_every_listed_file() {
    let baseline = &embedded_migrations()[0];
    assert_eq!(baseline.version, BASELINE_MIGRATION_VERSION);
    assert_eq!(baseline.description, "baseline");
    let on_disk: String = BASELINE_SQL_FILES
        .iter()
        .map(|name| {
            let path = baseline_sql_folder().join(name);
            std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        })
        .collect();
    assert_eq!(baseline.script.sql(), on_disk);
    let names: Vec<&str> = BASELINE_SQL_FILE_TEXTS
        .iter()
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(names, BASELINE_SQL_FILES);
    let texts: String = BASELINE_SQL_FILE_TEXTS
        .iter()
        .map(|(_, text)| *text)
        .collect();
    assert_eq!(texts, on_disk);
    assert_eq!(
        baseline.checksum.as_slice(),
        Sha384::digest(on_disk.as_bytes()).as_slice()
    );
    assert!(BASELINE_TABLES_SQL.contains(&format!("CREATE TABLE {MIGRATIONS_TABLE} (")));
}

/// The chain starts at the baseline and ascends; only the baseline writes the
/// family row and publishes the history readers; each checksum is SHA-384 of
/// its text.
#[test]
fn the_chain_ascends_from_the_baseline_and_only_the_baseline_writes_the_family() {
    let chain = embedded_migrations();
    let versions: Vec<i64> = chain.iter().map(EmbeddedMigration::version).collect();
    let mut sorted = versions.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(versions, sorted, "apply order is ascending and unique");
    assert_eq!(versions.first(), Some(&BASELINE_MIGRATION_VERSION));
    assert_eq!(versions.last(), Some(&expected_schema_version()));
    assert_eq!(shipped_versions(), versions);
    for (index, migration) in chain.iter().enumerate() {
        let (family_row, readers) = match index {
            0 => (
                FamilyRow::WrittenWithMigration,
                ReaderPublication::PublishedWithMigration,
            ),
            _ => (FamilyRow::NotWritten, ReaderPublication::NotPublished),
        };
        assert_eq!(migration.family_row, family_row, "{}", migration.version);
        assert_eq!(migration.readers, readers, "{}", migration.version);
        assert_eq!(
            migration.checksum.as_slice(),
            Sha384::digest(migration.script.sql().as_bytes()).as_slice(),
            "{}",
            migration.version
        );
    }
}

// ---- Classification and decision ----

/// A chain of three migrations for the classification, independent of the
/// shipped chain.
fn test_chain() -> Vec<EmbeddedMigration> {
    [1000_i64, 1001, 1002]
        .map(|version| EmbeddedMigration {
            version,
            description: "test",
            script: SqlScript::new("test_migration", "SELECT 1"),
            family_row: FamilyRow::NotWritten,
            readers: ReaderPublication::NotPublished,
            checksum: [u8::try_from(version - 999).expect("a small test version"); CHECKSUM_BYTES],
        })
        .into()
}

fn succeeded(migration: &EmbeddedMigration) -> AppliedMigration {
    AppliedMigration {
        version: migration.version,
        run: RecordedRun::Succeeded,
        checksum: migration.checksum.to_vec(),
    }
}

fn foreign(version: i64) -> AppliedMigration {
    AppliedMigration {
        version,
        run: RecordedRun::Succeeded,
        checksum: vec![0],
    }
}

fn ledger(applied: Vec<AppliedMigration>, family: RecordedFamily) -> SchemaFacts {
    SchemaFacts::Ledger(Bookkeeping { applied, family })
}

fn no_ledger(schema_version_table: bool, tables: &[&str]) -> SchemaFacts {
    SchemaFacts::NoLedger {
        schema_version_table,
        horsies_tables: tables.iter().map(|name| (*name).to_owned()).collect(),
    }
}

const RUST: RecordedFamily = RecordedFamily::Recorded(ClientFamily::Rust);

#[test]
fn a_schema_without_a_ledger_is_empty_zero_x_or_unknown() {
    let chain = test_chain();
    assert_eq!(
        classify(no_ledger(false, &[]), &chain),
        Classification::Settled(DatabaseState::Empty)
    );
    assert_eq!(
        classify(
            no_ledger(true, &["horsies_schema_version", "horsies_tasks"]),
            &chain
        ),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::ZeroXPython {
            tables: vec![
                "horsies_schema_version".to_owned(),
                "horsies_tasks".to_owned()
            ],
        })),
    );
    assert_eq!(
        classify(no_ledger(false, &["horsies_tasks"]), &chain),
        Classification::Settled(DatabaseState::Unsupported(
            SchemaRefusal::UnknownHorsiesObjects {
                tables: vec!["horsies_tasks".to_owned()],
            }
        )),
    );
}

#[test]
fn a_version_above_the_chain_is_newer() {
    let chain = test_chain();
    let mut applied: Vec<AppliedMigration> = chain.iter().map(succeeded).collect();
    applied.push(foreign(1003));
    assert_eq!(
        classify(ledger(applied, RUST), &chain),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored: 1003,
            own: 1002,
        })),
    );
}

/// No baseline row, versions this build does not ship (a horsies-rust 0.x
/// ledger), a later migration without the baseline, and a gap.
#[test]
fn bookkeeping_that_does_not_start_the_chain_is_foreign() {
    let chain = test_chain();
    let cases: [(Vec<AppliedMigration>, Vec<i64>); 4] = [
        (Vec::new(), Vec::new()),
        ((1..=65).map(foreign).collect(), (1..=65).collect()),
        (vec![succeeded(&chain[1])], vec![1001]),
        (
            vec![succeeded(&chain[0]), succeeded(&chain[2])],
            vec![1000, 1002],
        ),
    ];
    for (applied, versions) in cases {
        assert_eq!(
            classify(ledger(applied, RUST), &chain),
            Classification::Settled(DatabaseState::Unsupported(
                SchemaRefusal::ForeignMigrations { versions }
            )),
        );
    }
}

#[test]
fn a_failed_row_is_incomplete_and_a_changed_checksum_is_edited() {
    let chain = test_chain();
    let mut failed: Vec<AppliedMigration> = chain.iter().map(succeeded).collect();
    failed[1].run = RecordedRun::Failed;
    assert_eq!(
        classify(ledger(failed, RUST), &chain),
        Classification::Settled(DatabaseState::Unsupported(
            SchemaRefusal::MigrationIncomplete { version: 1001 }
        )),
    );

    let mut edited: Vec<AppliedMigration> = chain.iter().map(succeeded).collect();
    edited[0].checksum = vec![0];
    assert_eq!(
        classify(ledger(edited, RUST), &chain),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::MigrationEdited {
            version: 1000,
            stored_checksum: vec![0],
            own_checksum: chain[0].checksum.to_vec(),
        })),
    );
}

#[test]
fn a_start_of_the_chain_is_behind_and_the_whole_chain_is_current() {
    let chain = test_chain();
    assert_eq!(
        classify(
            ledger(
                vec![succeeded(&chain[0]), succeeded(&chain[1])],
                RecordedFamily::NotRecorded
            ),
            &chain
        ),
        Classification::Settled(DatabaseState::Behind {
            applied: 2,
            family: RecordedFamily::NotRecorded,
        }),
    );
    assert_eq!(
        classify(ledger(chain.iter().map(succeeded).collect(), RUST), &chain),
        Classification::ChainCurrent { family: RUST },
    );
}

#[test]
fn each_state_decides_its_action() {
    use crate::broker::client_family::ClientFamily::{Python, Rust};

    let refusal = SchemaRefusal::MigrationIncomplete { version: 1000 };
    let cases = [
        (DatabaseState::Empty, Rust, InstallAction::ApplyFrom(0)),
        (
            DatabaseState::Behind {
                applied: 1,
                family: RecordedFamily::Recorded(Rust),
            },
            Rust,
            InstallAction::ApplyFrom(1),
        ),
        (
            DatabaseState::Behind {
                applied: 1,
                family: RecordedFamily::NotRecorded,
            },
            Python,
            InstallAction::ApplyFrom(1),
        ),
        (
            DatabaseState::Behind {
                applied: 1,
                family: RecordedFamily::Recorded(Python),
            },
            Rust,
            InstallAction::Refuse(InstallRefusal::OtherFamily {
                stored: Python,
                own: Rust,
            }),
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::Recorded(Python),
                readers: ReaderPresence::Published,
            },
            Python,
            InstallAction::Ready,
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::Recorded(Python),
                readers: ReaderPresence::Missing,
            },
            Python,
            InstallAction::CompleteInstall,
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::NotRecorded,
                readers: ReaderPresence::Published,
            },
            Rust,
            InstallAction::CompleteInstall,
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::NotRecorded,
                readers: ReaderPresence::Missing,
            },
            Rust,
            InstallAction::CompleteInstall,
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::Recorded(Rust),
                readers: ReaderPresence::Published,
            },
            Python,
            InstallAction::Refuse(InstallRefusal::OtherFamily {
                stored: Rust,
                own: Python,
            }),
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::Recorded(Rust),
                readers: ReaderPresence::Missing,
            },
            Python,
            InstallAction::Refuse(InstallRefusal::OtherFamily {
                stored: Rust,
                own: Python,
            }),
        ),
        (
            DatabaseState::Unsupported(refusal.clone()),
            Rust,
            InstallAction::Refuse(InstallRefusal::Schema(refusal)),
        ),
    ];
    for (state, own, action) in cases {
        assert_eq!(decide(&state, own), action, "{state:?} for {own:?}");
    }
}

// ---- Errors ----

#[test]
fn only_a_deadlock_runs_the_install_again() {
    assert!(is_deadlock_code(&SqlState::T_R_DEADLOCK_DETECTED));
    for code in [
        SqlState::T_R_SERIALIZATION_FAILURE,
        SqlState::LOCK_NOT_AVAILABLE,
        SqlState::DUPLICATE_TABLE,
    ] {
        assert!(!is_deadlock_code(&code), "{code:?}");
    }
    let not_sent = SchemaInstallError::Statement(StatementFailure::NotSent {
        statement: "read_schema_catalog",
        cause: NotSentCause::ConnectionClosed,
    });
    assert!(!not_sent.is_deadlock());
    assert!(!SchemaInstallError::Lock(InstallLockFailure::OwnershipLost).is_deadlock());
    assert!(
        !SchemaInstallError::Refused(SchemaRefusal::MigrationIncomplete { version: 1000 })
            .is_deadlock()
    );
}

#[test]
fn a_deadlock_pause_stays_within_its_bounds() {
    for attempt in 1..INSTALL_MAX_ATTEMPTS {
        let longest = Duration::from_millis(50 + 200 * (1_u64 << (attempt - 1)));
        for _ in 0..32 {
            let pause = deadlock_backoff(attempt);
            assert!(
                pause >= Duration::from_millis(50) && pause <= longest,
                "attempt {attempt}: {pause:?}"
            );
        }
    }
}

#[test]
fn each_install_error_keeps_its_meaning_as_a_broker_error() {
    let refused = BrokerError::from(SchemaInstallError::Refused(SchemaRefusal::SchemaNewer {
        stored: 1002,
        own: 1001,
    }));
    assert!(matches!(
        refused,
        BrokerError::SchemaRefused(SchemaRefusal::SchemaNewer {
            stored: 1002,
            own: 1001
        })
    ));
    assert!(!refused.is_retryable());

    let other_family = BrokerError::from(SchemaInstallError::OtherFamily {
        stored: ClientFamily::Python,
        own: ClientFamily::Rust,
    });
    assert!(matches!(
        other_family,
        BrokerError::ClientFamilyMismatch {
            stored: RecordedFamily::Recorded(ClientFamily::Python),
            own: ClientFamily::Rust,
        }
    ));
    assert!(!other_family.is_retryable());

    let busy = BrokerError::from(SchemaInstallError::Lock(
        InstallLockFailure::WaitBoundReached {
            waited: INSTALL_LOCK_WAIT,
        },
    ));
    assert!(busy.is_retryable());
    let lost = BrokerError::from(SchemaInstallError::Lock(InstallLockFailure::OwnershipLost));
    assert!(!lost.is_retryable());

    let timed_out = BrokerError::from(SchemaInstallError::Statement(StatementFailure::NotSent {
        statement: "read_schema_catalog",
        cause: NotSentCause::Checkout(CheckoutFailure::TimedOut {
            role: PoolRole::Session,
            waited: Duration::from_secs(30),
        }),
    }));
    assert!(matches!(timed_out, BrokerError::Statement(_)));
    assert!(timed_out.is_retryable());
}

#[test]
fn each_refusal_names_what_the_database_holds() {
    let cases = [
        (
            SchemaRefusal::ZeroXPython {
                tables: vec!["horsies_schema_version".to_owned()],
            },
            "horsies 0.x schema".to_owned(),
        ),
        (
            SchemaRefusal::UnknownHorsiesObjects {
                tables: vec!["horsies_tasks".to_owned(), "horsies_workflows".to_owned()],
            },
            "no schema bookkeeping (tables: horsies_tasks, horsies_workflows)".to_owned(),
        ),
        (
            SchemaRefusal::SchemaNewer {
                stored: 1002,
                own: 1001,
            },
            "version 1002 is newer than this build supports (1001)".to_owned(),
        ),
        (
            SchemaRefusal::ForeignMigrations {
                versions: Vec::new(),
            },
            format!(
                "found: none; this build ships {}",
                listed(&shipped_versions())
            ),
        ),
        (
            SchemaRefusal::MigrationIncomplete { version: 1001 },
            "schema migration 1001 is recorded as failed".to_owned(),
        ),
        (
            SchemaRefusal::MigrationEdited {
                version: 1000,
                stored_checksum: vec![0, 255],
                own_checksum: vec![1],
            },
            "stored checksum 00ff, this build 01".to_owned(),
        ),
    ];
    for (refusal, words) in cases {
        let shown = refusal.to_string();
        assert!(shown.contains(&words), "{shown}");
    }
}

// ---- One database test per install outcome ----

/// An empty database gets the whole chain in its bookkeeping, the 0.x guard
/// row and this client's family row.
#[tokio::test]
#[serial]
async fn an_empty_database_gets_the_chain_the_guard_row_and_the_family() {
    let database = MigrationTestDatabase::create().await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");

    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    let run_facts: Vec<(i64, bool)> = database
        .client()
        .query(
            "SELECT execution_time, installed_on <= now() FROM horsies_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("read the run facts")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert!(
        run_facts
            .iter()
            .all(|(nanos, recorded)| *nanos >= 0 && *recorded),
        "{run_facts:?}"
    );
    let guard: i32 = database
        .client()
        .query_one("SELECT max(version) FROM horsies_schema_version", &[])
        .await
        .expect("read the guard row")
        .get(0);
    assert_eq!(i64::from(guard), BASELINE_MIGRATION_VERSION);
    assert_eq!(family_row(&database).await.as_deref(), Some("RUST"));
    database.drop().await;
}

/// A database that holds the chain and this client's family gets no lock
/// and no write: the install returns while another session holds its lock.
#[tokio::test]
#[serial]
async fn a_current_database_is_left_alone_without_the_lock() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    let before = installed_on(&database).await;
    let lock = test_lock();
    let holder = database.hold_lock(lock.key).await;

    database
        .install(ClientFamily::Rust, lock)
        .await
        .expect("a current database needs no lock");

    assert_eq!(installed_on(&database).await, before);
    drop(holder);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn a_zero_x_python_database_is_refused_unchanged() {
    let database = MigrationTestDatabase::create().await;
    database
        .execute(
            "CREATE TABLE horsies_schema_version \
             (version integer PRIMARY KEY, applied_at timestamptz NOT NULL); \
             CREATE TABLE horsies_tasks (id text PRIMARY KEY)",
        )
        .await;

    let refused = database
        .install(ClientFamily::Python, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Refused(SchemaRefusal::ZeroXPython { tables })) => assert_eq!(
            tables,
            vec![
                "horsies_schema_version".to_owned(),
                "horsies_tasks".to_owned()
            ]
        ),
        other => panic!("expected a 0.x refusal, got {other:?}"),
    }
    assert!(!relation_exists(&database, MIGRATIONS_TABLE).await);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn horsies_tables_with_no_bookkeeping_are_refused_unchanged() {
    let database = MigrationTestDatabase::create().await;
    database
        .execute("CREATE TABLE horsies_tasks (id uuid)")
        .await;

    let refused = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Refused(SchemaRefusal::UnknownHorsiesObjects { tables })) => {
            assert_eq!(tables, vec!["horsies_tasks".to_owned()]);
        }
        other => panic!("expected an unknown-objects refusal, got {other:?}"),
    }
    assert!(!relation_exists(&database, MIGRATIONS_TABLE).await);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn a_newer_version_is_refused_unchanged() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    let own = expected_schema_version();
    let newer = own + 1;
    database
        .client()
        .execute(
            "INSERT INTO horsies_migrations \
                 (version, description, installed_on, success, checksum, execution_time) \
             VALUES ($1, 'from a newer build', now(), TRUE, '\\x00'::bytea, 0)",
            &[&newer],
        )
        .await
        .expect("record a newer version");
    let before = installed_on(&database).await;

    let refused = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Refused(SchemaRefusal::SchemaNewer {
            stored,
            own: shipped,
        })) => {
            assert_eq!((stored, shipped), (newer, own));
        }
        other => panic!("expected a newer-schema refusal, got {other:?}"),
    }
    assert_eq!(installed_on(&database).await, before);
    database.drop().await;
}

/// A horsies-rust 0.x ledger (versions 1 to 65) is foreign. The read takes
/// the newest 64 rows, so the refusal names versions 2 to 65.
#[tokio::test]
#[serial]
async fn a_horsies_rust_zero_x_ledger_is_refused_as_foreign() {
    let database = MigrationTestDatabase::create().await;
    database
        .execute(
            "CREATE TABLE horsies_migrations (version bigint PRIMARY KEY, \
             description text NOT NULL, installed_on timestamptz NOT NULL, \
             success boolean NOT NULL, checksum bytea NOT NULL, execution_time bigint NOT NULL); \
             INSERT INTO horsies_migrations \
             SELECT v, 'zero x', now(), TRUE, '\\x00'::bytea, 0 FROM generate_series(1, 65) AS v",
        )
        .await;

    let refused = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Refused(SchemaRefusal::ForeignMigrations { versions })) => {
            assert_eq!(versions, (2..=65).collect::<Vec<i64>>());
        }
        other => panic!("expected a foreign-migrations refusal, got {other:?}"),
    }
    assert!(!relation_exists(&database, "horsies_tasks").await);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn a_failed_migration_row_is_refused_as_incomplete() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    let last = expected_schema_version();
    database
        .client()
        .execute(
            "UPDATE horsies_migrations SET success = FALSE WHERE version = $1",
            &[&last],
        )
        .await
        .expect("record the last migration as failed");

    let refused = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Refused(SchemaRefusal::MigrationIncomplete { version })) => {
            assert_eq!(version, last);
        }
        other => panic!("expected an incomplete-migration refusal, got {other:?}"),
    }
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn a_changed_checksum_is_refused_as_edited() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database
        .client()
        .execute(
            "UPDATE horsies_migrations SET checksum = '\\x00'::bytea WHERE version = $1",
            &[&BASELINE_MIGRATION_VERSION],
        )
        .await
        .expect("change the stored checksum");

    let refused = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Refused(SchemaRefusal::MigrationEdited {
            version,
            stored_checksum,
            own_checksum,
        })) => {
            assert_eq!(version, BASELINE_MIGRATION_VERSION);
            assert_eq!(stored_checksum, vec![0_u8]);
            assert_eq!(own_checksum, embedded_migrations()[0].checksum.to_vec());
        }
        other => panic!("expected an edited-migration refusal, got {other:?}"),
    }
    database.drop().await;
}

/// A database that holds migration 1000 alone, as Python's install leaves
/// it, gets the rest of the chain and keeps its family.
#[tokio::test]
#[serial]
async fn a_database_with_the_baseline_only_gets_the_rest_of_the_chain() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Python).await;
    assert_eq!(bookkeeping_rows(&database).await, chain_rows(1));

    database
        .install(ClientFamily::Python, InstallLockPlan::fleet())
        .await
        .expect("apply the rest of the chain");

    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    assert_eq!(family_row(&database).await.as_deref(), Some("PYTHON"));
    database.drop().await;
}

/// The other family is refused before any write: on a database behind the
/// chain no migration is applied, and on a current database nothing changes.
#[tokio::test]
#[serial]
async fn a_database_of_the_other_family_is_refused_before_any_write() {
    let behind = MigrationTestDatabase::create().await;
    behind.install_baseline_only(ClientFamily::Python).await;

    let refused = behind
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    assert!(
        matches!(
            refused,
            Err(SchemaInstallError::OtherFamily {
                stored: ClientFamily::Python,
                own: ClientFamily::Rust,
            })
        ),
        "{refused:?}"
    );
    assert_eq!(bookkeeping_rows(&behind).await, chain_rows(1));
    behind.drop().await;

    let current = MigrationTestDatabase::create().await;
    current
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");

    let refused = current
        .install(ClientFamily::Python, InstallLockPlan::fleet())
        .await;

    assert!(
        matches!(
            refused,
            Err(SchemaInstallError::OtherFamily {
                stored: ClientFamily::Rust,
                own: ClientFamily::Python,
            })
        ),
        "{refused:?}"
    );
    assert_eq!(family_row(&current).await.as_deref(), Some("RUST"));
    current.drop().await;
}

/// A database that holds the chain with no family row gets this client's
/// row, and no migration runs again (Python `CompleteInstall`).
#[tokio::test]
#[serial]
async fn a_database_with_no_family_row_gets_this_client_s_row() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database.execute("DELETE FROM horsies_client_family").await;
    let before = installed_on(&database).await;

    database
        .install(ClientFamily::Python, InstallLockPlan::fleet())
        .await
        .expect("record the family");

    assert_eq!(family_row(&database).await.as_deref(), Some("PYTHON"));
    assert_eq!(installed_on(&database).await, before);
    database.drop().await;
}

/// The signatures of the history readers that do not exist, in order.
async fn missing_readers(database: &MigrationTestDatabase) -> Vec<String> {
    database
        .client()
        .query(
            "SELECT s.signature FROM horsies_history_reader_signatures() AS s \
             WHERE to_regprocedure(s.signature) IS NULL ORDER BY 1",
            &[],
        )
        .await
        .expect("read the reader signatures")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// The leaf names of the lookup manifest, in probe order.
async fn manifest_leaves(database: &MigrationTestDatabase) -> Vec<String> {
    database
        .client()
        .query(
            "SELECT leaf_name FROM horsies_task_lookup_manifest ORDER BY probe_position",
            &[],
        )
        .await
        .expect("read the manifest")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// A database that holds the chain and its family row with readers missing
/// and an empty lookup manifest (an install that stopped before the
/// publication, as Python's install left it) gets the readers and the
/// manifest row of the static forever leaf, and no migration runs again
/// (Python `CompleteInstall`;
/// `test_an_install_that_stopped_before_the_publication_is_completed`).
#[tokio::test]
#[serial]
async fn a_database_whose_readers_are_missing_gets_them_published() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Python, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database
        .execute(
            "DROP FUNCTION horsies_task_detail_staged(uuid); \
             DROP FUNCTION horsies_task_result_staged(uuid); \
             DELETE FROM horsies_task_lookup_manifest;",
        )
        .await;
    assert_eq!(
        missing_readers(&database).await,
        vec![
            "horsies_task_detail_staged(uuid)".to_owned(),
            "horsies_task_result_staged(uuid)".to_owned(),
        ]
    );
    let before = installed_on(&database).await;

    database
        .install(ClientFamily::Python, InstallLockPlan::fleet())
        .await
        .expect("complete the install");

    assert!(missing_readers(&database).await.is_empty());
    assert_eq!(
        manifest_leaves(&database).await,
        vec!["horsies_task_history_forever_open".to_owned()]
    );
    let current: bool = database
        .client()
        .query_one("SELECT horsies_history_readers_current()", &[])
        .await
        .expect("read the reader stamps")
        .get(0);
    assert!(current);
    assert_eq!(installed_on(&database).await, before);
    assert_eq!(family_row(&database).await.as_deref(), Some("PYTHON"));
    database.drop().await;
}

/// A database with readers missing and no family row gets both, the
/// readers first (Python `CompleteInstall`).
#[tokio::test]
#[serial]
async fn a_database_with_no_readers_and_no_family_row_gets_both() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database
        .execute(
            "DROP FUNCTION horsies_task_history_duplicate_staged(uuid); \
             DELETE FROM horsies_task_lookup_manifest; \
             DELETE FROM horsies_client_family;",
        )
        .await;
    let before = installed_on(&database).await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("complete the install");

    assert!(missing_readers(&database).await.is_empty());
    assert_eq!(
        manifest_leaves(&database).await,
        vec!["horsies_task_history_forever_open".to_owned()]
    );
    assert_eq!(family_row(&database).await.as_deref(), Some("RUST"));
    assert_eq!(installed_on(&database).await, before);
    database.drop().await;
}

/// A database of the other family with readers missing is refused and
/// changed in nothing: the family is checked before the publication.
#[tokio::test]
#[serial]
async fn a_database_of_the_other_family_with_readers_missing_is_refused_unchanged() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database
        .execute("DROP FUNCTION horsies_task_detail_staged(uuid);")
        .await;

    let refused = database
        .install(ClientFamily::Python, InstallLockPlan::fleet())
        .await;

    assert!(
        matches!(
            refused,
            Err(SchemaInstallError::OtherFamily {
                stored: ClientFamily::Rust,
                own: ClientFamily::Python,
            })
        ),
        "{refused:?}"
    );
    assert_eq!(
        missing_readers(&database).await,
        vec!["horsies_task_detail_staged(uuid)".to_owned()]
    );
    database.drop().await;
}

/// While another session holds the lock for the whole wait, the install
/// refuses with a retryable failure and changes nothing; once the lock is
/// free, it installs.
#[tokio::test]
#[serial]
async fn a_busy_lock_refuses_after_its_wait_and_changes_nothing() {
    let database = MigrationTestDatabase::create().await;
    let lock = test_lock();
    let holder = database.hold_lock(lock.key).await;

    let refused = database.install(ClientFamily::Rust, lock).await;

    match refused {
        Err(SchemaInstallError::Lock(InstallLockFailure::WaitBoundReached { waited })) => {
            assert_eq!(waited, lock.wait);
        }
        other => panic!("expected the wait bound, got {other:?}"),
    }
    assert!(!relation_exists(&database, MIGRATIONS_TABLE).await);

    let released: bool = holder
        .query_one("SELECT pg_advisory_unlock($1)", &[&lock.key])
        .await
        .expect("release the lock")
        .get(0);
    assert!(released);
    database
        .install(ClientFamily::Rust, lock)
        .await
        .expect("the free lock is taken");
    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    drop(holder);
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestBaselineInstall::test_install_finished_by_another_broker_while_waiting_is_not_repeated`:
/// an installer that waits for the lock reads the state again under it. When
/// another installer finished the chain during the wait, the waiter applies
/// nothing and returns.
#[tokio::test]
#[serial]
async fn an_install_finished_by_another_installer_during_the_wait_is_not_repeated() {
    let database = MigrationTestDatabase::create().await;
    let lock = InstallLockPlan {
        key: rand::thread_rng().gen_range(1..i64::MAX),
        wait: Duration::from_secs(60),
    };
    let holder = database.hold_lock(lock.key).await;
    let other_installer = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        database
            .install(ClientFamily::Rust, test_lock())
            .await
            .expect("the other installer installs the chain");
        let installed = installed_on(&database).await;
        let released: bool = holder
            .query_one("SELECT pg_advisory_unlock($1)", &[&lock.key])
            .await
            .expect("release the lock")
            .get(0);
        assert!(released);
        installed
    };

    let (waited, installed) =
        tokio::join!(database.install(ClientFamily::Rust, lock), other_installer);

    waited.expect("the waiter finds the chain current");
    assert_eq!(installed_on(&database).await, installed);
    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestBaselineInstall::test_read_only_broker_runs_no_schema_statement`:
/// an observe-only broker runs no schema statement. On an empty database its
/// install calls return and the database stays empty.
#[tokio::test]
#[serial]
async fn an_observe_only_broker_runs_no_schema_statement() {
    let database = MigrationTestDatabase::create().await;
    let broker = crate::broker::PostgresBroker::connect_observe_only(
        &crate::core::config::PostgresConfig::from_url(database.database.url()),
    )
    .await
    .expect("connect an observe-only broker");

    broker
        .ensure_schema_initialized()
        .await
        .expect("an observe-only broker installs nothing");
    broker
        .migrate()
        .await
        .expect("an observe-only broker migrates nothing");

    assert!(!relation_exists(&database, MIGRATIONS_TABLE).await);
    assert!(!relation_exists(&database, "horsies_client_family").await);
    broker.close().await;
    drop(broker);
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestEnsureSchemaInitialized::test_ensure_schema_initialized_returns_ok_on_success`:
/// on a database that holds the chain and this client's family, the install
/// of a broker returns `Ok`, again on a second call, and writes nothing.
#[tokio::test]
#[serial]
async fn the_install_of_a_broker_on_a_current_database_is_ok_and_writes_nothing() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    let before = installed_on(&database).await;
    let broker = crate::broker::PostgresBroker::connect_with(
        &crate::core::config::PostgresConfig::from_url(database.database.url()),
    )
    .await
    .expect("connect the broker");

    broker
        .ensure_schema_initialized()
        .await
        .expect("a current database is installed");
    broker
        .ensure_schema_initialized()
        .await
        .expect("a second call is a no-op");

    assert_eq!(installed_on(&database).await, before);
    broker.close().await;
    drop(broker);
    database.drop().await;
}

/// A statement of migration 1000 fails: its transaction rolls back with the
/// bookkeeping and family rows, and the lock is released.
#[tokio::test]
#[serial]
async fn a_failed_migration_rolls_back_and_releases_the_lock() {
    let database = MigrationTestDatabase::create().await;
    // A view is not a table, so the state read sees an empty schema; the
    // baseline's CREATE TABLE horsies_tasks then fails.
    database
        .execute("CREATE VIEW horsies_tasks AS SELECT 1 AS id")
        .await;
    let lock = test_lock();

    let failed = database.install(ClientFamily::Rust, lock).await;

    match failed {
        Err(SchemaInstallError::Statement(StatementFailure::StatementRefused {
            statement,
            cause,
        })) => {
            assert_eq!(statement, "migration_1000_baseline");
            assert_eq!(cause.server_error().code(), &SqlState::DUPLICATE_TABLE);
        }
        other => panic!("expected the refused migration, got {other:?}"),
    }
    assert!(!relation_exists(&database, MIGRATIONS_TABLE).await);
    assert!(!relation_exists(&database, "horsies_client_family").await);
    let holder = database.hold_lock(lock.key).await;
    drop(holder);
    database.drop().await;
}

/// A release that frees no lock is reported as lost ownership.
#[tokio::test]
#[serial]
async fn a_release_that_frees_no_lock_is_ownership_lost() {
    let database = MigrationTestDatabase::create().await;
    let handle = core_handle(database.database.url());
    let open = handle
        .pools()
        .expect("the statement layer accepts the test URL");
    let connection = open
        .pools()
        .session_work()
        .checkout(CheckoutClass::Batch)
        .await
        .expect("check out a connection");
    let session = LockSession {
        connection,
        hold: LockHold::MayHold,
    };

    let released = session.release(test_lock().key, StatementLog::Off).await;

    assert!(
        matches!(
            released,
            Err(SchemaInstallError::Lock(InstallLockFailure::OwnershipLost))
        ),
        "{released:?}"
    );
    drop(open);
    let _closed = handle.close().await;
    database.drop().await;
}

/// A deadlock runs the whole install again: the first two runs meet one,
/// the third installs.
#[tokio::test]
#[serial]
async fn a_deadlock_runs_the_install_again() {
    let database = MigrationTestDatabase::create().await;
    inject_deadlocks(&database, 2).await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("the third run installs");

    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    database.drop().await;
}

/// A deadlock on every run ends after [`INSTALL_MAX_ATTEMPTS`] runs with the
/// deadlock, and nothing is installed.
#[tokio::test]
#[serial]
async fn a_deadlock_on_every_run_stops_after_the_last_attempt() {
    let database = MigrationTestDatabase::create().await;
    inject_deadlocks(&database, i64::MAX).await;

    let failed = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match failed {
        Err(SchemaInstallError::Statement(StatementFailure::StatementRefused {
            cause, ..
        })) => {
            assert!(is_deadlock_refusal(&cause), "{cause}");
        }
        other => panic!("expected the deadlock, got {other:?}"),
    }
    let runs: i64 = database
        .client()
        .query_one("SELECT last_value FROM install_deadlock_probe", &[])
        .await
        .expect("read the probe count")
        .get(0);
    assert_eq!(runs, i64::from(INSTALL_MAX_ATTEMPTS));
    assert!(!relation_exists(&database, MIGRATIONS_TABLE).await);
    database.drop().await;
}

// ---- The history readers of migration 1000 ----

/// The install returns with the history readers published: each reader of
/// `horsies_history_reader_signatures()` exists and is current, the manifest
/// is the static forever leaf, and the detail reader answers an unknown id
/// with no row (Python `test_install_returns_with_the_history_readers_published`).
#[tokio::test]
#[serial]
async fn an_installed_database_has_its_history_readers() {
    let database = MigrationTestDatabase::create().await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");

    let client = database.client();
    let missing: Vec<String> = client
        .query(
            "SELECT s.signature FROM horsies_history_reader_signatures() AS s \
             WHERE to_regprocedure(s.signature) IS NULL ORDER BY 1",
            &[],
        )
        .await
        .expect("read the reader signatures")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert!(missing.is_empty(), "missing readers: {missing:?}");
    let current: bool = client
        .query_one("SELECT horsies_history_readers_current()", &[])
        .await
        .expect("read the reader stamps")
        .get(0);
    assert!(current);
    let manifest: Vec<String> = client
        .query(
            "SELECT leaf_name FROM horsies_task_lookup_manifest ORDER BY probe_position",
            &[],
        )
        .await
        .expect("read the manifest")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        manifest,
        vec!["horsies_task_history_forever_open".to_owned()]
    );
    let unknown = uuid::Uuid::new_v4();
    let detail_rows: i64 = client
        .query_one(
            "SELECT count(*) FROM horsies_task_detail_staged($1)",
            &[&unknown],
        )
        .await
        .expect("call the detail reader")
        .get(0);
    assert_eq!(detail_rows, 0);
    database.drop().await;
}

/// The transaction of a migration is READ COMMITTED whatever the server
/// default is: the publication refuses any other isolation (25000).
#[tokio::test]
#[serial]
async fn the_readers_are_published_under_a_serializable_server_default() {
    let database = MigrationTestDatabase::create().await;
    database
        .execute(&format!(
            "ALTER DATABASE \"{}\" SET default_transaction_isolation = 'serializable'",
            database.database.name()
        ))
        .await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install under a serializable default");

    let current: bool = database
        .client()
        .query_one("SELECT horsies_history_readers_current()", &[])
        .await
        .expect("read the reader stamps")
        .get(0);
    assert!(current);
    database.drop().await;
}

/// A publication that fails rolls migration 1000 back with it: no
/// bookkeeping row, no family row, no table. An event trigger fails the DDL
/// of the publication statement only.
#[tokio::test]
#[serial]
async fn a_failed_reader_publication_rolls_back_migration_1000() {
    let database = MigrationTestDatabase::create().await;
    database
        .execute(
            "CREATE FUNCTION install_publication_probe() RETURNS event_trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF current_query() LIKE 'SELECT outcome FROM horsies_publish_history_readers()%' THEN
                     RAISE EXCEPTION 'injected publication failure';
                 END IF;
             END
             $$",
        )
        .await;
    database
        .execute(
            "CREATE EVENT TRIGGER install_publication_probe ON ddl_command_start
             EXECUTE FUNCTION install_publication_probe()",
        )
        .await;

    let failed = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match failed {
        Err(SchemaInstallError::Statement(StatementFailure::StatementRefused {
            statement,
            cause,
        })) => {
            assert_eq!(statement, "publish_history_readers");
            assert_eq!(cause.server_error().code(), &SqlState::RAISE_EXCEPTION);
        }
        other => panic!("expected the refused publication, got {other:?}"),
    }
    assert!(!relation_exists(&database, MIGRATIONS_TABLE).await);
    assert!(!relation_exists(&database, "horsies_client_family").await);
    assert!(!relation_exists(&database, "horsies_task_lookup_manifest").await);
    database.drop().await;
}
