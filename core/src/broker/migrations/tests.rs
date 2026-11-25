//! Tests of the schema install.
//!
//! The pure tests cover the chain, the classification, the decision and the
//! error forms. Each database test runs the install on an empty database of
//! its own (`crate::test_support::TestDatabase::create_empty`) through a core
//! handle, and reads the result through a plain driver connection; one test
//! per install outcome.

use std::time::Duration;

use rand::Rng;
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

    fn url(&self) -> &str {
        self.database.url()
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
        self.install_first_migrations(own, 1).await;
    }

    /// Applies the first `count` migrations of the embedded chain, in order,
    /// with `own` as the family ([`apply_chain`] on that start of the chain):
    /// the database that a build whose chain ends there leaves. The migration
    /// of them that admits `own` writes the family row; when none admits
    /// `own`, the database has no family row.
    async fn install_first_migrations(&self, own: ClientFamily, count: usize) {
        self.apply_embedded(own, 0, count).await;
    }

    /// Applies the migrations of the embedded chain at positions `first` up
    /// to `end` (not included), in order, with `own` as the family
    /// ([`apply_chain`] on the first `end` migrations, from `first`).
    async fn apply_embedded(&self, own: ClientFamily, first: usize, end: usize) {
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
        apply_chain(
            &mut connection,
            &embedded_migrations()[..end],
            first,
            own,
            StatementLog::Off,
        )
        .await
        .unwrap_or_else(|error| panic!("apply migrations {first} to {end}: {error}"));
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
    // The list equals the folder below, so a non-empty folder is a non-empty
    // list.
    assert!(!folder.is_empty(), "the baseline folder is empty");
    assert_eq!(BASELINE_SQL_FILES, folder.as_slice());
}

/// Migration 1000 is the listed files as they are on disk, concatenated in
/// list order, and its checksum is the SHA-384 of the UTF-8 bytes of that
/// text. `BASELINE_SQL_FILE_TEXTS`, which
/// Python reads, holds the same names and texts in the same order.
#[test]
fn baseline_is_one_migration_built_from_every_listed_file() {
    let baseline = &embedded_migrations()[0];
    assert_eq!(baseline.version, BASELINE_MIGRATION_VERSION);
    assert_eq!(baseline.description, "baseline");
    assert_eq!(
        baseline.admitted_families,
        [ClientFamily::Python, ClientFamily::Rust].as_slice()
    );
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

/// The checksum of migration 1000, pinned. An edit of a baseline SQL file
/// changes it, so the edit shows in this test's diff as well, and a database
/// installed before the edit refuses the build as edited
/// (`a_changed_checksum_is_refused_as_edited`). Python
/// `clients/python/tests/unit/test_baseline_migration.py::test_checksum_is_pinned`.
#[test]
fn the_checksum_of_the_baseline_is_pinned() {
    const PINNED_CHECKSUM_HEX: &str = concat!(
        "eaae7ceb72bbf0b8b80ae53938c5b1eaab3d4096ca78a99c",
        "4e61916f8241cf5e3bc9b2d5dfe6eea621bb40a9746f408d",
    );
    let baseline = &embedded_migrations()[0];
    assert_eq!(baseline.version, BASELINE_MIGRATION_VERSION);
    assert_eq!(hex(&baseline.checksum), PINNED_CHECKSUM_HEX);
}

/// The baseline text names no schema and holds no transaction control. The
/// install creates the objects in the current schema, so a qualifier would put
/// them elsewhere; it runs the text inside its own transaction with the
/// bookkeeping row, so a COMMIT in the text would split the two. Python
/// `clients/python/tests/unit/test_baseline_migration.py::test_text_names_no_schema`,
/// `::test_text_has_no_transaction_control`.
#[test]
fn the_baseline_text_names_no_schema_and_holds_no_transaction_control() {
    let text = embedded_migrations()[0].script.sql();
    assert!(
        !text.contains("public."),
        "the baseline names the public schema"
    );
    for line in text.lines() {
        let statement = line.trim().to_uppercase();
        assert!(
            !["BEGIN;", "COMMIT;", "ROLLBACK;", "START TRANSACTION;"].contains(&statement.as_str()),
            "the baseline holds transaction control: {line}"
        );
    }
}

/// The chain starts at the baseline and ascends; only the baseline publishes
/// the history readers; each checksum is SHA-384 of its text.
#[test]
fn the_chain_ascends_from_the_baseline_and_only_the_baseline_publishes_the_readers() {
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
        let readers = match index {
            0 => ReaderPublication::PublishedWithMigration,
            _ => ReaderPublication::NotPublished,
        };
        assert_eq!(migration.readers, readers, "{}", migration.version);
        assert_eq!(
            migration.checksum.as_slice(),
            Sha384::digest(migration.script.sql().as_bytes()).as_slice(),
            "{}",
            migration.version
        );
    }
}

/// Every client family. The `match` stops the build when a variant is added
/// without an entry in the list.
fn every_family() -> [ClientFamily; 3] {
    let families = [
        ClientFamily::Python,
        ClientFamily::Rust,
        ClientFamily::TypeScript,
    ];
    for family in families {
        match family {
            ClientFamily::Python | ClientFamily::Rust | ClientFamily::TypeScript => {}
        }
    }
    families
}

/// The position of the one migration of the embedded chain that admits
/// `family`. Each client family is in the admitted families of exactly one
/// migration.
fn admitting_position(family: ClientFamily) -> usize {
    let admitting: Vec<usize> = embedded_migrations()
        .iter()
        .enumerate()
        .filter(|(_, migration)| migration.admitted_families.contains(&family))
        .map(|(position, _)| position)
        .collect();
    match admitting.as_slice() {
        [position] => *position,
        other => panic!("{family:?} is admitted by the migrations at {other:?}, not by one"),
    }
}

/// The rule of the family row is the same for every client family: the
/// transaction of the first migration of the chain that admits the family
/// writes its row, and no other transaction does. PYTHON and RUST are
/// admitted by migration 1000, TYPESCRIPT by migration 1006. A start of the
/// chain that ends before that migration (the chain of an earlier build)
/// writes no row.
#[test]
fn only_the_migration_that_admits_a_family_writes_its_row() {
    let chain = embedded_migrations();
    assert_eq!(admitting_position(ClientFamily::Python), 0);
    assert_eq!(admitting_position(ClientFamily::Rust), 0);
    assert_eq!(
        admitting_position(ClientFamily::TypeScript),
        typescript_client_family_position()
    );
    for own in every_family() {
        let admitting = admitting_position(own);
        for end in 1..=chain.len() {
            let written: Vec<usize> = (0..end)
                .filter(|position| {
                    family_row_at(&chain[..end], *position, own) == FamilyRow::Written(own)
                })
                .collect();
            let expected: Vec<usize> = [admitting].into_iter().filter(|at| *at < end).collect();
            assert_eq!(written, expected, "{own:?}, a chain of {end}");
        }
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
            admitted_families: &[],
            readers: ReaderPublication::NotPublished,
            exact_indexes: &[],
            retired_indexes: &[],
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
    use crate::broker::client_family::ClientFamily::{Python, Rust, TypeScript};

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
            DatabaseState::Behind {
                applied: 1,
                family: RecordedFamily::Recorded(TypeScript),
            },
            Rust,
            InstallAction::Refuse(InstallRefusal::OtherFamily {
                stored: TypeScript,
                own: Rust,
            }),
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::Recorded(TypeScript),
                readers: ReaderPresence::Published,
            },
            Python,
            InstallAction::Refuse(InstallRefusal::OtherFamily {
                stored: TypeScript,
                own: Python,
            }),
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::Recorded(TypeScript),
                readers: ReaderPresence::Published,
            },
            TypeScript,
            InstallAction::Ready,
        ),
        (
            DatabaseState::Current {
                family: RecordedFamily::Recorded(Rust),
                readers: ReaderPresence::Published,
            },
            TypeScript,
            InstallAction::Refuse(InstallRefusal::OtherFamily {
                stored: Rust,
                own: TypeScript,
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

    let unknown = UnknownStoredFamily {
        stored: "GO".to_owned(),
    };
    let unknown_family =
        BrokerError::from(SchemaInstallError::UnknownStoredFamily(unknown.clone()));
    assert!(
        matches!(&unknown_family, BrokerError::UnknownStoredFamily(inner) if *inner == unknown),
        "{unknown_family:?}"
    );
    assert!(!unknown_family.is_retryable());

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

/// A family row that holds no client family is corrupt stored data: the
/// install of either family is refused with the stored value at its state
/// read, and changes nothing. The test writes the row past its CHECK
/// (`horsies_client_family_family_known`), which it drops first.
#[tokio::test]
async fn a_family_row_that_holds_no_client_family_is_refused_and_unchanged() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database
        .execute(
            "ALTER TABLE horsies_client_family \
             DROP CONSTRAINT horsies_client_family_family_known; \
             UPDATE horsies_client_family SET family = 'GO'",
        )
        .await;
    let before = installed_on(&database).await;

    for own in [ClientFamily::Python, ClientFamily::Rust] {
        let refused = database.install(own, InstallLockPlan::fleet()).await;
        assert!(
            matches!(
                &refused,
                Err(SchemaInstallError::UnknownStoredFamily(UnknownStoredFamily { stored }))
                    if stored == "GO"
            ),
            "{refused:?}"
        );
    }
    assert_eq!(family_row(&database).await.as_deref(), Some("GO"));
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

/// An installer that waits for the lock reads the state again under it. When
/// an installer of another family finished the chain during the wait, the
/// waiter is refused with the stored family and writes nothing: the read
/// under the lock is the barrier against another family during an install.
#[tokio::test]
async fn an_installer_of_another_family_that_waits_for_the_lock_is_refused_after_the_wait() {
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
            .expect("the Rust installer installs the chain");
        let installed = installed_on(&database).await;
        let released: bool = holder
            .query_one("SELECT pg_advisory_unlock($1)", &[&lock.key])
            .await
            .expect("release the lock")
            .get(0);
        assert!(released);
        installed
    };

    let (waited, installed) = tokio::join!(
        database.install(ClientFamily::TypeScript, lock),
        other_installer
    );

    assert!(
        matches!(
            waited,
            Err(SchemaInstallError::OtherFamily {
                stored: ClientFamily::Rust,
                own: ClientFamily::TypeScript,
            })
        ),
        "{waited:?}"
    );
    assert_eq!(installed_on(&database).await, installed);
    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    assert_eq!(family_row(&database).await.as_deref(), Some("RUST"));
    database.drop().await;
}

/// The polls of [`wait_for_a_lock_try`], 20 ms apart: at most 10 s.
const LOCK_TRY_POLLS: u32 = 500;

/// The pause between two polls of [`wait_for_a_lock_try`].
const LOCK_TRY_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The statement timeout of the index build that runs while an installer
/// waits for the lock.
const INDEX_BUILD_STATEMENT_TIMEOUT: &str = "2s";

/// Whether a backend other than `observer` and `holder_pid` ran the lock try
/// of the install ([`TRY_INSTALL_LOCK`]) within [`LOCK_TRY_POLLS`] polls.
async fn wait_for_a_lock_try(observer: &Client, holder_pid: i32) -> bool {
    for _ in 0..LOCK_TRY_POLLS {
        let seen: bool = observer
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND pid NOT IN (pg_backend_pid(), $1)
                       AND query LIKE '%' || $2 || '%'
                 )",
                &[&holder_pid, &TRY_INSTALL_LOCK.sql()],
            )
            .await
            .expect("read the backends of the database")
            .get(0);
        match seen {
            true => return true,
            false => tokio::time::sleep(LOCK_TRY_POLL_INTERVAL).await,
        }
    }
    false
}

/// Python `clients/python/tests/integration/test_schema_lock_wait.py::test_waiting_migrator_does_not_block_a_concurrent_index_build`:
/// an installer that waits for the install lock holds no snapshot while it
/// waits: it tries the lock in one statement every [`INSTALL_LOCK_POLL`] and
/// holds no transaction between the tries. An index build that waits for
/// every older snapshot (`CREATE INDEX CONCURRENTLY`, the form of the index
/// steps that a lock holder runs) ends within its statement timeout while the
/// installer waits, and the installer installs the chain once the lock is
/// released.
#[tokio::test]
async fn an_installer_that_waits_for_the_lock_does_not_block_a_concurrent_index_build() {
    let database = MigrationTestDatabase::create().await;
    let lock = InstallLockPlan {
        key: rand::thread_rng().gen_range(1..i64::MAX),
        wait: Duration::from_secs(60),
    };
    let holder = database.hold_lock(lock.key).await;
    let holder_pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("read the pid of the lock holder")
        .get(0);
    database
        .execute("CREATE TABLE lock_wait_probe (name text)")
        .await;
    let builder = database.database.connect().await;
    builder
        .batch_execute(&format!(
            "SET statement_timeout = '{INDEX_BUILD_STATEMENT_TIMEOUT}'"
        ))
        .await
        .expect("bound the index build");
    let build_during_the_wait = async {
        assert!(
            wait_for_a_lock_try(database.client(), holder_pid).await,
            "the installer tries the lock"
        );
        builder
            .batch_execute(
                "CREATE INDEX CONCURRENTLY lock_wait_probe_name ON lock_wait_probe (name)",
            )
            .await
            .expect("the index build ends while the installer waits");
        let released: bool = holder
            .query_one("SELECT pg_advisory_unlock($1)", &[&lock.key])
            .await
            .expect("release the lock")
            .get(0);
        assert!(released);
    };

    let (installed, ()) = tokio::join!(
        database.install(ClientFamily::Rust, lock),
        build_during_the_wait
    );

    installed.expect("the installer installs after the release");
    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    drop(builder);
    drop(holder);
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestBaselineInstall::test_read_only_broker_runs_no_schema_statement`:
/// an observe-only broker runs no schema statement. On an empty database its
/// install calls return and the database stays empty.
#[tokio::test]
async fn an_observe_only_broker_runs_no_schema_statement() {
    let database = MigrationTestDatabase::create().await;
    let broker = crate::broker::PostgresBroker::connect_observe_only(
        &crate::core::config::PostgresConfig::from_url(database.database.url()),
        crate::ClientFamily::Rust,
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
/// of a broker returns `Ok`, again on a second call, and applies no
/// migration: the bookkeeping rows keep their install times. (The install's
/// coverage pass writes leaves; `partition_maintenance` tests it.)
#[tokio::test]
async fn the_install_of_a_broker_on_a_current_database_is_ok_and_writes_nothing() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    let before = installed_on(&database).await;
    let broker = crate::broker::PostgresBroker::connect_with(
        &crate::core::config::PostgresConfig::from_url(database.database.url()),
        crate::ClientFamily::Rust,
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

/// The structural signature of one relation: a hash of its kind,
/// persistence, access method and options; its columns with their types,
/// defaults and identity; its constraints, indexes and triggers. `$1` is the
/// relation name.
const RELATION_SCHEMA_SIGNATURE_SQL: &str = "\
SELECT encode(sha256(convert_to(
    jsonb_build_object(
        'relation', jsonb_build_array(
            relation.relkind, relation.relpersistence, relation.relam, relation.reloptions
        ),
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    attribute.attnum, attribute.attname, attribute.atttypid, attribute.atttypmod,
                    attribute.attcollation, attribute.attnotnull, attribute.attidentity,
                    attribute.attgenerated, pg_get_expr(defaults.adbin, defaults.adrelid)
                ) ORDER BY attribute.attnum
            )
            FROM pg_attribute AS attribute
            LEFT JOIN pg_attrdef AS defaults
              ON defaults.adrelid = attribute.attrelid AND defaults.adnum = attribute.attnum
            WHERE attribute.attrelid = relation.oid AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    constraints.conname, constraints.contype, constraints.convalidated,
                    pg_get_constraintdef(constraints.oid, false)
                ) ORDER BY constraints.conname
            )
            FROM pg_constraint AS constraints
            WHERE constraints.conrelid = relation.oid
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    indexes.indisvalid, indexes.indisready, pg_get_indexdef(indexes.indexrelid)
                ) ORDER BY indexes.indexrelid
            )
            FROM pg_index AS indexes
            WHERE indexes.indrelid = relation.oid
        ), '[]'::jsonb),
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(triggers.tgenabled, pg_get_triggerdef(triggers.oid, false))
                ORDER BY triggers.tgname
            )
            FROM pg_trigger AS triggers
            WHERE triggers.tgrelid = relation.oid AND NOT triggers.tgisinternal
        ), '[]'::jsonb)
    )::text,
    'UTF8'
)), 'hex')
FROM pg_class AS relation
WHERE relation.oid = to_regclass($1)";

/// The structural signature of the history parent, the forever history
/// relation and the key reservation table, read in UTC so that two captures
/// compare.
async fn history_relation_signatures(database: &MigrationTestDatabase) -> Vec<(String, String)> {
    database
        .client()
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .expect("render in UTC");
    let mut signatures = Vec::new();
    for relation in [
        crate::core::history::names::TASK_HISTORY_PARENT,
        crate::core::history::names::TASK_HISTORY_FOREVER,
        crate::core::history::names::KEY_RESERVATIONS,
    ] {
        let signature: String = database
            .client()
            .query_one(RELATION_SCHEMA_SIGNATURE_SQL, &[&relation])
            .await
            .unwrap_or_else(|error| panic!("read the signature of {relation}: {error}"))
            .get(0);
        signatures.push((relation.to_owned(), signature));
    }
    signatures
}

/// Installs the schema through a broker of its own, as a new client process
/// does, and closes the broker.
async fn install_through_a_new_broker(database: &MigrationTestDatabase) {
    let broker = crate::broker::PostgresBroker::connect_with(
        &crate::core::config::PostgresConfig::from_url(database.database.url()),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");
    broker
        .ensure_schema_initialized()
        .await
        .expect("install the schema");
    broker.close().await;
}

/// A second install, by another broker, leaves the structure of the history
/// parent, the forever history relation and the key reservation table
/// unchanged, and the stored version stays the newest version of the chain.
#[tokio::test]
async fn a_second_install_leaves_the_history_relations_unchanged() {
    let database = MigrationTestDatabase::create().await;
    install_through_a_new_broker(&database).await;
    let before = history_relation_signatures(&database).await;

    install_through_a_new_broker(&database).await;

    assert_eq!(history_relation_signatures(&database).await, before);
    let version: i64 = database
        .client()
        .query_one("SELECT MAX(version) FROM horsies_migrations", &[])
        .await
        .expect("read the stored version")
        .get(0);
    assert_eq!(version, expected_schema_version());
    database.drop().await;
}

/// A statement of migration 1000 fails: its transaction rolls back with its
/// bookkeeping row and the family table, and the lock is released.
#[tokio::test]
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

/// Python `test_broker_postgres.py::TestPostgresBrokerPgBouncerWiring::test_schema_lock_cancellation_releases_and_closes`:
/// a lock session dropped while it may hold the lock (an install whose
/// caller left) discards its connection. The server ends that session and
/// its lock, so another session takes the lock.
#[tokio::test]
async fn a_dropped_lock_session_frees_the_lock_with_its_session() {
    let database = MigrationTestDatabase::create().await;
    let handle = core_handle(database.database.url());
    let open = handle
        .pools()
        .expect("the statement layer accepts the test URL");
    let lock = test_lock();
    let session = LockSession::acquire(open.pools().session_work(), lock, StatementLog::Off)
        .await
        .expect("a free lock is taken");
    let taken_by_another_session = || async {
        database
            .client()
            .query_one("SELECT pg_try_advisory_lock($1)", &[&lock.key])
            .await
            .expect("try the lock from another session")
            .get::<_, bool>(0)
    };
    assert!(
        !taken_by_another_session().await,
        "the lock session holds the lock"
    );

    drop(session);

    let mut freed = false;
    for _ in 0..250 {
        if taken_by_another_session().await {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(freed, "the lock is free once the dropped session ends");
    let _unlocked = database
        .client()
        .query_one("SELECT pg_advisory_unlock($1)", &[&lock.key])
        .await
        .expect("release the lock of the test session");
    drop(open);
    let _closed = handle.close().await;
    database.drop().await;
}

/// A deadlock runs the whole install again: the first two runs meet one,
/// the third installs.
#[tokio::test]
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
/// bookkeeping table, no family table, no lookup manifest. An event trigger
/// fails the DDL of the publication statement only.
#[tokio::test]
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

// ---- Index steps ----

/// The exact index of the index-step tests.
static STEP_EXACT: [ExactIndex; 1] = [exact_index!(
    "horsies_test_step_idx",
    "horsies_tasks",
    "(queue_name, priority)"
)];

/// The key of [`STEP_EXACT`], as `pg_get_indexdef` writes it.
const STEP_EXACT_KEY: &str = "(queue_name, priority)";

/// The retired index of the index-step tests.
static STEP_RETIRED: [RetiredIndex; 1] =
    [retired_index!("horsies_test_retired_idx", "horsies_tasks")];

/// A chain of migration 1000 (installed by the test, never applied from
/// here) and migration 1001 with `script`, [`STEP_EXACT`] and
/// [`STEP_RETIRED`].
fn step_chain(script: SqlScript) -> Vec<EmbeddedMigration> {
    vec![
        EmbeddedMigration {
            version: BASELINE_MIGRATION_VERSION,
            description: "baseline",
            script: SqlScript::new("test_baseline", "SELECT 1"),
            admitted_families: &[],
            readers: ReaderPublication::NotPublished,
            exact_indexes: &[],
            retired_indexes: &[],
            checksum: [0; CHECKSUM_BYTES],
        },
        EmbeddedMigration {
            version: 1001,
            description: "index steps",
            script,
            admitted_families: &[],
            readers: ReaderPublication::NotPublished,
            exact_indexes: &STEP_EXACT,
            retired_indexes: &STEP_RETIRED,
            checksum: [1; CHECKSUM_BYTES],
        },
    ]
}

/// The script of migration 1001 that commits.
const STEP_SCRIPT: SqlScript = SqlScript::new("test_index_steps", "SELECT 1");

/// The script of migration 1001 that fails: its transaction rolls back.
const FAILING_STEP_SCRIPT: SqlScript = SqlScript::new(
    "test_index_steps_failing",
    "SELECT horsies_test_no_such_function()",
);

impl MigrationTestDatabase {
    /// Installs migration 1000 alone, then applies `chain` from index 1 on
    /// ([`apply_chain`]) on one session-work connection of a new core
    /// handle.
    async fn apply_after_baseline(
        &self,
        chain: &[EmbeddedMigration],
    ) -> Result<(), SchemaInstallError> {
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
        let applied = apply_chain(
            &mut connection,
            chain,
            1,
            ClientFamily::Rust,
            StatementLog::Off,
        )
        .await;
        drop(connection);
        drop(open);
        let _closed = handle.close().await;
        applied
    }

    /// The oid of the relation `name`, or `None` when no relation has it.
    async fn relation_oid(&self, name: &str) -> Option<u32> {
        self.client()
            .query_one(
                "SELECT to_regclass($1)::oid FROM (SELECT 1) AS one",
                &[&name],
            )
            .await
            .expect("read the catalog")
            .get(0)
    }

    /// The definition and the validity of the index `name`, or `None` when
    /// no index has the name.
    async fn index_form(&self, name: &str) -> Option<(String, bool)> {
        self.client()
            .query_opt(
                "SELECT pg_get_indexdef(i.indexrelid), i.indisvalid FROM pg_index AS i \
                 WHERE i.indexrelid = to_regclass($1)",
                &[&name],
            )
            .await
            .expect("read the index")
            .map(|row| (row.get(0), row.get(1)))
    }
}

/// Every declared index name of the chain and of these tests is an unquoted
/// identifier whose build and claim names stay inside the identifier limit.
#[test]
fn every_declared_index_name_leaves_room_for_its_build_and_claim_names() {
    let exact = embedded_migrations()
        .iter()
        .flat_map(|migration| migration.exact_indexes.iter())
        .chain(STEP_EXACT.iter());
    for index in exact {
        assert!(
            index.name.len() <= MAX_STEP_INDEX_NAME_BYTES,
            "{}",
            index.name
        );
        for name in [index.name, index.table, index.build_name, index.claim_name] {
            assert!(crate::db::SqlIdentifier::parse(name).is_ok(), "{name}");
        }
    }
    let retired = embedded_migrations()
        .iter()
        .flat_map(|migration| migration.retired_indexes.iter())
        .chain(STEP_RETIRED.iter());
    for index in retired {
        assert!(
            index.name.len() <= MAX_STEP_INDEX_NAME_BYTES,
            "{}",
            index.name
        );
        for name in [index.name, index.table, index.claim_name] {
            assert!(crate::db::SqlIdentifier::parse(name).is_ok(), "{name}");
        }
    }
}

/// Python `TestExactIndexInstall::test_an_upgrade_keeps_each_exact_index_whose_shape_matches`:
/// an index whose name and form match the declaration is kept, not rebuilt.
#[tokio::test]
async fn an_index_step_keeps_an_exact_index_whose_form_matches() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Rust).await;
    database
        .execute("CREATE INDEX horsies_test_step_idx ON horsies_tasks (queue_name, priority)")
        .await;
    let before = database.relation_oid("horsies_test_step_idx").await;
    assert!(before.is_some());

    database
        .apply_after_baseline(&step_chain(STEP_SCRIPT))
        .await
        .expect("apply migration 1001");

    assert_eq!(database.relation_oid("horsies_test_step_idx").await, before);
    assert_eq!(
        bookkeeping_rows(&database)
            .await
            .iter()
            .map(|row| row.0)
            .collect::<Vec<i64>>(),
        vec![1000, 1001]
    );
    database.drop().await;
}

/// Python `TestExactIndexInstall::test_an_upgrade_drops_a_build_that_an_earlier_upgrade_left`:
/// an invalid index under the name (an interrupted concurrent build) is
/// built again, and the build and claim indexes that an interrupted
/// replacement left are dropped.
#[tokio::test]
async fn an_index_step_builds_an_invalid_index_again_and_drops_the_leftovers() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Rust).await;
    database
        .execute(
            "CREATE INDEX horsies_test_step_idx ON horsies_tasks (queue_name, priority); \
             UPDATE pg_index SET indisvalid = FALSE \
             WHERE indexrelid = 'horsies_test_step_idx'::regclass; \
             CREATE INDEX horsies_test_step_idx_build ON horsies_tasks (queue_name); \
             CREATE INDEX horsies_test_step_idx_claim ON horsies_tasks (priority);",
        )
        .await;
    let invalid = database.relation_oid("horsies_test_step_idx").await;
    assert_eq!(
        database
            .index_form("horsies_test_step_idx")
            .await
            .map(|form| form.1),
        Some(false)
    );

    database
        .apply_after_baseline(&step_chain(STEP_SCRIPT))
        .await
        .expect("apply migration 1001");

    let (definition, valid) = database
        .index_form("horsies_test_step_idx")
        .await
        .expect("the exact index exists");
    assert!(valid);
    assert!(definition.contains(STEP_EXACT_KEY), "{definition}");
    assert_ne!(
        database.relation_oid("horsies_test_step_idx").await,
        invalid
    );
    assert_eq!(
        database.relation_oid("horsies_test_step_idx_build").await,
        None
    );
    assert_eq!(
        database.relation_oid("horsies_test_step_idx_claim").await,
        None
    );
    database.drop().await;
}

/// Python `TestExactIndexInstall::test_a_replaced_index_keeps_its_name_while_the_old_one_drops`:
/// an index of the name with another form is replaced; each concurrent drop
/// of the step finds an index of the declared form under the name. An event
/// trigger records the form under the name at each `DROP INDEX`, through
/// dblink, so the recording writes nothing in the transaction of the drop
/// (`DROP INDEX CONCURRENTLY` must be its first action).
#[tokio::test]
async fn a_replaced_index_keeps_its_name_while_the_old_one_drops() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Rust).await;
    database
        .execute(
            "CREATE INDEX horsies_test_step_idx ON horsies_tasks (queue_name); \
             CREATE EXTENSION IF NOT EXISTS dblink; \
             CREATE TABLE horsies_test_seen_at_drop (definition text NOT NULL);",
        )
        .await;
    let before = database.relation_oid("horsies_test_step_idx").await;
    let recorder = format!(
        "CREATE FUNCTION horsies_test_record_drop() RETURNS event_trigger \
         LANGUAGE plpgsql AS $$ \
         BEGIN \
             PERFORM dblink_exec({url}, format( \
                 'INSERT INTO horsies_test_seen_at_drop VALUES (%L)', \
                 coalesce(pg_get_indexdef(to_regclass('horsies_test_step_idx')), '<absent>'))); \
         END $$; \
         CREATE EVENT TRIGGER horsies_test_record_drop ON ddl_command_start \
         WHEN TAG IN ('DROP INDEX') EXECUTE FUNCTION horsies_test_record_drop();",
        url = sql_literal(database.database.url()),
    );
    database.execute(&recorder).await;

    database
        .apply_after_baseline(&step_chain(STEP_SCRIPT))
        .await
        .expect("apply migration 1001");

    let seen: Vec<String> = database
        .client()
        .query("SELECT definition FROM horsies_test_seen_at_drop", &[])
        .await
        .expect("read the recorded forms")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert!(!seen.is_empty(), "the step dropped no index");
    for definition in &seen {
        assert!(definition.contains(STEP_EXACT_KEY), "{seen:?}");
    }
    let after = database.relation_oid("horsies_test_step_idx").await;
    assert!(after.is_some());
    assert_ne!(after, before);
    assert_eq!(
        database.relation_oid("horsies_test_step_idx_build").await,
        None
    );
    assert_eq!(
        database.relation_oid("horsies_test_step_idx_claim").await,
        None
    );
    database.drop().await;
}

/// `text` as an SQL string literal.
fn sql_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Python `TestRetiredClaimIndexes::test_a_failed_activation_keeps_the_retired_claim_indexes`:
/// a migration whose transaction fails keeps the indexes that it retires,
/// and no bookkeeping row is written; the next apply drops them after its
/// row commits.
#[tokio::test]
async fn a_failed_migration_keeps_the_indexes_that_it_retires() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Rust).await;
    database
        .execute("CREATE INDEX horsies_test_retired_idx ON horsies_tasks (enqueued_at)")
        .await;

    let failed = database
        .apply_after_baseline(&step_chain(FAILING_STEP_SCRIPT))
        .await;

    assert!(
        matches!(failed, Err(SchemaInstallError::Statement(_))),
        "{failed:?}"
    );
    assert!(database
        .relation_oid("horsies_test_retired_idx")
        .await
        .is_some());
    assert_eq!(bookkeeping_rows(&database).await.len(), 1);

    database
        .apply_after_baseline(&step_chain(STEP_SCRIPT))
        .await
        .expect("apply migration 1001");

    assert_eq!(
        database.relation_oid("horsies_test_retired_idx").await,
        None
    );
    assert_eq!(
        database
            .relation_oid("horsies_test_retired_idx_claim")
            .await,
        None
    );
    assert_eq!(bookkeeping_rows(&database).await.len(), 2);
    database.drop().await;
}

/// A relation other than an index of the table holds the name of an exact
/// index: the migration is refused, and nothing of it is written.
#[tokio::test]
async fn an_index_name_held_by_another_relation_refuses_the_migration() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Rust).await;
    database
        .execute("CREATE TABLE horsies_test_step_idx (id integer)")
        .await;

    let refused = database
        .apply_after_baseline(&step_chain(STEP_SCRIPT))
        .await;

    assert!(
        matches!(
            refused,
            Err(SchemaInstallError::Refused(
                SchemaRefusal::IndexNameHeldElsewhere {
                    version: 1001,
                    index: "horsies_test_step_idx",
                    table: "horsies_tasks",
                }
            ))
        ),
        "{refused:?}"
    );
    assert_eq!(bookkeeping_rows(&database).await.len(), 1);
    database.drop().await;
}

// ---- Migration 1001: the lifecycle record ----

/// The folder of the SQL files of the migrations after the baseline.
fn later_migration_folder() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

/// The list of later migrations holds every SQL file of `core/migrations/`,
/// in name order, each with the text on disk and a name that starts with its
/// version. A file added to the folder without a line in
/// `LATER_MIGRATION_FILES` fails here.
#[test]
fn later_migration_list_is_the_whole_folder() {
    let mut folder: Vec<String> = std::fs::read_dir(later_migration_folder())
        .expect("read the migrations folder")
        .map(|entry| {
            entry
                .expect("migrations folder entry")
                .file_name()
                .into_string()
                .expect("UTF-8 file name")
        })
        .filter(|name| name.ends_with(".sql"))
        .collect();
    folder.sort();
    let names: Vec<&str> = LATER_MIGRATION_FILES
        .iter()
        .map(|(_, name, _)| *name)
        .collect();
    assert_eq!(names, folder);
    for (version, name, text) in LATER_MIGRATION_FILES {
        let path = later_migration_folder().join(name);
        let on_disk = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        assert_eq!(text, on_disk, "{name}");
        assert!(name.starts_with(&format!("{version}_")), "{name}");
    }
}

/// Migration 1001 is the second link of the chain: it publishes no reader,
/// declares no index step, and its checksum is SHA-384 of its file.
/// Its text names no schema and holds no transaction control,
/// as the baseline's. The file list of the harnesses is the baseline files at
/// version 1000, then this file.
#[test]
fn the_lifecycle_record_is_the_second_migration_of_the_chain() {
    let chain = embedded_migrations();
    let (version, name, text) = LATER_MIGRATION_FILES[0];
    let migration = &chain[1];
    assert_eq!(version, LIFECYCLE_RECORD_MIGRATION_VERSION);
    assert_eq!(migration.version, 1001);
    assert_eq!(migration.description, "lifecycle record");
    assert!(migration.admitted_families.is_empty());
    assert_eq!(migration.readers, ReaderPublication::NotPublished);
    assert!(migration.exact_indexes.is_empty());
    assert!(migration.retired_indexes.is_empty());
    assert_eq!(migration.script.sql(), text);
    assert_eq!(
        migration.checksum.as_slice(),
        Sha384::digest(text.as_bytes()).as_slice()
    );
    assert!(
        !text.contains("public."),
        "the migration names the public schema"
    );
    for line in text.lines() {
        let statement = line.trim().to_uppercase();
        assert!(
            !["BEGIN;", "COMMIT;", "ROLLBACK;", "START TRANSACTION;"].contains(&statement.as_str()),
            "the migration holds transaction control: {line}"
        );
    }
    let files = migration_sql_file_texts();
    for ((file_version, file_name, file_text), (baseline_name, baseline_text)) in
        files.iter().zip(BASELINE_SQL_FILE_TEXTS)
    {
        assert_eq!(*file_version, BASELINE_MIGRATION_VERSION);
        assert_eq!(file_name, baseline_name);
        assert_eq!(file_text, baseline_text);
    }
    assert_eq!(
        files.get(BASELINE_SQL_FILE_TEXTS.len()),
        Some(&(version, name, text))
    );
}

/// Migration 1002 is the third link of the chain: it publishes no reader,
/// declares no index step, and its checksum is SHA-384 of its file.
/// Its text names no schema and holds no transaction control.
/// The file list of the harnesses holds this file after the file of 1001.
#[test]
fn the_start_command_fingerprint_is_the_third_migration_of_the_chain() {
    let chain = embedded_migrations();
    let (version, name, text) = LATER_MIGRATION_FILES[1];
    let migration = &chain[2];
    assert_eq!(version, START_COMMAND_FINGERPRINT_MIGRATION_VERSION);
    assert_eq!(migration.version, 1002);
    assert_eq!(migration.description, "workflow start command fingerprint");
    assert!(migration.admitted_families.is_empty());
    assert_eq!(migration.readers, ReaderPublication::NotPublished);
    assert!(migration.exact_indexes.is_empty());
    assert!(migration.retired_indexes.is_empty());
    assert_eq!(migration.script.sql(), text);
    assert_eq!(
        migration.checksum.as_slice(),
        Sha384::digest(text.as_bytes()).as_slice()
    );
    assert!(
        !text.contains("public."),
        "the migration names the public schema"
    );
    for line in text.lines() {
        let statement = line.trim().to_uppercase();
        assert!(
            !["BEGIN;", "COMMIT;", "ROLLBACK;", "START TRANSACTION;"].contains(&statement.as_str()),
            "the migration holds transaction control: {line}"
        );
    }
    let files = migration_sql_file_texts();
    assert_eq!(
        files.get(BASELINE_SQL_FILE_TEXTS.len() + 1),
        Some(&(version, name, text))
    );
}

/// Migration 1003 is the fourth link of the chain: it publishes no reader,
/// declares no index step, and its checksum is SHA-384 of its file.
/// Its text names no schema and holds no transaction control.
/// The file list of the harnesses holds this file after the file of 1002.
#[test]
fn the_attempt_cancelled_is_the_fourth_migration_of_the_chain() {
    let chain = embedded_migrations();
    let (version, name, text) = LATER_MIGRATION_FILES[2];
    let migration = &chain[3];
    assert_eq!(version, ATTEMPT_CANCELLED_MIGRATION_VERSION);
    assert_eq!(migration.version, 1003);
    assert_eq!(migration.description, "attempt cancelled");
    assert!(migration.admitted_families.is_empty());
    assert_eq!(migration.readers, ReaderPublication::NotPublished);
    assert!(migration.exact_indexes.is_empty());
    assert!(migration.retired_indexes.is_empty());
    assert_eq!(migration.script.sql(), text);
    assert_eq!(
        migration.checksum.as_slice(),
        Sha384::digest(text.as_bytes()).as_slice()
    );
    assert!(
        !text.contains("public."),
        "the migration names the public schema"
    );
    for line in text.lines() {
        let statement = line.trim().to_uppercase();
        assert!(
            !["BEGIN;", "COMMIT;", "ROLLBACK;", "START TRANSACTION;"].contains(&statement.as_str()),
            "the migration holds transaction control: {line}"
        );
    }
    let files = migration_sql_file_texts();
    assert_eq!(
        files.get(BASELINE_SQL_FILE_TEXTS.len() + 2),
        Some(&(version, name, text))
    );
}

/// Migration 1004 is the fifth link of the chain: it publishes no reader,
/// declares no index step, and its checksum is SHA-384 of its file.
/// Its text names no schema and holds no transaction control.
/// The file list of the harnesses holds this file after the file of 1003.
#[test]
fn the_worker_profile_name_is_the_fifth_migration_of_the_chain() {
    let chain = embedded_migrations();
    let (version, name, text) = LATER_MIGRATION_FILES[3];
    let migration = &chain[4];
    assert_eq!(version, WORKER_PROFILE_NAME_MIGRATION_VERSION);
    assert_eq!(migration.version, 1004);
    assert_eq!(migration.description, "worker profile name");
    assert!(migration.admitted_families.is_empty());
    assert_eq!(migration.readers, ReaderPublication::NotPublished);
    assert!(migration.exact_indexes.is_empty());
    assert!(migration.retired_indexes.is_empty());
    assert_eq!(migration.script.sql(), text);
    assert_eq!(
        migration.checksum.as_slice(),
        Sha384::digest(text.as_bytes()).as_slice()
    );
    assert!(
        !text.contains("public."),
        "the migration names the public schema"
    );
    for line in text.lines() {
        let statement = line.trim().to_uppercase();
        assert!(
            !["BEGIN;", "COMMIT;", "ROLLBACK;", "START TRANSACTION;"].contains(&statement.as_str()),
            "the migration holds transaction control: {line}"
        );
    }
    let files = migration_sql_file_texts();
    assert_eq!(
        files.get(BASELINE_SQL_FILE_TEXTS.len() + 3),
        Some(&(version, name, text))
    );
}

/// The position of migration 1005 in the embedded chain: the number of
/// migrations that a database before it holds.
fn claim_generation_by_status_position() -> usize {
    embedded_migrations()
        .iter()
        .position(|migration| migration.version == CLAIM_GENERATION_BY_STATUS_MIGRATION_VERSION)
        .expect("migration 1005 is a link of the chain")
}

/// Migration 1005 is the sixth link of the chain: it publishes no reader,
/// declares no index step, and its checksum is SHA-384 of its file.
/// Its text names no schema and holds no transaction control.
/// The file list of the harnesses holds this file after the file of 1004,
/// and migration 1006 follows it.
#[test]
fn the_claim_generation_by_status_is_the_sixth_migration_of_the_chain() {
    let chain = embedded_migrations();
    let (version, name, text) = LATER_MIGRATION_FILES[4];
    let migration = &chain[5];
    assert_eq!(claim_generation_by_status_position(), 5);
    assert_eq!(
        typescript_client_family_position(),
        claim_generation_by_status_position() + 1
    );
    assert_eq!(version, CLAIM_GENERATION_BY_STATUS_MIGRATION_VERSION);
    assert_eq!(migration.version, 1005);
    assert_eq!(migration.description, "claim generation by status");
    assert!(migration.admitted_families.is_empty());
    assert_eq!(migration.readers, ReaderPublication::NotPublished);
    assert!(migration.exact_indexes.is_empty());
    assert!(migration.retired_indexes.is_empty());
    assert_eq!(migration.script.sql(), text);
    assert_eq!(
        migration.checksum.as_slice(),
        Sha384::digest(text.as_bytes()).as_slice()
    );
    assert!(
        !text.contains("public."),
        "the migration names the public schema"
    );
    for line in text.lines() {
        let statement = line.trim().to_uppercase();
        assert!(
            !["BEGIN;", "COMMIT;", "ROLLBACK;", "START TRANSACTION;"].contains(&statement.as_str()),
            "the migration holds transaction control: {line}"
        );
    }
    let files = migration_sql_file_texts();
    assert_eq!(
        files.get(BASELINE_SQL_FILE_TEXTS.len() + 4),
        Some(&(version, name, text))
    );
}

/// The position of migration 1006 in the embedded chain: the number of
/// migrations that a database before it holds.
fn typescript_client_family_position() -> usize {
    embedded_migrations()
        .iter()
        .position(|migration| migration.version == TYPESCRIPT_CLIENT_FAMILY_MIGRATION_VERSION)
        .expect("migration 1006 is a link of the chain")
}

/// Migration 1006 is the newest link of the chain: it publishes no reader,
/// declares no index step, and its checksum is SHA-384 of its file.
/// Its text names no schema and holds no transaction control.
/// The file list of the harnesses ends with this file. It admits
/// `TYPESCRIPT`: in the install of a TypeScript client its transaction
/// writes the row `TYPESCRIPT`, after its text; in the install of a Python or
/// a Rust client it writes no family row.
#[test]
fn the_typescript_client_family_is_the_newest_migration() {
    let chain = embedded_migrations();
    let (version, name, text) = *LATER_MIGRATION_FILES
        .last()
        .expect("the chain has migrations after the baseline");
    let migration = chain.last().expect("the chain is not empty");
    let position = typescript_client_family_position();
    assert_eq!(chain.len(), LATER_MIGRATION_FILES.len() + 1);
    assert_eq!(position, chain.len() - 1);
    assert_eq!(version, TYPESCRIPT_CLIENT_FAMILY_MIGRATION_VERSION);
    assert_eq!(migration.version, 1006);
    assert_eq!(migration.description, "typescript client family");
    assert_eq!(
        migration.admitted_families,
        [ClientFamily::TypeScript].as_slice()
    );
    assert_eq!(
        family_row_at(chain, position, ClientFamily::TypeScript),
        FamilyRow::Written(ClientFamily::TypeScript)
    );
    for own in [ClientFamily::Python, ClientFamily::Rust] {
        assert_eq!(family_row_at(chain, position, own), FamilyRow::NotWritten);
    }
    assert_eq!(migration.readers, ReaderPublication::NotPublished);
    assert!(migration.exact_indexes.is_empty());
    assert!(migration.retired_indexes.is_empty());
    assert_eq!(migration.script.sql(), text);
    assert_eq!(
        migration.checksum.as_slice(),
        Sha384::digest(text.as_bytes()).as_slice()
    );
    assert_eq!(
        expected_schema_version(),
        TYPESCRIPT_CLIENT_FAMILY_MIGRATION_VERSION
    );
    assert!(
        !text.contains("public."),
        "the migration names the public schema"
    );
    for line in text.lines() {
        let statement = line.trim().to_uppercase();
        assert!(
            !["BEGIN;", "COMMIT;", "ROLLBACK;", "START TRANSACTION;"].contains(&statement.as_str()),
            "the migration holds transaction control: {line}"
        );
    }
    let files = migration_sql_file_texts();
    assert_eq!(
        files.len(),
        BASELINE_SQL_FILE_TEXTS.len() + LATER_MIGRATION_FILES.len()
    );
    assert_eq!(files.last(), Some(&(version, name, text)));
}

/// A build whose chain ends before migration 1006 meets a database that
/// applied migration 1006: the classification refuses it as newer.
#[test]
fn a_chain_without_the_typescript_client_family_refuses_a_database_that_has_it() {
    let chain = embedded_migrations();
    let position = typescript_client_family_position();
    let applied: Vec<AppliedMigration> = chain[..=position].iter().map(succeeded).collect();

    assert_eq!(
        classify(ledger(applied, RUST), &chain[..position]),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored: TYPESCRIPT_CLIENT_FAMILY_MIGRATION_VERSION,
            own: chain[position - 1].version,
        })),
    );
}

/// A build whose chain ends at migration 1004 meets a database that applied
/// migration 1005: the classification refuses it as newer.
#[test]
fn a_chain_without_the_claim_generation_by_status_refuses_a_database_that_has_it() {
    let chain = embedded_migrations();
    let position = claim_generation_by_status_position();
    let applied: Vec<AppliedMigration> = chain[..=position].iter().map(succeeded).collect();

    assert_eq!(
        classify(ledger(applied, RUST), &chain[..position]),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored: CLAIM_GENERATION_BY_STATUS_MIGRATION_VERSION,
            own: WORKER_PROFILE_NAME_MIGRATION_VERSION,
        })),
    );
}

/// A build whose chain ends at migration 1003 meets a database that applied
/// migration 1004: the classification refuses it as newer.
#[test]
fn a_chain_without_the_worker_profile_name_refuses_a_database_that_has_it() {
    let chain = embedded_migrations();
    let applied: Vec<AppliedMigration> = chain[..5].iter().map(succeeded).collect();

    assert_eq!(
        classify(ledger(applied, RUST), &chain[..4]),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored: 1004,
            own: ATTEMPT_CANCELLED_MIGRATION_VERSION,
        })),
    );
}

/// A build whose chain ends at migration 1002 meets a database that applied
/// migration 1003: the classification refuses it as newer.
#[test]
fn a_chain_without_the_attempt_cancelled_refuses_a_database_that_has_it() {
    let chain = embedded_migrations();
    let applied: Vec<AppliedMigration> = chain[..4].iter().map(succeeded).collect();

    assert_eq!(
        classify(ledger(applied, RUST), &chain[..3]),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored: 1003,
            own: START_COMMAND_FINGERPRINT_MIGRATION_VERSION,
        })),
    );
}

/// A build whose chain ends at migration 1001 meets a database that applied
/// migration 1002: the classification refuses it as newer.
#[test]
fn a_chain_without_the_start_command_fingerprint_refuses_a_database_that_has_it() {
    let chain = embedded_migrations();
    let applied: Vec<AppliedMigration> = chain[..3].iter().map(succeeded).collect();

    assert_eq!(
        classify(ledger(applied, RUST), &chain[..2]),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored: 1002,
            own: LIFECYCLE_RECORD_MIGRATION_VERSION,
        })),
    );
}

/// A build whose chain ends at the baseline meets a database that applied
/// migration 1001: the classification refuses it as newer.
#[test]
fn a_chain_without_the_lifecycle_record_refuses_a_database_that_has_it() {
    let chain = embedded_migrations();
    let applied: Vec<AppliedMigration> = chain[..2].iter().map(succeeded).collect();

    assert_eq!(
        classify(ledger(applied, RUST), &chain[..1]),
        Classification::Settled(DatabaseState::Unsupported(SchemaRefusal::SchemaNewer {
            stored: 1001,
            own: BASELINE_MIGRATION_VERSION,
        })),
    );
}

/// The live rows that a 0.8.0 process leaves: a PENDING, a CLAIMED and a
/// RUNNING task, and an attempt row of an earlier attempt of the RUNNING
/// task, each with every column of migration 1000 named.
const ROWS_OF_A_0_8_0_DATABASE: &str = "
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
    created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status,
    enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
    finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name, retry_count, max_retries,
    next_retry_at, good_until, is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
    rerun_root_task_id, trace_carrier, trace_id
) VALUES
    ('00000000-0000-4000-8000-00000000a001', 'upgrade.pending', 'default', 100, '{}', NULL,
     'ISOLATED', 'forever', now(), now(), 1, sha256('pending'::bytea), NULL, 'PENDING',
     now(), NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, 0, 0,
     NULL, NULL, FALSE, now(), FALSE, NULL, NULL, NULL, NULL),
    ('00000000-0000-4000-8000-00000000a002', 'upgrade.claimed', 'default', 100, '{}', NULL,
     'ISOLATED', 'forever', now(), now(), 1, sha256('claimed'::bytea), NULL, 'CLAIMED',
     now(), now(), 'worker-0.8.0', now() + interval '1 minute', NULL, NULL,
     NULL, NULL, NULL, NULL, 0, 0,
     NULL, NULL, FALSE, now(), FALSE, NULL, NULL, NULL, NULL),
    ('00000000-0000-4000-8000-00000000a003', 'upgrade.running', 'default', 100, '{}', NULL,
     'ISOLATED', 'forever', now(), now(), 1, sha256('running'::bytea), NULL, 'RUNNING',
     now(), now(), 'worker-0.8.0', NULL, now(), now(),
     'worker-0.8.0', 4242, 'host-0.8.0', 'process-0.8.0', 1, 3,
     NULL, NULL, FALSE, now(), FALSE, NULL, NULL, NULL, NULL);
INSERT INTO horsies_task_attempts (
    task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message,
    failed_reason, worker_id, worker_hostname, worker_pid, worker_process_name
) VALUES (
    '00000000-0000-4000-8000-00000000a003', 1, 'FAILED', TRUE, now() - interval '2 minutes',
    now() - interval '1 minute', 'BOOM', 'boom', NULL, 'worker-0.8.0', 'host-0.8.0', 4242,
    'process-0.8.0'
);
";

/// A database at migration 1000 that holds live PENDING, CLAIMED and
/// RUNNING rows and an attempt row gets migration 1001 from the install:
/// every old row keeps NULL in each new column (claims not counted, no
/// phase record), and both new CHECKs exist and are validated against the
/// rows.
#[tokio::test]
async fn a_0_8_0_database_with_live_rows_gets_the_lifecycle_record() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Rust).await;
    database.execute(ROWS_OF_A_0_8_0_DATABASE).await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migration 1001 over the live rows");

    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    let tasks: Vec<(String, Option<i32>, bool)> = database
        .client()
        .query(
            "SELECT status::text, attempt_claims, first_claimed_at IS NULL
             FROM horsies_tasks ORDER BY id",
            &[],
        )
        .await
        .expect("read the live rows")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(
        tasks,
        vec![
            ("PENDING".to_owned(), None, true),
            ("CLAIMED".to_owned(), None, true),
            ("RUNNING".to_owned(), None, true),
        ]
    );
    let attempts: Vec<(i32, i32)> = database
        .client()
        .query(
            "SELECT attempt, num_nonnulls(
                 phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at,
                 placed_at, finalizing_at, recorded_by, last_seen_at
             ) FROM horsies_task_attempts ORDER BY attempt",
            &[],
        )
        .await
        .expect("read the attempt rows")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(attempts, vec![(1, 0)]);
    let checks: Vec<(String, bool)> = database
        .client()
        .query(
            "SELECT conname::text, convalidated FROM pg_constraint
             WHERE conname IN (
                 'horsies_tasks_attempt_claims_shape', 'horsies_task_attempts_phase_record_shape'
             )
             ORDER BY conname",
            &[],
        )
        .await
        .expect("read the new checks")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        checks,
        vec![
            ("horsies_task_attempts_phase_record_shape".to_owned(), true),
            ("horsies_tasks_attempt_claims_shape".to_owned(), true),
        ]
    );
    database.drop().await;
}

/// The shape of the horsies objects of a database, as one text: the columns
/// of every `horsies_` relation (name, type, length, nullability, default,
/// generation expression, in column order), every constraint definition,
/// every index definition, every `horsies_` sequence (type, bounds, step,
/// cycle), and every `horsies_` function (identity arguments, result, and the
/// MD5 of its body).
async fn schema_shape(database: &MigrationTestDatabase) -> String {
    database
        .client()
        .query_one(
            r#"SELECT jsonb_build_object(
                 'columns', (
                     SELECT jsonb_agg(jsonb_build_array(
                         c.table_name, c.ordinal_position, c.column_name, c.data_type,
                         c.character_maximum_length, c.is_nullable, c.column_default,
                         c.generation_expression
                     ) ORDER BY c.table_name, c.ordinal_position)
                     FROM information_schema.columns AS c
                     WHERE c.table_schema = current_schema() AND c.table_name LIKE 'horsies\_%'
                 ),
                 'constraints', (
                     SELECT jsonb_agg(jsonb_build_array(
                         r.relname, k.conname, k.convalidated, pg_get_constraintdef(k.oid)
                     ) ORDER BY r.relname, k.conname)
                     FROM pg_constraint AS k
                     JOIN pg_class AS r ON r.oid = k.conrelid
                     JOIN pg_namespace AS n ON n.oid = r.relnamespace
                     WHERE n.nspname = current_schema() AND r.relname LIKE 'horsies\_%'
                 ),
                 'indexes', (
                     SELECT jsonb_agg(jsonb_build_array(i.indexname, i.indexdef)
                         ORDER BY i.indexname)
                     FROM pg_indexes AS i
                     WHERE i.schemaname = current_schema() AND i.tablename LIKE 'horsies\_%'
                 ),
                 'sequences', (
                     SELECT jsonb_agg(jsonb_build_array(
                         s.sequencename, s.data_type::text, s.start_value, s.min_value, s.max_value,
                         s.increment_by, s.cycle
                     ) ORDER BY s.sequencename)
                     FROM pg_sequences AS s
                     WHERE s.schemaname = current_schema() AND s.sequencename LIKE 'horsies\_%'
                 ),
                 'functions', (
                     SELECT jsonb_agg(jsonb_build_array(
                         p.proname, pg_get_function_identity_arguments(p.oid),
                         pg_get_function_result(p.oid), md5(p.prosrc)
                     ) ORDER BY p.proname, pg_get_function_identity_arguments(p.oid))
                     FROM pg_proc AS p
                     JOIN pg_namespace AS n ON n.oid = p.pronamespace
                     WHERE n.nspname = current_schema() AND p.proname LIKE 'horsies\_%'
                 )
             )::text"#,
            &[],
        )
        .await
        .expect("read the schema shape")
        .get(0)
}

/// The workflow rows that a 0.8.0 process leaves: a RUNNING workflow whose
/// sub-workflow node links a RUNNING child workflow, each with every column
/// of migration 1000 named.
const WORKFLOWS_OF_A_0_8_0_DATABASE: &str = "
INSERT INTO horsies_workflows (
    id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
    parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
    completed_at, updated_at, trace_carrier, trace_id
) VALUES
    ('00000000-0000-4000-8000-00000000b001', 'upgrade.parent', NULL, 'RUNNING', 'fail', NULL, NULL, NULL, NULL,
     NULL, NULL, '00000000-0000-4000-8000-00000000b001', 0, now(), now(), now(), NULL, now(), NULL, NULL),
    ('00000000-0000-4000-8000-00000000b002', 'upgrade.child', NULL, 'RUNNING', 'fail', NULL, NULL, NULL, NULL,
     '00000000-0000-4000-8000-00000000b001', 0, '00000000-0000-4000-8000-00000000b001', 1, now(), now(), now(),
     NULL, now(), NULL, NULL);
INSERT INTO horsies_workflow_tasks (
    id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
    workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
    good_until, retention_class_key, downstream_subworkflow, status, task_id, is_subworkflow, sub_workflow_id,
    sub_workflow_name, sub_definition_key, sub_workflow_params, sub_workflow_summary, result, error,
    created_at, started_at, completed_at
) VALUES (
    '00000000-0000-4000-8000-00000000b011', '00000000-0000-4000-8000-00000000b001', 0, 'n0', 'upgrade.sub', '{}',
    'default', 50, '{}', NULL, NULL, FALSE, 'all', NULL, NULL, NULL, NULL, NULL, NULL, 'NONE', 'RUNNING', NULL,
    TRUE, '00000000-0000-4000-8000-00000000b002', 'child.wf', 'child.key', 'NOT_GIVEN', NULL, NULL, NULL,
    now(), now(), NULL
);
";

/// A database at migration 1000 that holds a RUNNING workflow with a RUNNING
/// child gets migration 1001 from the install: the event table is empty (the
/// migration writes no event for a run that began before it), and its CHECK
/// and its two foreign keys are validated. A pause after the upgrade writes
/// one WORKFLOW_PAUSED for the workflow, then one for its child.
#[tokio::test]
async fn a_0_8_0_database_with_workflows_gets_the_event_table_with_no_event() {
    let database = MigrationTestDatabase::create().await;
    database.install_baseline_only(ClientFamily::Rust).await;
    database.execute(WORKFLOWS_OF_A_0_8_0_DATABASE).await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migration 1001 over the workflow rows");

    let events: i64 = database
        .client()
        .query_one("SELECT count(*) FROM horsies_workflow_events", &[])
        .await
        .expect("count the events")
        .get(0);
    assert_eq!(events, 0, "the migration writes no event");
    let constraints: Vec<(String, bool)> = database
        .client()
        .query(
            "SELECT conname::text, convalidated FROM pg_constraint
             WHERE conrelid = 'horsies_workflow_events'::regclass AND contype IN ('c', 'f')
             ORDER BY conname",
            &[],
        )
        .await
        .expect("read the constraints of the event table")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        constraints,
        vec![
            ("horsies_workflow_events_kind_shape".to_owned(), true),
            ("horsies_workflow_events_node_fkey".to_owned(), true),
            ("horsies_workflow_events_run_fkey".to_owned(), true),
        ]
    );

    database
        .execute("SELECT * FROM horsies_workflow_pause('00000000-0000-4000-8000-00000000b001')")
        .await;
    let paused: Vec<(String, String)> = database
        .client()
        .query(
            "SELECT workflow_id::text, event_kind::text FROM horsies_workflow_events ORDER BY event_id",
            &[],
        )
        .await
        .expect("read the events of the pause")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        paused,
        vec![
            (
                "00000000-0000-4000-8000-00000000b001".to_owned(),
                "WORKFLOW_PAUSED".to_owned()
            ),
            (
                "00000000-0000-4000-8000-00000000b002".to_owned(),
                "WORKFLOW_PAUSED".to_owned()
            ),
        ]
    );
    database.drop().await;
}

/// A database at migration 1001 that holds a RUNNING workflow with a RUNNING
/// child gets migration 1002 from the install: both rows keep NULL in the two
/// new columns (the migration writes no start command), the new CHECK is
/// validated, and an install again changes nothing.
#[tokio::test]
async fn a_1001_database_with_workflows_gets_the_start_command_columns_with_no_value() {
    let database = MigrationTestDatabase::create().await;
    database
        .install_first_migrations(ClientFamily::Rust, 2)
        .await;
    database.execute(WORKFLOWS_OF_A_0_8_0_DATABASE).await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1002 to 1006 over the workflow rows");

    let installed = bookkeeping_rows(&database).await;
    assert_eq!(installed, chain_rows(embedded_migrations().len()));
    let workflows: Vec<(String, i32)> = database
        .client()
        .query(
            "SELECT name::text, num_nonnulls(command_fingerprint_version, command_fingerprint)
             FROM horsies_workflows ORDER BY name",
            &[],
        )
        .await
        .expect("read the workflow rows")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        workflows,
        vec![
            ("upgrade.child".to_owned(), 0),
            ("upgrade.parent".to_owned(), 0),
        ]
    );
    let validated: bool = database
        .client()
        .query_one(
            "SELECT convalidated FROM pg_constraint
             WHERE conname = 'horsies_workflows_command_fingerprint_by_origin'",
            &[],
        )
        .await
        .expect("read the new check")
        .get(0);
    assert!(validated);

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install again");
    assert_eq!(bookkeeping_rows(&database).await, installed);
    database.drop().await;
}

/// A fresh install, an upgrade from migration 1000 with live rows and
/// workflow rows, an upgrade from migration 1001 with workflow rows, an
/// upgrade from migration 1002 with live rows, an upgrade from migration
/// 1003 with a worker snapshot, an upgrade from migration 1004 with live rows
/// through migrations 1005 and 1006, and an upgrade from the migration before
/// 1006 end in the same schema: the same columns, constraints, indexes,
/// sequences and functions, and the same bookkeeping.
#[tokio::test]
async fn a_fresh_install_and_an_upgraded_database_end_in_the_same_shape() {
    let fresh = MigrationTestDatabase::create().await;
    fresh
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    let upgraded = MigrationTestDatabase::create().await;
    upgraded.install_baseline_only(ClientFamily::Rust).await;
    upgraded.execute(ROWS_OF_A_0_8_0_DATABASE).await;
    upgraded.execute(WORKFLOWS_OF_A_0_8_0_DATABASE).await;
    upgraded
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1001 to 1006 over the live rows");
    let from_1001 = MigrationTestDatabase::create().await;
    from_1001
        .install_first_migrations(ClientFamily::Rust, 2)
        .await;
    from_1001.execute(WORKFLOWS_OF_A_0_8_0_DATABASE).await;
    from_1001
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1002 to 1006 over the workflow rows");
    let from_1002 = MigrationTestDatabase::create().await;
    from_1002
        .install_first_migrations(ClientFamily::Rust, 3)
        .await;
    from_1002.execute(ROWS_OF_A_0_8_0_DATABASE).await;
    from_1002
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1003 to 1006 over the live rows");
    let from_1003 = MigrationTestDatabase::create().await;
    from_1003
        .install_first_migrations(ClientFamily::Rust, 4)
        .await;
    from_1003.execute(WORKER_STATE_OF_A_1003_DATABASE).await;
    from_1003
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1004 to 1006 over the worker snapshot");
    let from_1004 = MigrationTestDatabase::create().await;
    from_1004
        .install_first_migrations(ClientFamily::Rust, claim_generation_by_status_position())
        .await;
    from_1004.execute(ROWS_OF_A_0_8_0_DATABASE).await;
    from_1004
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1005 and 1006 over the live rows");
    let before_1006 = MigrationTestDatabase::create().await;
    before_1006
        .install_first_migrations(ClientFamily::Rust, typescript_client_family_position())
        .await;
    before_1006
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migration 1006");

    let fresh_shape = schema_shape(&fresh).await;
    assert_eq!(fresh_shape, schema_shape(&upgraded).await);
    assert_eq!(fresh_shape, schema_shape(&from_1001).await);
    assert_eq!(fresh_shape, schema_shape(&from_1002).await);
    assert_eq!(fresh_shape, schema_shape(&from_1003).await);
    assert_eq!(fresh_shape, schema_shape(&from_1004).await);
    assert_eq!(fresh_shape, schema_shape(&before_1006).await);
    let fresh_rows = bookkeeping_rows(&fresh).await;
    assert_eq!(fresh_rows, bookkeeping_rows(&upgraded).await);
    assert_eq!(fresh_rows, bookkeeping_rows(&from_1001).await);
    assert_eq!(fresh_rows, bookkeeping_rows(&from_1002).await);
    assert_eq!(fresh_rows, bookkeeping_rows(&from_1003).await);
    assert_eq!(fresh_rows, bookkeeping_rows(&from_1004).await);
    assert_eq!(fresh_rows, bookkeeping_rows(&before_1006).await);
    fresh.drop().await;
    upgraded.drop().await;
    from_1001.drop().await;
    from_1002.drop().await;
    from_1003.drop().await;
    from_1004.drop().await;
    before_1006.drop().await;
}

/// A database at migration 1002 that holds live PENDING, CLAIMED and RUNNING
/// rows and an attempt row with no phase record gets migration 1003 from the
/// install: the attempt row does not change, both replaced CHECKs exist and
/// are validated against the rows and name the new values (the outcome
/// CANCELLED, the writer CANCEL), the cancel function and the read of the
/// cancelled runs exist, and an install again changes nothing.
#[tokio::test]
async fn a_1002_database_with_live_rows_gets_the_cancelled_attempt() {
    let database = MigrationTestDatabase::create().await;
    database
        .install_first_migrations(ClientFamily::Rust, 3)
        .await;
    database.execute(ROWS_OF_A_0_8_0_DATABASE).await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1003 to 1006 over the live rows");

    let installed = bookkeeping_rows(&database).await;
    assert_eq!(installed, chain_rows(embedded_migrations().len()));
    let attempts: Vec<(i32, String, i32)> = database
        .client()
        .query(
            "SELECT attempt, outcome::text, num_nonnulls(phase_record_version, recorded_by)
             FROM horsies_task_attempts ORDER BY attempt",
            &[],
        )
        .await
        .expect("read the attempt rows")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(attempts, vec![(1, "FAILED".to_owned(), 0)]);
    let checks: Vec<(String, bool, String)> = database
        .client()
        .query(
            "SELECT conname::text, convalidated, pg_get_constraintdef(oid) FROM pg_constraint
             WHERE conname IN (
                 'horsies_task_attempts_outcome_known', 'horsies_task_attempts_phase_record_shape'
             )
             ORDER BY conname",
            &[],
        )
        .await
        .expect("read the replaced checks")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    let [(outcome_name, outcome_valid, outcome_text), (shape_name, shape_valid, shape_text)] =
        checks.as_slice()
    else {
        panic!("two checks: {checks:?}");
    };
    assert_eq!(outcome_name, "horsies_task_attempts_outcome_known");
    assert!(*outcome_valid);
    assert!(outcome_text.contains("'CANCELLED'"), "{outcome_text}");
    assert_eq!(shape_name, "horsies_task_attempts_phase_record_shape");
    assert!(*shape_valid);
    assert!(shape_text.contains("'CANCEL'"), "{shape_text}");
    let functions: Vec<String> = database
        .client()
        .query(
            "SELECT p.proname::text || '(' || pg_get_function_identity_arguments(p.oid) || ')'
             FROM pg_proc AS p
             WHERE p.proname IN ('horsies_cancel_task', 'horsies_cancelled_runs')
             ORDER BY p.proname",
            &[],
        )
        .await
        .expect("read the functions")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        functions,
        vec![
            "horsies_cancel_task(p_task_id uuid, p_include_running boolean)".to_owned(),
            "horsies_cancelled_runs(p_task_ids uuid[], p_claimed_ats timestamp with time zone[])"
                .to_owned(),
        ]
    );

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install again");
    assert_eq!(bookkeeping_rows(&database).await, installed);
    database.drop().await;
}

/// A worker snapshot that a worker of a build before migration 1004 leaves,
/// with every column of migration 1000 named: it names no profile.
const WORKER_STATE_OF_A_1003_DATABASE: &str = r#"
INSERT INTO horsies_worker_states (
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch, max_claim_per_worker,
    cluster_wide_cap, queues, queue_priorities, queue_max_concurrency, recovery_config,
    tasks_running, tasks_claimed, memory_usage_mb, memory_percent, cpu_percent,
    children_memory_mb, worker_started_at, slots_down, class_capacity, telemetry
) VALUES (
    'worker-1003', now(), 'host-1003', 4242, 2, 0, 2,
    NULL, ARRAY['default'], '{}', '{}', '{}',
    0, 0, NULL, NULL, NULL,
    NULL, now(), '[]', '{}', jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
);
"#;

/// The names that the profile name rule and the CHECK of migration 1004 are
/// compared on: each arm of the rule (the first character, the other
/// characters, the length), a trailing newline and a non-ASCII letter.
fn profile_name_samples() -> Vec<String> {
    use crate::core::config::worker_profile::PROFILE_NAME_MAX_LENGTH;

    let mut samples: Vec<String> = [
        "default",
        "batch-high-memory",
        "v1.2",
        "_internal",
        "9lives",
        "A",
        "",
        "-flag",
        ".hidden",
        "has space",
        "a/b",
        "a\n",
        "caf\u{e9}",
    ]
    .iter()
    .map(|name| (*name).to_owned())
    .collect();
    samples.push("x".repeat(PROFILE_NAME_MAX_LENGTH));
    samples.push("x".repeat(PROFILE_NAME_MAX_LENGTH + 1));
    samples
}

/// A database at migration 1003 that holds a worker snapshot gets migration
/// 1004 from the install:
/// - `profile_name` is `character varying(64)`, nullable, with no default;
/// - the old row keeps NULL (the migration writes no name) and the worker
///   state read gives it as `SnapshotProfile::NotRecorded`;
/// - the CHECK `horsies_worker_states_profile_name_shape` is validated and
///   accepts exactly the names that the profile name rule accepts
///   (`is_valid_profile_name`): a refused name fails with 23514, or with
///   22001 above the column length;
/// - an install again changes nothing.
#[tokio::test]
async fn a_1003_database_with_a_worker_snapshot_gets_the_profile_name_with_no_value() {
    use crate::broker::{PostgresBroker, SnapshotProfile};
    use crate::core::config::worker_profile::is_valid_profile_name;

    let database = MigrationTestDatabase::create().await;
    database
        .install_first_migrations(ClientFamily::Rust, 4)
        .await;
    database.execute(WORKER_STATE_OF_A_1003_DATABASE).await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1004 to 1006 over the worker snapshot");

    let installed = bookkeeping_rows(&database).await;
    assert_eq!(installed, chain_rows(embedded_migrations().len()));
    let column = database
        .client()
        .query_one(
            "SELECT format_type(atttypid, atttypmod), attnotnull, atthasdef FROM pg_attribute
             WHERE attrelid = 'horsies_worker_states'::regclass
               AND attname = 'profile_name' AND NOT attisdropped",
            &[],
        )
        .await
        .expect("read the new column");
    let (column_type, not_null, has_default): (String, bool, bool) =
        (column.get(0), column.get(1), column.get(2));
    assert_eq!(column_type, "character varying(64)");
    assert!(!not_null, "a row of an earlier build holds no name");
    assert!(!has_default, "the migration sets no default");
    let stored: Option<String> = database
        .client()
        .query_one(
            "SELECT profile_name FROM horsies_worker_states WHERE worker_id = 'worker-1003'",
            &[],
        )
        .await
        .expect("read the old row")
        .get(0);
    assert_eq!(stored, None, "the migration writes no name");
    let check = database
        .client()
        .query_one(
            "SELECT convalidated, pg_get_constraintdef(oid) FROM pg_constraint
             WHERE conrelid = 'horsies_worker_states'::regclass
               AND conname = 'horsies_worker_states_profile_name_shape'",
            &[],
        )
        .await
        .expect("read the new check");
    let (validated, definition): (bool, String) = (check.get(0), check.get(1));
    assert!(validated);
    assert!(
        definition.contains("'^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$'"),
        "{definition}"
    );

    let broker = PostgresBroker::from_test_url(database.url(), ClientFamily::Rust);
    let read = broker.get_worker_state("worker-1003").await;
    broker.close().await;
    let snapshot = read
        .expect("the worker state read answers")
        .expect("the worker reported");
    assert_eq!(snapshot.profile, SnapshotProfile::NotRecorded);

    let refused_codes = [
        SqlState::CHECK_VIOLATION,
        SqlState::STRING_DATA_RIGHT_TRUNCATION,
    ];
    for name in profile_name_samples() {
        let written = database
            .client()
            .execute(
                "UPDATE horsies_worker_states SET profile_name = $1
                 WHERE worker_id = 'worker-1003'",
                &[&name],
            )
            .await;
        let accepted = match written {
            Ok(rows) => {
                assert_eq!(rows, 1, "{name:?}");
                true
            }
            Err(error) => match error.code() {
                Some(code) if refused_codes.contains(code) => false,
                _ => panic!("{name:?}: {error}"),
            },
        };
        assert_eq!(accepted, is_valid_profile_name(&name), "{name:?}");
    }

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install again");
    assert_eq!(bookkeeping_rows(&database).await, installed);
    database.drop().await;
}

/// The CHECK of migration 1005 on `horsies_tasks`.
const CLAIM_GENERATION_RULE: &str = "horsies_tasks_claim_generation_by_status";

/// A CLAIMED row and a RUNNING row of `worker-1004` with no claim generation
/// (`claimed_at` NULL). No writer leaves such a row; a statement written by
/// hand does. Every column of migration 1000 is named.
const ROWS_WITH_NO_CLAIM_GENERATION: &str = "
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
    created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status,
    enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
    finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name, retry_count, max_retries,
    next_retry_at, good_until, is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
    rerun_root_task_id, trace_carrier, trace_id
) VALUES
    ('00000000-0000-4000-8000-00000000c001', 'upgrade.claimed_no_claim', 'default', 100, '{}', NULL,
     'ISOLATED', 'forever', now(), now(), 1, sha256('c001'::bytea), NULL, 'CLAIMED',
     now(), NULL, 'worker-1004', now() + interval '1 minute', NULL, NULL,
     NULL, NULL, NULL, NULL, 0, 0,
     NULL, NULL, FALSE, now(), FALSE, NULL, NULL, NULL, NULL),
    ('00000000-0000-4000-8000-00000000c002', 'upgrade.running_no_claim', 'default', 100, '{}', NULL,
     'ISOLATED', 'forever', now(), now(), 1, sha256('c002'::bytea), NULL, 'RUNNING',
     now(), NULL, 'worker-1004', NULL, now(), NULL,
     NULL, 4242, 'host-1004', 'process-1004', 0, 0,
     NULL, NULL, FALSE, now(), FALSE, NULL, NULL, NULL, NULL);
";

/// The status and whether `claimed_at` is NULL, of each row of
/// [`ROWS_WITH_NO_CLAIM_GENERATION`], in id order.
async fn rows_with_no_claim_generation(database: &MigrationTestDatabase) -> Vec<(String, bool)> {
    database
        .client()
        .query(
            "SELECT status::text, claimed_at IS NULL FROM horsies_tasks
             WHERE id IN (
                 '00000000-0000-4000-8000-00000000c001', '00000000-0000-4000-8000-00000000c002'
             )
             ORDER BY id",
            &[],
        )
        .await
        .expect("read the rows with no claim generation")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// The number of constraints of `horsies_tasks` named [`CLAIM_GENERATION_RULE`].
async fn claim_generation_rules(database: &MigrationTestDatabase) -> i64 {
    database
        .client()
        .query_one(
            "SELECT count(*) FROM pg_constraint
             WHERE conrelid = 'horsies_tasks'::regclass AND conname = $1",
            &[&CLAIM_GENERATION_RULE],
        )
        .await
        .expect("read the constraints of horsies_tasks")
        .get(0)
}

/// A database at migration 1004 that holds a CLAIMED row and a RUNNING row
/// with no claim generation stops at migration 1005:
/// - the install fails with SQLSTATE 23514 and a message that names the
///   table, the rule, the row count and the query that finds the rows;
/// - migration 1005 has no bookkeeping row, the CHECK does not exist, and
///   the rows do not change;
/// - after the rows get a claim generation, the install applies migrations
///   1005 and 1006.
#[tokio::test]
async fn a_1004_database_with_claimed_rows_with_no_claim_generation_stops_at_1005() {
    let database = MigrationTestDatabase::create().await;
    database
        .install_first_migrations(ClientFamily::Rust, claim_generation_by_status_position())
        .await;
    database.execute(ROWS_OF_A_0_8_0_DATABASE).await;
    database.execute(ROWS_WITH_NO_CLAIM_GENERATION).await;

    let stopped = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    let refusal = match stopped {
        Err(SchemaInstallError::Statement(StatementFailure::StatementRefused {
            statement,
            cause,
        })) => {
            assert_eq!(statement, "migration_1005_claim_generation_by_status");
            cause.server_error().clone()
        }
        other => panic!("expected the refusal of migration 1005, got {other:?}"),
    };
    assert_eq!(refusal.code(), &SqlState::CHECK_VIOLATION, "{refusal}");
    for fragment in [
        "horsies_tasks holds CLAIMED or RUNNING rows with no claim generation",
        CLAIM_GENERATION_RULE,
        "Rows: 2.",
        "SELECT id, status, claimed_by_worker_id FROM horsies_tasks \
         WHERE status IN ('CLAIMED', 'RUNNING') AND claimed_at IS NULL.",
    ] {
        assert!(
            refusal.message().contains(fragment),
            "{fragment}: {refusal}"
        );
    }
    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(claim_generation_by_status_position())
    );
    assert_eq!(claim_generation_rules(&database).await, 0);
    assert_eq!(
        rows_with_no_claim_generation(&database).await,
        vec![("CLAIMED".to_owned(), true), ("RUNNING".to_owned(), true)]
    );

    database
        .execute(
            "UPDATE horsies_tasks SET claimed_at = now()
             WHERE status IN ('CLAIMED', 'RUNNING') AND claimed_at IS NULL",
        )
        .await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1005 and 1006 after each row holds a claim generation");
    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    assert_eq!(claim_generation_rules(&database).await, 1);
    database.drop().await;
}

/// A database at migration 1004 whose `horsies_tasks` holds a constraint of
/// the name of the rule of migration 1005 with another predicate gets the
/// rule of migration 1005: the install drops the old constraint and adds
/// the CHECK, validated, which refuses a RUNNING row with no claim
/// generation.
#[tokio::test]
async fn a_1004_database_with_another_constraint_of_the_rule_name_gets_the_rule_of_1005() {
    let database = MigrationTestDatabase::create().await;
    database
        .install_first_migrations(ClientFamily::Rust, claim_generation_by_status_position())
        .await;
    database.execute(ROWS_OF_A_0_8_0_DATABASE).await;
    database
        .execute(&format!(
            "ALTER TABLE horsies_tasks ADD CONSTRAINT {CLAIM_GENERATION_RULE} CHECK (TRUE)"
        ))
        .await;

    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("apply migrations 1005 and 1006 over a constraint of the same name");

    let rule = database
        .client()
        .query_one(
            "SELECT convalidated, pg_get_constraintdef(oid) FROM pg_constraint
             WHERE conrelid = 'horsies_tasks'::regclass AND conname = $1",
            &[&CLAIM_GENERATION_RULE],
        )
        .await
        .expect("read the rule");
    let (validated, definition): (bool, String) = (rule.get(0), rule.get(1));
    assert!(validated);
    for fragment in ["'CLAIMED'", "'RUNNING'", "claimed_at IS NOT NULL"] {
        assert!(definition.contains(fragment), "{fragment}: {definition}");
    }
    let cleared = database
        .client()
        .execute(
            "UPDATE horsies_tasks SET claimed_at = NULL
             WHERE id = '00000000-0000-4000-8000-00000000a003'",
            &[],
        )
        .await
        .expect_err("the rule refuses a RUNNING row with no claim generation");
    let refusal = cleared
        .as_db_error()
        .unwrap_or_else(|| panic!("a server error: {cleared}"));
    assert_eq!(refusal.code(), &SqlState::CHECK_VIOLATION, "{refusal}");
    assert_eq!(
        refusal.constraint(),
        Some(CLAIM_GENERATION_RULE),
        "{refusal}"
    );
    database.drop().await;
}

/// A database that applied another text of migration 1001 (its stored
/// checksum differs from this build's) is refused as edited, and nothing is
/// written.
#[tokio::test]
async fn a_changed_text_of_the_lifecycle_record_is_refused_as_edited() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database
        .client()
        .execute(
            "UPDATE horsies_migrations SET checksum = '\\x01'::bytea WHERE version = $1",
            &[&LIFECYCLE_RECORD_MIGRATION_VERSION],
        )
        .await
        .expect("change the stored checksum");
    let before = bookkeeping_rows(&database).await;

    let refused = database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Refused(SchemaRefusal::MigrationEdited {
            version,
            stored_checksum,
            own_checksum,
        })) => {
            assert_eq!(version, LIFECYCLE_RECORD_MIGRATION_VERSION);
            assert_eq!(stored_checksum, vec![1_u8]);
            assert_eq!(own_checksum, embedded_migrations()[1].checksum.to_vec());
        }
        other => panic!("expected an edited-migration refusal, got {other:?}"),
    }
    assert_eq!(bookkeeping_rows(&database).await, before);
    database.drop().await;
}

// ---- Migration 1006: the client family TYPESCRIPT ----

/// The CHECK `horsies_client_family_family_known` from migration 1006 on,
/// as `pg_get_constraintdef` gives it.
const FAMILY_CHECK_FROM_1006: &str =
    "CHECK ((family = ANY (ARRAY['PYTHON'::text, 'RUST'::text, 'TYPESCRIPT'::text])))";

/// Each CHECK named `horsies_client_family_family_known` on
/// `horsies_client_family`: whether it is validated, and its definition.
async fn family_checks(database: &MigrationTestDatabase) -> Vec<(bool, String)> {
    database
        .client()
        .query(
            "SELECT convalidated, pg_get_constraintdef(oid) FROM pg_constraint
             WHERE conrelid = 'horsies_client_family'::regclass
               AND conname = 'horsies_client_family_family_known'",
            &[],
        )
        .await
        .expect("read the family check")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Writes `family` into the one family row. `Err` holds the SQLSTATE of a
/// refused write.
async fn write_family(database: &MigrationTestDatabase, family: &str) -> Result<(), SqlState> {
    match database
        .client()
        .execute("UPDATE horsies_client_family SET family = $1", &[&family])
        .await
    {
        Ok(1) => Ok(()),
        Ok(rows) => {
            panic!("the family table holds one row; the write of {family:?} changed {rows}")
        }
        Err(error) => match error.code() {
            Some(code) => Err(code.clone()),
            None => panic!("write the family {family:?}: {error}"),
        },
    }
}

/// A database before migration 1006, of the family `PYTHON` and of the
/// family `RUST`, gets migration 1006 from the install:
/// - before it, the CHECK refuses `TYPESCRIPT` (23514);
/// - after it, the family row keeps its value, and the CHECK exists once,
///   validated, with the predicate of migration 1006: it accepts
///   `TYPESCRIPT` and refuses `GO` and `typescript` (23514);
/// - an install again changes nothing.
#[tokio::test]
async fn a_database_before_1006_of_each_family_gets_the_typescript_family_and_keeps_its_row() {
    for own in [ClientFamily::Python, ClientFamily::Rust] {
        let database = MigrationTestDatabase::create().await;
        database
            .install_first_migrations(own, typescript_client_family_position())
            .await;
        assert_eq!(
            write_family(&database, "TYPESCRIPT").await,
            Err(SqlState::CHECK_VIOLATION),
            "{own:?}"
        );

        database
            .install(own, InstallLockPlan::fleet())
            .await
            .expect("apply migration 1006");

        let installed = bookkeeping_rows(&database).await;
        assert_eq!(installed, chain_rows(embedded_migrations().len()));
        assert_eq!(family_row(&database).await.as_deref(), Some(own.as_str()));
        assert_eq!(
            family_checks(&database).await,
            vec![(true, FAMILY_CHECK_FROM_1006.to_owned())]
        );
        assert_eq!(write_family(&database, "TYPESCRIPT").await, Ok(()));
        for refused in ["GO", "typescript"] {
            assert_eq!(
                write_family(&database, refused).await,
                Err(SqlState::CHECK_VIOLATION),
                "{refused:?}"
            );
        }
        assert_eq!(write_family(&database, own.as_str()).await, Ok(()));

        database
            .install(own, InstallLockPlan::fleet())
            .await
            .expect("install again");
        assert_eq!(bookkeeping_rows(&database).await, installed);
        assert_eq!(family_row(&database).await.as_deref(), Some(own.as_str()));
        database.drop().await;
    }
}

/// Migration 1006 leaves the CHECK `horsies_client_family_family_known`
/// with its own predicate whatever the database holds before it: no CHECK
/// of that name (dropped out of band), or a CHECK of that name with another
/// predicate.
#[tokio::test]
async fn migration_1006_gives_the_family_check_its_predicate_whatever_the_database_held() {
    let changes = [
        "ALTER TABLE horsies_client_family DROP CONSTRAINT horsies_client_family_family_known",
        "ALTER TABLE horsies_client_family
             DROP CONSTRAINT horsies_client_family_family_known,
             ADD CONSTRAINT horsies_client_family_family_known CHECK (family IN ('RUST', 'GO'))",
    ];
    for change in changes {
        let database = MigrationTestDatabase::create().await;
        database
            .install_first_migrations(ClientFamily::Rust, typescript_client_family_position())
            .await;
        database.execute(change).await;

        database
            .install(ClientFamily::Rust, InstallLockPlan::fleet())
            .await
            .expect("apply migration 1006");

        assert_eq!(
            family_checks(&database).await,
            vec![(true, FAMILY_CHECK_FROM_1006.to_owned())],
            "{change}"
        );
        assert_eq!(family_row(&database).await.as_deref(), Some("RUST"));
        database.drop().await;
    }
}

/// A database whose family row holds `TYPESCRIPT` is of the TypeScript
/// client: the install of a Python and of a Rust client is refused with the
/// stored family before any write, and the install of a TypeScript client
/// finds it ready and writes nothing.
#[tokio::test]
async fn a_typescript_database_refuses_the_install_of_a_python_and_a_rust_client() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    assert_eq!(write_family(&database, "TYPESCRIPT").await, Ok(()));
    let before = installed_on(&database).await;

    for own in [ClientFamily::Python, ClientFamily::Rust] {
        let refused = database.install(own, InstallLockPlan::fleet()).await;
        assert!(
            matches!(
                &refused,
                Err(SchemaInstallError::OtherFamily {
                    stored: ClientFamily::TypeScript,
                    own: refused_own,
                }) if *refused_own == own
            ),
            "{refused:?}"
        );
        let message = BrokerError::from(refused.unwrap_err()).to_string();
        assert!(message.contains("the TYPESCRIPT client"), "{message}");
    }
    database
        .install(ClientFamily::TypeScript, InstallLockPlan::fleet())
        .await
        .expect("a TypeScript client installs into a TypeScript database");

    assert_eq!(family_row(&database).await.as_deref(), Some("TYPESCRIPT"));
    assert_eq!(installed_on(&database).await, before);
    database.drop().await;
}

/// A database that holds the whole chain with no family row gets the row
/// `TYPESCRIPT` from the install of a TypeScript client (Python
/// `CompleteInstall`), and no migration runs again.
#[tokio::test]
async fn a_current_database_with_no_family_row_gets_the_typescript_row() {
    let database = MigrationTestDatabase::create().await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("install into an empty database");
    database.execute("DELETE FROM horsies_client_family").await;
    let before = installed_on(&database).await;

    database
        .install(ClientFamily::TypeScript, InstallLockPlan::fleet())
        .await
        .expect("record the family");

    assert_eq!(family_row(&database).await.as_deref(), Some("TYPESCRIPT"));
    assert_eq!(installed_on(&database).await, before);
    database.drop().await;
}

// ---- The family row of an install ----

/// Installs `own` into an empty database: the install reaches the newest
/// migration of the chain and records `own` in the family row.
async fn assert_a_fresh_install_records_its_family(own: ClientFamily) {
    let database = MigrationTestDatabase::create().await;

    database
        .install(own, InstallLockPlan::fleet())
        .await
        .unwrap_or_else(|error| panic!("install {own:?} into an empty database: {error}"));

    let installed = bookkeeping_rows(&database).await;
    assert_eq!(
        installed,
        chain_rows(embedded_migrations().len()),
        "{own:?}"
    );
    assert_eq!(
        installed.last().map(|row| row.0),
        Some(expected_schema_version()),
        "{own:?}"
    );
    assert_eq!(family_row(&database).await.as_deref(), Some(own.as_str()));
    database.drop().await;
}

/// A fresh install of the Python client records `PYTHON` in the transaction
/// of migration 1000 and reaches the newest migration.
#[tokio::test]
async fn a_fresh_python_install_records_its_family_and_reaches_the_newest_migration() {
    assert_a_fresh_install_records_its_family(ClientFamily::Python).await;
}

/// A fresh install of the Rust client records `RUST` in the transaction of
/// migration 1000 and reaches the newest migration.
#[tokio::test]
async fn a_fresh_rust_install_records_its_family_and_reaches_the_newest_migration() {
    assert_a_fresh_install_records_its_family(ClientFamily::Rust).await;
}

/// A fresh install of the TypeScript client records `TYPESCRIPT` and reaches
/// the newest migration: the row is written in the transaction of migration
/// 1006, after its text admits the value.
#[tokio::test]
async fn a_fresh_typescript_install_records_its_family_and_reaches_the_newest_migration() {
    assert_a_fresh_install_records_its_family(ClientFamily::TypeScript).await;
}

/// Each family other than `stored` is refused by the database before any
/// write: its install with `OtherFamily`, and the family check before the
/// first write of its handle with `ClientFamilyMismatch`. Both name the
/// stored family.
async fn assert_each_other_family_is_refused(
    database: &MigrationTestDatabase,
    stored: ClientFamily,
) {
    for own in every_family()
        .into_iter()
        .filter(|family| *family != stored)
    {
        let refused = database.install(own, InstallLockPlan::fleet()).await;
        assert!(
            matches!(
                &refused,
                Err(SchemaInstallError::OtherFamily {
                    stored: refused_stored,
                    own: refused_own,
                }) if *refused_stored == stored && *refused_own == own
            ),
            "the install of {own:?} into a {stored:?} database: {refused:?}"
        );
        let broker = crate::broker::PostgresBroker::from_test_url_observe_only(database.url(), own);
        let checked = broker.ensure_client_family_for_write().await;
        broker.close().await;
        match checked {
            Err(BrokerError::ClientFamilyMismatch {
                stored: recorded,
                own: refused_own,
            }) => {
                assert_eq!(recorded, RecordedFamily::Recorded(stored));
                assert_eq!(refused_own, own);
            }
            other => {
                panic!("the write check of a {own:?} handle on a {stored:?} database: {other:?}")
            }
        }
    }
}

/// A database that a fresh install of one client family made refuses each
/// other family before any write, and keeps refusing it after an install
/// again of its own family. Its bookkeeping and its family row do not
/// change.
#[tokio::test]
async fn a_database_installed_by_a_family_keeps_refusing_another_family() {
    for stored in every_family() {
        let database = MigrationTestDatabase::create().await;
        database
            .install(stored, InstallLockPlan::fleet())
            .await
            .unwrap_or_else(|error| panic!("install {stored:?} into an empty database: {error}"));
        let before = installed_on(&database).await;

        assert_each_other_family_is_refused(&database, stored).await;
        database
            .install(stored, InstallLockPlan::fleet())
            .await
            .unwrap_or_else(|error| panic!("install {stored:?} again: {error}"));
        assert_each_other_family_is_refused(&database, stored).await;

        assert_eq!(installed_on(&database).await, before, "{stored:?}");
        assert_eq!(
            family_row(&database).await.as_deref(),
            Some(stored.as_str())
        );
        database.drop().await;
    }
}

/// Makes the DDL of the text of the migration at `position` of the embedded
/// chain fail with SQLSTATE P0001, through an event trigger: the install
/// stops at that migration, and its transaction rolls back. The text of
/// each migration starts with a line of its own (`-- horsies schema
/// migration <version>: ...`), and the install sends the whole text as one
/// query, so `current_query()` starts with that line.
async fn fail_the_migration_at(database: &MigrationTestDatabase, position: usize) {
    let first_line = embedded_migrations()[position]
        .script
        .sql()
        .lines()
        .next()
        .expect("the text of a migration has a first line");
    database
        .execute(&format!(
            "CREATE FUNCTION install_migration_probe() RETURNS event_trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF starts_with(current_query(), {first_line}) THEN
                     RAISE EXCEPTION 'injected failure of a migration';
                 END IF;
             END
             $$",
            first_line = sql_literal(first_line),
        ))
        .await;
    database
        .execute(
            "CREATE EVENT TRIGGER install_migration_probe ON ddl_command_start
             EXECUTE FUNCTION install_migration_probe()",
        )
        .await;
}

/// Removes the event trigger of [`fail_the_migration_at`].
async fn stop_failing_the_migration(database: &MigrationTestDatabase) {
    database
        .execute(
            "DROP EVENT TRIGGER install_migration_probe; \
             DROP FUNCTION install_migration_probe()",
        )
        .await;
}

/// Asserts that `stopped` is the refusal of the text of the migration at
/// `position`, by [`fail_the_migration_at`].
fn assert_stopped_at(stopped: Result<(), SchemaInstallError>, position: usize) {
    match stopped {
        Err(SchemaInstallError::Statement(StatementFailure::StatementRefused {
            statement,
            cause,
        })) => {
            assert_eq!(statement, embedded_migrations()[position].script.name());
            assert_eq!(cause.server_error().code(), &SqlState::RAISE_EXCEPTION);
        }
        other => panic!("expected the refused migration at {position}, got {other:?}"),
    }
}

/// A Python or a Rust install into an empty database that stops after
/// migration 1000 commits keeps the family row that migration 1000 wrote: the
/// database belongs to the family that started the install. The install of
/// each other family is refused before any write, and the next install of
/// the same family applies the rest of the chain.
#[tokio::test]
async fn a_python_or_rust_install_that_stops_after_migration_1000_keeps_its_family() {
    for stored in [ClientFamily::Python, ClientFamily::Rust] {
        let database = MigrationTestDatabase::create().await;
        fail_the_migration_at(&database, 1).await;

        let stopped = database.install(stored, InstallLockPlan::fleet()).await;

        assert_stopped_at(stopped, 1);
        assert_eq!(bookkeeping_rows(&database).await, chain_rows(1));
        assert_eq!(
            family_row(&database).await.as_deref(),
            Some(stored.as_str())
        );

        stop_failing_the_migration(&database).await;
        assert_each_other_family_is_refused(&database, stored).await;
        assert_eq!(bookkeeping_rows(&database).await, chain_rows(1));
        database
            .install(stored, InstallLockPlan::fleet())
            .await
            .unwrap_or_else(|error| panic!("{stored:?} completes its install: {error}"));
        assert_eq!(
            bookkeeping_rows(&database).await,
            chain_rows(embedded_migrations().len())
        );
        assert_eq!(
            family_row(&database).await.as_deref(),
            Some(stored.as_str())
        );
        database.drop().await;
    }
}

/// A TypeScript install into an empty database that stops before migration
/// 1006 commits keeps the migrations before it and writes no family row. No
/// handle writes into a database with no family row, so the next install, of
/// any client family, applies the rest of the chain and records its own
/// family: a Rust install applies migration 1006, then writes its row after
/// the chain (`CompleteInstall`).
#[tokio::test]
async fn a_typescript_install_that_stops_before_migration_1006_records_no_family() {
    let database = MigrationTestDatabase::create().await;
    let admitting = admitting_position(ClientFamily::TypeScript);
    fail_the_migration_at(&database, admitting).await;

    let stopped = database
        .install(ClientFamily::TypeScript, InstallLockPlan::fleet())
        .await;

    assert_stopped_at(stopped, admitting);
    assert_eq!(bookkeeping_rows(&database).await, chain_rows(admitting));
    assert_eq!(family_row(&database).await, None);

    stop_failing_the_migration(&database).await;
    database
        .install(ClientFamily::Rust, InstallLockPlan::fleet())
        .await
        .expect("the next install applies the rest of the chain");

    assert_eq!(
        bookkeeping_rows(&database).await,
        chain_rows(embedded_migrations().len())
    );
    assert_eq!(family_row(&database).await.as_deref(), Some("RUST"));
    database.drop().await;
}

/// The family row commits in the transaction of the migration that admits
/// its family, or neither commits: in the install of a TypeScript client, a
/// family row that the database refuses rolls migration 1006 back with it
/// (no bookkeeping row, the CHECK of migration 1005, no family row). A row
/// trigger on `horsies_client_family` refuses the insert.
#[tokio::test]
async fn a_refused_family_row_rolls_back_the_migration_that_admits_its_family() {
    let database = MigrationTestDatabase::create().await;
    let admitting = admitting_position(ClientFamily::TypeScript);
    database
        .install_first_migrations(ClientFamily::TypeScript, admitting)
        .await;
    assert_eq!(family_row(&database).await, None);
    let checks_before = family_checks(&database).await;
    database
        .execute(
            "CREATE FUNCTION family_row_probe() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 RAISE EXCEPTION 'injected refusal of the family row';
             END
             $$;
             CREATE TRIGGER family_row_probe BEFORE INSERT ON horsies_client_family
             FOR EACH ROW EXECUTE FUNCTION family_row_probe();",
        )
        .await;

    let refused = database
        .install(ClientFamily::TypeScript, InstallLockPlan::fleet())
        .await;

    match refused {
        Err(SchemaInstallError::Statement(StatementFailure::StatementRefused {
            statement,
            cause,
        })) => {
            assert_eq!(statement, RECORD_CLIENT_FAMILY.name());
            assert_eq!(cause.server_error().code(), &SqlState::RAISE_EXCEPTION);
        }
        other => panic!("expected the refused family row, got {other:?}"),
    }
    assert_eq!(bookkeeping_rows(&database).await, chain_rows(admitting));
    assert_eq!(family_checks(&database).await, checks_before);
    assert_eq!(family_row(&database).await, None);
    database.drop().await;
}

/// The admitted families of the chain are the CHECK of the family row:
/// after each migration, the CHECK `horsies_client_family_family_known`
/// names exactly the families that the migrations up to it admit.
#[tokio::test]
async fn the_admitted_families_of_the_chain_are_the_family_check() {
    let database = MigrationTestDatabase::create().await;
    let chain = embedded_migrations();
    for end in 1..=chain.len() {
        database
            .apply_embedded(ClientFamily::Rust, end - 1, end)
            .await;

        let checks = family_checks(&database).await;
        let [(_, definition)] = checks.as_slice() else {
            panic!("one family CHECK after {end} migrations: {checks:?}");
        };
        for family in every_family() {
            let admitted = chain[..end]
                .iter()
                .any(|migration| migration.admitted_families.contains(&family));
            assert_eq!(
                definition.contains(&format!("'{}'", family.as_str())),
                admitted,
                "{family:?} after {end} migrations: {definition}"
            );
        }
    }
    database.drop().await;
}
