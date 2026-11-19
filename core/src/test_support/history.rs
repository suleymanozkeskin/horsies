//! Fixtures of the history tests, through the functions that the core's
//! coverage pass calls (`baseline/0008_history_maintenance_functions.sql`),
//! on the test client: the registration of a retention class, the creation
//! of a history leaf or a heartbeat leaf, the guard of a seeded heartbeat
//! instant ([`covered_beat_instant`]), and the database clock.

use chrono::{DateTime, Utc};

use crate::core::config::retention::RetentionConfig;
use crate::core::history::commands::LeafRef;
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::coverage_functions::{
    leaf_creation_of, CreatedLeafKind, LeafCreationRow,
};
use crate::core::history::names::HEARTBEAT_CLASS_KEY;
use crate::core::history::outcomes::LeafCreation;
use crate::core::history::periods::RetentionDays;
use crate::test_support::sql::{self, Target};

/// `horsies_register_retention_classes` for the period class of `days`.
/// The heartbeat horizon passed is the stored one, so the call does not
/// change a stored heartbeat class. When no heartbeat class is stored, the
/// function registers it with the default horizon of the retention config,
/// as the Python fixture `register_class`
/// (`clients/python/tests/integration/task_history_harness.py`) does. Returns the outcome
/// of the period class: `REGISTERED`, or `VERIFIED` when the class was there.
///
/// # Panics
///
/// When the function refuses: the test cannot run.
pub(crate) async fn register_period_class<'t>(
    target: impl Into<Target<'t>>,
    days: RetentionDays,
) -> String {
    sql::query_scalar(
        "SELECT r.outcome
         FROM horsies_register_retention_classes(
             ARRAY[$1]::smallint[],
             COALESCE(
                 (SELECT c.heartbeat_horizon_hours FROM horsies_retention_classes AS c
                  WHERE c.class_key = $2),
                 $3
             )
         ) AS r
         WHERE r.class_key = $4",
    )
    .bind(days.as_stored())
    .bind(HEARTBEAT_CLASS_KEY)
    .bind(default_heartbeat_horizon_hours())
    .bind(days.class_key())
    .fetch_one(target)
    .await
    .expect("register the period class")
}

/// `horsies_register_retention_classes` with no period and the heartbeat
/// horizon `hours`: registers the heartbeat class, or updates its horizon.
/// Returns the outcome of the heartbeat class: `REGISTERED`, `VERIFIED` or
/// `HORIZON_UPDATED`.
///
/// # Panics
///
/// When the function refuses: the test cannot run.
pub(crate) async fn register_heartbeat_class<'t>(
    target: impl Into<Target<'t>>,
    hours: i16,
) -> String {
    sql::query_scalar(
        "SELECT r.outcome
         FROM horsies_register_retention_classes(ARRAY[]::smallint[], $1) AS r
         WHERE r.class_key = $2",
    )
    .bind(hours)
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(target)
    .await
    .expect("register the heartbeat class")
}

/// The heartbeat horizon of the default retention config, as stored.
fn default_heartbeat_horizon_hours() -> i16 {
    i16::try_from(RetentionConfig::default().heartbeat_leaf_horizon_hours)
        .expect("the default heartbeat horizon fits smallint")
}

/// One row of `horsies_create_history_leaf` or
/// `horsies_create_heartbeat_leaf`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeafCreated {
    /// `CREATED`, `ALREADY_CONFORMANT`, `INDEX_REPAIRED`,
    /// `COVERED_BY_OPEN_ENDED`, `BUSY`, `CATALOG_CONFLICT` or `CLASS_ABSENT`.
    pub(crate) outcome: String,
    pub(crate) leaf_name: Option<String>,
    pub(crate) conflict_kind: Option<String>,
}

impl sql::FromTestRow for LeafCreated {
    fn from_row(row: &tokio_postgres::Row) -> Result<Self, sql::Error> {
        Ok(Self {
            outcome: sql::named(row, "outcome")?,
            leaf_name: sql::named(row, "leaf_name")?,
            conflict_kind: sql::named(row, "conflict_kind")?,
        })
    }
}

/// `horsies_create_history_leaf` of class `class_key` for the UTC day that
/// starts at `lower` (`None`: the static forever leaf).
///
/// # Panics
///
/// When the function refuses: the test cannot run.
pub(crate) async fn create_history_leaf<'t>(
    target: impl Into<Target<'t>>,
    class_key: &str,
    lower: Option<DateTime<Utc>>,
) -> LeafCreated {
    try_create_history_leaf(target, class_key, lower)
        .await
        .expect("create the history leaf")
}

/// [`create_history_leaf`] for a test whose subject is the refusal.
///
/// Errors: the `sql::Error` of the refused statement.
pub(crate) async fn try_create_history_leaf<'t>(
    target: impl Into<Target<'t>>,
    class_key: &str,
    lower: Option<DateTime<Utc>>,
) -> Result<LeafCreated, sql::Error> {
    sql::query_as(
        "SELECT outcome, leaf_name, conflict_kind FROM horsies_create_history_leaf($1, $2)",
    )
    .bind(class_key)
    .bind(lower)
    .fetch_one(target)
    .await
}

/// `horsies_create_heartbeat_leaf` for the UTC hour that starts at `lower`.
///
/// # Panics
///
/// When the function refuses: the test cannot run.
pub(crate) async fn create_heartbeat_leaf<'t>(
    target: impl Into<Target<'t>>,
    lower: DateTime<Utc>,
) -> LeafCreated {
    sql::query_as("SELECT outcome, leaf_name, conflict_kind FROM horsies_create_heartbeat_leaf($1)")
        .bind(lower)
        .fetch_one(target)
        .await
        .expect("create the heartbeat leaf")
}

/// The daily history leaf of `leaf` through `horsies_create_history_leaf`
/// (no publication: the coverage pass publishes in its last step), decoded
/// by the coverage pass's decoder.
///
/// Errors: `HistoryError::Contract` for a row outside the function's set.
///
/// # Panics
///
/// When the function refuses: the test cannot run.
pub(crate) async fn create_history_leaf_of<'t>(
    target: impl Into<Target<'t>>,
    leaf: &LeafRef,
) -> Result<LeafCreation, HistoryError> {
    let created = create_history_leaf(target, leaf.class_key(), Some(leaf.bounds().lower())).await;
    leaf_creation_of(
        CreatedLeafKind::History,
        leaf.class_key(),
        creation_row(created),
    )
}

/// The hourly heartbeat leaf of `leaf` through
/// `horsies_create_heartbeat_leaf`, decoded by the coverage pass's decoder.
///
/// Errors: `HistoryError::Contract` for a row outside the function's set.
///
/// # Panics
///
/// When the function refuses: the test cannot run.
pub(crate) async fn create_heartbeat_leaf_of<'t>(
    target: impl Into<Target<'t>>,
    leaf: &LeafRef,
) -> Result<LeafCreation, HistoryError> {
    let created = create_heartbeat_leaf(target, leaf.bounds().lower()).await;
    leaf_creation_of(
        CreatedLeafKind::Heartbeat,
        HEARTBEAT_CLASS_KEY,
        creation_row(created),
    )
}

fn creation_row(created: LeafCreated) -> LeafCreationRow {
    LeafCreationRow {
        outcome: created.outcome,
        leaf_name: created.leaf_name,
        conflict_kind: created.conflict_kind,
    }
}

/// Where a seeded heartbeat lies in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeedInstant {
    /// This long before `NOW()` of the seed's transaction.
    BeforeNow(chrono::Duration),
    /// This instant.
    At(DateTime<Utc>),
}

/// A heartbeat `sent_at` whose hourly leaf exists. Only
/// [`covered_beat_instant`] constructs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoveredBeatInstant(DateTime<Utc>);

impl CoveredBeatInstant {
    /// The instant, for the `sent_at` of the seed.
    pub(crate) fn at(self) -> DateTime<Utc> {
        self.0
    }
}

/// The instant `$1` microseconds before `NOW()`, with the outcome of
/// `horsies_create_heartbeat_leaf` for its UTC hour.
const BEAT_INSTANT_BEFORE_NOW_SQL: &str = "
    SELECT s.at, l.outcome
    FROM (SELECT NOW() - $1 * INTERVAL '1 microsecond' AS at) AS s
    CROSS JOIN LATERAL horsies_create_heartbeat_leaf(date_trunc('hour', s.at, 'UTC')) AS l";

/// The instant `$1`, with the outcome of `horsies_create_heartbeat_leaf`
/// for its UTC hour.
const BEAT_INSTANT_AT_SQL: &str = "
    SELECT s.at, l.outcome
    FROM (SELECT $1::timestamptz AS at) AS s
    CROSS JOIN LATERAL horsies_create_heartbeat_leaf(date_trunc('hour', s.at, 'UTC')) AS l";

/// The guard of every heartbeat seed whose `sent_at` is not the `NOW()` of
/// a statement after the coverage pass: it creates the hourly leaf of the
/// instant, or finds it conformant, and returns the instant.
///
/// A coverage pass creates the leaves of its own hour and later hours
/// only. A seed in the past ("five minutes ago") falls in the hour before
/// the coverage when the test starts just after the hour, and the insert
/// fails with SQLSTATE 23514 (no partition for the row). The leaf of the
/// seed's own hour removes that dependence on the clock. The database needs
/// the heartbeat class (the coverage pass registers it).
///
/// # Panics
///
/// When the statement fails or the leaf outcome is not `CREATED`,
/// `ALREADY_CONFORMANT` or `INDEX_REPAIRED`: the test cannot run.
pub(crate) async fn covered_beat_instant<'t>(
    target: impl Into<Target<'t>>,
    instant: SeedInstant,
) -> CoveredBeatInstant {
    let (at, outcome): (DateTime<Utc>, String) = match instant {
        SeedInstant::BeforeNow(ago) => {
            let micros = ago
                .num_microseconds()
                .expect("a seed offset fits in microseconds");
            sql::query_as(BEAT_INSTANT_BEFORE_NOW_SQL)
                .bind(micros)
                .fetch_one(target)
                .await
        }
        SeedInstant::At(at) => {
            sql::query_as(BEAT_INSTANT_AT_SQL)
                .bind(at)
                .fetch_one(target)
                .await
        }
    }
    .expect("create the heartbeat leaf of the seeded instant");
    match outcome.as_str() {
        "CREATED" | "ALREADY_CONFORMANT" | "INDEX_REPAIRED" => CoveredBeatInstant(at),
        refused => panic!("the heartbeat leaf of the hour of {at}: {refused}"),
    }
}

/// The statement timestamp of the database.
///
/// # Panics
///
/// When the statement fails: the test cannot run.
pub(crate) async fn database_now<'t>(target: impl Into<Target<'t>>) -> DateTime<Utc> {
    sql::query_scalar("SELECT statement_timestamp()")
        .fetch_one(target)
        .await
        .expect("read the database clock")
}

#[cfg(test)]
mod tests {
    //! The guard of a seeded heartbeat instant ([`covered_beat_instant`]),
    //! at an instant in the hour before the first leaf of the coverage
    //! pass: the hour that a seed "a few minutes ago" falls in when the
    //! test starts just after the hour.

    use chrono::Duration;
    use uuid::Uuid;

    use super::{covered_beat_instant, database_now, SeedInstant};
    use crate::test_support::sql::{self, TestPool};

    /// The start of the earliest heartbeat leaf of the database: the hour
    /// in which its coverage pass ran.
    async fn first_heartbeat_hour(pool: &TestPool) -> chrono::DateTime<chrono::Utc> {
        let first: Option<chrono::DateTime<chrono::Utc>> = sql::query_scalar(
            "SELECT min(lower_anchor) FROM horsies_task_history_leaf_catalog
             WHERE class_key = 'heartbeats' AND dropped_at IS NULL",
        )
        .fetch_one(pool)
        .await
        .expect("read the first heartbeat hour");
        first.expect("the coverage pass created heartbeat leaves")
    }

    /// A runner beat of a fresh task id sent at `sent_at`.
    async fn insert_beat(
        pool: &TestPool,
        sent_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, sql::Error> {
        sql::query(
            "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
             VALUES ($1, 'guard-worker', 'runner', $2, 'guard-host', 1)",
        )
        .bind(Uuid::new_v4())
        .bind(sent_at)
        .execute(pool)
        .await
        .map(|executed| executed.rows_affected())
    }

    /// A beat one minute before the first leaf has no partition (the
    /// failure of a seed that is not guarded); the guard of that instant
    /// creates its leaf and returns the instant, and the beat lands.
    #[tokio::test]
    async fn a_seed_before_the_first_leaf_gets_the_leaf_of_its_hour() {
        let pool = crate::test_support::migrated_pool().await;
        let before = first_heartbeat_hour(&pool).await - Duration::minutes(1);

        let refused = insert_beat(&pool, before)
            .await
            .expect_err("no leaf holds the hour before the coverage pass");
        let code = refused
            .as_database_error()
            .expect("the refusal is a server error")
            .code()
            .code()
            .to_owned();
        let covered = covered_beat_instant(&pool, SeedInstant::At(before)).await;
        let landed = insert_beat(&pool, covered.at()).await;

        assert_eq!(code, "23514");
        assert_eq!(covered.at(), before);
        assert_eq!(landed.expect("the guarded beat lands"), 1);
        pool.close().await;
    }

    /// An offset before the database clock that reaches the hour before
    /// the first leaf: the guard returns `NOW()` minus the offset, in that
    /// hour, and the beat lands.
    #[tokio::test]
    async fn an_offset_into_the_hour_before_the_first_leaf_gets_its_leaf() {
        let pool = crate::test_support::migrated_pool().await;
        let first = first_heartbeat_hour(&pool).await;
        let now = database_now(&pool).await;
        let offset = now - (first - Duration::minutes(1));

        let covered = covered_beat_instant(&pool, SeedInstant::BeforeNow(offset)).await;
        let landed = insert_beat(&pool, covered.at()).await;

        assert!(
            covered.at() < first,
            "{} is not before {first}",
            covered.at()
        );
        assert!(
            covered.at() >= first - Duration::hours(1),
            "{} is not in the hour before {first}",
            covered.at()
        );
        assert_eq!(landed.expect("the guarded beat lands"), 1);
        pool.close().await;
    }
}
