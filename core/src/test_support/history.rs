//! Fixtures of the history tests, through the functions that the core's
//! coverage pass calls (`baseline/0008_history_maintenance_functions.sql`),
//! on the test client: the registration of a retention class, the creation
//! of a history leaf or a heartbeat leaf, and the database clock.

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
