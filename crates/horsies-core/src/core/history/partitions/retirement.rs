//! Retention retirement of PERIOD and HEARTBEAT leaves through the baseline
//! functions of `0006_history_functions.sql`. Each function is one typed
//! statement of the core statement layer here, and its row is parsed into the
//! outcome types:
//!
//! - `horsies_prune_candidates(p_limit)`: the expired leaves of one pass and
//!   their attachment, in one read;
//! - `horsies_inspect_history_leaf(p_leaf_name)`: the state of one leaf, in
//!   one read;
//! - `horsies_begin_leaf_detach(p_leaf_name)`: the inspection, the session
//!   leaf lock (for `DETACHABLE` and `DETACH_INTERRUPTED` only) and the
//!   inspection again under the lock; for a `DETACHED` leaf, the catalog
//!   record of its detach and, when the published manifest names it, the
//!   reader publication;
//! - `horsies_record_leaf_detached(p_leaf_name)`: the catalog record of a
//!   detach, and whether the published reader manifest still names the leaf;
//! - `horsies_drop_detached_leaf(p_leaf_name)`: the drop, inside the caller's
//!   core transaction.
//!
//! The `ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY | FINALIZE`
//! statement stays with the caller (`partitions::manager`, as a
//! [`crate::db::PartitionDdl`]): it cannot run in a function. A row outside
//! the documented values is `HistoryError::Contract`.
//!
//! Python: `python/horsies/core/history/partitions/retirement.py`.

use chrono::{DateTime, Utc};

use crate::broker::core_rows::{column, one_row, query_in, query_on, RowDecodeError, StatementLog};
use crate::core::history::commands::{LeafBounds, LeafRef};
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::pruning::PruneReach;
use crate::core::history::outcomes::{
    CatalogConflictKind, LeafAttachment, LeafDrop, LeafInspection,
};
use crate::db::{ParameterType, PooledConnection, Row, Transaction, TypedStatement};

use super::locks::unlock_leaf_in_session;

/// `horsies_prune_candidates`: `$1` the row limit. Reads only.
pub(crate) static PRUNE_CANDIDATES: TypedStatement = TypedStatement::new(
    "prune_candidates",
    "SELECT leaf_name, class_key, class_kind, lower_anchor, upper_anchor, attachment \
     FROM horsies_prune_candidates($1)",
    &[ParameterType::Int4],
);

/// `horsies_inspect_history_leaf`: `$1` the leaf name. Reads only. The
/// `state` column is read as `outcome`, the column of the two functions that
/// return an inspection row.
pub(crate) static INSPECT_LEAF: TypedStatement = TypedStatement::new(
    "inspect_history_leaf",
    "SELECT state AS outcome, class_key, parent_name, lower_anchor, upper_anchor, expires_at, \
     blocker_count, attachment, conflict_kind FROM horsies_inspect_history_leaf($1)",
    &[ParameterType::Text],
);

/// `horsies_begin_leaf_detach`: `$1` the leaf name.
pub(crate) static BEGIN_LEAF_DETACH: TypedStatement = TypedStatement::new(
    "begin_leaf_detach",
    "SELECT outcome, class_key, parent_name, lower_anchor, upper_anchor, expires_at, \
     blocker_count, attachment, conflict_kind FROM horsies_begin_leaf_detach($1)",
    &[ParameterType::Text],
);

/// `horsies_record_leaf_detached`: `$1` the leaf name.
pub(crate) static RECORD_LEAF_DETACHED: TypedStatement = TypedStatement::new(
    "record_leaf_detached",
    "SELECT outcome, manifest_names_leaf FROM horsies_record_leaf_detached($1)",
    &[ParameterType::Text],
);

const DROP_DETACHED_LEAF_SQL: &str = "SELECT outcome, class_key, parent_name, lower_anchor, \
     upper_anchor, expires_at, blocker_count, attachment, conflict_kind \
     FROM horsies_drop_detached_leaf($1)";

/// `horsies_drop_detached_leaf`: `$1` the leaf name.
pub(crate) static DROP_DETACHED_LEAF: TypedStatement = TypedStatement::new(
    "drop_detached_leaf",
    DROP_DETACHED_LEAF_SQL,
    &[ParameterType::Text],
);

// ---------------------------------------------------------------------------
// Candidates
// ---------------------------------------------------------------------------

/// The class kinds that retention detaches and drops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetiredClassKind {
    Period,
    Heartbeat,
}

/// Where an expired leaf stands relative to its parent at the candidate read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateAttachment {
    Attached,
    /// A concurrent detach is pending finalization.
    DetachPending,
    /// The relation exists and has no parent.
    Detached,
    /// No relation has the leaf's name.
    Missing,
}

/// One expired leaf of one pruning pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneCandidate {
    pub leaf: LeafRef,
    pub class_kind: RetiredClassKind,
    pub attachment: CandidateAttachment,
}

/// The candidates of one pass, oldest first, and whether the read was
/// truncated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneCandidates {
    pub leaves: Vec<PruneCandidate>,
    pub reach: PruneReach,
}

impl PruneCandidates {
    /// The candidate leaves of one class kind, in candidate order.
    pub fn of_kind(&self, kind: RetiredClassKind) -> Vec<LeafRef> {
        self.leaves
            .iter()
            .filter(|candidate| candidate.class_kind == kind)
            .map(|candidate| candidate.leaf.clone())
            .collect()
    }
}

/// One row of `horsies_prune_candidates`, as stored.
#[derive(Debug)]
struct CandidateRow {
    leaf_name: String,
    class_key: String,
    class_kind: String,
    lower_anchor: DateTime<Utc>,
    upper_anchor: DateTime<Utc>,
    attachment: String,
}

/// The oldest expired PERIOD and HEARTBEAT leaves, at most `bound`.
///
/// One statement on `connection`: `horsies_prune_candidates(bound + 1)`; a
/// row beyond the bound makes the read `Truncated`. Reads only.
///
/// Errors: `HistoryError::Statement` when the statement failed;
/// `HistoryError::RowDecode` for a column of another type;
/// `HistoryError::Contract` for a bound that does not fit the function's
/// `integer` parameter, or a row outside the documented values.
pub(crate) async fn read_prune_candidates(
    connection: &mut PooledConnection,
    log: StatementLog,
    bound: usize,
) -> Result<PruneCandidates, HistoryError> {
    let limit = bound
        .checked_add(1)
        .and_then(|limit| i32::try_from(limit).ok())
        .ok_or_else(|| {
            HistoryError::contract(format!(
                "prune candidate bound {bound} does not fit an integer limit"
            ))
        })?;
    let statement = &PRUNE_CANDIDATES;
    let rows = query_on(connection, log, &statement.bind().value(&limit).finish()).await?;
    let rows = rows
        .iter()
        .map(candidate_row)
        .collect::<Result<Vec<_>, _>>()?;
    prune_candidates_of(rows, bound)
}

/// One `horsies_prune_candidates` row, by column name.
fn candidate_row(row: &Row) -> Result<CandidateRow, RowDecodeError> {
    let statement = &PRUNE_CANDIDATES;
    Ok(CandidateRow {
        leaf_name: column(row, statement, "leaf_name")?,
        class_key: column(row, statement, "class_key")?,
        class_kind: column(row, statement, "class_kind")?,
        lower_anchor: column(row, statement, "lower_anchor")?,
        upper_anchor: column(row, statement, "upper_anchor")?,
        attachment: column(row, statement, "attachment")?,
    })
}

/// The candidates of the rows of one candidate read with `bound`. Pure.
fn prune_candidates_of(
    rows: Vec<CandidateRow>,
    bound: usize,
) -> Result<PruneCandidates, HistoryError> {
    let reach = match rows.len() > bound {
        true => PruneReach::Truncated,
        false => PruneReach::WithinBounds,
    };
    let leaves = rows
        .into_iter()
        .take(bound)
        .map(prune_candidate_of)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PruneCandidates { leaves, reach })
}

fn prune_candidate_of(row: CandidateRow) -> Result<PruneCandidate, HistoryError> {
    let class_kind = match row.class_kind.as_str() {
        "PERIOD" => RetiredClassKind::Period,
        "HEARTBEAT" => RetiredClassKind::Heartbeat,
        other => {
            return Err(HistoryError::contract(format!(
                "leaf {}: class_kind {other:?} is not PERIOD or HEARTBEAT",
                row.leaf_name
            )));
        }
    };
    let attachment = match row.attachment.as_str() {
        "ATTACHED" => CandidateAttachment::Attached,
        "DETACH_PENDING" => CandidateAttachment::DetachPending,
        "DETACHED" => CandidateAttachment::Detached,
        "MISSING" => CandidateAttachment::Missing,
        other => {
            return Err(HistoryError::contract(format!(
                "leaf {}: attachment {other:?} is not a candidate attachment",
                row.leaf_name
            )));
        }
    };
    let bounds = LeafBounds::new(row.lower_anchor, row.upper_anchor)
        .map_err(|error| HistoryError::contract(format!("leaf {}: {error}", row.leaf_name)))?;
    let leaf = LeafRef::new(row.leaf_name, row.class_key, bounds)
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    Ok(PruneCandidate {
        leaf,
        class_kind,
        attachment,
    })
}

// ---------------------------------------------------------------------------
// Inspection rows
// ---------------------------------------------------------------------------

/// One row of `horsies_begin_leaf_detach`, `horsies_drop_detached_leaf` or
/// `horsies_inspect_history_leaf` (its `state` read as `outcome`): the outcome
/// and the inspection columns, as stored.
#[derive(Debug, Clone)]
pub(crate) struct LeafStateRow {
    pub(crate) outcome: String,
    pub(crate) class_key: Option<String>,
    pub(crate) parent_name: Option<String>,
    pub(crate) lower_anchor: Option<DateTime<Utc>>,
    pub(crate) upper_anchor: Option<DateTime<Utc>>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) blocker_count: Option<i64>,
    pub(crate) attachment: Option<String>,
    pub(crate) conflict_kind: Option<String>,
}

/// One inspection row of `statement`, by column name.
fn leaf_state_row(row: &Row, statement: &TypedStatement) -> Result<LeafStateRow, RowDecodeError> {
    Ok(LeafStateRow {
        outcome: column(row, statement, "outcome")?,
        class_key: column(row, statement, "class_key")?,
        parent_name: column(row, statement, "parent_name")?,
        lower_anchor: column(row, statement, "lower_anchor")?,
        upper_anchor: column(row, statement, "upper_anchor")?,
        expires_at: column(row, statement, "expires_at")?,
        blocker_count: column(row, statement, "blocker_count")?,
        attachment: column(row, statement, "attachment")?,
        conflict_kind: column(row, statement, "conflict_kind")?,
    })
}

/// The state of one PERIOD or HEARTBEAT leaf (Python `inspect_leaf` for a
/// retention leaf): one statement on `connection`,
/// `horsies_inspect_history_leaf`. Reads only.
///
/// Errors: `HistoryError::Statement` when the statement failed (the function
/// raises for a forever leaf, an unknown class kind and a missing class
/// parent); `HistoryError::RowDecode` for a column of another type;
/// `HistoryError::Contract` for a state outside the documented values, or a
/// `LeafRef` whose class or bounds differ from the catalog row of its name
/// (a caller defect).
pub(crate) async fn inspect_retention_leaf(
    connection: &mut PooledConnection,
    log: StatementLog,
    leaf: &LeafRef,
) -> Result<LeafInspection, HistoryError> {
    let statement = &INSPECT_LEAF;
    let leaf_name = leaf.leaf_name();
    let rows = query_on(
        connection,
        log,
        &statement.bind().value(&leaf_name).finish(),
    )
    .await?;
    let row = leaf_state_row(&one_row(rows, statement)?, statement)?;
    match catalog_facet_mismatch(leaf, &row) {
        Some(mismatch) => Err(HistoryError::contract(mismatch)),
        None => inspection_of(leaf.leaf_name(), &row),
    }
}

/// The conflict kind and its detail of one `CATALOG_CONFLICT` row of the
/// inspection. The inspection names six kinds; any other value is a
/// contract error.
fn inspection_conflict(
    leaf_name: &str,
    value: Option<&str>,
) -> Result<LeafInspection, HistoryError> {
    let (kind, detail) = match value {
        Some("RELATION_WITHOUT_CATALOG") => (
            CatalogConflictKind::RelationWithoutCatalog,
            "relation exists but the leaf catalog has no row for it",
        ),
        Some("OPEN_LOWER_BOUND") => (
            CatalogConflictKind::OpenLowerBound,
            "a finite leaf has an open lower bound",
        ),
        Some("DROPPED_RELATION_EXISTS") => (
            CatalogConflictKind::DroppedRelationExists,
            "the catalog recorded a drop and the relation exists",
        ),
        Some("FOREIGN_PARENT") => (
            CatalogConflictKind::ForeignParent,
            "the leaf is attached to another parent",
        ),
        Some("DETACH_RECORDED_WHILE_ATTACHED") => (
            CatalogConflictKind::DetachRecordedWhileAttached,
            "the catalog recorded a detach and the leaf is attached",
        ),
        Some("PHYSICAL_NONCONFORMANT") => (
            CatalogConflictKind::PhysicalNonconformant,
            "attached leaf bound or task-ID index disagrees with catalog",
        ),
        other => {
            return Err(HistoryError::contract(format!(
                "leaf {leaf_name}: conflict_kind {other:?} is not a conflict kind of the inspection"
            )));
        }
    };
    Ok(LeafInspection::CatalogConflict {
        leaf_name: leaf_name.to_owned(),
        kind,
        detail: detail.to_owned(),
    })
}

fn required_expiry(leaf_name: &str, row: &LeafStateRow) -> Result<DateTime<Utc>, HistoryError> {
    row.expires_at.ok_or_else(|| {
        HistoryError::contract(format!(
            "leaf {leaf_name}: state {:?} has no expires_at",
            row.outcome
        ))
    })
}

fn positive_blocker_count(leaf_name: &str, value: Option<i64>) -> Result<i64, HistoryError> {
    match value {
        Some(count) if count >= 1 => Ok(count),
        other => Err(HistoryError::contract(format!(
            "leaf {leaf_name}: blocker_count {other:?} is not a positive integer"
        ))),
    }
}

fn blocked_attachment(
    leaf_name: &str,
    value: Option<&str>,
) -> Result<LeafAttachment, HistoryError> {
    match value {
        Some("ATTACHED") => Ok(LeafAttachment::Attached),
        Some("DETACH_PENDING") => Ok(LeafAttachment::DetachInterrupted),
        Some("DETACHED") => Ok(LeafAttachment::Detached),
        other => Err(HistoryError::contract(format!(
            "leaf {leaf_name}: attachment {other:?} is not ATTACHED, DETACH_PENDING or DETACHED"
        ))),
    }
}

/// The outcome type of the `horsies_inspect_history_leaf` state that `row`
/// carries in its `outcome` column. Pure.
///
/// A state, conflict kind, attachment or blocker count outside the
/// documented values, or a state without its expiry, is
/// `HistoryError::Contract`.
pub(crate) fn inspection_of(
    leaf_name: &str,
    row: &LeafStateRow,
) -> Result<LeafInspection, HistoryError> {
    let name = leaf_name.to_owned();
    match row.outcome.as_str() {
        "NOT_CATALOGED" => Ok(LeafInspection::Missing {
            leaf_name: name,
            cataloged: false,
            eligible_at: None,
        }),
        "CATALOG_CONFLICT" => inspection_conflict(leaf_name, row.conflict_kind.as_deref()),
        "ALREADY_DROPPED" => Ok(LeafInspection::Dropped { leaf_name: name }),
        "MISSING" => Ok(LeafInspection::Missing {
            leaf_name: name,
            cataloged: true,
            eligible_at: row.expires_at,
        }),
        "PENDING_BLOCKED" => Ok(LeafInspection::PendingBlocked {
            blocker_count: positive_blocker_count(leaf_name, row.blocker_count)?,
            eligible_at: required_expiry(leaf_name, row)?,
            attachment: blocked_attachment(leaf_name, row.attachment.as_deref())?,
            leaf_name: name,
        }),
        "DETACHED" => Ok(LeafInspection::Detached {
            eligible_at: required_expiry(leaf_name, row)?,
            leaf_name: name,
        }),
        "DETACH_INTERRUPTED" => Ok(LeafInspection::DetachInterrupted {
            eligible_at: required_expiry(leaf_name, row)?,
            leaf_name: name,
        }),
        "NOT_EXPIRED" => Ok(LeafInspection::NotExpired {
            eligible_at: required_expiry(leaf_name, row)?,
            leaf_name: name,
        }),
        "DETACHABLE" => Ok(LeafInspection::Detachable {
            eligible_at: required_expiry(leaf_name, row)?,
            leaf_name: name,
        }),
        other => Err(HistoryError::contract(format!(
            "leaf {leaf_name}: inspection state {other:?} is not a known state"
        ))),
    }
}

/// Why the row's class and bounds are not the ones `leaf` names, or `None`.
///
/// A function row carries the catalog's facets of the leaf name (NULL when
/// the catalog has no row). Every caller builds its `LeafRef` from that
/// catalog row, and the catalog never reuses a leaf name, so a difference is
/// a caller defect.
fn catalog_facet_mismatch(leaf: &LeafRef, row: &LeafStateRow) -> Option<String> {
    match (&row.class_key, row.lower_anchor, row.upper_anchor) {
        (None, _, _) => None,
        (Some(class_key), Some(lower), Some(upper))
            if class_key == leaf.class_key()
                && lower == leaf.bounds().lower()
                && upper == leaf.bounds().upper() =>
        {
            None
        }
        (Some(class_key), lower, upper) => Some(format!(
            "leaf {}: the caller names class {:?} and bounds {}..{}, the catalog row has class \
             {class_key:?} and bounds {lower:?}..{upper:?}",
            leaf.leaf_name(),
            leaf.class_key(),
            leaf.bounds().lower(),
            leaf.bounds().upper(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Detach
// ---------------------------------------------------------------------------

/// The outcome of `horsies_begin_leaf_detach`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BeginDetach {
    /// The session holds the leaf lock. The inspection is `Detachable` or
    /// `DetachInterrupted`.
    Locked {
        parent_name: String,
        inspection: LeafInspection,
    },
    /// Another session holds the leaf lock. This session holds none.
    Busy,
    /// Any other state. This session holds no lock.
    Unlocked(LeafInspection),
}

/// The begin outcome of one `horsies_begin_leaf_detach` row. Pure.
///
/// After a `Locked` state whose row cannot be parsed, the session still
/// holds the lock: the caller closes the connection.
pub(crate) fn begin_outcome(
    leaf_name: &str,
    row: LeafStateRow,
) -> Result<BeginDetach, HistoryError> {
    let inspection = match row.outcome.as_str() {
        "BUSY" => return Ok(BeginDetach::Busy),
        _ => inspection_of(leaf_name, &row)?,
    };
    match (inspection, row.parent_name) {
        (
            inspection @ (LeafInspection::Detachable { .. }
            | LeafInspection::DetachInterrupted { .. }),
            Some(parent_name),
        ) => Ok(BeginDetach::Locked {
            parent_name,
            inspection,
        }),
        (LeafInspection::Detachable { .. } | LeafInspection::DetachInterrupted { .. }, None) => {
            Err(HistoryError::contract(format!(
                "leaf {leaf_name}: a locked detach row has no parent name"
            )))
        }
        (other, _) => Ok(BeginDetach::Unlocked(other)),
    }
}

/// Inspect `leaf` and take its session lock when it is ready to detach or
/// to finalize. A `DETACHED` leaf gets its detach recorded, and the readers
/// published when the manifest names it, in the same statement.
///
/// One statement on `connection`, in autocommit: `horsies_begin_leaf_detach`;
/// the function releases a lock it took before a database error propagates.
/// A `LeafRef` whose class or bounds differ from the catalog row of its name
/// is a caller defect: the lock the call took is released (keyed by the
/// catalog's facets, as the function took it) and the result is
/// `HistoryError::Contract`.
///
/// Errors: `HistoryError::Statement`, `HistoryError::RowDecode`,
/// `HistoryError::Contract`. After an error the session may still hold the
/// lock (a failed release, or a locked row that does not parse): the caller
/// closes the connection.
pub(crate) async fn begin_leaf_detach(
    connection: &mut PooledConnection,
    log: StatementLog,
    leaf: &LeafRef,
) -> Result<BeginDetach, HistoryError> {
    let statement = &BEGIN_LEAF_DETACH;
    let leaf_name = leaf.leaf_name();
    let rows = query_on(
        connection,
        log,
        &statement.bind().value(&leaf_name).finish(),
    )
    .await?;
    let row = leaf_state_row(&one_row(rows, statement)?, statement)?;
    let Some(mismatch) = catalog_facet_mismatch(leaf, &row) else {
        return begin_outcome(leaf.leaf_name(), row);
    };
    match (
        row.outcome.as_str(),
        row.class_key.as_deref(),
        row.lower_anchor,
    ) {
        ("DETACHABLE" | "DETACH_INTERRUPTED", Some(class_key), Some(lower)) => {
            unlock_leaf_in_session(connection, log, class_key, lower).await?;
        }
        _ => {}
    }
    Err(HistoryError::contract(mismatch))
}

/// The outcome of `horsies_record_leaf_detached`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetachRecord {
    /// This call set `detached_at`.
    Recorded,
    /// `detached_at` or `dropped_at` was already set.
    AlreadyRecorded,
    /// The relation still has a parent, a pending detach included.
    StillAttached,
    /// The relation is gone and the catalog recorded no drop.
    Missing,
    /// No catalog row.
    NotCataloged,
}

impl DetachRecord {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recorded => "RECORDED",
            Self::AlreadyRecorded => "ALREADY_RECORDED",
            Self::StillAttached => "STILL_ATTACHED",
            Self::Missing => "MISSING",
            Self::NotCataloged => "NOT_CATALOGED",
        }
    }

    fn parse(leaf_name: &str, value: &str) -> Result<Self, HistoryError> {
        match value {
            "RECORDED" => Ok(Self::Recorded),
            "ALREADY_RECORDED" => Ok(Self::AlreadyRecorded),
            "STILL_ATTACHED" => Ok(Self::StillAttached),
            "MISSING" => Ok(Self::Missing),
            "NOT_CATALOGED" => Ok(Self::NotCataloged),
            other => Err(HistoryError::contract(format!(
                "leaf {leaf_name}: detach record outcome {other:?} is not a known outcome"
            ))),
        }
    }
}

/// One detach record, and whether the published manifest still names the
/// leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DetachRecorded {
    pub(crate) record: DetachRecord,
    pub(crate) manifest_names_leaf: bool,
}

/// Record a detach in the catalog, inside the caller's transaction.
///
/// One statement: `horsies_record_leaf_detached`. When the result says the
/// manifest names the leaf, the caller publishes the readers in the same
/// transaction. It does not commit.
///
/// Errors: `HistoryError::Statement` (the transaction is aborted),
/// `HistoryError::RowDecode`, `HistoryError::Contract` for an unknown
/// outcome.
pub(crate) async fn record_leaf_detached(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    leaf_name: &str,
) -> Result<DetachRecorded, HistoryError> {
    let statement = &RECORD_LEAF_DETACHED;
    let rows = query_in(
        transaction,
        log,
        &statement.bind().value(&leaf_name).finish(),
    )
    .await?;
    let row = one_row(rows, statement)?;
    let outcome: String = column(&row, statement, "outcome")?;
    let manifest_names_leaf: bool = column(&row, statement, "manifest_names_leaf")?;
    Ok(DetachRecorded {
        record: DetachRecord::parse(leaf_name, &outcome)?,
        manifest_names_leaf,
    })
}

/// What a completed detach statement left, as its record shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordedDetach {
    /// `RECORDED` or `ALREADY_RECORDED`: the leaf is detached and recorded.
    Detached,
    /// `MISSING`: the relation vanished after the detach.
    Missing,
}

/// The state that the record of a completed detach shows. Pure.
///
/// A leaf still attached, or a catalog row that vanished, contradicts the
/// detach that just completed: `HistoryError::Contract`.
pub(crate) fn recorded_detach(
    leaf_name: &str,
    record: DetachRecord,
) -> Result<RecordedDetach, HistoryError> {
    match record {
        DetachRecord::Recorded | DetachRecord::AlreadyRecorded => Ok(RecordedDetach::Detached),
        DetachRecord::Missing => Ok(RecordedDetach::Missing),
        DetachRecord::StillAttached | DetachRecord::NotCataloged => {
            Err(HistoryError::contract(format!(
                "leaf {leaf_name}: the detach record found {} after the detach completed",
                record.as_str()
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Drop
// ---------------------------------------------------------------------------

/// The drop outcome of one `horsies_drop_detached_leaf` row. Pure.
pub(crate) fn drop_outcome(leaf_name: &str, row: &LeafStateRow) -> Result<LeafDrop, HistoryError> {
    let name = leaf_name.to_owned();
    match row.outcome.as_str() {
        "DROPPED" => Ok(LeafDrop::Dropped { leaf_name: name }),
        "BUSY" => Ok(LeafDrop::Busy { leaf_name: name }),
        "REFERENCED_BY_MANIFEST" => Ok(LeafDrop::RefusedLoaderReferences { leaf_name: name }),
        _ => inspection_of(leaf_name, row).map(LeafDrop::Inspection),
    }
}

/// The drop of a detached retention leaf, inside the caller's core
/// transaction. It does not commit.
///
/// Errors: `HistoryError::Statement` (the transaction is aborted),
/// `HistoryError::RowDecode`, `HistoryError::Contract` (the transaction must
/// roll back, which undoes a drop the call made).
pub(crate) async fn drop_detached_leaf_in(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    leaf: &LeafRef,
) -> Result<LeafDrop, HistoryError> {
    let statement = &DROP_DETACHED_LEAF;
    let leaf_name = leaf.leaf_name();
    let rows = query_in(
        transaction,
        log,
        &statement.bind().value(&leaf_name).finish(),
    )
    .await?;
    let row = leaf_state_row(&one_row(rows, statement)?, statement)?;
    match catalog_facet_mismatch(leaf, &row) {
        Some(mismatch) => Err(HistoryError::contract(mismatch)),
        None => drop_outcome(leaf.leaf_name(), &row),
    }
}

#[cfg(test)]
mod tests {
    //! Python specification: `tests/unit/test_history_retirement_rows.py`.

    use super::*;

    const LEAF: &str = "horsies_task_history_standard_30d_2026_08_06";

    fn expires_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 6, 0, 0, 0)
            .single()
            .expect("expiry")
    }

    fn lower() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 6, 0, 0, 0)
            .single()
            .expect("lower anchor")
    }

    fn leaf() -> LeafRef {
        LeafRef::new(
            LEAF,
            "standard_30d",
            LeafBounds::new(lower(), lower() + chrono::Duration::days(1)).expect("bounds"),
        )
        .expect("leaf ref")
    }

    fn row(state: &str) -> LeafStateRow {
        LeafStateRow {
            outcome: state.to_owned(),
            class_key: Some("standard_30d".to_owned()),
            parent_name: Some("horsies_task_history_standard_30d".to_owned()),
            lower_anchor: Some(lower()),
            upper_anchor: Some(lower() + chrono::Duration::days(1)),
            expires_at: Some(expires_at()),
            blocker_count: None,
            attachment: None,
            conflict_kind: None,
        }
    }

    fn contract_detail(result: Result<LeafInspection, HistoryError>) -> String {
        match result {
            Err(HistoryError::Contract(detail)) => detail,
            other => panic!("expected a contract error, got {other:?}"),
        }
    }

    #[test]
    fn not_cataloged_is_an_uncataloged_missing_leaf() {
        let row = LeafStateRow {
            expires_at: None,
            ..row("NOT_CATALOGED")
        };
        assert_eq!(
            inspection_of(LEAF, &row).unwrap(),
            LeafInspection::Missing {
                leaf_name: LEAF.to_owned(),
                cataloged: false,
                eligible_at: None,
            }
        );
    }

    #[test]
    fn missing_is_a_cataloged_missing_leaf() {
        assert_eq!(
            inspection_of(LEAF, &row("MISSING")).unwrap(),
            LeafInspection::Missing {
                leaf_name: LEAF.to_owned(),
                cataloged: true,
                eligible_at: Some(expires_at()),
            }
        );
    }

    #[test]
    fn already_dropped() {
        assert_eq!(
            inspection_of(LEAF, &row("ALREADY_DROPPED")).unwrap(),
            LeafInspection::Dropped {
                leaf_name: LEAF.to_owned(),
            }
        );
    }

    #[test]
    fn pending_blocked_carries_the_count_and_the_attachment() {
        let row = LeafStateRow {
            blocker_count: Some(2),
            attachment: Some("DETACH_PENDING".to_owned()),
            ..row("PENDING_BLOCKED")
        };
        assert_eq!(
            inspection_of(LEAF, &row).unwrap(),
            LeafInspection::PendingBlocked {
                leaf_name: LEAF.to_owned(),
                blocker_count: 2,
                eligible_at: expires_at(),
                attachment: LeafAttachment::DetachInterrupted,
            }
        );
    }

    #[test]
    fn the_four_timed_states_carry_their_expiry() {
        let name = || LEAF.to_owned();
        for (state, expected) in [
            (
                "DETACHED",
                LeafInspection::Detached {
                    leaf_name: name(),
                    eligible_at: expires_at(),
                },
            ),
            (
                "DETACH_INTERRUPTED",
                LeafInspection::DetachInterrupted {
                    leaf_name: name(),
                    eligible_at: expires_at(),
                },
            ),
            (
                "NOT_EXPIRED",
                LeafInspection::NotExpired {
                    leaf_name: name(),
                    eligible_at: expires_at(),
                },
            ),
            (
                "DETACHABLE",
                LeafInspection::Detachable {
                    leaf_name: name(),
                    eligible_at: expires_at(),
                },
            ),
        ] {
            assert_eq!(
                inspection_of(LEAF, &row(state)).unwrap(),
                expected,
                "{state}"
            );
        }
    }

    #[test]
    fn every_conflict_kind_of_the_inspection_maps_to_its_member() {
        for kind in [
            CatalogConflictKind::RelationWithoutCatalog,
            CatalogConflictKind::OpenLowerBound,
            CatalogConflictKind::DroppedRelationExists,
            CatalogConflictKind::ForeignParent,
            CatalogConflictKind::DetachRecordedWhileAttached,
            CatalogConflictKind::PhysicalNonconformant,
        ] {
            let row = LeafStateRow {
                conflict_kind: Some(kind.as_str().to_owned()),
                ..row("CATALOG_CONFLICT")
            };
            match inspection_of(LEAF, &row).unwrap() {
                LeafInspection::CatalogConflict { kind: found, .. } => assert_eq!(found, kind),
                other => panic!("{kind:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn an_unknown_state_is_a_contract_error() {
        assert!(contract_detail(inspection_of(LEAF, &row("RETIRED"))).contains("inspection state"));
    }

    #[test]
    fn an_unknown_or_absent_conflict_kind_is_a_contract_error() {
        for conflict_kind in [Some("SHADOWED"), Some("METADATA_MISMATCH"), None] {
            let row = LeafStateRow {
                conflict_kind: conflict_kind.map(str::to_owned),
                ..row("CATALOG_CONFLICT")
            };
            assert!(
                contract_detail(inspection_of(LEAF, &row)).contains("conflict_kind"),
                "{conflict_kind:?}"
            );
        }
    }

    #[test]
    fn a_blocker_count_that_is_not_positive_is_a_contract_error() {
        for count in [Some(0), Some(-1), None] {
            let row = LeafStateRow {
                blocker_count: count,
                attachment: Some("ATTACHED".to_owned()),
                ..row("PENDING_BLOCKED")
            };
            assert!(
                contract_detail(inspection_of(LEAF, &row)).contains("blocker_count"),
                "{count:?}"
            );
        }
    }

    #[test]
    fn an_unknown_attachment_is_a_contract_error() {
        let row = LeafStateRow {
            blocker_count: Some(1),
            attachment: Some("MISSING".to_owned()),
            ..row("PENDING_BLOCKED")
        };
        assert!(contract_detail(inspection_of(LEAF, &row)).contains("attachment"));
    }

    #[test]
    fn a_timed_state_without_its_expiry_is_a_contract_error() {
        let row = LeafStateRow {
            expires_at: None,
            ..row("DETACHABLE")
        };
        assert!(contract_detail(inspection_of(LEAF, &row)).contains("expires_at"));
    }

    #[test]
    fn begin_locks_only_the_two_detachable_states() {
        assert_eq!(begin_outcome(LEAF, row("BUSY")).unwrap(), BeginDetach::Busy);
        for state in ["DETACHABLE", "DETACH_INTERRUPTED"] {
            assert!(
                matches!(
                    begin_outcome(LEAF, row(state)).unwrap(),
                    BeginDetach::Locked { ref parent_name, .. }
                        if parent_name == "horsies_task_history_standard_30d"
                ),
                "{state}"
            );
        }
        for state in ["DETACHED", "NOT_EXPIRED", "ALREADY_DROPPED", "MISSING"] {
            assert!(
                matches!(
                    begin_outcome(LEAF, row(state)).unwrap(),
                    BeginDetach::Unlocked(_)
                ),
                "{state}"
            );
        }
        let without_parent = LeafStateRow {
            parent_name: None,
            ..row("DETACHABLE")
        };
        assert!(matches!(
            begin_outcome(LEAF, without_parent),
            Err(HistoryError::Contract(detail)) if detail.contains("no parent name")
        ));
    }

    #[test]
    fn a_ref_that_differs_from_the_catalog_row_is_named() {
        assert_eq!(catalog_facet_mismatch(&leaf(), &row("DETACHABLE")), None);
        let uncataloged = LeafStateRow {
            class_key: None,
            lower_anchor: None,
            upper_anchor: None,
            ..row("NOT_CATALOGED")
        };
        assert_eq!(catalog_facet_mismatch(&leaf(), &uncataloged), None);
        let other_class = LeafStateRow {
            class_key: Some("standard_7d".to_owned()),
            ..row("DETACHED")
        };
        let shifted = LeafStateRow {
            lower_anchor: Some(lower() - chrono::Duration::days(1)),
            upper_anchor: Some(lower()),
            ..row("DETACHED")
        };
        for mismatched in [other_class, shifted] {
            assert!(catalog_facet_mismatch(&leaf(), &mismatched)
                .is_some_and(|detail| detail.contains("catalog row has class")));
        }
    }

    #[test]
    fn the_record_of_a_completed_detach_maps_to_its_state() {
        for (record, expected) in [
            (DetachRecord::Recorded, RecordedDetach::Detached),
            (DetachRecord::AlreadyRecorded, RecordedDetach::Detached),
            (DetachRecord::Missing, RecordedDetach::Missing),
        ] {
            assert_eq!(recorded_detach(LEAF, record).unwrap(), expected);
        }
        for record in [DetachRecord::StillAttached, DetachRecord::NotCataloged] {
            assert!(matches!(
                recorded_detach(LEAF, record),
                Err(HistoryError::Contract(detail)) if detail.contains(record.as_str())
            ));
        }
        for record in [
            DetachRecord::Recorded,
            DetachRecord::AlreadyRecorded,
            DetachRecord::StillAttached,
            DetachRecord::Missing,
            DetachRecord::NotCataloged,
        ] {
            assert_eq!(DetachRecord::parse(LEAF, record.as_str()).unwrap(), record);
        }
        assert!(DetachRecord::parse(LEAF, "DETACHED").is_err());
    }

    #[test]
    fn the_drop_outcomes_map_to_their_variants() {
        let name = || LEAF.to_owned();
        assert_eq!(
            drop_outcome(LEAF, &row("DROPPED")).unwrap(),
            LeafDrop::Dropped { leaf_name: name() }
        );
        assert_eq!(
            drop_outcome(LEAF, &row("BUSY")).unwrap(),
            LeafDrop::Busy { leaf_name: name() }
        );
        assert_eq!(
            drop_outcome(LEAF, &row("REFERENCED_BY_MANIFEST")).unwrap(),
            LeafDrop::RefusedLoaderReferences { leaf_name: name() }
        );
        assert_eq!(
            drop_outcome(LEAF, &row("DETACHABLE")).unwrap(),
            LeafDrop::Inspection(LeafInspection::Detachable {
                leaf_name: name(),
                eligible_at: expires_at(),
            })
        );
    }

    #[test]
    fn candidates_beyond_the_bound_truncate_the_read() {
        let candidate = |day: i64, kind: &str, attachment: &str| CandidateRow {
            leaf_name: format!("horsies_task_history_standard_1d_2026_08_0{day}"),
            class_key: "standard_1d".to_owned(),
            class_kind: kind.to_owned(),
            lower_anchor: lower() + chrono::Duration::days(day),
            upper_anchor: lower() + chrono::Duration::days(day + 1),
            attachment: attachment.to_owned(),
        };
        let rows = || {
            vec![
                candidate(1, "PERIOD", "ATTACHED"),
                candidate(2, "HEARTBEAT", "DETACH_PENDING"),
                candidate(3, "PERIOD", "DETACHED"),
            ]
        };
        let truncated = prune_candidates_of(rows(), 2).unwrap();
        assert_eq!(truncated.reach, PruneReach::Truncated);
        assert_eq!(truncated.leaves.len(), 2);
        assert_eq!(
            truncated.leaves[1].attachment,
            CandidateAttachment::DetachPending
        );
        let within = prune_candidates_of(rows(), 3).unwrap();
        assert_eq!(within.reach, PruneReach::WithinBounds);
        assert_eq!(within.of_kind(RetiredClassKind::Period).len(), 2);
        assert_eq!(within.of_kind(RetiredClassKind::Heartbeat).len(), 1);
        for (kind, attachment) in [("FOREVER", "ATTACHED"), ("PERIOD", "GONE")] {
            assert!(matches!(
                prune_candidates_of(vec![candidate(1, kind, attachment)], 3),
                Err(HistoryError::Contract(_))
            ));
        }
    }

    /// The pass hands each sweep only the candidates of its kind, in
    /// candidate order: the period leaves to the history sweep, the
    /// heartbeat leaves to the heartbeat sweep.
    #[test]
    fn each_kind_keeps_its_leaves_in_candidate_order() {
        let candidate = |name: &str, kind: &str| CandidateRow {
            leaf_name: name.to_owned(),
            class_key: "standard_1d".to_owned(),
            class_kind: kind.to_owned(),
            lower_anchor: lower(),
            upper_anchor: lower() + chrono::Duration::days(1),
            attachment: "ATTACHED".to_owned(),
        };
        let candidates = prune_candidates_of(
            vec![
                candidate("history_a", "PERIOD"),
                candidate("heartbeat_a", "HEARTBEAT"),
                candidate("history_b", "PERIOD"),
            ],
            3,
        )
        .unwrap();
        let names = |leaves: Vec<LeafRef>| -> Vec<String> {
            leaves
                .iter()
                .map(|leaf| leaf.leaf_name().to_owned())
                .collect()
        };

        assert_eq!(
            names(candidates.of_kind(RetiredClassKind::Period)),
            vec!["history_a".to_owned(), "history_b".to_owned()]
        );
        assert_eq!(
            names(candidates.of_kind(RetiredClassKind::Heartbeat)),
            vec!["heartbeat_a".to_owned()]
        );
    }
}
