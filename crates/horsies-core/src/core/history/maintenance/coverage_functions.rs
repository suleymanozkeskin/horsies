//! Clients of the coverage functions of
//! `crates/horsies-core/baseline/0008_history_maintenance_functions.sql`: the
//! probe (`horsies_coverage_probe`), the class registration
//! (`horsies_register_retention_classes`) and the leaf creation
//! (`horsies_create_history_leaf`, `horsies_create_heartbeat_leaf`).
//!
//! Python: `python/horsies/core/history/maintenance/coverage.py`
//! (`_probe_complete_coverage`, `_coverage_probe_of`,
//! `_register_required_classes`, `leaf_creation_of`).

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Timelike, Utc};

use crate::broker::core_rows::{column, one_row, query_in, query_on, RowDecodeError, StatementLog};
use crate::core::history::commands::EnsureLeafCoverage;
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::ddl::runtime_names::leaf_id_index_name;
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{probe_index_name, EnsureHeartbeatCoverage};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, TASK_HISTORY_FOREVER_LEAF};
use crate::core::history::outcomes::{CatalogConflictKind, LeafCreation};
use crate::core::history::partitions::catalog::LeafLowerBound;
use crate::core::history::periods::RetentionDays;
use crate::db::{ParameterType, PooledConnection, Row, Transaction, TypedStatement};

use super::coverage::{registered_periods, HistoryCoverage};

/// `horsies_coverage_probe`: the expected leaves of the pass and their
/// state. `$1` the declared day counts, `$2` the history horizon in days,
/// `$3` the heartbeat horizon in hours. Reads only.
pub(super) static COVERAGE_PROBE: TypedStatement = TypedStatement::new(
    "coverage_probe",
    "SELECT checked_at, leaf_kind, class_key, leaf_name, lower_bound_kind, lower_anchor, \
            upper_bound_kind, upper_anchor, conformant, registration_healthy, \
            publication_healthy, absent_leaves \
     FROM horsies_coverage_probe($1, $2, $3)",
    &[
        ParameterType::Int2Array,
        ParameterType::Int2,
        ParameterType::Int2,
    ],
);

/// `horsies_register_retention_classes`: `$1` the declared day counts, `$2`
/// the heartbeat horizon in hours.
pub(super) static REGISTER_CLASSES: TypedStatement = TypedStatement::new(
    "register_retention_classes",
    "SELECT class_key, outcome FROM horsies_register_retention_classes($1, $2)",
    &[ParameterType::Int2Array, ParameterType::Int2],
);

/// `horsies_create_history_leaf`: `$1` the class key, `$2` the lower anchor
/// (`NULL` for the static forever leaf).
pub(super) static CREATE_HISTORY_LEAF: TypedStatement = TypedStatement::new(
    "create_history_leaf",
    "SELECT outcome, leaf_name, conflict_kind FROM horsies_create_history_leaf($1, $2)",
    &[ParameterType::Text, ParameterType::Timestamptz],
);

/// `horsies_create_heartbeat_leaf`: `$1` the lower anchor of the hour.
pub(super) static CREATE_HEARTBEAT_LEAF: TypedStatement = TypedStatement::new(
    "create_heartbeat_leaf",
    "SELECT outcome, leaf_name, conflict_kind FROM horsies_create_heartbeat_leaf($1)",
    &[ParameterType::Timestamptz],
);

/// The arguments of one coverage pass, as the functions take them. Only
/// [`CoverageRequest::new`] constructs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageRequest {
    /// The default period and every declared period, each once, in day order.
    declared_days: Vec<i16>,
    history_horizon_days: i16,
    heartbeat_horizon_hours: i16,
}

impl CoverageRequest {
    /// Refuses a horizon below two future leaves (the rule of
    /// `EnsureLeafCoverage` and `EnsureHeartbeatCoverage`) or one that does
    /// not fit `smallint`, before any statement. The functions refuse a
    /// history horizon above 14 days or a heartbeat horizon above 48 hours
    /// (`22023`).
    ///
    /// Errors: [`HistoryError::Contract`] naming the refused horizon.
    pub fn new(
        history_horizon_days: u32,
        heartbeat_horizon_hours: u32,
        declared_periods: &[RetentionDays],
    ) -> Result<Self, HistoryError> {
        EnsureLeafCoverage::new(FOREVER_CLASS_KEY, history_horizon_days)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        EnsureHeartbeatCoverage::new(heartbeat_horizon_hours)?;
        let history_horizon_days = i16::try_from(history_horizon_days).map_err(|_| {
            HistoryError::contract(format!(
                "history horizon of {history_horizon_days} days does not fit smallint"
            ))
        })?;
        let heartbeat_horizon_hours = i16::try_from(heartbeat_horizon_hours).map_err(|_| {
            HistoryError::contract(format!(
                "heartbeat horizon of {heartbeat_horizon_hours} hours does not fit smallint"
            ))
        })?;
        Ok(Self {
            declared_days: registered_periods(declared_periods)
                .into_iter()
                .map(RetentionDays::as_stored)
                .collect(),
            history_horizon_days,
            heartbeat_horizon_hours,
        })
    }
}

/// One expected history leaf that the probe found not conformant. The
/// static forever leaf has the `Open` lower bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DamagedHistoryLeaf {
    pub(crate) class_key: String,
    pub(crate) leaf_name: String,
    pub(crate) lower: LeafLowerBound,
}

/// One expected hourly heartbeat leaf that the probe found not conformant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DamagedHeartbeatLeaf {
    pub(crate) leaf_name: String,
    pub(crate) lower_anchor: DateTime<Utc>,
}

/// The rows of one `horsies_coverage_probe` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoverageProbe {
    /// In probe order: class key, then lower anchor (the open bound first).
    pub(crate) damaged_history: Vec<DamagedHistoryLeaf>,
    /// In probe order: lower anchor.
    pub(crate) damaged_heartbeats: Vec<DamagedHeartbeatLeaf>,
    pub(crate) registration_healthy: bool,
    pub(crate) publication_healthy: bool,
    /// A conformant heartbeat leaf covers the probe's `checked_at`.
    pub(crate) heartbeat_covered_now: bool,
    /// The earliest, over the history classes, of the upper bound of each
    /// class's newest expected leaf; `Forever` when only the forever leaf
    /// (open upper bound) is expected.
    pub(crate) history_covered_through: HistoryCoverage,
    /// The start of the UTC hour of `checked_at` plus the heartbeat horizon
    /// plus one hour: the end of the last hourly leaf the pass keeps.
    pub(crate) heartbeats_covered_through: DateTime<Utc>,
    pub(crate) absent_leaves: Vec<String>,
}

impl CoverageProbe {
    /// Both flags hold and every expected leaf is conformant.
    pub(crate) fn is_healthy(&self) -> bool {
        self.registration_healthy
            && self.publication_healthy
            && self.damaged_history.is_empty()
            && self.damaged_heartbeats.is_empty()
    }

    /// The class of the first damaged leaf: history first, then heartbeat.
    pub(crate) fn first_damaged_class_key(&self) -> Option<String> {
        match (
            self.damaged_history.first(),
            self.damaged_heartbeats.is_empty(),
        ) {
            (Some(leaf), _) => Some(leaf.class_key.clone()),
            (None, false) => Some(HEARTBEAT_CLASS_KEY.to_owned()),
            (None, true) => None,
        }
    }

    /// What the probe still finds wrong, for a failed pass that recorded no
    /// leaf failure.
    pub(crate) fn unresolved_detail(&self) -> String {
        let mut unresolved: Vec<String> = Vec::new();
        if !self.registration_healthy {
            unresolved.push("retention class registration".to_owned());
        }
        if !self.publication_healthy {
            unresolved.push("loader publication".to_owned());
        }
        unresolved.extend(
            self.damaged_history
                .iter()
                .map(|leaf| leaf.leaf_name.clone()),
        );
        unresolved.extend(
            self.damaged_heartbeats
                .iter()
                .map(|leaf| leaf.leaf_name.clone()),
        );
        format!("unresolved coverage state: {}", unresolved.join(", "))
    }
}

/// One row of `horsies_coverage_probe`, the columns the pass reads.
#[derive(Debug)]
struct CoverageProbeRow {
    checked_at: DateTime<Utc>,
    leaf_kind: String,
    class_key: String,
    leaf_name: String,
    lower_bound_kind: String,
    lower_anchor: Option<DateTime<Utc>>,
    upper_bound_kind: String,
    upper_anchor: Option<DateTime<Utc>>,
    conformant: bool,
    registration_healthy: bool,
    publication_healthy: bool,
    absent_leaves: Vec<String>,
}

/// One coverage probe statement and its rows, in autocommit on a checked
/// out connection. Reads only.
///
/// Errors: `HistoryError::Statement` for a failed or refused call (`22023`
/// for a horizon outside the function's bounds); `HistoryError::RowDecode`
/// for a column that does not decode; `HistoryError::Contract` for rows that
/// break the probe's contract.
pub(crate) async fn probe_coverage(
    connection: &mut PooledConnection,
    log: StatementLog,
    request: &CoverageRequest,
) -> Result<CoverageProbe, HistoryError> {
    let statement = &COVERAGE_PROBE;
    let rows = query_on(
        connection,
        log,
        &statement
            .bind()
            .value(&request.declared_days)
            .value(&request.history_horizon_days)
            .value(&request.heartbeat_horizon_hours)
            .finish(),
    )
    .await?;
    let rows = rows
        .iter()
        .map(coverage_probe_row)
        .collect::<Result<Vec<_>, RowDecodeError>>()?;
    coverage_probe_of(rows, request)
}

/// The columns of one probe row. Pure.
fn coverage_probe_row(row: &Row) -> Result<CoverageProbeRow, RowDecodeError> {
    let statement = &COVERAGE_PROBE;
    Ok(CoverageProbeRow {
        checked_at: column(row, statement, "checked_at")?,
        leaf_kind: column(row, statement, "leaf_kind")?,
        class_key: column(row, statement, "class_key")?,
        leaf_name: column(row, statement, "leaf_name")?,
        lower_bound_kind: column(row, statement, "lower_bound_kind")?,
        lower_anchor: column(row, statement, "lower_anchor")?,
        upper_bound_kind: column(row, statement, "upper_bound_kind")?,
        upper_anchor: column(row, statement, "upper_anchor")?,
        conformant: column(row, statement, "conformant")?,
        registration_healthy: column(row, statement, "registration_healthy")?,
        publication_healthy: column(row, statement, "publication_healthy")?,
        absent_leaves: column(row, statement, "absent_leaves")?,
    })
}

/// The bounds of one probe row. The forever leaf has OPEN lower and upper
/// bounds and no anchors; every other expected leaf is AT with both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeLeafBounds {
    Open,
    At {
        lower: DateTime<Utc>,
        upper: DateTime<Utc>,
    },
}

impl ProbeLeafBounds {
    /// The bounds of a row; another pair breaks the probe's contract.
    fn of(row: &CoverageProbeRow) -> Result<Self, HistoryError> {
        match (
            row.lower_bound_kind.as_str(),
            row.lower_anchor,
            row.upper_bound_kind.as_str(),
            row.upper_anchor,
        ) {
            ("OPEN", None, "OPEN", None) => Ok(Self::Open),
            ("AT", Some(lower), "AT", Some(upper)) => Ok(Self::At { lower, upper }),
            (lower_kind, lower_anchor, upper_kind, upper_anchor) => {
                Err(HistoryError::contract(format!(
                    "probe row {}: lower bound {lower_kind:?} {lower_anchor:?} and upper bound \
                     {upper_kind:?} {upper_anchor:?} are not a pair of the probe",
                    row.leaf_name
                )))
            }
        }
    }

    fn lower(self) -> LeafLowerBound {
        match self {
            Self::Open => LeafLowerBound::Open,
            Self::At { lower, .. } => LeafLowerBound::At(lower),
        }
    }
}

/// The probe of the rows of one coverage probe call, in probe order. Pure.
fn coverage_probe_of(
    rows: Vec<CoverageProbeRow>,
    request: &CoverageRequest,
) -> Result<CoverageProbe, HistoryError> {
    let Some(first) = rows.first() else {
        return Err(HistoryError::contract(
            "coverage probe returned no expected leaf",
        ));
    };
    let checked_at = first.checked_at;
    let registration_healthy = first.registration_healthy;
    let publication_healthy = first.publication_healthy;
    let absent_leaves = first.absent_leaves.clone();
    let mut damaged_history = Vec::new();
    let mut damaged_heartbeats = Vec::new();
    let mut heartbeat_covered_now = false;
    let mut history_rows: usize = 0;
    // The upper bound of each class's newest expected daily leaf.
    let mut newest_history_ends: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
    for row in rows {
        let bounds = ProbeLeafBounds::of(&row)?;
        match (row.leaf_kind.as_str(), row.conformant, bounds) {
            ("history", conformant, bounds) => {
                history_rows += 1;
                match bounds {
                    ProbeLeafBounds::Open => {}
                    ProbeLeafBounds::At { upper, .. } => {
                        let end = newest_history_ends
                            .entry(row.class_key.clone())
                            .or_insert(upper);
                        *end = (*end).max(upper);
                    }
                }
                match conformant {
                    true => {}
                    false => damaged_history.push(DamagedHistoryLeaf {
                        class_key: row.class_key,
                        leaf_name: row.leaf_name,
                        lower: bounds.lower(),
                    }),
                }
            }
            ("heartbeat", _, ProbeLeafBounds::Open) => {
                return Err(HistoryError::contract(format!(
                    "heartbeat leaf {} has an open bound",
                    row.leaf_name
                )));
            }
            ("heartbeat", conformant, ProbeLeafBounds::At { lower, upper }) => match conformant {
                true => {
                    heartbeat_covered_now |= lower <= row.checked_at && row.checked_at < upper;
                }
                false => damaged_heartbeats.push(DamagedHeartbeatLeaf {
                    leaf_name: row.leaf_name,
                    lower_anchor: lower,
                }),
            },
            (other, _, _) => {
                return Err(HistoryError::contract(format!(
                    "coverage probe returned unknown leaf kind {other:?}"
                )));
            }
        }
    }
    if history_rows == 0 {
        return Err(HistoryError::contract(
            "coverage probe returned no history leaf",
        ));
    }
    let history_covered_through = match newest_history_ends.values().min() {
        Some(earliest) => HistoryCoverage::Until { at: *earliest },
        None => HistoryCoverage::Forever,
    };
    let hour_start = checked_at
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("probe timestamp cannot be truncated"))?;
    Ok(CoverageProbe {
        damaged_history,
        damaged_heartbeats,
        registration_healthy,
        publication_healthy,
        heartbeat_covered_now,
        history_covered_through,
        heartbeats_covered_through: hour_start
            + Duration::hours(i64::from(request.heartbeat_horizon_hours) + 1),
        absent_leaves,
    })
}

/// Registers the default, declared and heartbeat classes in the caller's
/// transaction: one call of `horsies_register_retention_classes`. Each class
/// answers REGISTERED, VERIFIED or (heartbeat) HORIZON_UPDATED; the pass
/// reads the result back through the probe's `registration_healthy`.
///
/// Errors: `HistoryError::Statement` for a failed or refused call;
/// `HistoryError::RowDecode` for a column that does not decode;
/// `HistoryError::Contract` for another outcome. A failure leaves the
/// caller's transaction to roll back.
pub(crate) async fn register_retention_classes(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    request: &CoverageRequest,
) -> Result<(), HistoryError> {
    let statement = &REGISTER_CLASSES;
    let rows = query_in(
        transaction,
        log,
        &statement
            .bind()
            .value(&request.declared_days)
            .value(&request.heartbeat_horizon_hours)
            .finish(),
    )
    .await?;
    for row in &rows {
        let class_key: String = column(row, statement, "class_key")?;
        let outcome: String = column(row, statement, "outcome")?;
        match outcome.as_str() {
            "REGISTERED" | "VERIFIED" | "HORIZON_UPDATED" => {}
            other => {
                return Err(HistoryError::contract(format!(
                    "class registration of {class_key:?} returned {other:?}"
                )));
            }
        }
    }
    Ok(())
}

/// One row of a leaf-creation function.
#[derive(Debug)]
struct LeafCreationRow {
    outcome: String,
    leaf_name: Option<String>,
    conflict_kind: Option<String>,
}

/// Which index of a created or repaired leaf names it in the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreatedLeafKind {
    /// The task-ID index `<leaf>_task_idx`.
    History,
    /// The probe index `<leaf>_probe_idx`.
    Heartbeat,
}

/// Creates, verifies or repairs one expected history leaf in the caller's
/// transaction: one call of `horsies_create_history_leaf`. A created leaf
/// is published inside the call. The static forever leaf (`Open` lower
/// bound) is verified, never created or repaired.
///
/// Errors: `HistoryError::Statement` for a failed or refused call (`22023`,
/// `25000`, `42P17`); `HistoryError::RowDecode` for no row or a column that
/// does not decode; `HistoryError::Contract` for a row outside the
/// function's outcomes.
pub(crate) async fn create_history_leaf(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    leaf: &DamagedHistoryLeaf,
) -> Result<LeafCreation, HistoryError> {
    let lower_anchor: Option<DateTime<Utc>> = match leaf.lower {
        LeafLowerBound::Open => None,
        LeafLowerBound::At(anchor) => Some(anchor),
    };
    let statement = &CREATE_HISTORY_LEAF;
    let rows = query_in(
        transaction,
        log,
        &statement
            .bind()
            .value(&leaf.class_key)
            .value(&lower_anchor)
            .finish(),
    )
    .await?;
    let row = leaf_creation_row(&one_row(rows, statement)?, statement)?;
    leaf_creation_of(CreatedLeafKind::History, &leaf.class_key, row)
}

/// Creates, verifies or repairs one expected hourly heartbeat leaf in the
/// caller's transaction: one call of `horsies_create_heartbeat_leaf`.
///
/// Errors: as [`create_history_leaf`].
pub(crate) async fn create_heartbeat_leaf(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    leaf: &DamagedHeartbeatLeaf,
) -> Result<LeafCreation, HistoryError> {
    let statement = &CREATE_HEARTBEAT_LEAF;
    let rows = query_in(
        transaction,
        log,
        &statement.bind().value(&leaf.lower_anchor).finish(),
    )
    .await?;
    let row = leaf_creation_row(&one_row(rows, statement)?, statement)?;
    leaf_creation_of(CreatedLeafKind::Heartbeat, HEARTBEAT_CLASS_KEY, row)
}

/// The columns of one leaf-creation row of `statement`. Pure.
fn leaf_creation_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<LeafCreationRow, RowDecodeError> {
    Ok(LeafCreationRow {
        outcome: column(row, statement, "outcome")?,
        leaf_name: column(row, statement, "leaf_name")?,
        conflict_kind: column(row, statement, "conflict_kind")?,
    })
}

/// The outcome of one leaf-creation row. Pure. An outcome outside the
/// function's set, a missing leaf name, or an unknown conflict kind is
/// `HistoryError::Contract`.
fn leaf_creation_of(
    kind: CreatedLeafKind,
    class_key: &str,
    row: LeafCreationRow,
) -> Result<LeafCreation, HistoryError> {
    let LeafCreationRow {
        outcome,
        leaf_name,
        conflict_kind,
    } = row;
    match (outcome.as_str(), leaf_name) {
        ("CLASS_ABSENT", _) => Ok(LeafCreation::RetentionClassAbsent {
            class_key: class_key.to_owned(),
        }),
        (outcome, None) => Err(HistoryError::contract(format!(
            "leaf creation of class {class_key:?} returned {outcome:?} with no leaf name"
        ))),
        ("CREATED", Some(leaf_name)) => Ok(LeafCreation::Created {
            id_index_name: id_index_name(kind, &leaf_name)?,
            leaf_name,
        }),
        ("INDEX_REPAIRED", Some(leaf_name)) => Ok(LeafCreation::IndexRepaired {
            id_index_name: id_index_name(kind, &leaf_name)?,
            leaf_name,
        }),
        ("ALREADY_CONFORMANT", Some(leaf_name)) => {
            Ok(LeafCreation::AlreadyConformant { leaf_name })
        }
        ("COVERED_BY_OPEN_ENDED", Some(leaf_name)) => Ok(LeafCreation::CoveredByOpenEndedLeaf {
            leaf_name,
            open_leaf_name: TASK_HISTORY_FOREVER_LEAF.to_owned(),
        }),
        ("BUSY", Some(leaf_name)) => Ok(LeafCreation::Busy { leaf_name }),
        ("CATALOG_CONFLICT", Some(leaf_name)) => {
            let Some(conflict_kind) = conflict_kind else {
                return Err(HistoryError::contract(format!(
                    "leaf {leaf_name}: CATALOG_CONFLICT with no conflict kind"
                )));
            };
            let kind = CatalogConflictKind::parse(&conflict_kind)?;
            Ok(LeafCreation::CatalogConflict {
                detail: format!("leaf creation found {}", kind.as_str()),
                leaf_name,
                kind,
            })
        }
        (outcome, Some(leaf_name)) => Err(HistoryError::contract(format!(
            "leaf creation of {leaf_name} returned {outcome:?}"
        ))),
    }
}

fn id_index_name(kind: CreatedLeafKind, leaf_name: &str) -> Result<String, HistoryError> {
    match kind {
        CreatedLeafKind::History => Ok(leaf_id_index_name(leaf_name)),
        CreatedLeafKind::Heartbeat => probe_index_name(leaf_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(outcome: &str, leaf_name: Option<&str>, conflict_kind: Option<&str>) -> LeafCreationRow {
        LeafCreationRow {
            outcome: outcome.to_owned(),
            leaf_name: leaf_name.map(str::to_owned),
            conflict_kind: conflict_kind.map(str::to_owned),
        }
    }

    const LEAF: &str = "horsies_task_history_standard_30d_2026_11_17";
    const HEARTBEAT_LEAF: &str = "horsies_heartbeats_2026_11_17_13";

    /// Python `tests/unit/test_coverage_function_rows.py`: every outcome of
    /// the leaf-creation functions maps onto its `LeafCreation`.
    #[test]
    fn every_leaf_creation_outcome_maps_onto_its_variant() {
        let history = |outcome: &str, conflict: Option<&str>| {
            leaf_creation_of(
                CreatedLeafKind::History,
                "standard_30d",
                row(outcome, Some(LEAF), conflict),
            )
            .unwrap()
        };
        assert_eq!(
            history("CREATED", None),
            LeafCreation::Created {
                leaf_name: LEAF.to_owned(),
                id_index_name: format!("{LEAF}_task_idx"),
            }
        );
        assert_eq!(
            history("INDEX_REPAIRED", None),
            LeafCreation::IndexRepaired {
                leaf_name: LEAF.to_owned(),
                id_index_name: format!("{LEAF}_task_idx"),
            }
        );
        assert_eq!(
            history("ALREADY_CONFORMANT", None),
            LeafCreation::AlreadyConformant {
                leaf_name: LEAF.to_owned(),
            }
        );
        assert_eq!(
            history("COVERED_BY_OPEN_ENDED", None),
            LeafCreation::CoveredByOpenEndedLeaf {
                leaf_name: LEAF.to_owned(),
                open_leaf_name: TASK_HISTORY_FOREVER_LEAF.to_owned(),
            }
        );
        assert_eq!(
            history("BUSY", None),
            LeafCreation::Busy {
                leaf_name: LEAF.to_owned(),
            }
        );
        assert_eq!(
            history("CATALOG_CONFLICT", Some("CATALOG_WITHOUT_RELATION")),
            LeafCreation::CatalogConflict {
                leaf_name: LEAF.to_owned(),
                kind: CatalogConflictKind::CatalogWithoutRelation,
                detail: "leaf creation found CATALOG_WITHOUT_RELATION".to_owned(),
            }
        );
        assert_eq!(
            leaf_creation_of(
                CreatedLeafKind::History,
                "standard_30d",
                row("CLASS_ABSENT", None, None)
            )
            .unwrap(),
            LeafCreation::RetentionClassAbsent {
                class_key: "standard_30d".to_owned(),
            }
        );
        assert_eq!(
            leaf_creation_of(
                CreatedLeafKind::Heartbeat,
                HEARTBEAT_CLASS_KEY,
                row("CREATED", Some(HEARTBEAT_LEAF), None)
            )
            .unwrap(),
            LeafCreation::Created {
                leaf_name: HEARTBEAT_LEAF.to_owned(),
                id_index_name: format!("{HEARTBEAT_LEAF}_probe_idx"),
            }
        );
    }

    /// An outcome outside the set, a missing leaf name, and a missing or
    /// unknown conflict kind are contract errors.
    #[test]
    fn leaf_creation_rows_outside_the_contract_are_refused() {
        for refused in [
            row("DROPPED", Some(LEAF), None),
            row("CREATED", None, None),
            row("CATALOG_CONFLICT", Some(LEAF), None),
            row("CATALOG_CONFLICT", Some(LEAF), Some("SOMETHING_ELSE")),
        ] {
            assert!(matches!(
                leaf_creation_of(CreatedLeafKind::History, "standard_30d", refused),
                Err(HistoryError::Contract(_))
            ));
        }
    }

    /// One probe row of `class_key`. A missing anchor is an OPEN bound.
    fn probe_row(
        leaf_kind: &str,
        class_key: &str,
        leaf_name: &str,
        lower: Option<DateTime<Utc>>,
        upper: Option<DateTime<Utc>>,
        conformant: bool,
    ) -> CoverageProbeRow {
        let kind = |anchor: Option<DateTime<Utc>>| match anchor {
            Some(_) => "AT".to_owned(),
            None => "OPEN".to_owned(),
        };
        CoverageProbeRow {
            checked_at: at("2026-11-17T13:20:00Z"),
            leaf_kind: leaf_kind.to_owned(),
            class_key: class_key.to_owned(),
            leaf_name: leaf_name.to_owned(),
            lower_bound_kind: kind(lower),
            lower_anchor: lower,
            upper_bound_kind: kind(upper),
            upper_anchor: upper,
            conformant,
            registration_healthy: true,
            publication_healthy: true,
            absent_leaves: Vec::new(),
        }
    }

    fn at(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn heartbeat_row(hour: u32, conformant: bool) -> CoverageProbeRow {
        let lower = at(&format!("2026-11-17T{hour:02}:00:00Z"));
        probe_row(
            "heartbeat",
            HEARTBEAT_CLASS_KEY,
            &format!("horsies_heartbeats_2026_11_17_{hour:02}"),
            Some(lower),
            Some(lower + Duration::hours(1)),
            conformant,
        )
    }

    fn forever_row(conformant: bool) -> CoverageProbeRow {
        probe_row(
            "history",
            FOREVER_CLASS_KEY,
            TASK_HISTORY_FOREVER_LEAF,
            None,
            None,
            conformant,
        )
    }

    fn daily_row(class_key: &str, day: u32, conformant: bool) -> CoverageProbeRow {
        let lower = at(&format!("2026-11-{day:02}T00:00:00Z"));
        probe_row(
            "history",
            class_key,
            &format!("horsies_task_history_{class_key}_2026_11_{day:02}"),
            Some(lower),
            Some(lower + Duration::days(1)),
            conformant,
        )
    }

    /// The probe rows decode into the damaged leaves in probe order, the
    /// current-hour heartbeat verdict and the covered-through instants; a
    /// probe with no row is refused.
    #[test]
    fn probe_rows_decode_damage_and_coverage() {
        let request = CoverageRequest::new(2, 2, &[]).unwrap();
        let rows = vec![
            heartbeat_row(13, true),
            heartbeat_row(14, false),
            forever_row(true),
            daily_row("standard_30d", 17, false),
        ];
        let probe = coverage_probe_of(rows, &request).unwrap();
        assert!(!probe.is_healthy());
        assert!(probe.heartbeat_covered_now);
        assert_eq!(
            probe.damaged_history,
            vec![DamagedHistoryLeaf {
                class_key: "standard_30d".to_owned(),
                leaf_name: LEAF.to_owned(),
                lower: LeafLowerBound::At(at("2026-11-17T00:00:00Z")),
            }]
        );
        assert_eq!(
            probe.damaged_heartbeats,
            vec![DamagedHeartbeatLeaf {
                leaf_name: "horsies_heartbeats_2026_11_17_14".to_owned(),
                lower_anchor: at("2026-11-17T14:00:00Z"),
            }]
        );
        assert_eq!(
            probe.first_damaged_class_key().as_deref(),
            Some("standard_30d")
        );
        assert_eq!(
            probe.history_covered_through,
            HistoryCoverage::Until {
                at: at("2026-11-18T00:00:00Z")
            }
        );
        assert_eq!(probe.heartbeats_covered_through, at("2026-11-17T16:00:00Z"));
        assert!(matches!(
            coverage_probe_of(Vec::new(), &request),
            Err(HistoryError::Contract(_))
        ));
    }

    /// Python `test_coverage_function_rows.py::TestCoverageProbeRows`: each
    /// class reaches the end of its newest leaf, and the probe reports the
    /// earliest; the forever leaf has no end and does not move it.
    #[test]
    fn history_coverage_is_the_earliest_newest_end_over_the_classes() {
        let request = CoverageRequest::new(2, 2, &[]).unwrap();
        let rows = vec![
            heartbeat_row(13, true),
            forever_row(true),
            daily_row("standard_30d", 17, true),
            daily_row("standard_30d", 18, true),
            daily_row("standard_30d", 19, false),
            daily_row("standard_7d", 17, true),
            daily_row("standard_7d", 18, true),
        ];
        let probe = coverage_probe_of(rows, &request).unwrap();
        assert_eq!(
            probe.history_covered_through,
            HistoryCoverage::Until {
                at: at("2026-11-19T00:00:00Z")
            }
        );
    }

    /// A fresh install has no period class yet: the forever leaf is the
    /// only history row, and the history coverage has no end. The forever
    /// row has OPEN bounds and no anchors.
    #[test]
    fn history_coverage_is_forever_when_only_the_forever_leaf_is_expected() {
        let request = CoverageRequest::new(2, 2, &[]).unwrap();
        let probe =
            coverage_probe_of(vec![heartbeat_row(13, true), forever_row(false)], &request).unwrap();
        assert_eq!(probe.history_covered_through, HistoryCoverage::Forever);
        assert_eq!(
            probe.damaged_history,
            vec![DamagedHistoryLeaf {
                class_key: FOREVER_CLASS_KEY.to_owned(),
                leaf_name: TASK_HISTORY_FOREVER_LEAF.to_owned(),
                lower: LeafLowerBound::Open,
            }]
        );
    }

    /// A row whose bound kinds and anchors are not a pair of the probe, a
    /// heartbeat row with open bounds, and a probe with no history row are
    /// contract errors.
    #[test]
    fn probe_rows_outside_the_contract_are_refused() {
        let request = CoverageRequest::new(2, 2, &[]).unwrap();
        let mut open_upper = daily_row("standard_30d", 17, true);
        open_upper.upper_bound_kind = "OPEN".to_owned();
        open_upper.upper_anchor = None;
        let mut open_lower = daily_row("standard_30d", 17, true);
        open_lower.lower_bound_kind = "OPEN".to_owned();
        open_lower.lower_anchor = None;
        let mut anchored_forever = forever_row(true);
        anchored_forever.upper_anchor = Some(at("9999-01-01T00:00:00Z"));
        let mut open_heartbeat = heartbeat_row(13, true);
        open_heartbeat.lower_bound_kind = "OPEN".to_owned();
        open_heartbeat.lower_anchor = None;
        open_heartbeat.upper_bound_kind = "OPEN".to_owned();
        open_heartbeat.upper_anchor = None;
        for refused in [
            vec![heartbeat_row(13, true), open_upper],
            vec![heartbeat_row(13, true), open_lower],
            vec![heartbeat_row(13, true), anchored_forever],
            vec![open_heartbeat, forever_row(true)],
            vec![heartbeat_row(13, true)],
        ] {
            assert!(matches!(
                coverage_probe_of(refused, &request),
                Err(HistoryError::Contract(_))
            ));
        }
    }

    /// The health payload form of the history coverage, as Python's
    /// `history_coverage_payload` writes it.
    #[test]
    fn history_coverage_serializes_as_the_health_payload() {
        assert_eq!(
            serde_json::to_value(HistoryCoverage::Forever).unwrap(),
            serde_json::json!({"kind": "FOREVER"})
        );
        assert_eq!(
            serde_json::to_value(HistoryCoverage::Until {
                at: at("2026-11-19T00:00:00Z")
            })
            .unwrap(),
            serde_json::json!({"kind": "UNTIL", "at": "2026-11-19T00:00:00Z"})
        );
    }

    /// The declared periods reach the functions as day counts, with the
    /// default period, each once, in day order; a horizon below two leaves is
    /// refused before any statement.
    #[test]
    fn the_request_carries_unique_day_counts_and_refuses_short_horizons() {
        let seven = RetentionDays::parse_period(Duration::days(7)).unwrap();
        let request = CoverageRequest::new(3, 6, &[seven, seven]).unwrap();
        assert_eq!(request.declared_days, vec![7, 30]);
        assert_eq!(request.history_horizon_days, 3);
        assert_eq!(request.heartbeat_horizon_hours, 6);
        assert!(CoverageRequest::new(1, 6, &[]).is_err());
        assert!(CoverageRequest::new(3, 1, &[]).is_err());
    }
}
