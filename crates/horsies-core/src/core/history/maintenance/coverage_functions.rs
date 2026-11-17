//! Clients of the coverage functions of
//! `horsies/core/baseline/sql/0008_history_maintenance_functions.sql`: the
//! probe (`horsies_coverage_probe`), the class registration
//! (`horsies_register_retention_classes`) and the leaf creation
//! (`horsies_create_history_leaf`, `horsies_create_heartbeat_leaf`).
//!
//! Python: `horsies/core/history/maintenance/coverage.py`
//! (`_probe_complete_coverage`, `_coverage_probe_of`,
//! `_register_required_classes`, `leaf_creation_of`).

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::{FromRow, PgConnection};

use crate::core::history::commands::EnsureLeafCoverage;
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::ddl::runtime_names::leaf_id_index_name;
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{probe_index_name, EnsureHeartbeatCoverage};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, TASK_HISTORY_FOREVER_LEAF};
use crate::core::history::outcomes::{CatalogConflictKind, LeafCreation};
use crate::core::history::partitions::catalog::LeafLowerBound;
use crate::core::history::periods::RetentionDays;

use super::coverage::registered_periods;

const COVERAGE_PROBE_SQL: &str = "SELECT checked_at, leaf_kind, class_key, leaf_name, \
     lower_bound_kind, lower_anchor, upper_anchor, conformant, registration_healthy, \
     publication_healthy, absent_leaves \
     FROM horsies_coverage_probe($1::smallint[], $2::smallint, $3::smallint)";

const REGISTER_CLASSES_SQL: &str = "SELECT class_key, outcome \
     FROM horsies_register_retention_classes($1::smallint[], $2::smallint)";

const CREATE_HISTORY_LEAF_SQL: &str =
    "SELECT outcome, leaf_name, conflict_kind FROM horsies_create_history_leaf($1, $2)";

const CREATE_HEARTBEAT_LEAF_SQL: &str =
    "SELECT outcome, leaf_name, conflict_kind FROM horsies_create_heartbeat_leaf($1)";

/// The arguments of one coverage pass, as the functions take them. Only
/// [`CoverageRequest::new`] constructs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoverageRequest {
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
    pub(crate) fn new(
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
    /// The start of the UTC day of `checked_at` plus the history horizon
    /// plus one day: the end of the last daily leaf the pass keeps.
    pub(crate) history_covered_through: DateTime<Utc>,
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

#[derive(Debug, FromRow)]
struct CoverageProbeRow {
    checked_at: DateTime<Utc>,
    leaf_kind: String,
    class_key: String,
    leaf_name: String,
    lower_bound_kind: String,
    lower_anchor: Option<DateTime<Utc>>,
    upper_anchor: DateTime<Utc>,
    conformant: bool,
    registration_healthy: bool,
    publication_healthy: bool,
    absent_leaves: Vec<String>,
}

/// One coverage probe statement and its rows. Reads only.
///
/// Errors: `HistoryError::Database` for a refused call (`22023` for a
/// horizon outside the function's bounds) or a row that does not decode;
/// `HistoryError::Contract` for rows that break the probe's contract.
pub(crate) async fn probe_coverage(
    connection: &mut PgConnection,
    request: &CoverageRequest,
) -> Result<CoverageProbe, HistoryError> {
    let rows: Vec<CoverageProbeRow> = sqlx::query_as(COVERAGE_PROBE_SQL)
        .bind(request.declared_days.as_slice())
        .bind(request.history_horizon_days)
        .bind(request.heartbeat_horizon_hours)
        .fetch_all(connection)
        .await?;
    coverage_probe_of(rows, request)
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
    for row in rows {
        let lower = LeafLowerBound::decode(&row.lower_bound_kind, row.lower_anchor)?;
        match (row.leaf_kind.as_str(), row.conformant) {
            ("history", true) => {}
            ("history", false) => damaged_history.push(DamagedHistoryLeaf {
                class_key: row.class_key,
                leaf_name: row.leaf_name,
                lower,
            }),
            ("heartbeat", conformant) => {
                let LeafLowerBound::At(lower_anchor) = lower else {
                    return Err(HistoryError::contract(format!(
                        "heartbeat leaf {} has an open lower bound",
                        row.leaf_name
                    )));
                };
                match conformant {
                    true => {
                        heartbeat_covered_now |=
                            lower_anchor <= row.checked_at && row.checked_at < row.upper_anchor;
                    }
                    false => damaged_heartbeats.push(DamagedHeartbeatLeaf {
                        leaf_name: row.leaf_name,
                        lower_anchor,
                    }),
                }
            }
            (other, _) => {
                return Err(HistoryError::contract(format!(
                    "coverage probe returned unknown leaf kind {other:?}"
                )));
            }
        }
    }
    let day_start = checked_at
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("probe timestamp cannot be truncated"))?;
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
        history_covered_through: day_start
            + Duration::days(i64::from(request.history_horizon_days) + 1),
        heartbeats_covered_through: hour_start
            + Duration::hours(i64::from(request.heartbeat_horizon_hours) + 1),
        absent_leaves,
    })
}

#[derive(Debug, FromRow)]
struct RegistrationRow {
    class_key: String,
    outcome: String,
}

/// Registers the default, declared and heartbeat classes in the caller's
/// transaction: one call of `horsies_register_retention_classes`. Each class
/// answers REGISTERED, VERIFIED or (heartbeat) HORIZON_UPDATED; the pass
/// reads the result back through the probe's `registration_healthy`.
///
/// Errors: `HistoryError::Database` for a refused call; `HistoryError::Contract`
/// for another outcome. A failure leaves the caller's transaction to roll
/// back.
pub(crate) async fn register_retention_classes(
    connection: &mut PgConnection,
    request: &CoverageRequest,
) -> Result<(), HistoryError> {
    let rows: Vec<RegistrationRow> = sqlx::query_as(REGISTER_CLASSES_SQL)
        .bind(request.declared_days.as_slice())
        .bind(request.heartbeat_horizon_hours)
        .fetch_all(connection)
        .await?;
    for row in rows {
        match row.outcome.as_str() {
            "REGISTERED" | "VERIFIED" | "HORIZON_UPDATED" => {}
            other => {
                return Err(HistoryError::contract(format!(
                    "class registration of {:?} returned {other:?}",
                    row.class_key
                )));
            }
        }
    }
    Ok(())
}

#[derive(Debug, FromRow)]
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
/// Errors: `HistoryError::Database` for a refused call (`22023`, `25000`,
/// `42P17`); `HistoryError::Contract` for a row outside the function's
/// outcomes.
pub(crate) async fn create_history_leaf(
    connection: &mut PgConnection,
    leaf: &DamagedHistoryLeaf,
) -> Result<LeafCreation, HistoryError> {
    let lower_anchor: Option<DateTime<Utc>> = match leaf.lower {
        LeafLowerBound::Open => None,
        LeafLowerBound::At(anchor) => Some(anchor),
    };
    let row: LeafCreationRow = sqlx::query_as(CREATE_HISTORY_LEAF_SQL)
        .bind(&leaf.class_key)
        .bind(lower_anchor)
        .fetch_one(connection)
        .await?;
    leaf_creation_of(CreatedLeafKind::History, &leaf.class_key, row)
}

/// Creates, verifies or repairs one expected hourly heartbeat leaf in the
/// caller's transaction: one call of `horsies_create_heartbeat_leaf`.
///
/// Errors: as [`create_history_leaf`].
pub(crate) async fn create_heartbeat_leaf(
    connection: &mut PgConnection,
    leaf: &DamagedHeartbeatLeaf,
) -> Result<LeafCreation, HistoryError> {
    let row: LeafCreationRow = sqlx::query_as(CREATE_HEARTBEAT_LEAF_SQL)
        .bind(leaf.lower_anchor)
        .fetch_one(connection)
        .await?;
    leaf_creation_of(CreatedLeafKind::Heartbeat, HEARTBEAT_CLASS_KEY, row)
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

    fn probe_row(
        leaf_kind: &str,
        leaf_name: &str,
        lower: Option<DateTime<Utc>>,
        upper: DateTime<Utc>,
        conformant: bool,
    ) -> CoverageProbeRow {
        CoverageProbeRow {
            checked_at: DateTime::parse_from_rfc3339("2026-11-17T13:20:00Z")
                .unwrap()
                .with_timezone(&Utc),
            leaf_kind: leaf_kind.to_owned(),
            class_key: match leaf_kind {
                "heartbeat" => HEARTBEAT_CLASS_KEY.to_owned(),
                _ => "standard_30d".to_owned(),
            },
            leaf_name: leaf_name.to_owned(),
            lower_bound_kind: match lower {
                Some(_) => "AT".to_owned(),
                None => "OPEN".to_owned(),
            },
            lower_anchor: lower,
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

    /// The probe rows decode into the damaged leaves in probe order, the
    /// current-hour heartbeat verdict and the covered-through instants of the
    /// horizons; a probe with no row is refused.
    #[test]
    fn probe_rows_decode_damage_and_coverage() {
        let request = CoverageRequest::new(2, 2, &[]).unwrap();
        let rows = vec![
            probe_row(
                "heartbeat",
                "horsies_heartbeats_2026_11_17_13",
                Some(at("2026-11-17T13:00:00Z")),
                at("2026-11-17T14:00:00Z"),
                true,
            ),
            probe_row(
                "heartbeat",
                "horsies_heartbeats_2026_11_17_14",
                Some(at("2026-11-17T14:00:00Z")),
                at("2026-11-17T15:00:00Z"),
                false,
            ),
            probe_row(
                "history",
                "horsies_task_history_forever_open",
                None,
                at("9999-01-01T00:00:00Z"),
                true,
            ),
            probe_row(
                "history",
                LEAF,
                Some(at("2026-11-17T00:00:00Z")),
                at("2026-11-18T00:00:00Z"),
                false,
            ),
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
        assert_eq!(probe.history_covered_through, at("2026-11-20T00:00:00Z"));
        assert_eq!(probe.heartbeats_covered_through, at("2026-11-17T16:00:00Z"));
        assert!(matches!(
            coverage_probe_of(Vec::new(), &request),
            Err(HistoryError::Contract(_))
        ));
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
