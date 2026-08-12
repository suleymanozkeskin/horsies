//! What a terminalization operation reports back, decoded in one place.
//!
//! Outcomes are data. A guarded transition that matches nothing has not
//! failed — it has learned something about the row, and the caller needs to
//! know which thing.
//!
//! Every operation returns the same row shape, so one decoder serves all of
//! them. The row carries what the caller could not already know: what the
//! database assigned, and what it saw under the lock at the moment it
//! decided.
//!
//! Decoding fails closed. An unknown discriminant, a missing column, an
//! unknown key in a diagnostic payload — each means the database and this
//! code disagree about the contract, which is infrastructure failure rather
//! than a task outcome, and it errors.

use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{Column, Row};
use uuid::Uuid;

use crate::core::types::status::TaskStatus;

use super::operations::TerminalizationKind;

/// The returned row does not satisfy the wire contract.
///
/// Never produced for a task outcome — only when the database returned
/// something this code cannot interpret, which means one side is running
/// against a contract the other does not implement.
#[derive(Debug, thiserror::Error)]
#[error("terminalization outcome decode failed: {0}")]
pub struct OutcomeDecodeError(pub String);

/// Which guard's evidence a diagnostic payload carries.
///
/// Absent (a NULL column) means the evidence is claim-shaped and lives in
/// the uniform observed columns rather than in a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardKind {
    Deadline,
    Staleness,
    WorkflowStatus,
    WorkflowLinkAbsent,
    WorkflowLinkState,
    ForeignTerminalization,
}

impl GuardKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deadline => "DEADLINE",
            Self::Staleness => "STALENESS",
            Self::WorkflowStatus => "WORKFLOW_STATUS",
            Self::WorkflowLinkAbsent => "WORKFLOW_LINK_ABSENT",
            Self::WorkflowLinkState => "WORKFLOW_LINK_STATE",
            Self::ForeignTerminalization => "FOREIGN_TERMINALIZATION",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "DEADLINE" => Some(Self::Deadline),
            "STALENESS" => Some(Self::Staleness),
            "WORKFLOW_STATUS" => Some(Self::WorkflowStatus),
            "WORKFLOW_LINK_ABSENT" => Some(Self::WorkflowLinkAbsent),
            "WORKFLOW_LINK_STATE" => Some(Self::WorkflowLinkState),
            "FOREIGN_TERMINALIZATION" => Some(Self::ForeignTerminalization),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Observed evidence
// ---------------------------------------------------------------------------

/// The locked pre-transition image, for every outcome alike.
///
/// On an applied transition this is what the guarded update matched; on a
/// refusal it is what it found instead. One rule rather than a per-outcome
/// convention, so a log line means the same thing wherever it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedTaskState {
    pub status: Option<TaskStatus>,
    pub worker_id: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
}

/// Claim-shaped evidence: the fence could not match this row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedClaim {
    pub worker_id: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
}

/// A deadline guard's evidence, as the database evaluated it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedDeadline {
    pub good_until: Option<DateTime<Utc>>,
    pub evaluated_at: DateTime<Utc>,
}

/// A staleness guard's evidence, captured in the snapshot that judged it.
///
/// Every value the two arms compared travels together with the instant they
/// were compared at, so both comparisons are reconstructible from the log
/// exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedStaleness {
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finalizing_at: Option<DateTime<Utc>>,
    pub stale_after_ms: i64,
    pub finalizing_stale_after_ms: i64,
    pub evaluated_at: DateTime<Utc>,
}

/// A workflow-status guard's evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedWorkflowState {
    pub workflow_id: String,
    pub workflow_status: String,
}

/// A workflow-link guard's evidence; `None` means the link is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedWorkflowLink {
    pub node_status: Option<String>,
}

/// The row is terminal, but another operation put it there.
///
/// Claim-shaped evidence would be all-NULL on a terminal row — precisely
/// where the log has to name who won. A committed kind of `None` is a row
/// written before the kind column existed: unknown provenance, never
/// inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedForeignTerminalization {
    pub observed_status: TaskStatus,
    pub committed_kind: Option<TerminalizationKind>,
    pub terminal_at: Option<DateTime<Utc>>,
}

/// The typed diagnostic variant carried by a conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardEvidence {
    Claim(ObservedClaim),
    Deadline(ObservedDeadline),
    Staleness(ObservedStaleness),
    WorkflowState(ObservedWorkflowState),
    WorkflowLink(ObservedWorkflowLink),
    ForeignTerminalization(ObservedForeignTerminalization),
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// What one operation reported for one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalizationOutcome {
    /// The transition committed. `terminal_at` and `kind` are the row's now.
    Applied {
        task_id: Uuid,
        ordinality: Option<i64>,
        terminal_at: DateTime<Utc>,
        kind: TerminalizationKind,
        observed: ObservedTaskState,
    },
    /// This operation's own effect was already committed.
    ///
    /// Equivalent kind, not merely equal status: five operations write
    /// CANCELLED, and only a kind in the same class proves the coupled
    /// workflow-node write committed too.
    AlreadyApplied {
        task_id: Uuid,
        ordinality: Option<i64>,
        terminal_at: DateTime<Utc>,
        kind: TerminalizationKind,
        observed: ObservedTaskState,
    },
    /// The row is live but this caller's fence cannot match it.
    ///
    /// Includes a row requeued to PENDING, whose claim fields are cleared:
    /// the generation that held it is gone, which is what the caller must
    /// act on.
    LostClaim {
        task_id: Uuid,
        ordinality: Option<i64>,
        observed: ObservedTaskState,
    },
    /// The row exists and is not this operation's to end.
    ///
    /// Carries the guard's own evidence, so a refusal is diagnosable from
    /// the log without re-reading the row — by which time it has moved on.
    SourceStateConflict {
        task_id: Uuid,
        ordinality: Option<i64>,
        observed: ObservedTaskState,
        evidence: GuardEvidence,
    },
    /// No such row. Observed columns are empty because there was nothing to
    /// see.
    TaskAbsent {
        task_id: Uuid,
        ordinality: Option<i64>,
    },
}

impl TerminalizationOutcome {
    pub fn task_id(&self) -> Uuid {
        match self {
            Self::Applied { task_id, .. }
            | Self::AlreadyApplied { task_id, .. }
            | Self::LostClaim { task_id, .. }
            | Self::SourceStateConflict { task_id, .. }
            | Self::TaskAbsent { task_id, .. } => *task_id,
        }
    }

    pub fn ordinality(&self) -> Option<i64> {
        match self {
            Self::Applied { ordinality, .. }
            | Self::AlreadyApplied { ordinality, .. }
            | Self::LostClaim { ordinality, .. }
            | Self::SourceStateConflict { ordinality, .. }
            | Self::TaskAbsent { ordinality, .. } => *ordinality,
        }
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

const ROW_COLUMNS: [&str; 10] = [
    "task_id",
    "ordinality",
    "outcome",
    "terminal_at",
    "terminalization_kind",
    "observed_status",
    "observed_worker_id",
    "observed_claimed_at",
    "guard_kind",
    "observed_guard",
];

/// One returned row, extracted with types but not yet interpreted.
///
/// Split from [`decode_outcome_row`] so the contract logic is testable
/// without a database round trip.
#[derive(Debug, Clone)]
pub struct RawOutcomeRow {
    pub task_id: Uuid,
    pub ordinality: Option<i64>,
    pub outcome: String,
    pub terminal_at: Option<DateTime<Utc>>,
    pub terminalization_kind: Option<String>,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
    pub guard_kind: Option<String>,
    pub observed_guard: Option<serde_json::Value>,
}

/// Decode one returned row into its typed outcome. Fails closed on any
/// deviation from the wire contract.
pub fn decode_outcome_row(row: &PgRow) -> Result<TerminalizationOutcome, OutcomeDecodeError> {
    require_exact_columns(row)?;
    let raw = RawOutcomeRow {
        task_id: get(row, "task_id")?,
        ordinality: get(row, "ordinality")?,
        outcome: get(row, "outcome")?,
        terminal_at: get(row, "terminal_at")?,
        terminalization_kind: get(row, "terminalization_kind")?,
        observed_status: get(row, "observed_status")?,
        observed_worker_id: get(row, "observed_worker_id")?,
        observed_claimed_at: get(row, "observed_claimed_at")?,
        guard_kind: get(row, "guard_kind")?,
        observed_guard: get(row, "observed_guard")?,
    };
    decode_raw(raw)
}

fn get<'r, T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>>(
    row: &'r PgRow,
    column: &str,
) -> Result<T, OutcomeDecodeError> {
    row.try_get(column)
        .map_err(|e| OutcomeDecodeError(format!("column {column}: {e}")))
}

fn require_exact_columns(row: &PgRow) -> Result<(), OutcomeDecodeError> {
    let present: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
    let missing: Vec<&str> = ROW_COLUMNS
        .iter()
        .copied()
        .filter(|c| !present.contains(c))
        .collect();
    let unexpected: Vec<&str> = present
        .iter()
        .copied()
        .filter(|c| !ROW_COLUMNS.contains(c))
        .collect();
    if !missing.is_empty() || !unexpected.is_empty() {
        return Err(OutcomeDecodeError(format!(
            "row shape does not match the wire contract. \
             missing={missing:?} unexpected={unexpected:?}"
        )));
    }
    Ok(())
}

/// Interpret an extracted row against the wire contract.
pub fn decode_raw(raw: RawOutcomeRow) -> Result<TerminalizationOutcome, OutcomeDecodeError> {
    let observed = ObservedTaskState {
        status: parse_optional_status(raw.observed_status.as_deref(), "observed_status")?,
        worker_id: raw.observed_worker_id.clone(),
        claimed_at: raw.observed_claimed_at,
    };

    match raw.outcome.as_str() {
        "APPLIED" => Ok(TerminalizationOutcome::Applied {
            task_id: raw.task_id,
            ordinality: raw.ordinality,
            terminal_at: require_terminal_at(&raw)?,
            kind: require_kind(&raw)?,
            observed,
        }),
        "ALREADY_APPLIED" => Ok(TerminalizationOutcome::AlreadyApplied {
            task_id: raw.task_id,
            ordinality: raw.ordinality,
            terminal_at: require_terminal_at(&raw)?,
            kind: require_kind(&raw)?,
            observed,
        }),
        "LOST_CLAIM" => Ok(TerminalizationOutcome::LostClaim {
            task_id: raw.task_id,
            ordinality: raw.ordinality,
            observed,
        }),
        "SOURCE_STATE_CONFLICT" => {
            let evidence = decode_evidence(&raw, &observed)?;
            Ok(TerminalizationOutcome::SourceStateConflict {
                task_id: raw.task_id,
                ordinality: raw.ordinality,
                observed,
                evidence,
            })
        }
        "TASK_ABSENT" => {
            require_absent_row_is_empty(&raw)?;
            Ok(TerminalizationOutcome::TaskAbsent {
                task_id: raw.task_id,
                ordinality: raw.ordinality,
            })
        }
        unknown => Err(OutcomeDecodeError(format!(
            "unknown outcome {unknown:?}. The database implements an outcome \
             this driver does not; refusing to guess which of the known ones \
             it resembles."
        ))),
    }
}

fn decode_evidence(
    raw: &RawOutcomeRow,
    observed: &ObservedTaskState,
) -> Result<GuardEvidence, OutcomeDecodeError> {
    let Some(raw_guard_kind) = raw.guard_kind.as_deref() else {
        if raw.observed_guard.is_some() {
            return Err(OutcomeDecodeError(
                "observed_guard is populated with no guard_kind to interpret \
                 it by. A payload without its discriminant cannot be decoded."
                    .to_owned(),
            ));
        }
        return Ok(GuardEvidence::Claim(ObservedClaim {
            worker_id: observed.worker_id.clone(),
            claimed_at: observed.claimed_at,
        }));
    };

    let guard_kind = GuardKind::parse(raw_guard_kind)
        .ok_or_else(|| OutcomeDecodeError(format!("unknown guard_kind {raw_guard_kind:?}")))?;

    match guard_kind {
        GuardKind::Deadline => {
            let payload = require_payload(raw, guard_kind, &["good_until", "evaluated_at"])?;
            Ok(GuardEvidence::Deadline(ObservedDeadline {
                good_until: optional_timestamp(&payload, "good_until")?,
                evaluated_at: require_timestamp(&payload, "evaluated_at")?,
            }))
        }
        GuardKind::Staleness => {
            let payload = require_payload(
                raw,
                guard_kind,
                &[
                    "last_heartbeat_at",
                    "started_at",
                    "finalizing_at",
                    "stale_after_ms",
                    "finalizing_stale_after_ms",
                    "evaluated_at",
                ],
            )?;
            Ok(GuardEvidence::Staleness(ObservedStaleness {
                last_heartbeat_at: optional_timestamp(&payload, "last_heartbeat_at")?,
                started_at: optional_timestamp(&payload, "started_at")?,
                finalizing_at: optional_timestamp(&payload, "finalizing_at")?,
                stale_after_ms: require_integer(&payload, "stale_after_ms")?,
                finalizing_stale_after_ms: require_integer(&payload, "finalizing_stale_after_ms")?,
                evaluated_at: require_timestamp(&payload, "evaluated_at")?,
            }))
        }
        GuardKind::WorkflowStatus => {
            let payload = require_payload(raw, guard_kind, &["workflow_id", "workflow_status"])?;
            Ok(GuardEvidence::WorkflowState(ObservedWorkflowState {
                workflow_id: require_string(&payload, "workflow_id")?,
                workflow_status: require_string(&payload, "workflow_status")?,
            }))
        }
        GuardKind::WorkflowLinkAbsent => {
            require_payload(raw, guard_kind, &[])?;
            Ok(GuardEvidence::WorkflowLink(ObservedWorkflowLink {
                node_status: None,
            }))
        }
        GuardKind::WorkflowLinkState => {
            let payload = require_payload(raw, guard_kind, &["node_status"])?;
            Ok(GuardEvidence::WorkflowLink(ObservedWorkflowLink {
                node_status: Some(require_string(&payload, "node_status")?),
            }))
        }
        GuardKind::ForeignTerminalization => {
            require_payload(raw, guard_kind, &[])?;
            let Some(observed_status) = observed.status else {
                return Err(OutcomeDecodeError(
                    "foreign terminalization without an observed status. The \
                     evidence that another operation won is the row it left."
                        .to_owned(),
                ));
            };
            Ok(GuardEvidence::ForeignTerminalization(
                ObservedForeignTerminalization {
                    observed_status,
                    committed_kind: optional_kind(raw)?,
                    terminal_at: raw.terminal_at,
                },
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Field readers. Each states what it required, so a decode failure names the
// column or key rather than the type that rejected it.
// ---------------------------------------------------------------------------

fn require_terminal_at(raw: &RawOutcomeRow) -> Result<DateTime<Utc>, OutcomeDecodeError> {
    raw.terminal_at
        .ok_or_else(|| OutcomeDecodeError("terminal_at is required and was NULL".to_owned()))
}

fn optional_kind(raw: &RawOutcomeRow) -> Result<Option<TerminalizationKind>, OutcomeDecodeError> {
    match raw.terminalization_kind.as_deref() {
        None => Ok(None),
        Some(value) => TerminalizationKind::parse(value).map(Some).ok_or_else(|| {
            OutcomeDecodeError(format!(
                "unknown terminalization kind {value:?}. A kind this driver \
                 does not know cannot be placed in an equivalence class, so \
                 its provenance cannot be judged."
            ))
        }),
    }
}

fn require_kind(raw: &RawOutcomeRow) -> Result<TerminalizationKind, OutcomeDecodeError> {
    optional_kind(raw)?.ok_or_else(|| {
        OutcomeDecodeError(
            "an applied transition returned no terminalization kind. Every \
             function writes its own; a NULL here means the row was not \
             written by one of them."
                .to_owned(),
        )
    })
}

fn parse_optional_status(
    raw: Option<&str>,
    key: &str,
) -> Result<Option<TaskStatus>, OutcomeDecodeError> {
    match raw {
        None => Ok(None),
        Some(value) => value
            .parse::<TaskStatus>()
            .map(Some)
            .map_err(|e| OutcomeDecodeError(format!("{key} {e}"))),
    }
}

fn require_absent_row_is_empty(raw: &RawOutcomeRow) -> Result<(), OutcomeDecodeError> {
    let mut populated: Vec<&str> = Vec::new();
    if raw.terminal_at.is_some() {
        populated.push("terminal_at");
    }
    if raw.terminalization_kind.is_some() {
        populated.push("terminalization_kind");
    }
    if raw.observed_status.is_some() {
        populated.push("observed_status");
    }
    if raw.observed_worker_id.is_some() {
        populated.push("observed_worker_id");
    }
    if raw.observed_claimed_at.is_some() {
        populated.push("observed_claimed_at");
    }
    if raw.guard_kind.is_some() {
        populated.push("guard_kind");
    }
    if raw.observed_guard.is_some() {
        populated.push("observed_guard");
    }
    if !populated.is_empty() {
        return Err(OutcomeDecodeError(format!(
            "task-absent row carries observations of a row that does not \
             exist: {populated:?}"
        )));
    }
    Ok(())
}

/// The diagnostic payload, checked against exactly the keys it must carry.
///
/// Both directions are errors: a missing key leaves the variant unbuildable,
/// and an unknown key means the function is sending evidence this decoder
/// would silently drop.
fn require_payload(
    raw: &RawOutcomeRow,
    guard_kind: GuardKind,
    required: &[&str],
) -> Result<serde_json::Map<String, serde_json::Value>, OutcomeDecodeError> {
    if required.is_empty() {
        if raw.observed_guard.is_some() {
            return Err(OutcomeDecodeError(format!(
                "{} carries its evidence in the uniform columns and must not \
                 send a payload",
                guard_kind.as_str()
            )));
        }
        return Ok(serde_json::Map::new());
    }
    let Some(serde_json::Value::Object(payload)) = &raw.observed_guard else {
        return Err(OutcomeDecodeError(format!(
            "{} requires a payload object, got {:?}",
            guard_kind.as_str(),
            raw.observed_guard
        )));
    };
    let missing: Vec<&&str> = required
        .iter()
        .filter(|k| !payload.contains_key(**k))
        .collect();
    let unexpected: Vec<&String> = payload
        .keys()
        .filter(|k| !required.contains(&k.as_str()))
        .collect();
    if !missing.is_empty() || !unexpected.is_empty() {
        return Err(OutcomeDecodeError(format!(
            "{} payload does not match its documented keys. \
             missing={missing:?} unexpected={unexpected:?}",
            guard_kind.as_str()
        )));
    }
    Ok(payload.clone())
}

fn require_string(
    payload: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, OutcomeDecodeError> {
    match payload.get(key) {
        Some(serde_json::Value::String(value)) => Ok(value.clone()),
        other => Err(OutcomeDecodeError(format!(
            "{key} must be a string, got {other:?}"
        ))),
    }
}

fn require_integer(
    payload: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<i64, OutcomeDecodeError> {
    match payload.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_i64()
            .ok_or_else(|| OutcomeDecodeError(format!("{key} must be an integer, got {value}"))),
        other => Err(OutcomeDecodeError(format!(
            "{key} must be an integer, got {other:?}"
        ))),
    }
}

/// jsonb payloads carry timestamps as text; columns arrive typed.
fn optional_timestamp(
    payload: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<DateTime<Utc>>, OutcomeDecodeError> {
    match payload.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) => DateTime::parse_from_rfc3339(value)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .map_err(|_| {
                OutcomeDecodeError(format!("{key} is not an ISO-8601 timestamp: {value:?}"))
            }),
        other => Err(OutcomeDecodeError(format!(
            "{key} must be a timestamp, got {other:?}"
        ))),
    }
}

fn require_timestamp(
    payload: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<DateTime<Utc>, OutcomeDecodeError> {
    optional_timestamp(payload, key)?
        .ok_or_else(|| OutcomeDecodeError(format!("{key} is required and was NULL")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_raw(outcome: &str) -> RawOutcomeRow {
        RawOutcomeRow {
            task_id: Uuid::nil(),
            ordinality: None,
            outcome: outcome.to_owned(),
            terminal_at: None,
            terminalization_kind: None,
            observed_status: None,
            observed_worker_id: None,
            observed_claimed_at: None,
            guard_kind: None,
            observed_guard: None,
        }
    }

    #[test]
    fn unknown_outcome_fails_closed() {
        let err = decode_raw(base_raw("SOMETHING_NEW")).unwrap_err();
        assert!(err.to_string().contains("unknown outcome"));
    }

    #[test]
    fn applied_requires_kind_and_terminal_at() {
        let mut raw = base_raw("APPLIED");
        raw.observed_status = Some("RUNNING".to_owned());
        let err = decode_raw(raw.clone()).unwrap_err();
        assert!(err.to_string().contains("terminal_at is required"));

        raw.terminal_at = Some(Utc::now());
        let err = decode_raw(raw.clone()).unwrap_err();
        assert!(err.to_string().contains("no terminalization kind"));

        raw.terminalization_kind = Some("COMPLETE_FUSED".to_owned());
        let decoded = decode_raw(raw).unwrap();
        assert!(matches!(
            decoded,
            TerminalizationOutcome::Applied {
                kind: TerminalizationKind::CompleteFused,
                ..
            }
        ));
    }

    #[test]
    fn unknown_kind_fails_closed() {
        let mut raw = base_raw("ALREADY_APPLIED");
        raw.terminal_at = Some(Utc::now());
        raw.terminalization_kind = Some("KIND_FROM_THE_FUTURE".to_owned());
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("unknown terminalization kind"));
    }

    #[test]
    fn unknown_guard_kind_fails_closed() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.observed_status = Some("RUNNING".to_owned());
        raw.guard_kind = Some("GUARD_FROM_THE_FUTURE".to_owned());
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("unknown guard_kind"));
    }

    #[test]
    fn payload_without_discriminant_fails_closed() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.observed_guard = Some(serde_json::json!({"x": 1}));
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("no guard_kind"));
    }

    #[test]
    fn conflict_without_guard_kind_is_claim_evidence() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.observed_status = Some("PENDING".to_owned());
        raw.observed_worker_id = Some("w-other".to_owned());
        let decoded = decode_raw(raw).unwrap();
        let TerminalizationOutcome::SourceStateConflict { evidence, .. } = decoded else {
            panic!("expected conflict");
        };
        assert!(matches!(
            evidence,
            GuardEvidence::Claim(ObservedClaim { ref worker_id, .. })
                if worker_id.as_deref() == Some("w-other")
        ));
    }

    #[test]
    fn payload_key_mismatch_fails_closed_both_directions() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.guard_kind = Some("DEADLINE".to_owned());
        raw.observed_guard = Some(serde_json::json!({"good_until": null}));
        let err = decode_raw(raw.clone()).unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");

        raw.observed_guard = Some(serde_json::json!({
            "good_until": null, "evaluated_at": "2026-08-06T10:00:00+00:00", "extra": 1
        }));
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("unexpected"), "{err}");
    }

    #[test]
    fn empty_payload_guards_reject_payloads() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.observed_status = Some("COMPLETED".to_owned());
        raw.terminal_at = Some(Utc::now());
        raw.terminalization_kind = Some("CANCEL_ADMIN".to_owned());
        raw.guard_kind = Some("FOREIGN_TERMINALIZATION".to_owned());
        raw.observed_guard = Some(serde_json::json!({"why": "not"}));
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("must not send a payload"));
    }

    #[test]
    fn foreign_terminalization_decodes_committed_kind() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.observed_status = Some("COMPLETED".to_owned());
        raw.terminal_at = Some(Utc::now());
        raw.terminalization_kind = Some("COMPLETE_FUSED".to_owned());
        raw.guard_kind = Some("FOREIGN_TERMINALIZATION".to_owned());
        let decoded = decode_raw(raw).unwrap();
        let TerminalizationOutcome::SourceStateConflict { evidence, .. } = decoded else {
            panic!("expected conflict");
        };
        assert!(matches!(
            evidence,
            GuardEvidence::ForeignTerminalization(ObservedForeignTerminalization {
                observed_status: TaskStatus::Completed,
                committed_kind: Some(TerminalizationKind::CompleteFused),
                ..
            })
        ));
    }

    #[test]
    fn foreign_terminalization_without_status_fails_closed() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.guard_kind = Some("FOREIGN_TERMINALIZATION".to_owned());
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("without an observed status"));
    }

    #[test]
    fn task_absent_with_observations_fails_closed() {
        let mut raw = base_raw("TASK_ABSENT");
        raw.observed_status = Some("PENDING".to_owned());
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("observations of a row"));

        let decoded = decode_raw(base_raw("TASK_ABSENT")).unwrap();
        assert!(matches!(decoded, TerminalizationOutcome::TaskAbsent { .. }));
    }

    #[test]
    fn staleness_payload_decodes_with_jsonb_timestamps() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.observed_status = Some("RUNNING".to_owned());
        raw.guard_kind = Some("STALENESS".to_owned());
        raw.observed_guard = Some(serde_json::json!({
            "last_heartbeat_at": "2026-08-06T10:00:00.123456+00:00",
            "started_at": null,
            "finalizing_at": null,
            "stale_after_ms": 30000,
            "finalizing_stale_after_ms": 60000,
            "evaluated_at": "2026-08-06T10:00:30+00:00"
        }));
        let decoded = decode_raw(raw).unwrap();
        let TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Staleness(staleness),
            ..
        } = decoded
        else {
            panic!("expected staleness conflict");
        };
        assert_eq!(staleness.stale_after_ms, 30000);
        assert!(staleness.last_heartbeat_at.is_some());
        assert!(staleness.started_at.is_none());
    }

    #[test]
    fn malformed_jsonb_timestamp_fails_closed() {
        let mut raw = base_raw("SOURCE_STATE_CONFLICT");
        raw.guard_kind = Some("DEADLINE".to_owned());
        raw.observed_guard = Some(serde_json::json!({
            "good_until": "yesterday-ish", "evaluated_at": "2026-08-06T10:00:00+00:00"
        }));
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("ISO-8601"));
    }

    #[test]
    fn unknown_observed_status_fails_closed() {
        let mut raw = base_raw("LOST_CLAIM");
        raw.observed_status = Some("LIMBO".to_owned());
        let err = decode_raw(raw).unwrap_err();
        assert!(err.to_string().contains("not a task status"));
    }
}
