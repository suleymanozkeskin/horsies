//! What the two one-statement attempt writers report, decoded in one place.
//!
//! `horsies_finish_failed_attempt` returns the ten shared outcome columns and
//! seven more; `horsies_finish_workflow_success` returns the ten and two
//! more. A miss carries the shared classifier's row unchanged with the extra
//! columns NULL, so it decodes through the shared decoder.
//!
//! Python reference: `horsies/core/lifecycle/outcomes.py`
//! (`decode_failed_attempt_row`, `decode_workflow_success_row`).
//!
//! Decoding fails closed: a missing or unknown column, a column set or NULL
//! against its outcome, or an unknown discriminant is an
//! [`OutcomeDecodeError`].

use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{Column, Row};
use uuid::Uuid;

use crate::core::types::status::TaskStatus;

use super::operations::TerminalizationKind;
use super::outcomes::{
    decode_raw, GuardEvidence, ObservedTaskState, OutcomeDecodeError, RawOutcomeRow,
    TerminalizationOutcome,
};

const SHARED_COLUMNS: [&str; 10] = [
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

const FAILED_ATTEMPT_COLUMNS: [&str; 7] = [
    "retry_disposition",
    "retry_count",
    "next_retry_at",
    "good_until",
    "is_workflow_task",
    "queue_name",
    "task_name",
];

const WORKFLOW_SUCCESS_COLUMNS: [&str; 2] = ["queue_name", "task_name"];

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// A writer that wrote nothing: a replay of its own committed effect, or a
/// refusal with what it saw under the lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptWriteRefusal {
    /// This writer's effect was already committed (equivalent kind).
    AlreadyApplied {
        task_id: Uuid,
        terminal_at: DateTime<Utc>,
        kind: TerminalizationKind,
        observed: ObservedTaskState,
    },
    /// The row is live, and the fence (worker, claim generation, retry
    /// count) does not match it.
    LostClaim {
        task_id: Uuid,
        observed: ObservedTaskState,
    },
    /// The row exists and is not this writer's to end.
    SourceStateConflict {
        task_id: Uuid,
        observed: ObservedTaskState,
        evidence: GuardEvidence,
    },
    /// No live or history row has this id.
    TaskAbsent { task_id: Uuid },
}

/// Phase 2 of a workflow task is owed: it advances the node and wakes the
/// queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseTwoOwed {
    pub task_name: String,
    pub queue_name: String,
}

/// Why a terminal failure was written instead of a retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryRefusal {
    /// The caller asked for no retry.
    NotRequested,
    /// The caller asked for a retry, and its next attempt would start at or
    /// after `good_until` on the database clock.
    PastDeadline {
        next_retry_at: DateTime<Utc>,
        good_until: DateTime<Utc>,
    },
}

/// What is owed after a terminal failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterFailure {
    /// A plain task: the statement sent its capacity wake. Nothing is owed.
    CapacityWoken,
    /// A workflow task: phase 2 is owed.
    PhaseTwoOwed(PhaseTwoOwed),
}

/// What `horsies_finish_failed_attempt` reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailedAttemptOutcome {
    /// The terminal failure and its attempt row committed.
    FailureApplied {
        task_id: Uuid,
        terminal_at: DateTime<Utc>,
        observed: ObservedTaskState,
        retry: RetryRefusal,
        after: AfterFailure,
    },
    /// The retry and its attempt row (`will_retry`) committed. `retry_count`
    /// is the count the row now holds; the caller wakes the queue when the
    /// retry is due.
    RetryScheduled {
        task_id: Uuid,
        retry_count: i32,
        next_retry_at: DateTime<Utc>,
        queue_name: String,
        observed: ObservedTaskState,
    },
    Refused(AttemptWriteRefusal),
}

/// What `horsies_finish_workflow_success` reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowSuccessOutcome {
    /// The attempt row, the history row and the phase-2 record of the node
    /// committed. Phase 2 is owed.
    SuccessApplied {
        task_id: Uuid,
        terminal_at: DateTime<Utc>,
        observed: ObservedTaskState,
        phase_two: PhaseTwoOwed,
    },
    Refused(AttemptWriteRefusal),
}

impl FailedAttemptOutcome {
    /// The outcome text, for the log.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FailureApplied { .. } => "APPLIED",
            Self::RetryScheduled { .. } => "RETRY_SCHEDULED",
            Self::Refused(refusal) => refusal.as_str(),
        }
    }
}

impl WorkflowSuccessOutcome {
    /// The outcome text, for the log.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SuccessApplied { .. } => "APPLIED",
            Self::Refused(refusal) => refusal.as_str(),
        }
    }
}

impl AttemptWriteRefusal {
    /// The outcome text, for the log.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AlreadyApplied { .. } => "ALREADY_APPLIED",
            Self::LostClaim { .. } => "LOST_CLAIM",
            Self::SourceStateConflict { .. } => "SOURCE_STATE_CONFLICT",
            Self::TaskAbsent { .. } => "TASK_ABSENT",
        }
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// The seven columns that only the failed-attempt row carries.
struct FailedAttemptColumns {
    retry_disposition: Option<String>,
    retry_count: Option<i32>,
    next_retry_at: Option<DateTime<Utc>>,
    good_until: Option<DateTime<Utc>>,
    is_workflow_task: Option<bool>,
    queue_name: Option<String>,
    task_name: Option<String>,
}

impl FailedAttemptColumns {
    fn populated(&self) -> Vec<&'static str> {
        let mut populated = Vec::new();
        if self.retry_disposition.is_some() {
            populated.push("retry_disposition");
        }
        if self.retry_count.is_some() {
            populated.push("retry_count");
        }
        if self.next_retry_at.is_some() {
            populated.push("next_retry_at");
        }
        if self.good_until.is_some() {
            populated.push("good_until");
        }
        if self.is_workflow_task.is_some() {
            populated.push("is_workflow_task");
        }
        if self.queue_name.is_some() {
            populated.push("queue_name");
        }
        if self.task_name.is_some() {
            populated.push("task_name");
        }
        populated
    }
}

/// Decode the one row of `horsies_finish_failed_attempt`.
pub fn decode_failed_attempt_row(row: &PgRow) -> Result<FailedAttemptOutcome, OutcomeDecodeError> {
    require_exact_columns(row, &FAILED_ATTEMPT_COLUMNS, "failed-attempt")?;
    let raw = shared_columns(row)?;
    let extra = FailedAttemptColumns {
        retry_disposition: get(row, "retry_disposition")?,
        retry_count: get(row, "retry_count")?,
        next_retry_at: get(row, "next_retry_at")?,
        good_until: get(row, "good_until")?,
        is_workflow_task: get(row, "is_workflow_task")?,
        queue_name: get(row, "queue_name")?,
        task_name: get(row, "task_name")?,
    };
    decode_failed_attempt(raw, extra)
}

fn decode_failed_attempt(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<FailedAttemptOutcome, OutcomeDecodeError> {
    match raw.outcome.as_str() {
        "APPLIED" => decode_failure_applied(raw, extra),
        "RETRY_SCHEDULED" => decode_retry_scheduled(raw, extra),
        other => {
            require_null(&extra.populated(), other)?;
            refusal_of(decode_raw(raw)?).map(FailedAttemptOutcome::Refused)
        }
    }
}

fn decode_failure_applied(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<FailedAttemptOutcome, OutcomeDecodeError> {
    if extra.retry_count.is_some() {
        return Err(OutcomeDecodeError(
            "APPLIED failed-attempt row carries retry_count, which only a retry sets".to_owned(),
        ));
    }
    let (task_id, terminal_at, kind, observed) = match decode_raw(raw)? {
        TerminalizationOutcome::Applied {
            task_id,
            terminal_at,
            kind,
            observed,
            ..
        } => (task_id, terminal_at, kind, observed),
        TerminalizationOutcome::AlreadyApplied { .. }
        | TerminalizationOutcome::LostClaim { .. }
        | TerminalizationOutcome::SourceStateConflict { .. }
        | TerminalizationOutcome::TaskAbsent { .. } => {
            unreachable!("the shared decoder keeps the APPLIED discriminant")
        }
    };
    if kind != TerminalizationKind::FailRunning {
        return Err(OutcomeDecodeError(format!(
            "failed-attempt APPLIED carries kind {}, not FAIL_RUNNING",
            kind.as_str()
        )));
    }
    let retry = match extra.retry_disposition.as_deref() {
        Some("NOT_REQUESTED") => {
            if extra.next_retry_at.is_some() || extra.good_until.is_some() {
                return Err(OutcomeDecodeError(
                    "NOT_REQUESTED failure carries a retry time or a deadline".to_owned(),
                ));
            }
            RetryRefusal::NotRequested
        }
        Some("PAST_DEADLINE") => RetryRefusal::PastDeadline {
            next_retry_at: require(extra.next_retry_at, "next_retry_at")?,
            good_until: require(extra.good_until, "good_until")?,
        },
        Some(unknown) => {
            return Err(OutcomeDecodeError(format!(
                "unknown retry_disposition {unknown:?}"
            )))
        }
        None => {
            return Err(OutcomeDecodeError(
                "APPLIED failed-attempt row has no retry_disposition".to_owned(),
            ))
        }
    };
    let after = match require(extra.is_workflow_task, "is_workflow_task")? {
        true => AfterFailure::PhaseTwoOwed(PhaseTwoOwed {
            task_name: require(extra.task_name, "task_name")?,
            queue_name: require(extra.queue_name, "queue_name")?,
        }),
        false => AfterFailure::CapacityWoken,
    };
    Ok(FailedAttemptOutcome::FailureApplied {
        task_id,
        terminal_at,
        observed,
        retry,
        after,
    })
}

fn decode_retry_scheduled(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<FailedAttemptOutcome, OutcomeDecodeError> {
    let mut populated: Vec<&'static str> = Vec::new();
    if raw.terminal_at.is_some() {
        populated.push("terminal_at");
    }
    if raw.terminalization_kind.is_some() {
        populated.push("terminalization_kind");
    }
    if raw.guard_kind.is_some() {
        populated.push("guard_kind");
    }
    if raw.observed_guard.is_some() {
        populated.push("observed_guard");
    }
    if extra.retry_disposition.is_some() {
        populated.push("retry_disposition");
    }
    if extra.good_until.is_some() {
        populated.push("good_until");
    }
    if extra.is_workflow_task.is_some() {
        populated.push("is_workflow_task");
    }
    if extra.task_name.is_some() {
        populated.push("task_name");
    }
    require_null(&populated, "RETRY_SCHEDULED")?;
    Ok(FailedAttemptOutcome::RetryScheduled {
        task_id: raw.task_id,
        retry_count: require(extra.retry_count, "retry_count")?,
        next_retry_at: require(extra.next_retry_at, "next_retry_at")?,
        queue_name: require(extra.queue_name, "queue_name")?,
        observed: observed_of(&raw)?,
    })
}

/// Decode the one row of `horsies_finish_workflow_success`.
pub fn decode_workflow_success_row(
    row: &PgRow,
) -> Result<WorkflowSuccessOutcome, OutcomeDecodeError> {
    require_exact_columns(row, &WORKFLOW_SUCCESS_COLUMNS, "workflow-success")?;
    let raw = shared_columns(row)?;
    let queue_name: Option<String> = get(row, "queue_name")?;
    let task_name: Option<String> = get(row, "task_name")?;
    decode_workflow_success(raw, queue_name, task_name)
}

fn decode_workflow_success(
    raw: RawOutcomeRow,
    queue_name: Option<String>,
    task_name: Option<String>,
) -> Result<WorkflowSuccessOutcome, OutcomeDecodeError> {
    match decode_raw(raw)? {
        TerminalizationOutcome::Applied {
            task_id,
            terminal_at,
            kind,
            observed,
            ..
        } => {
            if kind != TerminalizationKind::CompleteLocked {
                return Err(OutcomeDecodeError(format!(
                    "workflow-success APPLIED carries kind {}, not COMPLETE_LOCKED",
                    kind.as_str()
                )));
            }
            Ok(WorkflowSuccessOutcome::SuccessApplied {
                task_id,
                terminal_at,
                observed,
                phase_two: PhaseTwoOwed {
                    task_name: require(task_name, "task_name")?,
                    queue_name: require(queue_name, "queue_name")?,
                },
            })
        }
        refused @ (TerminalizationOutcome::AlreadyApplied { .. }
        | TerminalizationOutcome::LostClaim { .. }
        | TerminalizationOutcome::SourceStateConflict { .. }
        | TerminalizationOutcome::TaskAbsent { .. }) => {
            let mut populated: Vec<&'static str> = Vec::new();
            if queue_name.is_some() {
                populated.push("queue_name");
            }
            if task_name.is_some() {
                populated.push("task_name");
            }
            require_null(&populated, "refused workflow-success")?;
            refusal_of(refused).map(WorkflowSuccessOutcome::Refused)
        }
    }
}

/// A shared outcome that wrote nothing, as a refusal. An applied outcome is
/// decoded by its writer's own arm, so reaching here with one is a contract
/// breach of the row.
fn refusal_of(outcome: TerminalizationOutcome) -> Result<AttemptWriteRefusal, OutcomeDecodeError> {
    match outcome {
        TerminalizationOutcome::Applied { .. } => Err(OutcomeDecodeError(
            "an APPLIED outcome reached the refusal decoder".to_owned(),
        )),
        TerminalizationOutcome::AlreadyApplied {
            task_id,
            terminal_at,
            kind,
            observed,
            ..
        } => Ok(AttemptWriteRefusal::AlreadyApplied {
            task_id,
            terminal_at,
            kind,
            observed,
        }),
        TerminalizationOutcome::LostClaim {
            task_id, observed, ..
        } => Ok(AttemptWriteRefusal::LostClaim { task_id, observed }),
        TerminalizationOutcome::SourceStateConflict {
            task_id,
            observed,
            evidence,
            ..
        } => Ok(AttemptWriteRefusal::SourceStateConflict {
            task_id,
            observed,
            evidence,
        }),
        TerminalizationOutcome::TaskAbsent { task_id, .. } => {
            Ok(AttemptWriteRefusal::TaskAbsent { task_id })
        }
    }
}

fn observed_of(raw: &RawOutcomeRow) -> Result<ObservedTaskState, OutcomeDecodeError> {
    let status = match raw.observed_status.as_deref() {
        None => None,
        Some(text) => Some(
            text.parse::<TaskStatus>()
                .map_err(|error| OutcomeDecodeError(format!("observed_status {error}")))?,
        ),
    };
    Ok(ObservedTaskState {
        status,
        worker_id: raw.observed_worker_id.clone(),
        claimed_at: raw.observed_claimed_at,
    })
}

fn shared_columns(row: &PgRow) -> Result<RawOutcomeRow, OutcomeDecodeError> {
    Ok(RawOutcomeRow {
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
    })
}

fn get<'r, T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>>(
    row: &'r PgRow,
    column: &str,
) -> Result<T, OutcomeDecodeError> {
    row.try_get(column)
        .map_err(|error| OutcomeDecodeError(format!("column {column}: {error}")))
}

fn require_exact_columns(
    row: &PgRow,
    extra: &[&str],
    writer: &str,
) -> Result<(), OutcomeDecodeError> {
    let present: Vec<&str> = row.columns().iter().map(|column| column.name()).collect();
    let expected: Vec<&str> = SHARED_COLUMNS.iter().chain(extra.iter()).copied().collect();
    let missing: Vec<&str> = expected
        .iter()
        .copied()
        .filter(|column| !present.contains(column))
        .collect();
    let unexpected: Vec<&str> = present
        .iter()
        .copied()
        .filter(|column| !expected.contains(column))
        .collect();
    if !missing.is_empty() || !unexpected.is_empty() {
        return Err(OutcomeDecodeError(format!(
            "{writer} row shape does not match the wire contract. \
             missing={missing:?} unexpected={unexpected:?}"
        )));
    }
    Ok(())
}

fn require<T>(value: Option<T>, column: &str) -> Result<T, OutcomeDecodeError> {
    value.ok_or_else(|| OutcomeDecodeError(format!("{column} is required and was NULL")))
}

fn require_null(populated: &[&str], outcome: &str) -> Result<(), OutcomeDecodeError> {
    match populated.is_empty() {
        true => Ok(()),
        false => Err(OutcomeDecodeError(format!(
            "{outcome} row carries columns its outcome does not set: {populated:?}"
        ))),
    }
}

#[cfg(test)]
mod tests;
