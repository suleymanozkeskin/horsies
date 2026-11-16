//! What the one-statement attempt writers report, decoded in one place.
//!
//! `horsies_finish_failed_attempt` and `horsies_finish_stale_attempt` return
//! the ten shared outcome columns and the same eight more;
//! `horsies_finish_workflow_success` returns the ten and three more. A miss
//! carries the shared classifier's row unchanged with the extra columns NULL,
//! so it decodes through the shared decoder.
//!
//! `node_has_dependents` is a boolean on the applied row of a workflow task
//! and NULL on every other row. The core's phase 2 reads the nodes itself,
//! so the decoders check the column and do not carry it (as Python).
//!
//! Python reference: `horsies/core/lifecycle/outcomes.py`
//! (`decode_failed_attempt_row`, `decode_workflow_success_row`).
//!
//! Decoding fails closed: a missing or unknown column, a column set or NULL
//! against its outcome, or an unknown discriminant is an
//! [`OutcomeDecodeError`].

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::core::types::status::TaskStatus;
use crate::db::{FromSql, Row};

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

const FAILED_ATTEMPT_COLUMNS: [&str; 8] = [
    "retry_disposition",
    "retry_count",
    "next_retry_at",
    "good_until",
    "is_workflow_task",
    "queue_name",
    "task_name",
    "node_has_dependents",
];

const WORKFLOW_SUCCESS_COLUMNS: [&str; 3] = ["queue_name", "task_name", "node_has_dependents"];

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

/// A claimed row that no child started, returned to the queue by the
/// failed-attempt statement under the claimed-row rule `RELEASE`: PENDING
/// again with every claim and run field cleared and no attempt row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasedClaim {
    pub task_id: Uuid,
    /// The queue of the released row.
    pub queue_name: String,
    /// The row as the statement saw it under the lock: CLAIMED by this
    /// worker at this claim generation.
    pub observed: ObservedTaskState,
}

/// What `horsies_finish_failed_attempt` reported under the claimed-row rule
/// `RELEASE`, which the worker uses when a child ended without a report
/// (timeout, loop kill, crash).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnreportedAttemptOutcome {
    /// The row was still CLAIMED: no child started it, and it is back in
    /// the queue.
    Released(ReleasedClaim),
    /// The row was not CLAIMED by this claim generation: the statement wrote
    /// or refused the failed attempt as under the rule `REFUSE`.
    Failed(FailedAttemptOutcome),
}

/// What is owed after the terminal failure of a stale RUNNING task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterStaleFailure {
    /// A plain task: the statement sends no capacity wake (as Python), and
    /// nothing is owed.
    NothingOwed,
    /// A workflow task: phase 2 is owed. The move recorded it, and phase-2
    /// recovery runs it.
    PhaseTwoOwed(PhaseTwoOwed),
}

/// What `horsies_finish_stale_attempt` reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleAttemptOutcome {
    /// The terminal failure (`FAIL_STALE`) and its attempt row committed.
    FailureApplied {
        task_id: Uuid,
        terminal_at: DateTime<Utc>,
        observed: ObservedTaskState,
        retry: RetryRefusal,
        after: AfterStaleFailure,
    },
    /// The crash retry and its attempt row (`will_retry`) committed.
    /// `retry_count` is the count the row now holds.
    RetryScheduled {
        task_id: Uuid,
        retry_count: i32,
        next_retry_at: DateTime<Utc>,
        queue_name: String,
        observed: ObservedTaskState,
    },
    /// Nothing was written: the row is not RUNNING, it is not stale under
    /// the lock (guard `STALENESS`), or its retry count is not the one the
    /// retry was computed from (guard `RETRY_COUNT`).
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

impl StaleAttemptOutcome {
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

/// The eight columns that only the failed-attempt row carries.
struct FailedAttemptColumns {
    retry_disposition: Option<String>,
    retry_count: Option<i32>,
    next_retry_at: Option<DateTime<Utc>>,
    good_until: Option<DateTime<Utc>>,
    is_workflow_task: Option<bool>,
    queue_name: Option<String>,
    task_name: Option<String>,
    node_has_dependents: Option<bool>,
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
        if self.node_has_dependents.is_some() {
            populated.push("node_has_dependents");
        }
        populated
    }
}

/// The three columns that only the workflow-success row carries.
struct WorkflowSuccessColumns {
    queue_name: Option<String>,
    task_name: Option<String>,
    node_has_dependents: Option<bool>,
}

/// Decode the one row of `horsies_finish_failed_attempt`. A `RELEASED` row
/// decodes to [`UnreportedAttemptOutcome::Released`]; whether the claimed-row
/// rule of the call allows it is the writer's check.
pub fn decode_failed_attempt_row(
    row: &Row,
) -> Result<UnreportedAttemptOutcome, OutcomeDecodeError> {
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
        node_has_dependents: get(row, "node_has_dependents")?,
    };
    decode_failed_attempt(raw, extra)
}

fn decode_failed_attempt(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<UnreportedAttemptOutcome, OutcomeDecodeError> {
    match raw.outcome.as_str() {
        "APPLIED" => decode_failure_applied(raw, extra).map(UnreportedAttemptOutcome::Failed),
        "RETRY_SCHEDULED" => {
            decode_retry_scheduled(raw, extra).map(UnreportedAttemptOutcome::Failed)
        }
        "RELEASED" => decode_released(raw, extra).map(UnreportedAttemptOutcome::Released),
        other => {
            require_null(&extra.populated(), other)?;
            refusal_of(decode_raw(raw)?)
                .map(FailedAttemptOutcome::Refused)
                .map(UnreportedAttemptOutcome::Failed)
        }
    }
}

/// The kind of task whose terminal failure applied.
enum FailedTask {
    Plain,
    Workflow(PhaseTwoOwed),
}

/// An `APPLIED` row of a failure writer, before the writer names what the
/// failure owes.
struct AppliedFailure {
    task_id: Uuid,
    terminal_at: DateTime<Utc>,
    observed: ObservedTaskState,
    retry: RetryRefusal,
    task: FailedTask,
}

fn decode_failure_applied(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<FailedAttemptOutcome, OutcomeDecodeError> {
    let applied = decode_applied_failure(raw, extra, TerminalizationKind::FailRunning)?;
    Ok(FailedAttemptOutcome::FailureApplied {
        task_id: applied.task_id,
        terminal_at: applied.terminal_at,
        observed: applied.observed,
        retry: applied.retry,
        after: match applied.task {
            FailedTask::Plain => AfterFailure::CapacityWoken,
            FailedTask::Workflow(owed) => AfterFailure::PhaseTwoOwed(owed),
        },
    })
}

/// An `APPLIED` row of the kind `expected`: the retry disposition, and the
/// facts of a workflow task (its dependents fact, task and queue).
fn decode_applied_failure(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
    expected: TerminalizationKind,
) -> Result<AppliedFailure, OutcomeDecodeError> {
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
    if kind != expected {
        return Err(OutcomeDecodeError(format!(
            "failed-attempt APPLIED carries kind {}, not {}",
            kind.as_str(),
            expected.as_str()
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
    let task = match require(extra.is_workflow_task, "is_workflow_task")? {
        true => {
            require_dependents_fact(extra.node_has_dependents)?;
            FailedTask::Workflow(PhaseTwoOwed {
                task_name: require(extra.task_name, "task_name")?,
                queue_name: require(extra.queue_name, "queue_name")?,
            })
        }
        false => {
            require_null(
                &no_dependents_fact(extra.node_has_dependents),
                "APPLIED plain-task failed-attempt",
            )?;
            FailedTask::Plain
        }
    };
    Ok(AppliedFailure {
        task_id,
        terminal_at,
        observed,
        retry,
        task,
    })
}

fn decode_retry_scheduled(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<FailedAttemptOutcome, OutcomeDecodeError> {
    let scheduled = decode_scheduled_retry(raw, extra)?;
    Ok(FailedAttemptOutcome::RetryScheduled {
        task_id: scheduled.task_id,
        retry_count: scheduled.retry_count,
        next_retry_at: scheduled.next_retry_at,
        queue_name: scheduled.queue_name,
        observed: scheduled.observed,
    })
}

/// A `RETRY_SCHEDULED` row of a failure writer.
struct ScheduledRetry {
    task_id: Uuid,
    retry_count: i32,
    next_retry_at: DateTime<Utc>,
    queue_name: String,
    observed: ObservedTaskState,
}

/// A `RETRY_SCHEDULED` row: the new retry count, the retry time and the
/// queue; every terminal and refusal column NULL.
fn decode_scheduled_retry(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<ScheduledRetry, OutcomeDecodeError> {
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
    if extra.node_has_dependents.is_some() {
        populated.push("node_has_dependents");
    }
    require_null(&populated, "RETRY_SCHEDULED")?;
    Ok(ScheduledRetry {
        task_id: raw.task_id,
        retry_count: require(extra.retry_count, "retry_count")?,
        next_retry_at: require(extra.next_retry_at, "next_retry_at")?,
        queue_name: require(extra.queue_name, "queue_name")?,
        observed: observed_of(&raw)?,
    })
}

/// A `RELEASED` row: the row and its queue, every other column NULL, and
/// the row seen CLAIMED.
fn decode_released(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<ReleasedClaim, OutcomeDecodeError> {
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
    let FailedAttemptColumns {
        retry_disposition,
        retry_count,
        next_retry_at,
        good_until,
        is_workflow_task,
        queue_name,
        task_name,
        node_has_dependents,
    } = extra;
    let named = [
        ("retry_disposition", retry_disposition.is_some()),
        ("retry_count", retry_count.is_some()),
        ("next_retry_at", next_retry_at.is_some()),
        ("good_until", good_until.is_some()),
        ("is_workflow_task", is_workflow_task.is_some()),
        ("task_name", task_name.is_some()),
        ("node_has_dependents", node_has_dependents.is_some()),
    ];
    populated.extend(
        named
            .iter()
            .filter(|(_, is_set)| *is_set)
            .map(|(column, _)| *column),
    );
    require_null(&populated, "RELEASED")?;
    let observed = observed_of(&raw)?;
    if observed.status != Some(TaskStatus::Claimed) {
        return Err(OutcomeDecodeError(format!(
            "RELEASED row observed status {:?}, not CLAIMED",
            observed.status
        )));
    }
    Ok(ReleasedClaim {
        task_id: raw.task_id,
        queue_name: require(queue_name, "queue_name")?,
        observed,
    })
}

/// Decode the one row of `horsies_finish_stale_attempt`: the columns of
/// `horsies_finish_failed_attempt`. `APPLIED` carries the kind
/// `FAIL_STALE`; the function never returns `RELEASED`.
pub fn decode_stale_attempt_row(row: &Row) -> Result<StaleAttemptOutcome, OutcomeDecodeError> {
    require_exact_columns(row, &FAILED_ATTEMPT_COLUMNS, "stale-attempt")?;
    let raw = shared_columns(row)?;
    let extra = FailedAttemptColumns {
        retry_disposition: get(row, "retry_disposition")?,
        retry_count: get(row, "retry_count")?,
        next_retry_at: get(row, "next_retry_at")?,
        good_until: get(row, "good_until")?,
        is_workflow_task: get(row, "is_workflow_task")?,
        queue_name: get(row, "queue_name")?,
        task_name: get(row, "task_name")?,
        node_has_dependents: get(row, "node_has_dependents")?,
    };
    decode_stale_attempt(raw, extra)
}

fn decode_stale_attempt(
    raw: RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<StaleAttemptOutcome, OutcomeDecodeError> {
    match raw.outcome.as_str() {
        "APPLIED" => {
            let applied = decode_applied_failure(raw, extra, TerminalizationKind::FailStale)?;
            Ok(StaleAttemptOutcome::FailureApplied {
                task_id: applied.task_id,
                terminal_at: applied.terminal_at,
                observed: applied.observed,
                retry: applied.retry,
                after: match applied.task {
                    FailedTask::Plain => AfterStaleFailure::NothingOwed,
                    FailedTask::Workflow(owed) => AfterStaleFailure::PhaseTwoOwed(owed),
                },
            })
        }
        "RETRY_SCHEDULED" => {
            let scheduled = decode_scheduled_retry(raw, extra)?;
            Ok(StaleAttemptOutcome::RetryScheduled {
                task_id: scheduled.task_id,
                retry_count: scheduled.retry_count,
                next_retry_at: scheduled.next_retry_at,
                queue_name: scheduled.queue_name,
                observed: scheduled.observed,
            })
        }
        "RELEASED" => Err(OutcomeDecodeError(
            "horsies_finish_stale_attempt returned RELEASED, which it never writes".to_owned(),
        )),
        other => {
            require_null(&extra.populated(), other)?;
            refusal_of(decode_raw(raw)?).map(StaleAttemptOutcome::Refused)
        }
    }
}

/// Decode the one row of `horsies_finish_workflow_success`.
pub fn decode_workflow_success_row(
    row: &Row,
) -> Result<WorkflowSuccessOutcome, OutcomeDecodeError> {
    require_exact_columns(row, &WORKFLOW_SUCCESS_COLUMNS, "workflow-success")?;
    let raw = shared_columns(row)?;
    let extra = WorkflowSuccessColumns {
        queue_name: get(row, "queue_name")?,
        task_name: get(row, "task_name")?,
        node_has_dependents: get(row, "node_has_dependents")?,
    };
    decode_workflow_success(raw, extra)
}

fn decode_workflow_success(
    raw: RawOutcomeRow,
    extra: WorkflowSuccessColumns,
) -> Result<WorkflowSuccessOutcome, OutcomeDecodeError> {
    let WorkflowSuccessColumns {
        queue_name,
        task_name,
        node_has_dependents,
    } = extra;
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
            require_dependents_fact(node_has_dependents)?;
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
            populated.extend(no_dependents_fact(node_has_dependents));
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

/// `node_has_dependents` after the move of a workflow task: a boolean.
fn require_dependents_fact(value: Option<bool>) -> Result<(), OutcomeDecodeError> {
    match value {
        Some(_) => Ok(()),
        None => Err(OutcomeDecodeError(
            "an applied workflow task needs node_has_dependents as a boolean, got NULL".to_owned(),
        )),
    }
}

/// The column name when `node_has_dependents` is set on a row that must
/// leave it NULL.
fn no_dependents_fact(value: Option<bool>) -> Vec<&'static str> {
    match value {
        Some(_) => vec!["node_has_dependents"],
        None => Vec::new(),
    }
}

fn shared_columns(row: &Row) -> Result<RawOutcomeRow, OutcomeDecodeError> {
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

fn get<'r, T: FromSql<'r>>(row: &'r Row, column: &str) -> Result<T, OutcomeDecodeError> {
    row.try_get(column)
        .map_err(|error| OutcomeDecodeError(format!("column {column}: {error}")))
}

fn require_exact_columns(
    row: &Row,
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
