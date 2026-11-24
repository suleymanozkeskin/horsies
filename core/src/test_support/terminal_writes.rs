//! Terminal writes of test seeds, on a test statement target
//! ([`crate::test_support::sql::Target`]): a pool, a session, or the session
//! of an open transaction (`&mut *transaction`).
//!
//! Each function runs the function that a run of the core calls for that end
//! (`baseline/0004_lifecycle_functions.sql`, the writers that
//! `migrations/1001_lifecycle_record.sql` replaces), with no retry and, for a
//! writer that takes one, no observed placement (`NOT_OBSERVED`), and
//! decodes the ten outcome columns of its row with the product decoder
//! ([`decode_outcome_row`]). A seed that needs a terminal row gets it
//! here, through the write path of a real run, never through an insert of a
//! terminal state.
//!
//! The fence is the claim of the live row: the worker that the caller names,
//! and the claim generation (`claimed_at`) that the statement reads from the
//! row it ends. The `_at` forms take the generation from the caller instead:
//! a replay after the move, when no live row is left to read it from, or a
//! fence test. A test of the generation fence of the plain success passes
//! its own generation through `broker::terminalization`.
//!
//! Every end except the cancel of a PENDING or CLAIMED row writes an attempt
//! row from the process facts of the live row (`worker_pid`, `worker_hostname`, `worker_process_name`,
//! NOT NULL in `horsies_task_attempts`), so a RUNNING seed row carries the
//! facts that `horsies_start_attempt` writes. The attempt is
//! `retry_count + 1`; an attempt row of that number that the seed wrote
//! before is replaced.

use chrono::{DateTime, Utc};
use tokio_postgres::Row;
use uuid::Uuid;

use crate::broker::attempt_writes::{AttemptFailure, AttemptFailureKind};
use crate::core::lifecycle::outcomes::decode_outcome_row;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::test_support::sql::{self, Target};

/// `horsies_complete_task_fused` with the capacity wake, as the worker's
/// plain success. Parameters: task id, worker id, result.
pub(super) const COMPLETE_PLAIN_TASK_SQL: &str = "SELECT task_id, ordinality, outcome, terminal_at,
        terminalization_kind, observed_status, observed_worker_id, observed_claimed_at,
        guard_kind, observed_guard
     FROM horsies_complete_task_fused(
         $1, $2, (SELECT t.claimed_at FROM horsies_tasks t WHERE t.id = $1), $3, TRUE,
         'NOT_OBSERVED', NULL)";

/// `horsies_finish_workflow_success`, phase 1 of a workflow task's success.
/// Parameters: task id, worker id, result.
pub(super) const COMPLETE_WORKFLOW_TASK_SQL: &str =
    "SELECT task_id, ordinality, outcome, terminal_at,
        terminalization_kind, observed_status, observed_worker_id, observed_claimed_at,
        guard_kind, observed_guard
     FROM horsies_finish_workflow_success(
         $1, $2, (SELECT t.claimed_at FROM horsies_tasks t WHERE t.id = $1), $3, 'NOT_OBSERVED',
         NULL)";

/// [`COMPLETE_WORKFLOW_TASK_SQL`] under the caller's claim generation.
/// Parameters: task id, worker id, claim generation, result.
pub(super) const COMPLETE_WORKFLOW_TASK_AT_SQL: &str =
    "SELECT task_id, ordinality, outcome, terminal_at,
        terminalization_kind, observed_status, observed_worker_id, observed_claimed_at,
        guard_kind, observed_guard
     FROM horsies_finish_workflow_success($1, $2, $3, $4, 'NOT_OBSERVED', NULL)";

/// `horsies_finish_failed_attempt` with no retry, under the claimed-row rule
/// `REFUSE` and with the capacity wake, as the worker's failure of a task
/// whose child reported. Parameters: task id, worker id, result, error code,
/// failed reason, attempt outcome, attempt message, attempt reason.
pub(super) const FAIL_RUNNING_TASK_SQL: &str = "SELECT task_id, ordinality, outcome, terminal_at,
        terminalization_kind, observed_status, observed_worker_id, observed_claimed_at,
        guard_kind, observed_guard
     FROM horsies_finish_failed_attempt(
         $1, $2, (SELECT t.claimed_at FROM horsies_tasks t WHERE t.id = $1),
         'NONE', NULL, NULL, NULL, $3, $4, $5, $6, $7, $8, 'REFUSE', TRUE, 'NOT_OBSERVED', NULL)";

/// [`FAIL_RUNNING_TASK_SQL`] under the caller's claim generation.
/// Parameters: task id, worker id, claim generation, result, error code,
/// failed reason, attempt outcome, attempt message, attempt reason.
pub(super) const FAIL_RUNNING_TASK_AT_SQL: &str =
    "SELECT task_id, ordinality, outcome, terminal_at,
        terminalization_kind, observed_status, observed_worker_id, observed_claimed_at,
        guard_kind, observed_guard
     FROM horsies_finish_failed_attempt(
         $1, $2, $3, 'NONE', NULL, NULL, NULL, $4, $5, $6, $7, $8, $9, 'REFUSE', TRUE,
         'NOT_OBSERVED', NULL)";

/// `horsies_finish_stale_attempt` with no retry, as the stale RUNNING pass
/// writes the failure: the attempt message and reason are the failed reason.
/// Parameters: task id, stale threshold (ms), finalizing threshold (ms),
/// result, error code, failed reason.
pub(super) const FAIL_STALE_TASK_SQL: &str = "SELECT task_id, ordinality, outcome, terminal_at,
        terminalization_kind, observed_status, observed_worker_id, observed_claimed_at,
        guard_kind, observed_guard
     FROM horsies_finish_stale_attempt(
         $1, 'BEAT_AGE', $2, $3, NULL, NULL, 'NONE', NULL, NULL, $4, $5, $6, $6, $6)";

/// `horsies_cancel_task`, the administrative cancel of one plain task.
/// Parameters: task id, whether a RUNNING row may be cancelled.
pub(super) const CANCEL_LIVE_TASK_SQL: &str = "SELECT task_id, ordinality, outcome, terminal_at,
        terminalization_kind, observed_status, observed_worker_id, observed_claimed_at,
        guard_kind, observed_guard
     FROM horsies_cancel_task($1, $2)";

/// A terminal failure of a RUNNING task, with no retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TerminalFailure {
    /// The stored result: the error envelope.
    pub(crate) result_json: String,
    /// The error code of the task; the attempt row carries the same code.
    pub(crate) error_code: Option<String>,
    /// The failed reason of the task.
    pub(crate) failed_reason: Option<String>,
    /// The outcome, message and reason of the attempt row.
    pub(crate) attempt: AttemptFailure,
}

impl TerminalFailure {
    /// A failure of the task: the attempt row is FAILED, has no message, and
    /// records the failed reason of the task.
    pub(crate) fn of_task(
        result_json: &str,
        error_code: Option<&str>,
        failed_reason: Option<&str>,
    ) -> TerminalFailure {
        TerminalFailure {
            result_json: result_json.to_owned(),
            error_code: error_code.map(str::to_owned),
            failed_reason: failed_reason.map(str::to_owned),
            attempt: AttemptFailure {
                kind: AttemptFailureKind::Failed,
                error_message: None,
                failed_reason: failed_reason.map(str::to_owned),
            },
        }
    }
}

/// A stale RUNNING failure, with no retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaleFailure {
    pub(crate) stale_after_ms: i32,
    pub(crate) finalizing_stale_after_ms: i32,
    /// The stored result: the error envelope.
    pub(crate) result_json: String,
    pub(crate) error_code: String,
    /// The failed reason of the task, and the message and reason of its
    /// attempt row.
    pub(crate) failed_reason: String,
}

/// The success of the RUNNING plain task `task_id` that `worker_id` holds.
///
/// Errors: the database's refusal of the statement.
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn complete_plain_task<'e>(
    target: impl Into<Target<'e>>,
    task_id: Uuid,
    worker_id: &str,
    result_json: &str,
) -> Result<Vec<TerminalizationOutcome>, sql::Error> {
    let rows = sql::query(COMPLETE_PLAIN_TASK_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(result_json)
        .fetch_all(target)
        .await?;
    Ok(decoded("horsies_complete_task_fused", task_id, &rows))
}

/// Phase 1 of the success of the RUNNING workflow task `task_id` that
/// `worker_id` holds: the move writes the phase-2 record of its node, so the
/// node row must exist first.
///
/// Errors: the database's refusal of the statement (22023 for a plain
/// task).
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn complete_workflow_task<'e>(
    target: impl Into<Target<'e>>,
    task_id: Uuid,
    worker_id: &str,
    result_json: &str,
) -> Result<Vec<TerminalizationOutcome>, sql::Error> {
    let rows = sql::query(COMPLETE_WORKFLOW_TASK_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(result_json)
        .fetch_all(target)
        .await?;
    Ok(decoded("horsies_finish_workflow_success", task_id, &rows))
}

/// [`complete_workflow_task`] under the fence of `worker_id` at
/// `claimed_at`.
///
/// Errors: the database's refusal of the statement.
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn complete_workflow_task_at<'e>(
    target: impl Into<Target<'e>>,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: DateTime<Utc>,
    result_json: &str,
) -> Result<Vec<TerminalizationOutcome>, sql::Error> {
    let rows = sql::query(COMPLETE_WORKFLOW_TASK_AT_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(claimed_at)
        .bind(result_json)
        .fetch_all(target)
        .await?;
    Ok(decoded("horsies_finish_workflow_success", task_id, &rows))
}

/// The terminal failure of the RUNNING task `task_id` that `worker_id`
/// holds, plain or workflow. The move of a workflow task writes the phase-2
/// record of its node, so the node row must exist first.
///
/// Errors: the database's refusal of the statement.
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn fail_running_task<'e>(
    target: impl Into<Target<'e>>,
    task_id: Uuid,
    worker_id: &str,
    failure: &TerminalFailure,
) -> Result<Vec<TerminalizationOutcome>, sql::Error> {
    let rows = sql::query(FAIL_RUNNING_TASK_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(failure.result_json.as_str())
        .bind(failure.error_code.as_deref())
        .bind(failure.failed_reason.as_deref())
        .bind(attempt_outcome_of(failure))
        .bind(failure.attempt.error_message.as_deref())
        .bind(failure.attempt.failed_reason.as_deref())
        .fetch_all(target)
        .await?;
    Ok(decoded("horsies_finish_failed_attempt", task_id, &rows))
}

/// [`fail_running_task`] under the fence of `worker_id` at `claimed_at`.
///
/// Errors: the database's refusal of the statement.
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn fail_running_task_at<'e>(
    target: impl Into<Target<'e>>,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: DateTime<Utc>,
    failure: &TerminalFailure,
) -> Result<Vec<TerminalizationOutcome>, sql::Error> {
    let rows = sql::query(FAIL_RUNNING_TASK_AT_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(claimed_at)
        .bind(failure.result_json.as_str())
        .bind(failure.error_code.as_deref())
        .bind(failure.failed_reason.as_deref())
        .bind(attempt_outcome_of(failure))
        .bind(failure.attempt.error_message.as_deref())
        .bind(failure.attempt.failed_reason.as_deref())
        .fetch_all(target)
        .await?;
    Ok(decoded("horsies_finish_failed_attempt", task_id, &rows))
}

/// The stale RUNNING failure of `task_id`, judged on the row's latest
/// runner beat and the thresholds of `failure`.
///
/// Errors: the database's refusal of the statement.
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn fail_stale_task<'e>(
    target: impl Into<Target<'e>>,
    task_id: Uuid,
    failure: &StaleFailure,
) -> Result<Vec<TerminalizationOutcome>, sql::Error> {
    let rows = sql::query(FAIL_STALE_TASK_SQL)
        .bind(task_id)
        .bind(failure.stale_after_ms)
        .bind(failure.finalizing_stale_after_ms)
        .bind(failure.result_json.as_str())
        .bind(failure.error_code.as_str())
        .bind(failure.failed_reason.as_str())
        .fetch_all(target)
        .await?;
    Ok(decoded("horsies_finish_stale_attempt", task_id, &rows))
}

/// The administrative cancel of the live plain task `task_id`; a RUNNING
/// row only when `include_running` is true.
///
/// Errors: the database's refusal of the statement.
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn cancel_live_task<'e>(
    target: impl Into<Target<'e>>,
    task_id: Uuid,
    include_running: bool,
) -> Result<Vec<TerminalizationOutcome>, sql::Error> {
    let rows = sql::query(CANCEL_LIVE_TASK_SQL)
        .bind(task_id)
        .bind(include_running)
        .fetch_all(target)
        .await?;
    Ok(decoded("horsies_cancel_task", task_id, &rows))
}

/// The `p_attempt_outcome` of the attempt row of `failure`.
pub(super) fn attempt_outcome_of(failure: &TerminalFailure) -> &'static str {
    match failure.attempt.kind {
        AttemptFailureKind::Failed => "FAILED",
        AttemptFailureKind::WorkerFailure => "WORKER_FAILURE",
    }
}

/// The outcome of each row of `function` for `task_id`.
fn decoded(function: &str, task_id: Uuid, rows: &[Row]) -> Vec<TerminalizationOutcome> {
    rows.iter()
        .map(|row| {
            decode_outcome_row(row).unwrap_or_else(|error| {
                panic!("{function} for task {task_id} returned a row outside the contract: {error}")
            })
        })
        .collect()
}
