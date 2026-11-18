//! Terminal writes of test seeds on a sqlx executor (a pool, or the
//! connection of an open transaction), for the test modules that still hold
//! a sqlx pool. The same statements as [`super::terminal_writes`], which
//! runs them on the test client; this form goes when the last of those
//! modules moves.

use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::PgExecutor;
use uuid::Uuid;

use super::terminal_writes::{
    attempt_outcome_of, CANCEL_LIVE_TASK_SQL, COMPLETE_PLAIN_TASK_SQL,
    COMPLETE_WORKFLOW_TASK_AT_SQL, COMPLETE_WORKFLOW_TASK_SQL, FAIL_RUNNING_TASK_AT_SQL,
    FAIL_RUNNING_TASK_SQL, FAIL_STALE_TASK_SQL,
};
pub(crate) use super::terminal_writes::{StaleFailure, TerminalFailure};
use crate::core::lifecycle::outcomes::decode_outcome_pg_row;
use crate::core::lifecycle::TerminalizationOutcome;

/// The success of the RUNNING plain task `task_id` that `worker_id` holds.
///
/// Errors: the database's refusal of the statement.
///
/// # Panics
///
/// When a row does not decode as an outcome.
pub(crate) async fn complete_plain_task<'e>(
    executor: impl PgExecutor<'e>,
    task_id: Uuid,
    worker_id: &str,
    result_json: &str,
) -> Result<Vec<TerminalizationOutcome>, sqlx::Error> {
    let rows = sqlx::query(COMPLETE_PLAIN_TASK_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(result_json)
        .fetch_all(executor)
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
    executor: impl PgExecutor<'e>,
    task_id: Uuid,
    worker_id: &str,
    result_json: &str,
) -> Result<Vec<TerminalizationOutcome>, sqlx::Error> {
    let rows = sqlx::query(COMPLETE_WORKFLOW_TASK_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(result_json)
        .fetch_all(executor)
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
    executor: impl PgExecutor<'e>,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: DateTime<Utc>,
    result_json: &str,
) -> Result<Vec<TerminalizationOutcome>, sqlx::Error> {
    let rows = sqlx::query(COMPLETE_WORKFLOW_TASK_AT_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(claimed_at)
        .bind(result_json)
        .fetch_all(executor)
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
    executor: impl PgExecutor<'e>,
    task_id: Uuid,
    worker_id: &str,
    failure: &TerminalFailure,
) -> Result<Vec<TerminalizationOutcome>, sqlx::Error> {
    let rows = sqlx::query(FAIL_RUNNING_TASK_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(failure.result_json.as_str())
        .bind(failure.error_code.as_deref())
        .bind(failure.failed_reason.as_deref())
        .bind(attempt_outcome_of(failure))
        .bind(failure.attempt.error_message.as_deref())
        .bind(failure.attempt.failed_reason.as_deref())
        .fetch_all(executor)
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
    executor: impl PgExecutor<'e>,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: DateTime<Utc>,
    failure: &TerminalFailure,
) -> Result<Vec<TerminalizationOutcome>, sqlx::Error> {
    let rows = sqlx::query(FAIL_RUNNING_TASK_AT_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(claimed_at)
        .bind(failure.result_json.as_str())
        .bind(failure.error_code.as_deref())
        .bind(failure.failed_reason.as_deref())
        .bind(attempt_outcome_of(failure))
        .bind(failure.attempt.error_message.as_deref())
        .bind(failure.attempt.failed_reason.as_deref())
        .fetch_all(executor)
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
    executor: impl PgExecutor<'e>,
    task_id: Uuid,
    failure: &StaleFailure,
) -> Result<Vec<TerminalizationOutcome>, sqlx::Error> {
    let rows = sqlx::query(FAIL_STALE_TASK_SQL)
        .bind(task_id)
        .bind(failure.stale_after_ms)
        .bind(failure.finalizing_stale_after_ms)
        .bind(failure.result_json.as_str())
        .bind(failure.error_code.as_str())
        .bind(failure.failed_reason.as_str())
        .fetch_all(executor)
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
    executor: impl PgExecutor<'e>,
    task_id: Uuid,
    include_running: bool,
) -> Result<Vec<TerminalizationOutcome>, sqlx::Error> {
    let rows = sqlx::query(CANCEL_LIVE_TASK_SQL)
        .bind(task_id)
        .bind(include_running)
        .fetch_all(executor)
        .await?;
    Ok(decoded("horsies_cancel_task", task_id, &rows))
}

/// The outcome of each row of `function` for `task_id`.
fn decoded(function: &str, task_id: Uuid, rows: &[PgRow]) -> Vec<TerminalizationOutcome> {
    rows.iter()
        .map(|row| {
            decode_outcome_pg_row(row).unwrap_or_else(|error| {
                panic!("{function} for task {task_id} returned a row outside the contract: {error}")
            })
        })
        .collect()
}
