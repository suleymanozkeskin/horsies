//! The two one-statement writers of a RUNNING task's attempt.
//!
//! - `horsies_finish_failed_attempt`: the attempt row with the retry or the
//!   terminal failure (Python `horsies/core/worker/failed_attempt.py`).
//! - `horsies_finish_workflow_success`: the attempt row, the history row and
//!   the phase-2 record of a workflow task's success (Python
//!   `horsies/core/worker/workflow_success.py`).
//!
//! Each function takes the task advisory lock, then the row lock under the
//! full fence (worker and claim generation; the failed attempt also fences on
//! the retry count its delay was computed for), writes the attempt row from
//! the locked row on the database clock, and moves the row. Each runs alone
//! on the pool, so it commits alone and its locks end with it.
//!
//! A refusal writes nothing and is an outcome, not an error. A lost reply is
//! an unknown outcome: the same command sent again reports a committed
//! failure or success as `AlreadyApplied`, and a committed retry as
//! `LostClaim` (the retry ended the claim generation).

use sqlx::PgPool;
use uuid::Uuid;

use crate::core::lifecycle::{
    decode_failed_attempt_row, decode_workflow_success_row, FailedAttemptOutcome, OwnedClaim,
    RetryRefusal, WorkflowSuccessOutcome,
};

use super::error::BrokerError;

const FINISH_FAILED_ATTEMPT_FUNCTION: &str = "horsies_finish_failed_attempt";
const FINISH_WORKFLOW_SUCCESS_FUNCTION: &str = "horsies_finish_workflow_success";

const FINISH_FAILED_ATTEMPT_SQL: &str = "\
SELECT * FROM horsies_finish_failed_attempt(
    $1, $2, $3::timestamptz, $4::integer, $5::double precision, $6,
    $7, $8, $9, $10, $11, $12
)";

const FINISH_WORKFLOW_SUCCESS_SQL: &str =
    "SELECT * FROM horsies_finish_workflow_success($1, $2, $3::timestamptz, $4)";

/// Where a retried row runs next (Python `RetryPlacement`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPlacement {
    /// The row keeps its execution class.
    SameClass,
    /// The row moves to the `ISOLATED` class.
    Isolated,
}

impl RetryPlacement {
    fn as_sql(self) -> &'static str {
        match self {
            Self::SameClass => "same_class",
            Self::Isolated => "isolated",
        }
    }
}

/// The retry the task's policy asks for (Python `RetryRequest`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RetryRequest {
    /// The function writes the terminal failure.
    NoRetry,
    /// The function writes the retry, unless its next attempt would start at
    /// or after `good_until` on the database clock. `expected_retry_count`
    /// is the count the delay was computed for; it is part of the fence.
    RetryAfter {
        expected_retry_count: i32,
        delay_seconds: f64,
        placement: RetryPlacement,
    },
}

/// The outcome the attempt row records (Python `AttemptFailureKind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptFailureKind {
    /// The task failed: an error result, a result that does not decode, a
    /// timeout or a crash.
    Failed,
    /// The worker reported a failure of its own.
    WorkerFailure,
}

impl AttemptFailureKind {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Failed => "FAILED",
            Self::WorkerFailure => "WORKER_FAILURE",
        }
    }
}

/// What the attempt row records about the failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptFailure {
    pub kind: AttemptFailureKind,
    pub error_message: Option<String>,
    pub failed_reason: Option<String>,
}

/// A failed attempt of a RUNNING task, for one statement.
///
/// `result_json`, `error_code` and `failed_reason` are the terminal failure
/// of the task; `error_code` is also the code of the attempt row.
#[derive(Debug, Clone, PartialEq)]
pub struct FinishFailedAttempt {
    pub task_id: Uuid,
    pub fence: OwnedClaim,
    pub retry: RetryRequest,
    pub result_json: String,
    pub error_code: Option<String>,
    pub failed_reason: Option<String>,
    pub attempt: AttemptFailure,
}

/// The success of a RUNNING workflow task, for one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishWorkflowSuccess {
    pub task_id: Uuid,
    pub fence: OwnedClaim,
    pub result_json: String,
}

/// Run `horsies_finish_failed_attempt` on the pool and decode its one row.
///
/// Errors: a database error ([`BrokerError::Database`], retryable when the
/// connection failed), or a row that breaks the wire contract
/// ([`BrokerError::TerminalizationContract`]).
pub async fn finish_failed_attempt(
    pool: &PgPool,
    command: &FinishFailedAttempt,
) -> Result<FailedAttemptOutcome, BrokerError> {
    let (expected_retry_count, delay_seconds, placement) = match command.retry {
        RetryRequest::NoRetry => (None, None, None),
        RetryRequest::RetryAfter {
            expected_retry_count,
            delay_seconds,
            placement,
        } => (
            Some(expected_retry_count),
            Some(delay_seconds),
            Some(placement.as_sql()),
        ),
    };
    let rows = sqlx::query(FINISH_FAILED_ATTEMPT_SQL)
        .bind(command.task_id)
        .bind(&command.fence.worker_id)
        .bind(command.fence.claimed_at)
        .bind(expected_retry_count)
        .bind(delay_seconds)
        .bind(placement)
        .bind(&command.result_json)
        .bind(command.error_code.as_deref())
        .bind(command.failed_reason.as_deref())
        .bind(command.attempt.kind.as_sql())
        .bind(command.attempt.error_message.as_deref())
        .bind(command.attempt.failed_reason.as_deref())
        .fetch_all(pool)
        .await
        .map_err(BrokerError::Database)?;
    let row = exactly_one(rows, FINISH_FAILED_ATTEMPT_FUNCTION, command.task_id)?;
    let outcome = decode_failed_attempt_row(&row).map_err(|error| {
        BrokerError::TerminalizationContract(format!("{FINISH_FAILED_ATTEMPT_FUNCTION}: {error}"))
    })?;
    log_failed_attempt(command, &outcome);
    Ok(outcome)
}

/// Run `horsies_finish_workflow_success` on the pool and decode its one row.
///
/// Errors: as [`finish_failed_attempt`].
pub async fn finish_workflow_success(
    pool: &PgPool,
    command: &FinishWorkflowSuccess,
) -> Result<WorkflowSuccessOutcome, BrokerError> {
    let rows = sqlx::query(FINISH_WORKFLOW_SUCCESS_SQL)
        .bind(command.task_id)
        .bind(&command.fence.worker_id)
        .bind(command.fence.claimed_at)
        .bind(&command.result_json)
        .fetch_all(pool)
        .await
        .map_err(BrokerError::Database)?;
    let row = exactly_one(rows, FINISH_WORKFLOW_SUCCESS_FUNCTION, command.task_id)?;
    let outcome = decode_workflow_success_row(&row).map_err(|error| {
        BrokerError::TerminalizationContract(format!("{FINISH_WORKFLOW_SUCCESS_FUNCTION}: {error}"))
    })?;
    log_workflow_success(command, &outcome);
    Ok(outcome)
}

fn exactly_one(
    mut rows: Vec<sqlx::postgres::PgRow>,
    function: &str,
    task_id: Uuid,
) -> Result<sqlx::postgres::PgRow, BrokerError> {
    match rows.len() {
        1 => Ok(rows.remove(0)),
        count => Err(BrokerError::TerminalizationContract(format!(
            "{function} returned {count} rows for task {task_id}; it reports exactly one outcome"
        ))),
    }
}

/// Log the outcome with the fence and the retry it was asked for (Python
/// `failed_attempt._log_outcome`). A write is debug-level; a retry refused
/// for its deadline is info-level; a refusal or a replay is warning-level,
/// because its observation cannot be read again later.
fn log_failed_attempt(command: &FinishFailedAttempt, outcome: &FailedAttemptOutcome) {
    match outcome {
        FailedAttemptOutcome::FailureApplied {
            retry:
                RetryRefusal::PastDeadline {
                    next_retry_at,
                    good_until,
                },
            ..
        } => {
            tracing::info!(
                task_id = %command.task_id,
                %next_retry_at,
                %good_until,
                "retry skipped: the next attempt is not before good_until; the failure is terminal",
            );
            log_at_debug(
                FINISH_FAILED_ATTEMPT_FUNCTION,
                command.task_id,
                outcome.as_str(),
                &command.fence,
                &command.retry,
            );
        }
        FailedAttemptOutcome::FailureApplied {
            retry: RetryRefusal::NotRequested,
            ..
        }
        | FailedAttemptOutcome::RetryScheduled { .. } => {
            log_at_debug(
                FINISH_FAILED_ATTEMPT_FUNCTION,
                command.task_id,
                outcome.as_str(),
                &command.fence,
                &command.retry,
            );
        }
        FailedAttemptOutcome::Refused(refusal) => {
            tracing::warn!(
                function = FINISH_FAILED_ATTEMPT_FUNCTION,
                task_id = %command.task_id,
                outcome = refusal.as_str(),
                ?refusal,
                fence = "OwnedClaim",
                expected_worker_id = %command.fence.worker_id,
                expected_claimed_at = %command.fence.claimed_at,
                retry = ?command.retry,
                "failed attempt wrote nothing",
            );
        }
    }
}

fn log_workflow_success(command: &FinishWorkflowSuccess, outcome: &WorkflowSuccessOutcome) {
    match outcome {
        WorkflowSuccessOutcome::SuccessApplied { .. } => {
            tracing::debug!(
                function = FINISH_WORKFLOW_SUCCESS_FUNCTION,
                task_id = %command.task_id,
                outcome = outcome.as_str(),
                fence = "OwnedClaim",
                expected_worker_id = %command.fence.worker_id,
                expected_claimed_at = %command.fence.claimed_at,
                "workflow task success applied",
            );
        }
        WorkflowSuccessOutcome::Refused(refusal) => {
            tracing::warn!(
                function = FINISH_WORKFLOW_SUCCESS_FUNCTION,
                task_id = %command.task_id,
                outcome = refusal.as_str(),
                ?refusal,
                fence = "OwnedClaim",
                expected_worker_id = %command.fence.worker_id,
                expected_claimed_at = %command.fence.claimed_at,
                "workflow task success wrote nothing",
            );
        }
    }
}

fn log_at_debug(
    function: &str,
    task_id: Uuid,
    outcome: &str,
    fence: &OwnedClaim,
    retry: &RetryRequest,
) {
    tracing::debug!(
        function,
        %task_id,
        outcome,
        fence = "OwnedClaim",
        expected_worker_id = %fence.worker_id,
        expected_claimed_at = %fence.claimed_at,
        ?retry,
        "failed attempt applied",
    );
}

#[cfg(test)]
mod tests;
