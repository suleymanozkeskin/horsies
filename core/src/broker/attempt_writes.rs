//! The two one-statement writers of a RUNNING task's attempt.
//!
//! - `horsies_finish_failed_attempt`: the attempt row with the retry or the
//!   terminal failure.
//! - `horsies_finish_workflow_success`: the attempt row, the history row and
//!   the phase-2 record of a workflow task's success.
//!
//! Each function takes the task advisory lock, then the row lock under the
//! full fence (worker and claim generation; the failed attempt also fences on
//! the retry count its delay was computed for), writes the attempt row from
//! the locked row on the database clock, and moves the row. Each runs alone
//! on its connection in autocommit, so it commits alone and its locks end
//! with it.
//!
//! Each write is one statement on the statement layer ([`CorePool`]): one
//! round trip once the statement is prepared on the connection, and the
//! layer's idle-ping rule at checkout. A write is a per-task write, so its
//! checkout never waits on the reserve gate ([`CheckoutClass::Reserved`]).
//!
//! A refusal writes nothing and is an outcome, not an error. A failure is one
//! of the statement layer's three outcomes ([`BrokerError::Statement`]). After
//! `OutcomeUnknown` the same command sent again reports a committed failure
//! or success as `AlreadyApplied`, a committed retry as `LostClaim` (the
//! retry ended the claim generation), and a committed release as `LostClaim`
//! or a new `Released` when the row was claimed again by this worker.

use uuid::Uuid;

use crate::broker::core_rows::StatementLog;
use crate::core::lifecycle::{
    decode_failed_attempt_row, decode_workflow_success_row, AttemptPlacement, FailedAttemptOutcome,
    OwnedClaim, RetryRefusal, UnreportedAttemptOutcome, WorkflowSuccessOutcome,
};
use crate::db::{CheckoutClass, CorePool, ParameterType, Row, StatementBound, TypedStatement};
use crate::log_targets::{FAILED_ATTEMPT, WORKFLOW_SUCCESS};

use super::error::BrokerError;

/// `horsies_finish_failed_attempt`. Parameters in order: task id, worker id,
/// claim generation, retry kind, expected retry count, retry delay in
/// seconds, retry placement, result, error code, failed reason, attempt
/// outcome, attempt message, attempt reason, claimed-row rule, attempt
/// placement kind, slot wait in microseconds. The wake argument is the
/// constant TRUE: the function sends the capacity wake of a plain task's
/// queue after its terminal move (as Python).
static FINISH_FAILED_ATTEMPT: TypedStatement = TypedStatement::new(
    "finish_failed_attempt",
    "SELECT * FROM horsies_finish_failed_attempt(\
     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, TRUE, $15, $16)",
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Float8,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int8,
    ],
);

/// `horsies_finish_workflow_success`. Parameters in order: task id, worker
/// id, claim generation, result, attempt placement kind, slot wait in
/// microseconds.
static FINISH_WORKFLOW_SUCCESS: TypedStatement = TypedStatement::new(
    "finish_workflow_success",
    "SELECT * FROM horsies_finish_workflow_success($1, $2, $3, $4, $5, $6)",
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int8,
    ],
);

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
            Self::SameClass => "SAME_CLASS",
            Self::Isolated => "ISOLATED",
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
/// `placement` is the placement of the attempt that the phase record of
/// the attempt row keeps; a release writes no attempt row and ignores it.
#[derive(Debug, Clone, PartialEq)]
pub struct FinishFailedAttempt {
    pub task_id: Uuid,
    pub fence: OwnedClaim,
    pub retry: RetryRequest,
    pub result_json: String,
    pub error_code: Option<String>,
    pub failed_reason: Option<String>,
    pub attempt: AttemptFailure,
    pub placement: AttemptPlacement,
}

/// What the statement does with a row that is still CLAIMED by this worker
/// at this claim generation (`p_claimed_row_rule`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimedRowRule {
    /// The child reported a result, so the row must be RUNNING; a CLAIMED
    /// row is a miss.
    Refuse,
    /// The child ended without a report: a CLAIMED row goes back to the
    /// queue with no attempt row.
    Release,
}

impl ClaimedRowRule {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Refuse => "REFUSE",
            Self::Release => "RELEASE",
        }
    }
}

/// The success of a RUNNING workflow task, for one statement. `placement`
/// is the placement of the attempt that the phase record of the attempt row
/// keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishWorkflowSuccess {
    pub task_id: Uuid,
    pub fence: OwnedClaim,
    pub result_json: String,
    pub placement: AttemptPlacement,
}

/// Write the failed attempt of a task whose child reported a result
/// (claimed-row rule `REFUSE`): the row must be RUNNING under the fence.
///
/// One statement on `pool`, checked out as reserved work. Writes the attempt
/// row and the retry, or the terminal failure with its capacity wake (plain
/// task) in that statement; writes nothing on a refusal.
///
/// Errors: [`BrokerError::Statement`] (nothing sent, refused by the server,
/// or an unknown outcome; see the module notes for a replay), or
/// [`BrokerError::TerminalizationContract`] when the row breaks the wire
/// contract, a `RELEASED` row included.
pub(crate) async fn finish_failed_attempt(
    pool: &CorePool,
    log: StatementLog,
    command: &FinishFailedAttempt,
) -> Result<FailedAttemptOutcome, BrokerError> {
    match run_failed_attempt(pool, log, command, ClaimedRowRule::Refuse).await? {
        UnreportedAttemptOutcome::Failed(outcome) => {
            log_failed_attempt(command, &outcome);
            Ok(outcome)
        }
        UnreportedAttemptOutcome::Released(released) => {
            Err(BrokerError::TerminalizationContract(format!(
                "{}: task {} returned RELEASED under the claimed-row rule REFUSE: {released:?}",
                FINISH_FAILED_ATTEMPT.name(),
                command.task_id,
            )))
        }
    }
}

/// Write the failed attempt of a task whose child ended without a report:
/// a timeout, a loop kill or a crash (claimed-row rule `RELEASE`).
///
/// One statement on `pool`, checked out as reserved work. A row still
/// CLAIMED by this worker at this claim generation goes back to the queue
/// with no attempt row (`Released`); any other row is written or refused as
/// [`finish_failed_attempt`] does.
///
/// Errors: as [`finish_failed_attempt`], without the `RELEASED` refusal.
pub(crate) async fn finish_unreported_attempt(
    pool: &CorePool,
    log: StatementLog,
    command: &FinishFailedAttempt,
) -> Result<UnreportedAttemptOutcome, BrokerError> {
    let outcome = run_failed_attempt(pool, log, command, ClaimedRowRule::Release).await?;
    match &outcome {
        UnreportedAttemptOutcome::Failed(failed) => log_failed_attempt(command, failed),
        UnreportedAttemptOutcome::Released(released) => tracing::debug!(
            target: FAILED_ATTEMPT,
            function = FINISH_FAILED_ATTEMPT.name(),
            task_id = %command.task_id,
            queue = %released.queue_name,
            expected_worker_id = %command.fence.worker_id,
            expected_claimed_at = %command.fence.claimed_at,
            "claimed row released: no child started it",
        ),
    }
    Ok(outcome)
}

/// Run `horsies_finish_failed_attempt` under `rule` and decode its one row.
async fn run_failed_attempt(
    pool: &CorePool,
    log: StatementLog,
    command: &FinishFailedAttempt,
    rule: ClaimedRowRule,
) -> Result<UnreportedAttemptOutcome, BrokerError> {
    let (retry_kind, expected_retry_count, delay_seconds, placement) = match command.retry {
        RetryRequest::NoRetry => ("NONE", None, None, None),
        RetryRequest::RetryAfter {
            expected_retry_count,
            delay_seconds,
            placement,
        } => (
            "RETRY",
            Some(expected_retry_count),
            Some(delay_seconds),
            Some(placement.as_sql()),
        ),
    };
    let error_code = command.error_code.as_deref();
    let failed_reason = command.failed_reason.as_deref();
    let attempt_outcome = command.attempt.kind.as_sql();
    let attempt_message = command.attempt.error_message.as_deref();
    let attempt_reason = command.attempt.failed_reason.as_deref();
    let claimed_row_rule = rule.as_sql();
    let placement_kind = command.placement.kind_sql();
    let slot_wait_us = command.placement.slot_wait_us();
    let statement = &FINISH_FAILED_ATTEMPT;
    let bound = statement
        .bind()
        .value(&command.task_id)
        .value(&command.fence.worker_id)
        .value(&command.fence.claimed_at)
        .value(&retry_kind)
        .value(&expected_retry_count)
        .value(&delay_seconds)
        .value(&placement)
        .value(&command.result_json)
        .value(&error_code)
        .value(&failed_reason)
        .value(&attempt_outcome)
        .value(&attempt_message)
        .value(&attempt_reason)
        .value(&claimed_row_rule)
        .value(&placement_kind)
        .value(&slot_wait_us)
        .finish();
    log.log(statement);
    let rows = pool
        .query(
            CheckoutClass::Reserved,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    let row = exactly_one(rows, statement, command.task_id)?;
    decode_failed_attempt_row(&row).map_err(|error| {
        BrokerError::TerminalizationContract(format!("{}: {error}", statement.name()))
    })
}

/// Write the success of a RUNNING workflow task: the attempt row, the move
/// to history and the phase-2 record, in one statement on `pool`, checked
/// out as reserved work. Writes nothing on a refusal. Sends no capacity
/// wake: phase 2 sends it.
///
/// Errors: as [`finish_failed_attempt`].
pub(crate) async fn finish_workflow_success(
    pool: &CorePool,
    log: StatementLog,
    command: &FinishWorkflowSuccess,
) -> Result<WorkflowSuccessOutcome, BrokerError> {
    let placement_kind = command.placement.kind_sql();
    let slot_wait_us = command.placement.slot_wait_us();
    let statement = &FINISH_WORKFLOW_SUCCESS;
    let bound = statement
        .bind()
        .value(&command.task_id)
        .value(&command.fence.worker_id)
        .value(&command.fence.claimed_at)
        .value(&command.result_json)
        .value(&placement_kind)
        .value(&slot_wait_us)
        .finish();
    log.log(statement);
    let rows = pool
        .query(
            CheckoutClass::Reserved,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    let row = exactly_one(rows, statement, command.task_id)?;
    let outcome = decode_workflow_success_row(&row).map_err(|error| {
        BrokerError::TerminalizationContract(format!("{}: {error}", statement.name()))
    })?;
    log_workflow_success(command, &outcome);
    Ok(outcome)
}

/// The one row of a writer: each reports exactly one outcome.
fn exactly_one(
    mut rows: Vec<Row>,
    statement: &TypedStatement,
    task_id: Uuid,
) -> Result<Row, BrokerError> {
    match rows.len() {
        1 => Ok(rows.remove(0)),
        count => Err(BrokerError::TerminalizationContract(format!(
            "{} returned {count} rows for task {task_id}; it reports exactly one outcome",
            statement.name(),
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
                target: FAILED_ATTEMPT,
                task_id = %command.task_id,
                %next_retry_at,
                %good_until,
                "retry skipped: the next attempt is not before good_until; the failure is terminal",
            );
            log_at_debug(
                FINISH_FAILED_ATTEMPT.name(),
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
                FINISH_FAILED_ATTEMPT.name(),
                command.task_id,
                outcome.as_str(),
                &command.fence,
                &command.retry,
            );
        }
        FailedAttemptOutcome::Refused(refusal) => {
            tracing::warn!(
                target: FAILED_ATTEMPT,
                function = FINISH_FAILED_ATTEMPT.name(),
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
                target: WORKFLOW_SUCCESS,
                function = FINISH_WORKFLOW_SUCCESS.name(),
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
                target: WORKFLOW_SUCCESS,
                function = FINISH_WORKFLOW_SUCCESS.name(),
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
        target: FAILED_ATTEMPT,
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
