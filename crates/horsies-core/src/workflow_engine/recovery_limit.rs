//! The bound on workflow recovery failures.
//!
//! A recovery candidate that fails is selected again in the next pass. A failure that repeats would repeat in
//! every pass, and its workflow would stay RUNNING. This module stops that: a workflow whose recovery failed with
//! a persistent error in `workflow_recovery_failure_limit` reaper passes fails closed with
//! `WORKFLOW_RECOVERY_LIMIT_REACHED`.
//!
//! A failure is persistent unless it is a retryable database or connection error. Those clear by themselves and
//! do not count, so an outage cannot fail a workflow. A panic of a candidate is persistent. The count is stored,
//! because a count in memory resets at each restart and splits across the workers that take turns at the reaper.
//!
//! Python reference: `core/workflows/recovery_limit.py` and `_run_recovery_candidate` in
//! `core/workflows/recovery.py`.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, scalar, RowDecodeError};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::{OperationalErrorCode, TaskError, TaskResult};
use crate::db::{CheckoutClass, ParameterType, Row, TypedStatement};
use crate::workflow_engine::child_spec::ChildSpecUnavailable;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::steps::{run_alone, StepContractError};

/// Characters of a failure text that the limit error and the log keep.
pub(crate) const MAX_FAILURE_TEXT_CHARS: usize = 500;

/// The width of `horsies_workflow_recovery_failures.last_error_type`.
const MAX_ERROR_TYPE_CHARS: usize = 255;

/// The error type stored for a candidate that panicked.
const PANIC_ERROR_TYPE: &str = "Panic";

/// `horsies_workflow_recovery_record_raise(workflow_id, pass_id, case, task_index, error_type, error_detail,
/// limit)`: counts one persistent raise once per pass, and at the limit writes the recovery limit, in one
/// statement in autocommit (Python `record_recovery_raise`, `core/workflows/recovery_limit.py`).
static RECORD_RAISE: TypedStatement = TypedStatement::new(
    "horsies_workflow_recovery_record_raise",
    "SELECT outcome, failed_passes, limit_write_error
     FROM horsies_workflow_recovery_record_raise($1, $2, $3, $4, $5, $6, $7)",
    &[
        ParameterType::Uuid,
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
    ],
);

/// RUNNING workflows at or over the limit. A live node keeps a workflow here, and so does a limit write that
/// keeps failing, so such a workflow is visible, not only absent from the log (Python
/// `COUNT_OVER_FAILURE_LIMIT_SQL`).
static COUNT_OVER_FAILURE_LIMIT: TypedStatement = TypedStatement::new(
    "recovery_count_over_failure_limit",
    "SELECT count(*)
     FROM horsies_workflow_recovery_failures failure
     JOIN horsies_workflows w ON w.id = failure.workflow_id
     WHERE failure.failed_passes >= $1
       AND w.status = 'RUNNING'",
    &[ParameterType::Int8],
);

/// The recovery path that ran a candidate, with the node it worked on.
///
/// Its stored name goes into `horsies_workflow_recovery_failures.last_case`; the node index goes into
/// `last_task_index`. Only the workflow-level case has no node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryCase {
    /// Case 0: a PENDING node whose dependencies are all terminal.
    PendingTerminalDeps { task_index: i32 },
    /// Case 1: a READY task node with no task.
    ReadyNotEnqueued { task_index: i32 },
    /// Case 1.5: a READY sub-workflow node with no child workflow.
    ReadySubworkflowNotStarted { task_index: i32 },
    /// Case 1.6: a terminal child whose parent node did not advance. The index is the parent node's.
    CompletedChildParentNotUpdated { parent_task_index: i32 },
    /// Cases 2 and 3: a RUNNING workflow whose nodes are all terminal.
    TerminalWorkflowRunning,
    /// The progression of a phase-2 record (phase-2 recovery). Only the limit error of a raised record names
    /// it: phase-2 recovery counts a raise on the record (`attempt_count`), never in
    /// `horsies_workflow_recovery_failures`, whose CHECK names the five other values. Python:
    /// `RecoveryCase.PHASE2_PROGRESSION`.
    Phase2Progression { task_index: i32 },
}

impl RecoveryCase {
    /// The value stored in `last_case` (the baseline CHECK `horsies_workflow_recovery_failures_case_known` names
    /// the five workflow recovery cases), and the case that a limit error names.
    pub(crate) fn stored_name(self) -> &'static str {
        match self {
            Self::PendingTerminalDeps { .. } => "pending_terminal_deps",
            Self::ReadyNotEnqueued { .. } => "ready_not_enqueued",
            Self::ReadySubworkflowNotStarted { .. } => "ready_subworkflow_not_started",
            Self::CompletedChildParentNotUpdated { .. } => "completed_child_parent_not_updated",
            Self::TerminalWorkflowRunning => "terminal_workflow_running",
            Self::Phase2Progression { .. } => "phase2_progression",
        }
    }

    /// The node the candidate worked on; `None` for the workflow-level case.
    pub(crate) fn task_index(self) -> Option<i32> {
        match self {
            Self::PendingTerminalDeps { task_index }
            | Self::ReadyNotEnqueued { task_index }
            | Self::ReadySubworkflowNotStarted { task_index }
            | Self::Phase2Progression { task_index } => Some(task_index),
            Self::CompletedChildParentNotUpdated { parent_task_index } => Some(parent_task_index),
            Self::TerminalWorkflowRunning => None,
        }
    }

    /// The node of the candidate as Python's log lines write it: the index,
    /// or `None` for the workflow-level case.
    pub(crate) fn task_index_text(self) -> String {
        match self.task_index() {
            Some(task_index) => task_index.to_string(),
            None => "None".to_owned(),
        }
    }
}

/// Whether failures of a pass count toward the bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryFailurePolicy {
    /// Count persistent failures. The reaper's global pass uses this.
    Count(CountFailures),
    /// Log failures and count nothing. The tree pass of a resume uses this: the reaper counts the same
    /// candidate.
    LogOnly,
}

/// The counting facts of one reaper pass.
///
/// Every candidate of a pass gets the same `pass_id`, so a workflow counts at most once per pass: a fan-out with
/// many failing nodes does not reach the limit in one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CountFailures {
    pub(crate) pass_id: Uuid,
    /// `RecoveryConfig::workflow_recovery_failure_limit`, validated to 3-1000 by the config.
    pub(crate) limit: u32,
}

impl CountFailures {
    /// The counting facts of a new reaper pass, with a fresh pass id.
    pub(crate) fn new_pass(limit: u32) -> Self {
        Self {
            pass_id: Uuid::new_v4(),
            limit,
        }
    }
}

/// Why a recovery candidate did not finish.
#[derive(Debug)]
pub(crate) enum CandidateFailure {
    /// The candidate returned an error.
    Error(WorkflowError),
    /// The candidate panicked. The text is the panic message, or a fixed text when the payload is not text.
    Panic(String),
}

/// Whether a failure can count toward the bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureClass {
    /// A retryable database or connection error. It clears by itself and never counts.
    Transient,
    /// Every other failure, a panic included.
    Persistent,
}

impl CandidateFailure {
    /// Classifies the failure with the core's retryable database rule (`is_retryable_sqlx_error`).
    pub(crate) fn class(&self) -> FailureClass {
        match self {
            Self::Error(WorkflowError::Database(error)) => {
                match crate::broker::is_retryable_sqlx_error(error) {
                    true => FailureClass::Transient,
                    false => FailureClass::Persistent,
                }
            }
            Self::Error(WorkflowError::Broker(error)) => match error.is_retryable() {
                true => FailureClass::Transient,
                false => FailureClass::Persistent,
            },
            // A build that did not run, or an interrupt of the process, is
            // not a property of the candidate: it is not counted.
            Self::Error(WorkflowError::ChildSpecSource(
                ChildSpecUnavailable::NotRun { .. } | ChildSpecUnavailable::Interrupted { .. },
            )) => FailureClass::Transient,
            // A build that raised raises again on the next pass: it counts, as
            // Python counts every exception of a candidate that is not a
            // connection error.
            Self::Error(WorkflowError::ChildSpecSource(ChildSpecUnavailable::Raised {
                ..
            })) => FailureClass::Persistent,
            Self::Error(
                WorkflowError::Serialization(_)
                | WorkflowError::WorkflowNotFound { .. }
                | WorkflowError::InvalidStatus(_)
                | WorkflowError::Validation(_)
                | WorkflowError::StepContract(_),
            )
            | Self::Panic(_) => FailureClass::Persistent,
        }
    }

    /// The stored error type: the variant name of the error, or `Panic`.
    pub(crate) fn error_type(&self) -> &'static str {
        match self {
            Self::Error(WorkflowError::Database(_)) => "Database",
            Self::Error(WorkflowError::Broker(_)) => "Broker",
            Self::Error(WorkflowError::Serialization(_)) => "Serialization",
            Self::Error(WorkflowError::WorkflowNotFound { .. }) => "WorkflowNotFound",
            Self::Error(WorkflowError::InvalidStatus(_)) => "InvalidStatus",
            Self::Error(WorkflowError::Validation(_)) => "Validation",
            Self::Error(WorkflowError::StepContract(_)) => "StepContract",
            Self::Error(WorkflowError::ChildSpecSource(_)) => "ChildSpecSource",
            Self::Panic(_) => PANIC_ERROR_TYPE,
        }
    }

    /// The failure text, cut to [`MAX_FAILURE_TEXT_CHARS`] characters.
    pub(crate) fn detail(&self) -> String {
        let text = match self {
            Self::Error(error) => error.to_string(),
            Self::Panic(message) => message.clone(),
        };
        text.chars().take(MAX_FAILURE_TEXT_CHARS).collect()
    }
}

/// What one recovery candidate did.
#[derive(Debug)]
pub(crate) enum CandidateOutcome<T> {
    /// The candidate returned a value; the caller reads whether it applied or was refused.
    Finished(T),
    /// The candidate failed. Its own writes did not commit, or committed only in part: a candidate runs its
    /// statements as its code says, and a later pass repairs what is left.
    Failed(FailureRecord),
}

/// What the failure bound did with one failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureRecord {
    /// Nothing was counted: the failure was transient, the pass only logs, this pass already counted the
    /// workflow, or the count write failed. The log says which.
    NotCounted,
    /// The count moved and the workflow did not change. The count is under the limit, or the limit write was
    /// refused or failed.
    Counted,
    /// The limit write stopped the workflow.
    LimitReached,
}

/// What `horsies_workflow_recovery_record_raise` did with one persistent raise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RaiseRecorded {
    /// The workflow row is gone; nothing written.
    WorkflowAbsent,
    /// An earlier raise of this workflow already counted this pass; nothing written.
    AlreadyCounted,
    /// The count moved and is under the limit.
    Counted { failed_passes: i32 },
    /// The count reached the limit and the workflow is not RUNNING (a pause or a cancel won); the count stays
    /// and the workflow did not change. A later pass after a resume tries again.
    Refused { failed_passes: i32 },
    /// The count reached the limit and no live node remained: the workflow is FAILED.
    LimitReachedFailed { failed_passes: i32 },
    /// The count reached the limit and a live node remains: the workflow stays RUNNING with its error stored,
    /// and the last node ends it.
    LimitReachedLiveNodes { failed_passes: i32 },
    /// The count reached the limit and the limit write raised; the count stays and the next failure tries
    /// again. `cause`: the SQLSTATE and message of the write.
    LimitWriteFailed { failed_passes: i32, cause: String },
}

/// The limit error in the two forms it is stored in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LimitPayloads {
    /// `horsies_workflows.error`: the error itself.
    pub(crate) workflow_error_json: String,
    /// `horsies_workflow_tasks.result` of the candidate's node: the error in its result envelope.
    pub(crate) node_result_json: String,
}

/// Runs one recovery candidate, catches its panic, and applies the failure bound to its failure.
///
/// - Inputs: the broker, the failure policy of the pass, the workflow and case of the candidate, and the
///   candidate future.
/// - Outcomes: `Finished` with the candidate's value; `Failed` with what the bound did.
/// - Side effects: on a failure, one log record with the subject and the cause, and under
///   [`RecoveryFailurePolicy::Count`] one statement that counts the failure and, at the limit, writes it.
/// - A panic is caught only here, at the boundary of one candidate. The candidate's open transaction rolls
///   back when it is dropped during the unwind.
pub(crate) async fn run_recovery_candidate<T, F>(
    broker: &PostgresBroker,
    policy: RecoveryFailurePolicy,
    workflow_id: Uuid,
    case: RecoveryCase,
    candidate: F,
) -> CandidateOutcome<T>
where
    F: Future<Output = Result<T, WorkflowError>>,
{
    let failure = match run_catching_panic(candidate).await {
        Ok(value) => return CandidateOutcome::Finished(value),
        Err(failure) => failure,
    };
    tracing::error!(
        case = case.stored_name(),
        workflow_id = %workflow_id,
        task_index = ?case.task_index(),
        error_type = failure.error_type(),
        error = %failure.detail(),
        "Workflow recovery candidate failed: case={} workflow_id={workflow_id} task_index={} \
         exception_type={} exception={}",
        case.stored_name(),
        case.task_index_text(),
        failure.error_type(),
        failure.detail(),
    );
    CandidateOutcome::Failed(
        record_candidate_failure(broker, policy, workflow_id, case, &failure).await,
    )
}

/// Runs `candidate` and catches a panic inside its polls: the boundary of one recovery candidate. The
/// candidate's open transaction rolls back when it is dropped during the unwind.
///
/// Returns the candidate's value, or its failure: its error ([`CandidateFailure::Error`]) or its panic
/// ([`CandidateFailure::Panic`]). Logs nothing.
pub(crate) async fn run_catching_panic<T, F>(candidate: F) -> Result<T, CandidateFailure>
where
    F: Future<Output = Result<T, WorkflowError>>,
{
    match (CatchPanic {
        candidate: Box::pin(candidate),
    })
    .await
    {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(CandidateFailure::Error(error)),
        Err(payload) => Err(CandidateFailure::Panic(panic_text(&*payload))),
    }
}

/// Decides whether a failure counts, and counts it.
///
/// A transient failure and a [`RecoveryFailurePolicy::LogOnly`] pass write nothing.
pub(crate) async fn record_candidate_failure(
    broker: &PostgresBroker,
    policy: RecoveryFailurePolicy,
    workflow_id: Uuid,
    case: RecoveryCase,
    failure: &CandidateFailure,
) -> FailureRecord {
    let counting = match policy {
        RecoveryFailurePolicy::LogOnly => return FailureRecord::NotCounted,
        RecoveryFailurePolicy::Count(counting) => counting,
    };
    match failure.class() {
        FailureClass::Transient => FailureRecord::NotCounted,
        FailureClass::Persistent => {
            match record_raise(broker, counting, workflow_id, case, failure).await {
                Ok(recorded) => failure_record(workflow_id, case, failure, recorded),
                Err(error) => {
                    tracing::error!(
                        workflow_id = %workflow_id,
                        %error,
                        "Workflow recovery count write failed: workflow_id={workflow_id} \
                         exception={error}",
                    );
                    FailureRecord::NotCounted
                }
            }
        }
    }
}

/// Counts one persistent failure with `horsies_workflow_recovery_record_raise`, one statement in autocommit on
/// the reserve (the reaper's class). The function builds the limit error from the stored count.
///
/// Errors: [`WorkflowError::Broker`] when the statement failed (nothing counted, or `OutcomeUnknown`);
/// [`WorkflowError::StepContract`] for a row outside its contract; [`WorkflowError::Validation`] for a limit
/// above the integer range (the config bounds it to 3-1000).
async fn record_raise(
    broker: &PostgresBroker,
    counting: CountFailures,
    workflow_id: Uuid,
    case: RecoveryCase,
    failure: &CandidateFailure,
) -> Result<RaiseRecorded, WorkflowError> {
    let limit = i32::try_from(counting.limit).map_err(|_| {
        WorkflowError::Validation(format!(
            "workflow recovery failure limit {} is above the integer range",
            counting.limit
        ))
    })?;
    let case_name = case.stored_name();
    let task_index = case.task_index();
    let error_type = stored_error_type(failure.error_type());
    let error_detail = failure.detail();
    let rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &RECORD_RAISE
            .bind()
            .value(&workflow_id)
            .value(&counting.pass_id)
            .value(&case_name)
            .value(&task_index)
            .value(&error_type)
            .value(&error_detail)
            .value(&limit)
            .finish(),
    )
    .await?;
    Ok(raise_recorded(rows)?)
}

/// Decodes the one row of `horsies_workflow_recovery_record_raise`. Pure.
pub(crate) fn raise_recorded(rows: Vec<Row>) -> Result<RaiseRecorded, StepContractError> {
    let row = one_row(rows, &RECORD_RAISE)?;
    let outcome: String = column(&row, &RECORD_RAISE, "outcome")?;
    let failed_passes =
        |row: &Row| -> Result<i32, RowDecodeError> { column(row, &RECORD_RAISE, "failed_passes") };
    match outcome.as_str() {
        "WORKFLOW_ABSENT" => Ok(RaiseRecorded::WorkflowAbsent),
        "ALREADY_COUNTED" => Ok(RaiseRecorded::AlreadyCounted),
        "COUNTED" => Ok(RaiseRecorded::Counted {
            failed_passes: failed_passes(&row)?,
        }),
        "REFUSED" => Ok(RaiseRecorded::Refused {
            failed_passes: failed_passes(&row)?,
        }),
        "LIMIT_REACHED_FAILED" => Ok(RaiseRecorded::LimitReachedFailed {
            failed_passes: failed_passes(&row)?,
        }),
        "LIMIT_REACHED_LIVE_NODES" => Ok(RaiseRecorded::LimitReachedLiveNodes {
            failed_passes: failed_passes(&row)?,
        }),
        "LIMIT_WRITE_FAILED" => Ok(RaiseRecorded::LimitWriteFailed {
            failed_passes: failed_passes(&row)?,
            cause: column(&row, &RECORD_RAISE, "limit_write_error")?,
        }),
        _ => Err(StepContractError::UnknownOutcome {
            function: RECORD_RAISE.name(),
            outcome,
        }),
    }
}

/// What the bound did with one recorded failure, with its log record.
fn failure_record(
    workflow_id: Uuid,
    case: RecoveryCase,
    failure: &CandidateFailure,
    recorded: RaiseRecorded,
) -> FailureRecord {
    match recorded {
        RaiseRecorded::WorkflowAbsent | RaiseRecorded::AlreadyCounted => FailureRecord::NotCounted,
        RaiseRecorded::Counted { failed_passes } => {
            tracing::error!(
                failed_passes,
                case = case.stored_name(),
                workflow_id = %workflow_id,
                task_index = ?case.task_index(),
                error_type = failure.error_type(),
                "workflow recovery failure counted",
            );
            FailureRecord::Counted
        }
        RaiseRecorded::Refused { failed_passes } => {
            tracing::warn!(
                workflow_id = %workflow_id,
                failed_passes,
                "workflow recovery limit refused: the workflow is not RUNNING",
            );
            FailureRecord::Counted
        }
        RaiseRecorded::LimitWriteFailed {
            failed_passes,
            cause,
        } => {
            tracing::error!(
                workflow_id = %workflow_id,
                failed_passes,
                cause = %cause,
                "workflow recovery limit write failed; the count stays and the next failure tries again",
            );
            FailureRecord::Counted
        }
        RaiseRecorded::LimitReachedFailed { failed_passes }
        | RaiseRecorded::LimitReachedLiveNodes { failed_passes } => {
            tracing::error!(
                workflow_id = %workflow_id,
                failed_passes,
                outcome = ?recorded,
                case = case.stored_name(),
                task_index = ?case.task_index(),
                error_type = failure.error_type(),
                error = %failure.detail(),
                "Workflow recovery limit reached: workflow_id={workflow_id} \
                 failed_passes={failed_passes} case={} task_index={} exception_type={} \
                 exception={}",
                case.stored_name(),
                case.task_index_text(),
                failure.error_type(),
                failure.detail(),
            );
            FailureRecord::LimitReached
        }
    }
}

/// Counts the RUNNING workflows at or over the limit, once per pass, on the reserve.
///
/// Errors: [`WorkflowError::Broker`] when the statement failed; [`WorkflowError::StepContract`] for a row that
/// does not decode.
pub(crate) async fn count_over_failure_limit(
    broker: &PostgresBroker,
    limit: u32,
) -> Result<i64, WorkflowError> {
    let limit = i64::from(limit);
    let rows: Vec<Row> = run_alone(
        broker,
        CheckoutClass::Reserved,
        &COUNT_OVER_FAILURE_LIMIT.bind().value(&limit).finish(),
    )
    .await
    .map_err(|error: BrokerError| WorkflowError::Broker(error))?;
    let row = one_row(rows, &COUNT_OVER_FAILURE_LIMIT).map_err(StepContractError::from)?;
    Ok(scalar::<i64>(&row, &COUNT_OVER_FAILURE_LIMIT).map_err(StepContractError::from)?)
}

/// The error that the limit stores. Python: `recovery_limit_error`.
pub(crate) fn recovery_limit_error(
    workflow_id: Uuid,
    case: RecoveryCase,
    failure: &CandidateFailure,
    failed_passes: i32,
) -> TaskError {
    let error_type = stored_error_type(failure.error_type());
    TaskError {
        error_code: OperationalErrorCode::WorkflowRecoveryLimitReached.into(),
        message: Some(format!(
            "Workflow recovery raised a persistent error in {failed_passes} passes. \
             Last case: {}. Last raise: {error_type}: {}",
            case.stored_name(),
            failure.detail(),
        )),
        exception: None,
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "case": case.stored_name(),
                "task_index": case.task_index(),
                "failed_passes": failed_passes,
                "exception_type": error_type,
            }),
        )),
        kind: crate::core::TaskErrorKind::Plain,
    }
}

/// Encodes the limit error once, for the workflow row and the node row.
pub(crate) fn recovery_limit_payloads(
    error: &TaskError,
) -> Result<LimitPayloads, serde_json::Error> {
    Ok(LimitPayloads {
        workflow_error_json: serde_json::to_string(error)?,
        node_result_json: serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
            error.clone(),
        ))?,
    })
}

fn stored_error_type(error_type: &str) -> String {
    error_type.chars().take(MAX_ERROR_TYPE_CHARS).collect()
}

/// The text of a panic payload. `panic!` with a literal gives `&str`; with format arguments, `String`.
fn panic_text(payload: &(dyn Any + Send)) -> String {
    match (
        payload.downcast_ref::<&str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "panic payload is not text".to_owned(),
    }
}

/// Polls a candidate and turns a panic inside a poll into an `Err` with the panic payload.
struct CatchPanic<F> {
    candidate: Pin<Box<F>>,
}

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let candidate = self.candidate.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| candidate.poll(context))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use sqlx::PgPool;

    /// A broker on the database of `pool`: the failure writes run on its core pools.
    fn broker_of(pool: &PgPool) -> PostgresBroker {
        PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust)
    }

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@localhost/postgres")
            .unwrap()
    }

    fn validation(text: &str) -> CandidateFailure {
        CandidateFailure::Error(WorkflowError::Validation(text.to_owned()))
    }

    async fn seed_workflow(pool: &PgPool, workflow_id: Uuid, status: &str, error: Option<&str>) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, definition_key, status, on_error, error, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'recovery_limit', $2, $3, 'fail', $4, 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.recovery-limit.{workflow_id}"))
        .bind(status)
        .bind(error)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_node(pool: &PgPool, workflow_id: Uuid, task_index: i32, status: &str) {
        let task_id = match status {
            "ENQUEUED" | "RUNNING" => Some(Uuid::new_v4()),
            _ => None,
        };
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies, allow_failed_deps, join_type,
                 max_retries, execution, retention_class_key, status, task_id,
                 is_subworkflow, created_at
             ) VALUES ('NONE', $1, $2, $3, $4, 'recovery_limit_task', '{}', 'default', 100,
                       '{}', FALSE, 'all', 0, 'ISOLATED', 'forever', $5, $6,
                       FALSE, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(task_index)
        .bind(format!("node_{task_index}"))
        .bind(status)
        .bind(task_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn failed_passes(pool: &PgPool, workflow_id: Uuid) -> Option<i32> {
        sqlx::query_scalar(
            "SELECT failed_passes FROM horsies_workflow_recovery_failures WHERE workflow_id = $1",
        )
        .bind(workflow_id)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    async fn workflow_status_and_error(
        pool: &PgPool,
        workflow_id: Uuid,
    ) -> (String, Option<String>) {
        sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn node_state(
        pool: &PgPool,
        workflow_id: Uuid,
        task_index: i32,
    ) -> (String, Option<String>, bool) {
        sqlx::query_as(
            "SELECT status, result, completed_at IS NOT NULL
             FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
        )
        .bind(workflow_id)
        .bind(task_index)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A candidate defect whose panic payload is `&'static str`.
    fn panic_with_literal() -> Result<(), WorkflowError> {
        panic!("candidate defect")
    }

    /// A candidate defect whose panic payload is `String`.
    fn panic_with_formatted_text(index: u32) -> Result<(), WorkflowError> {
        panic!("candidate defect at {index}")
    }

    fn counting(limit: u32) -> RecoveryFailurePolicy {
        RecoveryFailurePolicy::Count(CountFailures::new_pass(limit))
    }

    #[test]
    fn each_case_names_its_stored_value_and_node() {
        let cases = [
            (
                RecoveryCase::PendingTerminalDeps { task_index: 1 },
                "pending_terminal_deps",
                Some(1),
            ),
            (
                RecoveryCase::ReadyNotEnqueued { task_index: 2 },
                "ready_not_enqueued",
                Some(2),
            ),
            (
                RecoveryCase::ReadySubworkflowNotStarted { task_index: 3 },
                "ready_subworkflow_not_started",
                Some(3),
            ),
            (
                RecoveryCase::CompletedChildParentNotUpdated {
                    parent_task_index: 4,
                },
                "completed_child_parent_not_updated",
                Some(4),
            ),
            (
                RecoveryCase::TerminalWorkflowRunning,
                "terminal_workflow_running",
                None,
            ),
            (
                RecoveryCase::Phase2Progression { task_index: 5 },
                "phase2_progression",
                Some(5),
            ),
        ];
        for (case, name, task_index) in cases {
            assert_eq!(case.stored_name(), name);
            assert_eq!(case.task_index(), task_index);
        }
    }

    #[test]
    fn retryable_database_errors_are_transient() {
        for error in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset",
            )),
        ] {
            let failure = CandidateFailure::Error(WorkflowError::Database(error));
            assert_eq!(failure.class(), FailureClass::Transient);
            assert_eq!(failure.error_type(), "Database");
        }
    }

    #[test]
    fn defects_and_other_errors_are_persistent() {
        let failures = [
            CandidateFailure::Error(WorkflowError::Database(sqlx::Error::RowNotFound)),
            validation("bad stored data"),
            CandidateFailure::Error(WorkflowError::InvalidStatus("BOGUS".to_owned())),
            CandidateFailure::Panic("index out of bounds".to_owned()),
            CandidateFailure::Error(WorkflowError::ChildSpecSource(
                ChildSpecUnavailable::Raised {
                    subject: "build of the child of nightly:0 (definition report.v1)".to_owned(),
                    cause: "SerializationError: Corrupt task_options".to_owned(),
                },
            )),
        ];
        for failure in &failures {
            assert_eq!(failure.class(), FailureClass::Persistent);
        }
        assert_eq!(failures[3].error_type(), "Panic");
        assert_eq!(failures[4].error_type(), "ChildSpecSource");
    }

    #[test]
    fn failure_text_is_bounded() {
        let failure = CandidateFailure::Panic("é".repeat(MAX_FAILURE_TEXT_CHARS + 20));
        assert_eq!(failure.detail().chars().count(), MAX_FAILURE_TEXT_CHARS);
        assert_eq!(
            stored_error_type(&"T".repeat(MAX_ERROR_TYPE_CHARS + 1))
                .chars()
                .count(),
            MAX_ERROR_TYPE_CHARS
        );
    }

    #[test]
    fn limit_error_carries_subject_and_cause() {
        let workflow_id = Uuid::new_v4();
        let error = recovery_limit_error(
            workflow_id,
            RecoveryCase::ReadyNotEnqueued { task_index: 3 },
            &validation("node options do not parse"),
            25,
        );
        assert_eq!(
            error.error_code.to_string(),
            "WORKFLOW_RECOVERY_LIMIT_REACHED"
        );
        let message = error.message.clone().unwrap();
        assert!(message.contains("25 passes"));
        assert!(message.contains("Last case: ready_not_enqueued"));
        assert!(message.contains("Validation"));
        assert!(message.contains("node options do not parse"));
        assert_eq!(
            error.data_value(),
            Some(serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "case": "ready_not_enqueued",
                "task_index": 3,
                "failed_passes": 25,
                "exception_type": "Validation",
            }))
        );
    }

    #[test]
    fn both_payload_forms_hold_the_same_error() {
        let error = recovery_limit_error(
            Uuid::new_v4(),
            RecoveryCase::TerminalWorkflowRunning,
            &CandidateFailure::Panic("defect".to_owned()),
            3,
        );
        let payloads = recovery_limit_payloads(&error).unwrap();
        let workflow_error: TaskError =
            serde_json::from_str(&payloads.workflow_error_json).unwrap();
        let node_result: TaskResult<serde_json::Value> =
            serde_json::from_str(&payloads.node_result_json).unwrap();
        let TaskResult::Err(node_error) = node_result else {
            panic!("the node result must be an error envelope");
        };
        assert_eq!(
            serde_json::to_value(&workflow_error).unwrap(),
            serde_json::to_value(&node_error).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&workflow_error).unwrap()["data"]["task_index"],
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn a_finished_candidate_returns_its_value() {
        let outcome = run_recovery_candidate(
            &broker_of(&lazy_pool()),
            RecoveryFailurePolicy::LogOnly,
            Uuid::new_v4(),
            RecoveryCase::TerminalWorkflowRunning,
            async { Ok::<_, WorkflowError>(7_u8) },
        )
        .await;
        assert!(matches!(outcome, CandidateOutcome::Finished(7)));
    }

    #[tokio::test]
    async fn a_panic_is_caught_at_the_candidate_boundary() {
        let literal = run_recovery_candidate(
            &broker_of(&lazy_pool()),
            RecoveryFailurePolicy::LogOnly,
            Uuid::new_v4(),
            RecoveryCase::PendingTerminalDeps { task_index: 0 },
            async {
                tokio::task::yield_now().await;
                panic_with_literal()
            },
        )
        .await;
        assert!(matches!(
            literal,
            CandidateOutcome::Failed(FailureRecord::NotCounted)
        ));
        let formatted = run_recovery_candidate(
            &broker_of(&lazy_pool()),
            RecoveryFailurePolicy::LogOnly,
            Uuid::new_v4(),
            RecoveryCase::PendingTerminalDeps { task_index: 0 },
            async { panic_with_formatted_text(9) },
        )
        .await;
        assert!(matches!(
            formatted,
            CandidateOutcome::Failed(FailureRecord::NotCounted)
        ));
        assert_eq!(panic_text(&"literal"), "literal");
        assert_eq!(panic_text(&"formatted".to_owned()), "formatted");
        assert_eq!(panic_text(&17_u32), "panic payload is not text");
    }

    #[tokio::test]
    async fn a_log_only_pass_and_a_transient_failure_write_nothing() {
        let pool = lazy_pool();
        pool.close().await;
        let workflow_id = Uuid::new_v4();
        let case = RecoveryCase::TerminalWorkflowRunning;
        assert_eq!(
            record_candidate_failure(
                &broker_of(&pool),
                RecoveryFailurePolicy::LogOnly,
                workflow_id,
                case,
                &validation("persistent")
            )
            .await,
            FailureRecord::NotCounted
        );
        // The pool is closed: a write would fail and log. Neither path writes, so the record is the same.
        assert_eq!(
            record_candidate_failure(
                &broker_of(&pool),
                counting(3),
                workflow_id,
                case,
                &CandidateFailure::Error(WorkflowError::Database(sqlx::Error::PoolTimedOut))
            )
            .await,
            FailureRecord::NotCounted
        );
    }

    #[tokio::test]
    #[serial]
    async fn a_workflow_counts_once_per_pass() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", None).await;
        seed_node(&pool, workflow_id, 0, "PENDING").await;
        seed_node(&pool, workflow_id, 1, "READY").await;

        let pass = counting(5);
        for case in [
            RecoveryCase::PendingTerminalDeps { task_index: 0 },
            RecoveryCase::ReadyNotEnqueued { task_index: 1 },
        ] {
            let _counted = record_candidate_failure(
                &broker_of(&pool),
                pass,
                workflow_id,
                case,
                &validation("x"),
            )
            .await;
        }
        assert_eq!(failed_passes(&pool, workflow_id).await, Some(1));

        let record = record_candidate_failure(
            &broker_of(&pool),
            counting(5),
            workflow_id,
            RecoveryCase::ReadyNotEnqueued { task_index: 1 },
            &validation("x"),
        )
        .await;
        assert_eq!(record, FailureRecord::Counted);
        assert_eq!(failed_passes(&pool, workflow_id).await, Some(2));
        let (last_case, last_task_index, last_error_type): (String, Option<i32>, String) =
            sqlx::query_as(
                "SELECT last_case, last_task_index, last_error_type
                 FROM horsies_workflow_recovery_failures WHERE workflow_id = $1",
            )
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(last_case, "ready_not_enqueued");
        assert_eq!(last_task_index, Some(1));
        assert_eq!(last_error_type, "Validation");
        assert_eq!(
            workflow_status_and_error(&pool, workflow_id).await,
            ("RUNNING".to_owned(), None)
        );
        assert_eq!(node_state(&pool, workflow_id, 1).await.0, "READY");
    }

    #[tokio::test]
    #[serial]
    async fn the_limit_fails_a_workflow_with_no_live_node() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", None).await;
        seed_node(&pool, workflow_id, 0, "READY").await;
        seed_node(&pool, workflow_id, 1, "PENDING").await;
        seed_node(&pool, workflow_id, 2, "COMPLETED").await;
        let case = RecoveryCase::ReadyNotEnqueued { task_index: 0 };

        let records = [
            record_candidate_failure(
                &broker_of(&pool),
                counting(3),
                workflow_id,
                case,
                &validation("x"),
            )
            .await,
            record_candidate_failure(
                &broker_of(&pool),
                counting(3),
                workflow_id,
                case,
                &validation("x"),
            )
            .await,
            record_candidate_failure(
                &broker_of(&pool),
                counting(3),
                workflow_id,
                case,
                &validation("x"),
            )
            .await,
        ];
        assert_eq!(
            records,
            [
                FailureRecord::Counted,
                FailureRecord::Counted,
                FailureRecord::LimitReached
            ]
        );

        let (status, error) = workflow_status_and_error(&pool, workflow_id).await;
        assert_eq!(status, "FAILED");
        let error: TaskError = serde_json::from_str(&error.unwrap()).unwrap();
        assert_eq!(
            error.error_code.to_string(),
            "WORKFLOW_RECOVERY_LIMIT_REACHED"
        );
        let (node_status, node_result, node_done) = node_state(&pool, workflow_id, 0).await;
        assert_eq!(node_status, "FAILED");
        assert!(node_done);
        let node_result: TaskResult<serde_json::Value> =
            serde_json::from_str(&node_result.unwrap()).unwrap();
        assert!(node_result.is_err());
        let (skipped_status, _, skipped_done) = node_state(&pool, workflow_id, 1).await;
        assert_eq!(skipped_status, "SKIPPED");
        assert!(skipped_done);
        assert_eq!(node_state(&pool, workflow_id, 2).await.0, "COMPLETED");
    }

    #[tokio::test]
    #[serial]
    async fn the_limit_keeps_a_stored_first_error_and_leaves_a_live_node_to_finish() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let first_error =
            serde_json::to_string(&TaskError::new("FIRST", "first failure").unwrap()).unwrap();
        seed_workflow(&pool, workflow_id, "RUNNING", Some(&first_error)).await;
        seed_node(&pool, workflow_id, 0, "RUNNING").await;
        seed_node(&pool, workflow_id, 1, "PENDING").await;
        let case = RecoveryCase::TerminalWorkflowRunning;

        for _ in 0..2 {
            let _counted = record_candidate_failure(
                &broker_of(&pool),
                counting(3),
                workflow_id,
                case,
                &validation("x"),
            )
            .await;
        }
        let record = record_candidate_failure(
            &broker_of(&pool),
            counting(3),
            workflow_id,
            case,
            &validation("x"),
        )
        .await;
        assert_eq!(record, FailureRecord::LimitReached);

        assert_eq!(
            workflow_status_and_error(&pool, workflow_id).await,
            ("RUNNING".to_owned(), Some(first_error))
        );
        assert_eq!(node_state(&pool, workflow_id, 0).await.0, "RUNNING");
        assert_eq!(node_state(&pool, workflow_id, 1).await.0, "SKIPPED");
        assert!(
            count_over_failure_limit(&broker_of(&pool), 3)
                .await
                .unwrap()
                >= 1
        );
    }

    #[tokio::test]
    #[serial]
    async fn a_workflow_that_is_not_running_refuses_the_limit_and_keeps_the_count() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "PAUSED", None).await;
        seed_node(&pool, workflow_id, 0, "PENDING").await;
        let case = RecoveryCase::PendingTerminalDeps { task_index: 0 };

        let mut records = Vec::new();
        for _ in 0..3 {
            records.push(
                record_candidate_failure(
                    &broker_of(&pool),
                    counting(3),
                    workflow_id,
                    case,
                    &validation("x"),
                )
                .await,
            );
        }
        assert_eq!(records, vec![FailureRecord::Counted; 3]);
        assert_eq!(failed_passes(&pool, workflow_id).await, Some(3));
        assert_eq!(
            workflow_status_and_error(&pool, workflow_id).await,
            ("PAUSED".to_owned(), None)
        );
        assert_eq!(node_state(&pool, workflow_id, 0).await.0, "PENDING");
    }

    #[tokio::test]
    #[serial]
    async fn a_panicking_candidate_counts_toward_the_bound() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", None).await;
        seed_node(&pool, workflow_id, 0, "PENDING").await;

        let outcome = run_recovery_candidate(
            &broker_of(&pool),
            counting(3),
            workflow_id,
            RecoveryCase::PendingTerminalDeps { task_index: 0 },
            async { panic_with_literal().map(|()| true) },
        )
        .await;
        assert!(matches!(
            outcome,
            CandidateOutcome::Failed(FailureRecord::Counted)
        ));
        let last_error_type: String = sqlx::query_scalar(
            "SELECT last_error_type FROM horsies_workflow_recovery_failures WHERE workflow_id = $1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(last_error_type, "Panic");
    }

    #[tokio::test]
    #[serial]
    async fn the_over_limit_count_takes_running_workflows_only() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let before = count_over_failure_limit(&broker_of(&pool), 1_000)
            .await
            .unwrap();
        let running = Uuid::new_v4();
        let paused = Uuid::new_v4();
        seed_workflow(&pool, running, "RUNNING", None).await;
        seed_workflow(&pool, paused, "PAUSED", None).await;
        for workflow_id in [running, paused] {
            sqlx::query(
                "INSERT INTO horsies_workflow_recovery_failures (
                     workflow_id, failed_passes, last_pass_id, first_failed_at,
                     last_failed_at, last_case, last_task_index, last_error_type
                 ) VALUES ($1, 1000, $2, NOW(), NOW(), 'terminal_workflow_running', NULL, 'Panic')",
            )
            .bind(workflow_id)
            .bind(Uuid::new_v4())
            .execute(&pool)
            .await
            .unwrap();
        }
        assert_eq!(
            count_over_failure_limit(&broker_of(&pool), 1_000)
                .await
                .unwrap(),
            before + 1
        );
    }
}
