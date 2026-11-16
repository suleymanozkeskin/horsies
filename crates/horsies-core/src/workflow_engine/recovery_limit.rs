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

use sqlx::PgPool;
use uuid::Uuid;

use crate::core::{OperationalErrorCode, TaskError, TaskResult};
use crate::workflow_engine::error::WorkflowError;

/// Characters of a failure text that the limit error and the log keep.
pub(crate) const MAX_FAILURE_TEXT_CHARS: usize = 500;

/// The width of `horsies_workflow_recovery_failures.last_error_type`.
const MAX_ERROR_TYPE_CHARS: usize = 255;

/// The error type stored for a candidate that panicked.
const PANIC_ERROR_TYPE: &str = "Panic";

/// Adds this pass to the stored count of one workflow. No row comes back when this pass already counted it.
const COUNT_RECOVERY_FAILURE_SQL: &str = "\
INSERT INTO horsies_workflow_recovery_failures AS failure (
    workflow_id, failed_passes, last_pass_id,
    first_failed_at, last_failed_at,
    last_case, last_task_index, last_error_type
)
VALUES ($1, 1, $2, statement_timestamp(), statement_timestamp(), $3, $4, $5)
ON CONFLICT (workflow_id) DO UPDATE
SET failed_passes = failure.failed_passes + 1,
    last_pass_id = EXCLUDED.last_pass_id,
    last_failed_at = EXCLUDED.last_failed_at,
    last_case = EXCLUDED.last_case,
    last_task_index = EXCLUDED.last_task_index,
    last_error_type = EXCLUDED.last_error_type
WHERE failure.last_pass_id <> EXCLUDED.last_pass_id
RETURNING failed_passes";

const LOCK_WORKFLOW_STATUS_SQL: &str = "\
SELECT status FROM horsies_workflows WHERE id = $1 FOR UPDATE";

const FAIL_CANDIDATE_NODE_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'FAILED', result = $3, completed_at = NOW()
WHERE workflow_id = $1 AND task_index = $2
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')";

const SKIP_UNSTARTED_NODES_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'SKIPPED', completed_at = NOW()
WHERE workflow_id = $1 AND status IN ('PENDING', 'READY')";

const KEEP_FIRST_WORKFLOW_ERROR_SQL: &str = "\
UPDATE horsies_workflows
SET error = COALESCE(error, $2), updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING'";

const HAS_LIVE_NODE_SQL: &str = "\
SELECT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks
    WHERE workflow_id = $1 AND status IN ('ENQUEUED', 'RUNNING')
)";

/// Fails the workflow and keeps its stored error.
const FAIL_WORKFLOW_KEEP_ERROR_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'FAILED', result = NULL, completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING' AND completed_at IS NULL";

/// RUNNING workflows at or over the limit. A live node keeps a workflow here, and so does a limit write that
/// keeps failing, so such a workflow is visible, not only absent from the log.
const COUNT_OVER_FAILURE_LIMIT_SQL: &str = "\
SELECT count(*)
FROM horsies_workflow_recovery_failures failure
JOIN horsies_workflows w ON w.id = failure.workflow_id
WHERE failure.failed_passes >= $1
  AND w.status = 'RUNNING'";

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
            Self::Error(
                WorkflowError::Serialization(_)
                | WorkflowError::WorkflowNotFound { .. }
                | WorkflowError::WorkflowTimeout { .. }
                | WorkflowError::WorkflowError(_)
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
            Self::Error(WorkflowError::WorkflowTimeout { .. }) => "WorkflowTimeout",
            Self::Error(WorkflowError::WorkflowError(_)) => "WorkflowError",
            Self::Error(WorkflowError::InvalidStatus(_)) => "InvalidStatus",
            Self::Error(WorkflowError::Validation(_)) => "Validation",
            Self::Error(WorkflowError::StepContract(_)) => "StepContract",
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

/// What the limit write did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LimitOutcome {
    /// No live node remained, so the workflow is FAILED.
    WorkflowFailed,
    /// A live node remains, so the workflow stays RUNNING with its error stored, and the last node ends it.
    LiveNodesFinish,
    /// The workflow was not RUNNING (a pause or a cancel won, or the row is gone). Nothing changed.
    Refused { status: Option<String> },
}

/// The result of the count write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassCount {
    /// This failure added one to the count.
    Counted { failed_passes: i32 },
    /// An earlier failure of this workflow already counted this pass.
    AlreadyCounted,
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
/// - Inputs: the pool, the failure policy of the pass, the workflow and case of the candidate, and the
///   candidate future.
/// - Outcomes: `Finished` with the candidate's value; `Failed` with what the bound did.
/// - Side effects: on a failure, one log record with the subject and the cause, and under
///   [`RecoveryFailurePolicy::Count`] the count write and, at the limit, the limit write.
/// - A panic is caught only here, at the boundary of one candidate. The candidate's open transaction rolls
///   back when it is dropped during the unwind.
pub(crate) async fn run_recovery_candidate<T, F>(
    pool: &PgPool,
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
        "workflow recovery candidate failed",
    );
    CandidateOutcome::Failed(
        record_candidate_failure(pool, policy, workflow_id, case, &failure).await,
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
    pool: &PgPool,
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
            record_persistent_failure(pool, counting, workflow_id, case, failure).await
        }
    }
}

/// Counts one persistent failure, and applies the limit when the count reaches it.
async fn record_persistent_failure(
    pool: &PgPool,
    counting: CountFailures,
    workflow_id: Uuid,
    case: RecoveryCase,
    failure: &CandidateFailure,
) -> FailureRecord {
    let error_type = stored_error_type(failure.error_type());
    let failed_passes =
        match count_failure(pool, counting.pass_id, workflow_id, case, &error_type).await {
            Err(error) => {
                tracing::error!(
                    workflow_id = %workflow_id,
                    %error,
                    "workflow recovery count write failed",
                );
                return FailureRecord::NotCounted;
            }
            Ok(PassCount::AlreadyCounted) => return FailureRecord::NotCounted,
            Ok(PassCount::Counted { failed_passes }) => failed_passes,
        };
    tracing::error!(
        failed_passes,
        limit = counting.limit,
        case = case.stored_name(),
        workflow_id = %workflow_id,
        task_index = ?case.task_index(),
        error_type = %error_type,
        "workflow recovery failure counted",
    );
    match i64::from(failed_passes) < i64::from(counting.limit) {
        true => FailureRecord::Counted,
        false => apply_limit(pool, workflow_id, case, failure, failed_passes).await,
    }
}

/// Writes the limit and logs what it did.
async fn apply_limit(
    pool: &PgPool,
    workflow_id: Uuid,
    case: RecoveryCase,
    failure: &CandidateFailure,
    failed_passes: i32,
) -> FailureRecord {
    let payloads = match recovery_limit_payloads(&recovery_limit_error(
        workflow_id,
        case,
        failure,
        failed_passes,
    )) {
        Ok(payloads) => payloads,
        Err(error) => {
            tracing::error!(
                workflow_id = %workflow_id,
                %error,
                "workflow recovery limit error did not encode",
            );
            return FailureRecord::Counted;
        }
    };
    match write_limit(pool, workflow_id, case, &payloads).await {
        Err(error) => {
            tracing::error!(
                workflow_id = %workflow_id,
                failed_passes,
                %error,
                "workflow recovery limit write failed; the count stays and the next failure tries again",
            );
            FailureRecord::Counted
        }
        Ok(LimitOutcome::Refused { status }) => {
            tracing::warn!(
                workflow_id = %workflow_id,
                status = ?status,
                "workflow recovery limit refused: the workflow is not RUNNING",
            );
            FailureRecord::Counted
        }
        Ok(outcome @ (LimitOutcome::WorkflowFailed | LimitOutcome::LiveNodesFinish)) => {
            tracing::error!(
                workflow_id = %workflow_id,
                failed_passes,
                outcome = ?outcome,
                case = case.stored_name(),
                task_index = ?case.task_index(),
                error_type = failure.error_type(),
                error = %failure.detail(),
                "workflow recovery limit reached",
            );
            FailureRecord::LimitReached
        }
    }
}

/// Adds this pass to the stored count of the workflow, in one statement.
async fn count_failure(
    pool: &PgPool,
    pass_id: Uuid,
    workflow_id: Uuid,
    case: RecoveryCase,
    error_type: &str,
) -> Result<PassCount, sqlx::Error> {
    let failed_passes: Option<i32> = sqlx::query_scalar(COUNT_RECOVERY_FAILURE_SQL)
        .bind(workflow_id)
        .bind(pass_id)
        .bind(case.stored_name())
        .bind(case.task_index())
        .bind(error_type)
        .fetch_optional(pool)
        .await?;
    Ok(match failed_passes {
        Some(failed_passes) => PassCount::Counted { failed_passes },
        None => PassCount::AlreadyCounted,
    })
}

/// Stops the workflow with SQL only, under the workflow row lock, in one transaction.
///
/// It runs no engine code, because engine code is what failed, so it does not evaluate the success policy. A
/// stored first error stays. A live node finishes the workflow later, because a terminal workflow has no node
/// that is not terminal. A parent workflow hears through recovery case 1.6.
///
/// After `Err` the transaction rolled back and the workflow did not change; the count stays.
async fn write_limit(
    pool: &PgPool,
    workflow_id: Uuid,
    case: RecoveryCase,
    payloads: &LimitPayloads,
) -> Result<LimitOutcome, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    let status: Option<String> = sqlx::query_scalar(LOCK_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(&mut *transaction)
        .await?;
    match status.as_deref() {
        Some("RUNNING") => {}
        _ => {
            transaction.rollback().await?;
            return Ok(LimitOutcome::Refused { status });
        }
    }
    if let Some(task_index) = case.task_index() {
        sqlx::query(FAIL_CANDIDATE_NODE_SQL)
            .bind(workflow_id)
            .bind(task_index)
            .bind(&payloads.node_result_json)
            .execute(&mut *transaction)
            .await?;
    }
    sqlx::query(SKIP_UNSTARTED_NODES_SQL)
        .bind(workflow_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(KEEP_FIRST_WORKFLOW_ERROR_SQL)
        .bind(workflow_id)
        .bind(&payloads.workflow_error_json)
        .execute(&mut *transaction)
        .await?;
    let has_live_node: bool = sqlx::query_scalar(HAS_LIVE_NODE_SQL)
        .bind(workflow_id)
        .fetch_one(&mut *transaction)
        .await?;
    let outcome = match has_live_node {
        true => LimitOutcome::LiveNodesFinish,
        false => {
            sqlx::query(FAIL_WORKFLOW_KEEP_ERROR_SQL)
                .bind(workflow_id)
                .execute(&mut *transaction)
                .await?;
            LimitOutcome::WorkflowFailed
        }
    };
    transaction.commit().await?;
    Ok(outcome)
}

/// Counts the RUNNING workflows at or over the limit, once per pass.
pub(crate) async fn count_over_failure_limit(
    pool: &PgPool,
    limit: u32,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(COUNT_OVER_FAILURE_LIMIT_SQL)
        .bind(i64::from(limit))
        .fetch_one(pool)
        .await
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
        data: Some(serde_json::json!({
            "workflow_id": workflow_id.to_string(),
            "case": case.stored_name(),
            "task_index": case.task_index(),
            "failed_passes": failed_passes,
            "exception_type": error_type,
        })),
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
        ];
        for failure in &failures {
            assert_eq!(failure.class(), FailureClass::Persistent);
        }
        assert_eq!(failures[3].error_type(), "Panic");
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
            error.data,
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
            &lazy_pool(),
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
            &lazy_pool(),
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
            &lazy_pool(),
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
                &pool,
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
                &pool,
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
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", None).await;
        seed_node(&pool, workflow_id, 0, "PENDING").await;
        seed_node(&pool, workflow_id, 1, "READY").await;

        let pass = counting(5);
        for case in [
            RecoveryCase::PendingTerminalDeps { task_index: 0 },
            RecoveryCase::ReadyNotEnqueued { task_index: 1 },
        ] {
            let _counted =
                record_candidate_failure(&pool, pass, workflow_id, case, &validation("x")).await;
        }
        assert_eq!(failed_passes(&pool, workflow_id).await, Some(1));

        let record = record_candidate_failure(
            &pool,
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
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", None).await;
        seed_node(&pool, workflow_id, 0, "READY").await;
        seed_node(&pool, workflow_id, 1, "PENDING").await;
        seed_node(&pool, workflow_id, 2, "COMPLETED").await;
        let case = RecoveryCase::ReadyNotEnqueued { task_index: 0 };

        let records = [
            record_candidate_failure(&pool, counting(3), workflow_id, case, &validation("x")).await,
            record_candidate_failure(&pool, counting(3), workflow_id, case, &validation("x")).await,
            record_candidate_failure(&pool, counting(3), workflow_id, case, &validation("x")).await,
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
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let first_error =
            serde_json::to_string(&TaskError::new("FIRST", "first failure").unwrap()).unwrap();
        seed_workflow(&pool, workflow_id, "RUNNING", Some(&first_error)).await;
        seed_node(&pool, workflow_id, 0, "RUNNING").await;
        seed_node(&pool, workflow_id, 1, "PENDING").await;
        let case = RecoveryCase::TerminalWorkflowRunning;

        for _ in 0..2 {
            let _counted =
                record_candidate_failure(&pool, counting(3), workflow_id, case, &validation("x"))
                    .await;
        }
        let record =
            record_candidate_failure(&pool, counting(3), workflow_id, case, &validation("x")).await;
        assert_eq!(record, FailureRecord::LimitReached);

        assert_eq!(
            workflow_status_and_error(&pool, workflow_id).await,
            ("RUNNING".to_owned(), Some(first_error))
        );
        assert_eq!(node_state(&pool, workflow_id, 0).await.0, "RUNNING");
        assert_eq!(node_state(&pool, workflow_id, 1).await.0, "SKIPPED");
        assert!(count_over_failure_limit(&pool, 3).await.unwrap() >= 1);
    }

    #[tokio::test]
    #[serial]
    async fn a_workflow_that_is_not_running_refuses_the_limit_and_keeps_the_count() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "PAUSED", None).await;
        seed_node(&pool, workflow_id, 0, "PENDING").await;
        let case = RecoveryCase::PendingTerminalDeps { task_index: 0 };

        let mut records = Vec::new();
        for _ in 0..3 {
            records.push(
                record_candidate_failure(&pool, counting(3), workflow_id, case, &validation("x"))
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
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", None).await;
        seed_node(&pool, workflow_id, 0, "PENDING").await;

        let outcome = run_recovery_candidate(
            &pool,
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
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let before = count_over_failure_limit(&pool, 1_000).await.unwrap();
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
            count_over_failure_limit(&pool, 1_000).await.unwrap(),
            before + 1
        );
    }
}
