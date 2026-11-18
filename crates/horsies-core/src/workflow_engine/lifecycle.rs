use crate::broker::core_rows::{column, one_row, RowDecodeError};
use crate::broker::PostgresBroker;
use crate::core::config::retention::PausedWorkflowAge;
use crate::core::workflow::handle_types::{HandleOperation, HandleOperationError, HandleResult};
#[cfg(test)]
use crate::test_support::child_specs::TestChildSpecs;
use uuid::Uuid;

use crate::db::{CheckoutClass, ParameterType, Row, TypedStatement};
use crate::workflow_engine::child_spec::ChildSpecSource;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::steps::{
    self, advance_parents, run_alone, CancelOutcome, PauseOutcome, ResumeDisposition,
    StepContractError,
};

#[cfg(test)]
fn test_uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test identity must be UUID")
}

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

/// The PAUSED workflows whose last status change is older than `$1`
/// microseconds, oldest first, at most `$2` (`idx_horsies_workflows_paused_age`).
/// Python: the candidate read of `expire_paused_workflows`
/// (`core/workflows/lifecycle.py:1019-1031`).
static PAUSED_EXPIRY_CANDIDATES: TypedStatement = TypedStatement::new(
    "paused_expiry_candidates",
    "SELECT id FROM horsies_workflows
     WHERE status = 'PAUSED'
       AND updated_at < now() - $1 * interval '1 microsecond'
     ORDER BY updated_at, id
     LIMIT $2",
    &[ParameterType::Int8, ParameterType::Int8],
);

/// `horsies_workflow_expire_paused(workflow_id, older_than_us, error)`: expires
/// one PAUSED workflow older than the age, in one statement in autocommit.
static EXPIRE_PAUSED: TypedStatement = TypedStatement::new(
    "horsies_workflow_expire_paused",
    "SELECT outcome, observed_status, expired_task_ids
     FROM horsies_workflow_expire_paused($1, $2, $3)",
    &[
        ParameterType::Uuid,
        ParameterType::Int8,
        ParameterType::Text,
    ],
);

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// What `horsies_workflow_expire_paused` did with one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PausedExpiry {
    /// The workflow is EXPIRED; the tasks behind its ENQUEUED nodes moved to
    /// history as cancelled, and its waiting nodes are SKIPPED.
    Expired { expired_task_ids: Vec<Uuid> },
    /// The workflow is not PAUSED, or was PAUSED for less than the age (a
    /// resume committed first). Nothing was written.
    NotExpired,
    /// No workflow has this id (a retention delete after the candidate
    /// read). Nothing was written.
    NotFound,
}

/// Decodes the one row of `horsies_workflow_expire_paused`. Pure.
fn paused_expiry_outcome(rows: Vec<Row>) -> Result<PausedExpiry, StepContractError> {
    let row = one_row(rows, &EXPIRE_PAUSED)?;
    let outcome: String = column(&row, &EXPIRE_PAUSED, "outcome")?;
    match outcome.as_str() {
        "EXPIRED" => Ok(PausedExpiry::Expired {
            expired_task_ids: column(&row, &EXPIRE_PAUSED, "expired_task_ids")?,
        }),
        "NOT_EXPIRED" => Ok(PausedExpiry::NotExpired),
        "NOT_FOUND" => Ok(PausedExpiry::NotFound),
        _ => Err(StepContractError::UnknownOutcome {
            function: EXPIRE_PAUSED.name(),
            outcome,
        }),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The handle catalog entry of a workflow engine error of `operation` on `workflow_id`. The error is the
/// variant's typed cause, and the message is Python's: the operation's label, the workflow id and the
/// error's text ([`HandleOperation::failure_message`]).
///
/// - No workflow has the id: `WORKFLOW_NOT_FOUND` ([`HandleOperationError::not_found`]).
/// - A database error is retryable only when the core's retryable rule (`is_retryable_sqlx_error`) says
///   so, as Python's `is_retryable_connection_error` (`core/models/workflow/handle.py:928`).
/// - A stored value that does not parse (an unknown status, a stored JSON value) is `STORED_DATA_INVALID`,
///   not retryable: the same call gets the same answer (`core/models/workflow/handle_types.py:33-57`).
/// - A failure of a step on the statement layer (`WorkflowError::Broker` with `BrokerError::Statement`) is
///   `DB_OPERATION_FAILED`, retryable by the statement layer's transience.
///
/// The other variants have no entry in the handle catalog (Python's `HandleErrorCode`,
/// `python/horsies/core/models/workflow/handle_types.py`). Each is a defect at this boundary, and the call
/// panics with the workflow id and the cause, where Python raises:
/// - `StepContract`: a step row outside its function's contract; the core and the installed baseline
///   disagree (Python raises `OutcomeDecodeError`, `python/horsies/core/workflows/lifecycle.py`).
/// - `Validation`: only the host build of a child that a resume owes gives it (a stored node input that the
///   `args_from` merge refuses, a built child spec that the tree arguments refuse); Python's resume lets
///   every exception other than a database error propagate.
pub(crate) fn handle_operation_error(
    operation: HandleOperation,
    error: WorkflowError,
    workflow_id: Uuid,
) -> HandleOperationError {
    match error {
        WorkflowError::WorkflowNotFound { .. } => HandleOperationError::not_found(workflow_id),
        // The step rolled back. A build that did not run is retryable; an
        // interrupted one, or one that raised, is not. The Python client
        // raises the interrupt, or the exception of a start or a resume,
        // again to its caller before it reads this error.
        WorkflowError::Database(_)
        | WorkflowError::Broker(_)
        | WorkflowError::ChildSpecSource(_) => HandleOperationError::DbOperationFailed {
            message: operation.failure_message(workflow_id, &error),
            retryable: error.is_retryable(),
            workflow_id,
            cause: error,
        },
        WorkflowError::InvalidStatus(_) | WorkflowError::Serialization(_) => {
            HandleOperationError::StoredDataInvalid {
                message: operation.failure_message(workflow_id, &error),
                workflow_id,
                cause: error,
            }
        }
        WorkflowError::Validation(_) | WorkflowError::StepContract(_) => panic!(
            "workflow handle operation on workflow {workflow_id} failed with an error that has no \
             handle catalog entry (a defect): {error}"
        ),
    }
}

/// Checks, once per broker handle, that the database records the handle's client family, before the
/// first write of a workflow operation (pause, resume, cancel).
///
/// A database of the other family, or with no family row, is `BrokerError::ClientFamilyMismatch`:
/// `DB_OPERATION_FAILED`, not retryable, and nothing is written. A database failure of the check is
/// `DB_OPERATION_FAILED`, retryable by the core's rule; the next call checks again.
async fn admit_workflow_write(
    broker: &PostgresBroker,
    operation: HandleOperation,
    workflow_id: Uuid,
) -> HandleResult<()> {
    broker
        .ensure_client_family_for_write()
        .await
        .map_err(|error| {
            handle_operation_error(operation, WorkflowError::Broker(error), workflow_id)
        })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Cancels a workflow tree: `horsies_workflow_cancel`, one statement in autocommit (Python
/// `WorkflowHandle.cancel_async`, `core/models/workflow/handle.py:1588-1765`).
///
/// The function locks the root row, then its PENDING, RUNNING and PAUSED descendants in `(depth, id)`
/// order, then the live tasks behind their nodes in id order, then their live nodes; it sets every locked
/// workflow CANCELLED with `completed_at`, moves the tasks behind ENQUEUED nodes to history as cancelled,
/// and skips the waiting nodes. A node whose task already started stays RUNNING until the task ends. The
/// CANCELLED status change fires the `workflow_done` trigger per row.
///
/// Outcomes:
/// - `Ok(true)`: the tree is cancelled.
/// - `Ok(false)`: the workflow is COMPLETED, FAILED, CANCELLED or EXPIRED; nothing was written.
/// - `WORKFLOW_NOT_FOUND`: no workflow has this id; nothing was written.
/// - `DB_OPERATION_FAILED`: the client family check refused the handle, or the statement failed
///   (retryable by the statement layer's transience; an `OutcomeUnknown` may have committed).
///
/// Panics: a row outside the function's contract ([`handle_operation_error`]).
///
/// The handle's client family is checked before the first write (`admit_workflow_write`).
pub async fn cancel_workflow(broker: &PostgresBroker, workflow_id: Uuid) -> HandleResult<bool> {
    admit_workflow_write(broker, HandleOperation::Cancel, workflow_id).await?;
    match Box::pin(steps::cancel(broker, workflow_id)).await {
        Ok(CancelOutcome::Cancelled {
            cancelled_workflow_ids,
            cancelled_task_ids,
        }) => {
            tracing::info!(
                workflow_id = %workflow_id,
                cancelled_workflows = cancelled_workflow_ids.len(),
                cancelled_tasks = cancelled_task_ids.len(),
                "workflow cancelled",
            );
            Ok(true)
        }
        Ok(CancelOutcome::NotCancellable) => Ok(false),
        Ok(CancelOutcome::NotFound) => Err(HandleOperationError::not_found(workflow_id)),
        Err(error) => Err(handle_operation_error(
            HandleOperation::Cancel,
            error,
            workflow_id,
        )),
    }
}

/// Expires the PAUSED workflows older than the age policy `older_than`, at most `batch_size` per call (Python
/// `expire_paused_workflows`, `core/workflows/lifecycle.py:964-1096`).
///
/// - One read of the candidates, oldest first ([`PAUSED_EXPIRY_CANDIDATES`]), then
///   `horsies_workflow_expire_paused` per candidate, each one statement in autocommit. Every statement runs on
///   the main core pool of `broker` with the reserve ([`CheckoutClass::Reserved`]): the reaper calls this.
/// - The function locks the workflow row, then the live tasks behind its nodes, then its live nodes; it writes
///   EXPIRED only while the workflow is PAUSED and older than the age, so a resume that committed first wins.
///   The stored error is `WORKFLOW_EXPIRED` with the policy and the age in seconds, and the message
///   `paused_workflow_auto_cancel_after elapsed: <age>` with Python's `str(timedelta)` of the age.
/// - The age goes to the database in exact microseconds (`<micros> * interval '1 microsecond'`, a time interval
///   with no day field), as Python sends it: no rounding, and the same age in any session time zone. The config
///   reader built the age ([`PausedWorkflowAge::parse`]), so it is a whole number of microseconds from 1 to
///   [`PausedWorkflowAge::MAX_MICROS`], and `NOW()` minus the age stays inside PostgreSQL's timestamp range.
///
/// Returns: the count of workflows that this call expired.
///
/// Errors (nothing was read or written when the arguments are refused):
/// - [`WorkflowError::Validation`]: a `batch_size` below 1.
/// - [`WorkflowError::Serialization`]: the stored error does not encode.
/// - [`WorkflowError::Broker`]: the candidate read or a candidate's statement failed. The candidates before
///   it are expired; the rest wait for the next call. A statement with a lost reply may have committed
///   (`OutcomeUnknown`).
/// - [`WorkflowError::StepContract`]: a row outside the function's contract. The candidate's statement
///   committed whatever it wrote.
pub async fn expire_paused_workflows(
    broker: &PostgresBroker,
    older_than: PausedWorkflowAge,
    batch_size: i64,
) -> Result<u64, WorkflowError> {
    if batch_size <= 0 {
        return Err(WorkflowError::Validation(format!(
            "paused expiry requires a positive batch size, got {batch_size}"
        )));
    }
    let older_than_us = older_than.micros();
    let error_json = paused_expiry_error(older_than)?;
    let candidates = run_alone(
        broker,
        CheckoutClass::Reserved,
        &PAUSED_EXPIRY_CANDIDATES
            .bind()
            .value(&older_than_us)
            .value(&batch_size)
            .finish(),
    )
    .await?;
    let workflow_ids = candidates
        .iter()
        .map(|row| column::<Uuid>(row, &PAUSED_EXPIRY_CANDIDATES, "id"))
        .collect::<Result<Vec<Uuid>, RowDecodeError>>()
        .map_err(StepContractError::from)?;
    let mut expired = 0_u64;
    for workflow_id in workflow_ids {
        let rows = run_alone(
            broker,
            CheckoutClass::Reserved,
            &EXPIRE_PAUSED
                .bind()
                .value(&workflow_id)
                .value(&older_than_us)
                .value(&error_json)
                .finish(),
        )
        .await?;
        match paused_expiry_outcome(rows)? {
            PausedExpiry::Expired { expired_task_ids } => {
                expired += 1;
                tracing::info!(
                    workflow_id = %workflow_id,
                    expired_tasks = expired_task_ids.len(),
                    "paused workflow expired by its age policy",
                );
            }
            PausedExpiry::NotExpired | PausedExpiry::NotFound => {}
        }
    }
    Ok(expired)
}

/// The stored error of a workflow that the age policy expired: `WORKFLOW_EXPIRED`, the policy name and the
/// age in seconds (Python `older_than.total_seconds()`), and the message with Python's `str(timedelta)`.
/// Pure.
///
/// Errors: [`WorkflowError::Serialization`] when the error does not encode.
fn paused_expiry_error(older_than: PausedWorkflowAge) -> Result<String, WorkflowError> {
    let micros = older_than.micros();
    let message = format!(
        "paused_workflow_auto_cancel_after elapsed: {}",
        // The age is above zero, so its absolute value is the age.
        python_timedelta_text(micros.unsigned_abs()),
    );
    Ok(serde_json::to_string(&crate::core::TaskError {
        exception: None,
        error_code: crate::core::OutcomeCode::WorkflowExpired.into(),
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "policy": "paused_workflow_auto_cancel_after",
                "older_than_seconds": micros as f64 / 1_000_000.0,
            }),
        )),
        message: Some(message),
        kind: crate::core::TaskErrorKind::Plain,
    })?)
}

/// Python's `str(timedelta)` of a duration of `total_micros` microseconds, 0 or more: `H:MM:SS`, with
/// `.ffffff` when a part below one second is set, after `N day(s), ` when the duration is a day or more. Pure.
fn python_timedelta_text(total_micros: u64) -> String {
    const MICROS_PER_SECOND: u64 = 1_000_000;
    const SECONDS_PER_DAY: u64 = 86_400;
    let total_seconds = total_micros / MICROS_PER_SECOND;
    let micros = total_micros % MICROS_PER_SECOND;
    let days = total_seconds / SECONDS_PER_DAY;
    let day_seconds = total_seconds % SECONDS_PER_DAY;
    let hours = day_seconds / 3_600;
    let minutes = (day_seconds % 3_600) / 60;
    let seconds = day_seconds % 60;
    let clock = if micros == 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{hours}:{minutes:02}:{seconds:02}.{micros:06}")
    };
    match days {
        0 => clock,
        1 => format!("1 day, {clock}"),
        days => format!("{days} days, {clock}"),
    }
}

/// Pauses a workflow tree: `horsies_workflow_pause`, one statement in autocommit (Python
/// `pause_workflow`, `core/workflows/lifecycle.py:886-994`, with `cascade_pause_to_children`).
///
/// The function moves the workflow RUNNING -> PAUSED, then its RUNNING descendants level by level in
/// `(depth, id)` order; it abandons every CLAIMED task behind an ENQUEUED or RUNNING node of the paused tree
/// (`PAUSE_ABANDON_WORKFLOW`) and puts the nodes of the abandoned tasks back to READY with no task, so a
/// resume enqueues new task rows for them. Tasks that already run go on; their results do not enqueue
/// new work until a resume. The status trigger sends one `workflow_done` per paused workflow at commit.
///
/// Outcomes:
/// - `Ok(true)`: the workflow was RUNNING and is PAUSED with its RUNNING descendants.
/// - `Ok(false)`: the workflow exists and is not RUNNING; nothing was written.
/// - `WORKFLOW_NOT_FOUND`: no workflow has this id; nothing was written.
/// - `DB_OPERATION_FAILED`: as [`cancel_workflow`].
///
/// Panics: as [`cancel_workflow`].
///
/// The handle's client family is checked before the first write (`admit_workflow_write`).
pub async fn pause_workflow(broker: &PostgresBroker, workflow_id: Uuid) -> HandleResult<bool> {
    admit_workflow_write(broker, HandleOperation::Pause, workflow_id).await?;
    match Box::pin(steps::pause(broker, workflow_id)).await {
        Ok(PauseOutcome::Paused {
            paused_workflow_ids,
            abandoned_task_ids,
        }) => {
            tracing::info!(
                workflow_id = %workflow_id,
                paused_workflows = paused_workflow_ids.len(),
                abandoned_tasks = abandoned_task_ids.len(),
                "workflow paused",
            );
            Ok(true)
        }
        Ok(PauseOutcome::NotRunning) => Ok(false),
        Ok(PauseOutcome::NotFound) => Err(HandleOperationError::not_found(workflow_id)),
        Err(error) => Err(handle_operation_error(
            HandleOperation::Pause,
            error,
            workflow_id,
        )),
    }
}

/// Resumes a workflow tree: `horsies_workflow_resume`, in a transaction (`BEGIN`, the step, one
/// `horsies_workflow_child_start` per owed child start, `COMMIT`) on the main core pool as producer work (Python
/// `resume_workflow`, `core/workflows/lifecycle.py:1084-1263`).
///
/// The function moves the workflow PAUSED -> RUNNING with its PAUSED descendants, promotes their PENDING nodes,
/// enqueues their READY nodes (task rows minted), runs the completion checks deepest first, and advances the
/// parent node of a resumed child that ended, when that parent is resumed too. A READY sub-workflow node owes a
/// child start, which runs in the same transaction.
///
/// Outcomes:
/// - `Ok(true)`: the tree was resumed. The parent advances that the step and its child starts owe run after the
///   commit ([`advance_parents`]). Then the tree recovery pass runs as before.
/// - `Ok(false)`: the workflow exists and is not PAUSED, RUNNING included; nothing was written (Python's rule).
/// - `WORKFLOW_NOT_FOUND`: no workflow has this id; nothing was written.
/// - `DB_OPERATION_FAILED`: as [`cancel_workflow`]; nothing of the step committed, or its `COMMIT` reply was
///   lost.
/// - `STORED_DATA_INVALID`: the host build of an owed child met a value that does not encode; nothing
///   committed.
/// - `RESUME_RECOVERY_FAILED`, not retryable: the resume committed, and the parent advance or the tree recovery
///   pass after it failed.
///
/// Panics ([`handle_operation_error`]), and nothing committed: a row outside a function's contract, or a host
/// build of an owed child that the core refuses (`Validation`).
///
/// The children of the owed child starts come from the client's `source` (the Python client's
/// `build_with`, the Rust client's registry); a child build that `source` did not run is
/// `DB_OPERATION_FAILED`, retryable, and nothing committed.
///
/// The handle's client family is checked before the first write (`admit_workflow_write`).
pub(crate) async fn resume_client_workflow(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    source: &dyn ChildSpecSource,
) -> HandleResult<bool> {
    Box::pin(resume_with(broker, workflow_id, source))
        .await
        .map_err(|failure| failure.into_handle_error(workflow_id))
}

/// Why [`resume_with`] failed.
#[derive(Debug)]
pub(crate) enum ResumeFailure {
    /// The resume step failed, with its own error: nothing of it committed,
    /// or its `COMMIT` reply was lost. A caller tells a child build that
    /// raised (`ChildSpecUnavailable::Raised`) from a failed statement by it.
    Step(WorkflowError),
    /// Every other failure, as the handle catalog names it: the client family
    /// check, an unknown workflow, and a failure after the commit
    /// (`RESUME_RECOVERY_FAILED`).
    Handle(HandleOperationError),
}

impl ResumeFailure {
    /// The failure as the handle catalog names it: the error of a step
    /// through [`handle_operation_error`]. Pure.
    ///
    /// Panics: as [`handle_operation_error`].
    pub(crate) fn into_handle_error(self, workflow_id: Uuid) -> HandleOperationError {
        match self {
            Self::Step(error) => {
                handle_operation_error(HandleOperation::Resume, error, workflow_id)
            }
            Self::Handle(error) => error,
        }
    }
}

/// The body of [`resume_client_workflow`] with the children built by
/// `build`. Outcomes as [`resume_client_workflow`]. The error of the step is
/// [`ResumeFailure::Step`], unconverted, and every other failure is
/// [`ResumeFailure::Handle`]. The monitoring resume action calls it with the
/// build of its process.
pub(crate) async fn resume_with(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    build: &dyn ChildSpecSource,
) -> Result<bool, ResumeFailure> {
    admit_workflow_write(broker, HandleOperation::Resume, workflow_id)
        .await
        .map_err(ResumeFailure::Handle)?;
    let committed = Box::pin(steps::resume(broker, workflow_id, build))
        .await
        .map_err(ResumeFailure::Step)?;
    match committed.disposition {
        ResumeDisposition::NotFound => {
            return Err(ResumeFailure::Handle(HandleOperationError::not_found(
                workflow_id,
            )))
        }
        ResumeDisposition::NotPaused => return Ok(false),
        ResumeDisposition::Resumed => {}
    }
    Box::pin(advance_parents(
        broker,
        CheckoutClass::Batch,
        committed.parent_advances,
        build,
    ))
    .await
    .map_err(|failure| {
        ResumeFailure::Handle(resume_recovery_failed(
            workflow_id,
            AfterResumeCommit::ParentAdvance {
                child_workflow_id: failure.failed_child_workflow_id,
            },
            failure.error,
        ))
    })?;
    // Recovery completion pass: a child workflow may have COMPLETED while the parent was paused, leaving the
    // parent's sub-workflow node stale. The sweep is restricted to this workflow's tree (Python
    // `core/workflows/lifecycle.py:1234-1262`).
    Box::pin(crate::workflow_engine::recovery::recover_workflow_tree(
        broker,
        build,
        workflow_id,
    ))
    .await
    .map_err(|error| {
        ResumeFailure::Handle(resume_recovery_failed(
            workflow_id,
            AfterResumeCommit::TreeRecovery,
            error,
        ))
    })?;
    Ok(true)
}

/// The work after a resume commit that failed.
enum AfterResumeCommit {
    /// The advance of the parent of `child_workflow_id`, a child workflow
    /// that the resume ended.
    ParentAdvance { child_workflow_id: Uuid },
    /// The recovery pass over the tree of the resumed workflow.
    TreeRecovery,
}

/// The error of a resume that committed and whose work after the commit failed: `RESUME_RECOVERY_FAILED`, not
/// retryable (the workflow is RUNNING). The message names the failed work as the Python resume did
/// (`tests/dualrun/reference/workflow_resume_caller.py`); the cause is `error`, so the exception of the
/// client has the error's own text. Pure.
fn resume_recovery_failed(
    workflow_id: Uuid,
    failed: AfterResumeCommit,
    error: WorkflowError,
) -> HandleOperationError {
    let message = match failed {
        AfterResumeCommit::ParentAdvance { child_workflow_id } => format!(
            "Workflow {workflow_id} resumed; the parent advance of child workflow \
             {child_workflow_id} failed: {error}"
        ),
        AfterResumeCommit::TreeRecovery => format!(
            "Workflow {workflow_id} resumed; the recovery pass for its tree failed: {error}"
        ),
    };
    HandleOperationError::ResumeRecoveryFailed {
        message,
        workflow_id,
        cause: error,
    }
}

#[cfg(test)]
mod resume_idempotency_tests {
    //! Resume outcomes through `horsies_workflow_resume`.
    use super::*;
    use crate::broker::PostgresBroker;
    use crate::test_support::sql::{self, TestPool};
    use serial_test::serial;
    use uuid::Uuid;

    /// A child row names its parent node (`horsies_workflows_parent_pair`) and sits one level below its
    /// parent (`horsies_workflows_root_depth`).
    async fn insert_workflow(pool: &TestPool, id: &str, status: &str, parent: Option<&str>) {
        sql::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id, parent_workflow_id, parent_task_index,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1::uuid, 'c8_wf', $2, 'fail', NULL,
                'test.c8.v1', $3, $4::uuid, $5::uuid, $6,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(test_uuid(id))
        .bind(status)
        .bind(if parent.is_some() { 1_i32 } else { 0_i32 })
        .bind(test_uuid(parent.unwrap_or(id)))
        .bind(parent.map(test_uuid))
        .bind(parent.map(|_| 0_i32))
        .execute(pool)
        .await
        .expect("insert workflow");
    }

    // A RUNNING task keeps its workflow non-terminal (untouched by re-evaluation).
    async fn insert_running_task(pool: &TestPool, wf_id: &str) {
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'node_0', 'c8_task', 'null', 'default', 100, '{}', FALSE, 'all',
                'RUNNING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(wf_id))
        .execute(pool)
        .await
        .expect("insert task");
    }

    async fn status_of(pool: &TestPool, id: &str) -> String {
        sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(id))
            .fetch_one(pool)
            .await
            .expect("status")
    }

    async fn cleanup(pool: &TestPool, ids: &[&str]) {
        for id in ids {
            sql::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid")
                .bind(test_uuid(id))
                .execute(pool)
                .await
                .ok();
        }
        // Delete children before parents (FK).
        for id in ids.iter().rev() {
            sql::query("DELETE FROM horsies_workflows WHERE id = $1::uuid")
                .bind(test_uuid(id))
                .execute(pool)
                .await
                .ok();
        }
    }

    /// Python `resume_workflow` (`core/workflows/lifecycle.py:1084-1263`): a
    /// resume changes only a PAUSED workflow. On a RUNNING parent it returns
    /// `false` and changes nothing, also when a child paused on its own (its
    /// `on_error = pause`); that child is resumed by its own handle. The
    /// stranded state that the earlier rule recovered cannot arise when the
    /// status change and the cascade commit together (#872).
    #[tokio::test]
    #[serial]
    async fn resume_of_a_running_workflow_writes_nothing() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let child_specs = TestChildSpecs::empty();

        let parent = Uuid::new_v4().to_string();
        let child = Uuid::new_v4().to_string();
        // A RUNNING parent with a child that paused on its own.
        insert_workflow(&pool, &parent, "RUNNING", None).await;
        insert_running_task(&pool, &parent).await;
        insert_workflow(&pool, &child, "PAUSED", Some(&parent)).await;
        insert_running_task(&pool, &child).await;

        let resumed = resume_client_workflow(
            &broker,
            Uuid::parse_str(&parent).expect("test identity must be UUID"),
            &child_specs,
        )
        .await
        .expect("resume");
        assert!(!resumed, "a resume of a RUNNING workflow changes nothing");
        assert_eq!(status_of(&pool, &parent).await, "RUNNING");
        assert_eq!(
            status_of(&pool, &child).await,
            "PAUSED",
            "a child that paused on its own stays PAUSED",
        );

        cleanup(&pool, &[&parent, &child]).await;
    }

    #[tokio::test]
    #[serial]
    async fn resume_on_consistent_running_workflow_is_noop_false() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let child_specs = TestChildSpecs::empty();

        let id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &id, "RUNNING", None).await;
        insert_running_task(&pool, &id).await;

        // No paused children, no ready nodes → genuinely nothing to do.
        let resumed = resume_client_workflow(
            &broker,
            Uuid::parse_str(&id).expect("test identity must be UUID"),
            &child_specs,
        )
        .await
        .expect("resume");
        assert!(
            !resumed,
            "a consistent RUNNING workflow resume must return Ok(false)"
        );

        cleanup(&pool, &[&id]).await;
    }

    /// A READY task node whose `args_from` names a member of its static input fails on resume with
    /// Python's conflict error and gets no task row (`core/workflows/engine.py:834-839`).
    #[tokio::test]
    #[serial]
    async fn resume_fails_a_ready_node_whose_args_from_names_a_static_member() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        let workflow_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &workflow_id, "PAUSED", None).await;
        for (index, status, dependencies, input, args_from) in [
            (0, "COMPLETED", "{}", "{}", None),
            (1, "READY", "{0}", r#"{"x": 5}"#, Some(r#"{"x": 0}"#)),
        ] {
            sql::query(
                "INSERT INTO horsies_workflow_tasks (
                    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                    dependencies, args_from, allow_failed_deps, join_type, status, is_subworkflow,
                    created_at, max_retries, execution, retention_class_key
                ) VALUES (
                    'NONE', $1, $2::uuid, $3, $4, 'conflict_task', $5, 'default', 100, $6::int[],
                    $7::jsonb, FALSE, 'all', $8, FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
                )",
            )
            .bind(Uuid::new_v4())
            .bind(&workflow_id)
            .bind(index)
            .bind(format!("node_{index}"))
            .bind(input)
            .bind(dependencies)
            .bind(args_from)
            .bind(status)
            .execute(&pool)
            .await
            .expect("insert node");
        }

        assert!(resume_client_workflow(
            &broker,
            Uuid::parse_str(&workflow_id).unwrap(),
            &TestChildSpecs::empty()
        )
        .await
        .expect("resume"));

        let (status, task_id, result): (String, Option<Uuid>, Option<String>) = sql::query_as(
            "SELECT status, task_id, result FROM horsies_workflow_tasks
             WHERE workflow_id = $1::uuid AND task_index = 1",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read the node");
        assert_eq!(status, "FAILED");
        assert!(task_id.is_none());
        let stored: crate::core::TaskResult<serde_json::Value> =
            serde_json::from_str(&result.expect("the failed node stores its error"))
                .expect("stored node result");
        let crate::core::TaskResult::Err(error) = stored else {
            panic!("the failed node must store an error");
        };
        assert_eq!(
            error.message.as_deref(),
            Some("args_from key 'x' conflicts with static kwarg (task 1)")
        );

        cleanup(&pool, &[&workflow_id]).await;
    }

    /// The resume mints the task row of a READY node with the typed facts of the node row: its
    /// retention class, no command fingerprint, no retained input, and the node's enqueue time.
    #[tokio::test]
    #[serial]
    async fn p6_workflow_enqueue_lifecycle_resume_persists_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        let workflow_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &workflow_id, "PAUSED", None).await;
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1::uuid, $2::uuid, 0, 'p6_resume', 'p6_resume_task', '{\"value\": 1}', 'bulk', 17, '{}',
                FALSE, 'all', 'READY', FALSE, NOW(), 0, 'ISOLATED', 'standard_7d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .expect("insert ready resume node");
        sql::query(
            "UPDATE horsies_workflows SET sent_at = timestamptz '2026-01-01 00:00:00+00'
             WHERE id = $1::uuid",
        )
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .expect("set the start call time");

        assert!(resume_client_workflow(
            &broker,
            Uuid::parse_str(&workflow_id).unwrap(),
            &TestChildSpecs::empty()
        )
        .await
        .expect("resume"));
        // The node holds its enqueue time: the node write and the task insert share a transaction.
        let node_holds_enqueue_time: bool = sql::query_scalar(
            "SELECT wt.started_at IS NOT NULL AND wt.started_at = t.enqueued_at
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1::uuid AND wt.task_index = 0",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read the resume node enqueue time");
        assert!(node_holds_enqueue_time);
        let facts: (String, Option<i32>, Option<i16>, bool) = sql::query_as(
            "SELECT t.retention_class_key, octet_length(t.command_fingerprint),
                    t.command_fingerprint_version, t.retain_rerun_input
             FROM horsies_tasks t
             JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
             WHERE wt.workflow_id = $1::uuid AND wt.task_index = 0",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read resume enqueue facts");
        // The class is the node row's, stored at start.
        assert_eq!(facts.0, "standard_7d");
        assert!(facts.1.is_none());
        assert!(facts.2.is_none());
        assert!(!facts.3);
        // The task row stores the start call time of its workflow as `sent_at`.
        let sent_at_is_the_start_call_time: bool = sql::query_scalar(
            "SELECT t.sent_at = timestamptz '2026-01-01 00:00:00+00'
             FROM horsies_tasks t
             JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
             WHERE wt.workflow_id = $1::uuid AND wt.task_index = 0",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read resume task sent_at");
        assert!(sent_at_is_the_start_call_time);

        sql::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid
             )",
        )
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .ok();
        cleanup(&pool, &[&workflow_id]).await;
    }
}

#[cfg(test)]
mod p7_lifecycle_tests {
    use super::*;
    use crate::core::{OutcomeCode, TaskError};
    use crate::test_support::sql::{self, TestPool};
    use serial_test::serial;

    /// An age that the config reader accepted. A test defect panics.
    fn age(duration: chrono::Duration) -> PausedWorkflowAge {
        PausedWorkflowAge::parse(duration).expect("a test age of whole microseconds above zero")
    }

    /// Python's `str(timedelta)` of the paused age: a zero duration is `0:00:00`, the smallest age has six
    /// digits after the point, and a day or more names its days. A negative or a zero age never reaches the
    /// text: the config reader refuses it (`config::retention` tests).
    #[test]
    fn the_paused_age_text_is_python_s_timedelta_text() {
        assert_eq!(python_timedelta_text(0), "0:00:00");
        assert_eq!(python_timedelta_text(1), "0:00:00.000001");
        assert_eq!(
            python_timedelta_text(86_401_500_000),
            "1 day, 0:00:01.500000"
        );
        assert_eq!(
            python_timedelta_text(2 * 86_400_000_000 + 3_600_000_000),
            "2 days, 1:00:00"
        );
    }

    /// A broker on a port that refuses connections: a refusal of the arguments must come before any read.
    fn unreachable_broker() -> PostgresBroker {
        PostgresBroker::from_test_url(
            "postgres://postgres@127.0.0.1:1/horsies_unreachable",
            crate::ClientFamily::Rust,
        )
    }

    /// A batch size below 1 is refused before any read.
    #[tokio::test]
    async fn a_batch_size_below_one_is_refused() {
        let refused =
            expire_paused_workflows(&unreachable_broker(), age(chrono::Duration::hours(1)), 0)
                .await;
        assert!(
            matches!(refused, Err(WorkflowError::Validation(_))),
            "{refused:?}"
        );
    }

    /// A failed candidate read is a broker error.
    #[tokio::test]
    async fn a_failed_candidate_read_is_a_broker_error() {
        let failed =
            expire_paused_workflows(&unreachable_broker(), age(chrono::Duration::hours(1)), 10)
                .await;
        assert!(
            matches!(failed, Err(WorkflowError::Broker(_))),
            "{failed:?}"
        );
    }

    /// The age goes to the database in exact microseconds, as Python's `older_than // timedelta(microseconds=1)`:
    /// a part below one millisecond is kept, and one day is 86 400 000 000 microseconds. The sweep sends
    /// `PausedWorkflowAge::micros`; the config reader's tests pin the refusals (a negative or zero age, a part
    /// below one microsecond, an age longer than `PausedWorkflowAge::MAX_DAYS`).
    #[test]
    fn the_paused_age_is_exact_microseconds() {
        assert_eq!(age(chrono::Duration::microseconds(500)).micros(), 500);
        assert_eq!(age(chrono::Duration::microseconds(1_500)).micros(), 1_500);
        assert_eq!(age(chrono::Duration::hours(1)).micros(), 3_600_000_000);
        assert_eq!(age(chrono::Duration::days(1)).micros(), 86_400_000_000);
    }

    /// The stored error carries the age in seconds with Python's `total_seconds()` precision (microseconds).
    #[test]
    fn the_stored_error_names_the_policy_and_the_age() {
        let error: TaskError = serde_json::from_str(
            &paused_expiry_error(age(chrono::Duration::microseconds(3_600_000_250))).unwrap(),
        )
        .unwrap();
        assert_eq!(error.error_code, OutcomeCode::WorkflowExpired.into());
        assert_eq!(
            error.message.as_deref(),
            Some("paused_workflow_auto_cancel_after elapsed: 1:00:00.000250"),
        );
        assert_eq!(
            error.data_value(),
            Some(serde_json::json!({
                "policy": "paused_workflow_auto_cancel_after",
                "older_than_seconds": 3_600.000_25,
            })),
        );
    }

    async fn seed_workflow(pool: &TestPool, id: Uuid, status: &str, parent: Option<Uuid>) {
        sql::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, parent_workflow_id, parent_task_index,
                 sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_lifecycle', $2, 'fail', $3, $4, $5, $6, $7,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(id)
        .bind(status)
        .bind(format!("test.p7.lifecycle.{id}"))
        .bind(if parent.is_some() { 1_i32 } else { 0_i32 })
        .bind(parent.unwrap_or(id))
        .bind(parent)
        .bind(parent.map(|_| 0_i32))
        .execute(pool)
        .await
        .expect("seed workflow");
    }

    async fn seed_claimed_backing(pool: &TestPool, workflow_id: Uuid, task_id: Uuid) {
        sql::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
                claimed_at, claimed_by_worker_id, max_retries, retry_count, is_workflow_task,
                command_fingerprint_version, command_fingerprint, retention_class_key,
                retain_rerun_input, created_at, updated_at, execution, claim_expires_at
            ) VALUES (
                $1, 'p7_lifecycle_task', 'default', 100, 'null', 'CLAIMED', NOW(), NOW(),
                NOW(), 'p7-worker', 0, 0, TRUE, NULL, NULL, 'forever', FALSE, NOW(), NOW(),
                'ISOLATED', NOW() + interval '1 hour'
            )",
        )
        .bind(task_id)
        .execute(pool)
        .await
        .expect("seed backing task");
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'root', 'p7_lifecycle_task', '{}', 'default', 100, '{}', FALSE,
                'all', 'ENQUEUED', FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(task_id)
        .execute(pool)
        .await
        .expect("seed workflow node");
    }

    async fn seed_running_node(pool: &TestPool, workflow_id: Uuid) {
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'root', 'p7_running_node', 'null', 'default', 100, '{}', FALSE,
                'all', 'RUNNING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("seed running workflow node");
    }

    #[tokio::test]
    #[serial]
    async fn resume_recovery_is_restricted_to_the_resumed_workflow_tree() {
        let pool = crate::test_support::migrated_pool().await;
        let root_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let unrelated_id = Uuid::new_v4();
        seed_workflow(&pool, root_id, "PAUSED", None).await;
        seed_running_node(&pool, root_id).await;
        seed_workflow(&pool, child_id, "RUNNING", Some(root_id)).await;
        seed_workflow(&pool, unrelated_id, "RUNNING", None).await;

        assert!(resume_client_workflow(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            root_id,
            &TestChildSpecs::empty()
        )
        .await
        .expect("resume workflow tree"));

        let child_status: String =
            sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(child_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let unrelated_status: String =
            sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(unrelated_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(child_status, "FAILED", "tree-local orphan must recover");
        assert_eq!(
            unrelated_status, "RUNNING",
            "resume must not run global workflow recovery",
        );
    }

    #[tokio::test]
    #[serial]
    async fn pause_relocates_claimed_backing_and_resume_mints_a_fresh_uuid7() {
        let pool = crate::test_support::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let old_task_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", None).await;
        seed_claimed_backing(&pool, workflow_id, old_task_id).await;

        assert!(pause_workflow(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            workflow_id
        )
        .await
        .unwrap());
        let paused: (String, String, Option<Uuid>) = sql::query_as(
            "SELECT w.status, wt.status, wt.task_id
             FROM horsies_workflows w
             JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
             WHERE w.id = $1 AND wt.task_index = 0",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(paused, ("PAUSED".to_owned(), "READY".to_owned(), None));
        let archived: (String, String) = sql::query_as(
            "SELECT status, terminalization_kind
             FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(old_task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            archived,
            ("CANCELLED".to_owned(), "PAUSE_ABANDON_WORKFLOW".to_owned())
        );

        assert!(resume_client_workflow(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            workflow_id,
            &TestChildSpecs::empty()
        )
        .await
        .unwrap());
        let fresh: (String, Uuid) = sql::query_as(
            "SELECT status, task_id FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(fresh.0, "ENQUEUED");
        assert_ne!(fresh.1, old_task_id);
        assert_eq!(fresh.1.get_version_num(), 7);
    }

    #[tokio::test]
    #[serial]
    async fn paused_expiry_stores_structured_error_and_archives_backing_task() {
        let pool = crate::test_support::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "PAUSED", None).await;
        seed_claimed_backing(&pool, workflow_id, task_id).await;
        sql::query(
            "UPDATE horsies_workflows SET updated_at = NOW() - INTERVAL '2 hours' WHERE id = $1",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let expired = expire_paused_workflows(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            age(chrono::Duration::hours(1)),
            10,
        )
        .await
        .unwrap();
        assert_eq!(expired, 1);
        let (status, error_json, node_status): (String, String, String) = sql::query_as(
            "SELECT w.status, w.error, wt.status
             FROM horsies_workflows w
             JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
             WHERE w.id = $1 AND wt.task_index = 0",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "EXPIRED");
        assert_eq!(node_status, "SKIPPED");
        let error: TaskError = serde_json::from_str(&error_json).unwrap();
        assert_eq!(error.error_code, OutcomeCode::WorkflowExpired.into());
        assert_eq!(
            error.message.as_deref(),
            Some("paused_workflow_auto_cancel_after elapsed: 1:00:00"),
        );
        let archived: (String, String) = sql::query_as(
            "SELECT status, terminalization_kind
             FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            archived,
            (
                "CANCELLED".to_owned(),
                "WORKFLOW_CANCEL_WORKFLOW".to_owned(),
            )
        );
    }

    #[tokio::test]
    #[serial]
    async fn expired_child_fails_its_parent_node_and_parent_workflow() {
        let pool = crate::test_support::migrated_pool().await;
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        seed_workflow(&pool, parent_id, "RUNNING", None).await;
        seed_workflow(&pool, child_id, "PAUSED", Some(parent_id)).await;
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
                 priority, dependencies, allow_failed_deps, join_type, status,
                 is_subworkflow, sub_workflow_params, sub_workflow_id, created_at
             ) VALUES ('NONE', $1, $2, 0, 'child', '__sub_workflow:p7_child', '{}',
                       'default', 100, '{}', FALSE, 'all', 'RUNNING', TRUE, 'NOT_GIVEN',
                       $3, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(parent_id)
        .bind(child_id)
        .execute(&pool)
        .await
        .unwrap();
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'pending', 'p7_child_pending', 'null', 'default', 100, '{}', FALSE,
                'all', 'PENDING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(child_id)
        .execute(&pool)
        .await
        .unwrap();
        sql::query(
            "UPDATE horsies_workflows SET updated_at = NOW() - INTERVAL '2 hours' WHERE id = $1",
        )
        .bind(child_id)
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            expire_paused_workflows(
                &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
                age(chrono::Duration::hours(1)),
                10,
            )
            .await
            .unwrap(),
            1,
        );
        let recovery = crate::workflow_engine::recovery::recover_workflow_tree(
            &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            &TestChildSpecs::empty(),
            parent_id,
        )
        .await
        .unwrap();
        assert_eq!(recovery.case1_6_subworkflow_completed, 1);
        let facts: (String, String, String) = sql::query_as(
            "SELECT child.status, parent.status, node.status
             FROM horsies_workflows child
             JOIN horsies_workflows parent ON parent.id = $2
             JOIN horsies_workflow_tasks node ON node.workflow_id = parent.id
             WHERE child.id = $1 AND node.task_index = 0",
        )
        .bind(child_id)
        .bind(parent_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            facts,
            (
                "EXPIRED".to_owned(),
                "FAILED".to_owned(),
                "FAILED".to_owned()
            )
        );
    }

    #[tokio::test]
    #[serial]
    async fn paused_child_expiry_commits_before_poisoned_parent_recovery() {
        let pool = crate::test_support::migrated_pool().await;
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        seed_workflow(&pool, parent_id, "RUNNING", None).await;
        seed_workflow(&pool, child_id, "PAUSED", Some(parent_id)).await;
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
                 priority, dependencies, allow_failed_deps, join_type, status,
                 is_subworkflow, sub_workflow_params, sub_workflow_id, created_at
             ) VALUES ('NONE', $1, $2, 0, 'child', '__sub_workflow:p7_poison_child', '{}',
                       'default', 100, '{}', FALSE, 'all', 'RUNNING', TRUE, 'NOT_GIVEN',
                       $3, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(parent_id)
        .bind(child_id)
        .execute(&pool)
        .await
        .unwrap();
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'pending', 'p7_poison_child_pending', 'null', 'default', 100, '{}',
                FALSE, 'all', 'PENDING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(child_id)
        .execute(&pool)
        .await
        .unwrap();
        sql::query(
            "UPDATE horsies_workflows
             SET updated_at = NOW() - INTERVAL '2 hours' WHERE id = $1",
        )
        .bind(child_id)
        .execute(&pool)
        .await
        .unwrap();
        sql::query(&format!(
            "CREATE OR REPLACE FUNCTION p7_poison_parent_progression() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{parent_id}'::uuid THEN
                     RAISE EXCEPTION 'poisoned parent progression';
                 END IF;
                 RETURN NEW;
             END
             $body$",
        ))
        .execute(&pool)
        .await
        .unwrap();
        sql::query(
            "CREATE TRIGGER p7_poison_parent_progression
             BEFORE UPDATE ON horsies_workflow_tasks FOR EACH ROW
             EXECUTE FUNCTION p7_poison_parent_progression()",
        )
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            expire_paused_workflows(
                &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
                age(chrono::Duration::hours(1)),
                10,
            )
            .await
            .unwrap(),
            1,
        );
        let child_status: String =
            sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(child_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(child_status, "EXPIRED");

        let recovery = crate::workflow_engine::recovery::recover_workflow_tree(
            &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            &TestChildSpecs::empty(),
            parent_id,
        )
        .await
        .unwrap();
        assert_eq!(recovery.errors, 1);
        let facts: (String, String) = sql::query_as(
            "SELECT child.status, node.status
             FROM horsies_workflows child
             JOIN horsies_workflow_tasks node ON node.workflow_id = $2
             WHERE child.id = $1 AND node.task_index = 0",
        )
        .bind(child_id)
        .bind(parent_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(facts, ("EXPIRED".to_owned(), "RUNNING".to_owned()));

        sql::query("DROP TRIGGER p7_poison_parent_progression ON horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sql::query("DROP FUNCTION p7_poison_parent_progression()")
            .execute(&pool)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod cancel_lock_order_tests {
    //! N6: the cancel transaction must lock the workflow row before workflow_task
    //! rows, matching COMPLETE_WORKFLOW_TASK_SQL (workflows before workflow_tasks).
    //! Opposite orders across the two paths deadlock under contention (Postgres
    //! aborts one side, SQLSTATE 40P01). This test drives the real cancel tx
    //! against a held completion-order lock pair and asserts neither side
    //! deadlocks and both commit.
    use super::*;
    use crate::broker::PostgresBroker;
    use crate::test_support::sql::{self, TestPool};
    use serial_test::serial;
    use uuid::Uuid;

    async fn insert_running_workflow(pool: &TestPool, id: &str) {
        sql::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id, parent_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'n6_wf', 'RUNNING', 'fail', NULL,
                'test.n6.v1', 0, $1, NULL,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(test_uuid(id))
        .execute(pool)
        .await
        .expect("insert workflow");
    }

    async fn insert_wf_task(pool: &TestPool, wf_id: &str) {
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'node_0', 'n6_task', 'null', 'default', 100, '{}', FALSE, 'all',
                'RUNNING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(wf_id))
        .execute(pool)
        .await
        .expect("insert workflow_task");
    }

    async fn cleanup(pool: &TestPool, id: &str) {
        sql::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
    }

    /// Drive the real cancel transaction while a separate connection holds locks
    /// in completion order (workflow row first, then workflow_task row). With the
    /// fixed cancel path (workflow row first), cancel waits on the workflow row
    /// holding no workflow_task lock, so there is no circular wait — both sides
    /// commit. Before the fix cancel held the workflow_task row while waiting on
    /// the workflow row, closing a cycle against the completion-order holder and
    /// deadlocking (Postgres aborts one side with 40P01).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn cancel_matches_completion_lock_order_no_deadlock() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");

        let wf = Uuid::new_v4().to_string();
        insert_running_workflow(&pool, &wf).await;
        insert_wf_task(&pool, &wf).await;

        // Conn A simulates an in-flight completion holding its first lock: the
        // workflow row (COMPLETE_WORKFLOW_TASK_SQL's `FOR UPDATE OF w`).
        let mut conn_a = pool.begin().await.expect("begin conn A");
        sql::query("SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE")
            .bind(test_uuid(&wf))
            .execute(&mut *conn_a)
            .await
            .expect("conn A locks the workflow row");

        // Run the real cancel statement concurrently, on a broker of its own.
        let cancel_pool = pool.clone();
        let cancel_wf = wf.clone();
        let cancel_task = tokio::spawn(async move {
            let cancel_broker =
                PostgresBroker::from_test_url(cancel_pool.url(), crate::ClientFamily::Rust);
            cancel_workflow(
                &cancel_broker,
                Uuid::parse_str(&cancel_wf).expect("test identity must be UUID"),
            )
            .await
        });

        // Barrier: let the cancel task reach its first lock-wait. Its lock
        // acquisition is sub-millisecond; this only has to exceed that so the
        // interleaving is deterministic (pre-fix, cancel is now parked holding the
        // workflow_task row and waiting on the workflow row).
        tokio::time::sleep(std::time::Duration::from_millis(750)).await;

        // Conn A now takes its second lock — the workflow_task row (completion's
        // `upd`). Pre-fix this closes the deadlock cycle; post-fix it succeeds
        // immediately because cancel holds no workflow_task lock while it waits.
        let a_wf_task_lock = sql::query(
            "SELECT task_index FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0 FOR UPDATE",
        )
        .bind(test_uuid(&wf))
        .execute(&mut *conn_a)
        .await;

        // Release conn A so the (fixed) cancel path can acquire the workflow row.
        conn_a.commit().await.ok();

        let cancel_result = cancel_task.await.expect("cancel task joins");

        assert!(
            a_wf_task_lock.is_ok(),
            "completion-order lock pair must not deadlock against cancel (40P01): {:?}",
            a_wf_task_lock.err(),
        );
        let cancelled =
            cancel_result.expect("cancel tx must not deadlock against completion order (40P01)");
        assert!(cancelled, "a RUNNING workflow must be cancelled");
        assert_eq!(
            status_of(&pool, &wf).await,
            "CANCELLED",
            "cancel must commit the CANCELLED status",
        );

        cleanup(&pool, &wf).await;
    }

    async fn status_of(pool: &TestPool, id: &str) -> String {
        sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(id))
            .fetch_one(pool)
            .await
            .expect("status")
    }
}

#[cfg(test)]
mod cancel_tree_tests {
    use super::*;
    use crate::core::workflow::handle_types::HandleErrorCode;
    use crate::test_support::sql::{self, TestPool};
    use serial_test::serial;

    async fn seed_workflow(pool: &TestPool, id: Uuid, status: &str, parent: Option<Uuid>) {
        sql::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, parent_workflow_id, parent_task_index,
                 sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'cancel_tree', $2, 'fail', $3, $4, $5, $6, $7,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(id)
        .bind(status)
        .bind(format!("test.cancel-tree.{id}"))
        .bind(if parent.is_some() { 1_i32 } else { 0_i32 })
        .bind(parent.unwrap_or(id))
        .bind(parent)
        .bind(parent.map(|_| 0_i32))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_pending_node(pool: &TestPool, workflow_id: Uuid) {
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies, allow_failed_deps, join_type,
                 max_retries, execution, retention_class_key, status,
                 is_subworkflow, created_at
             ) VALUES ('NONE', $1, $2, 1, 'later', 'later_task', '{}', 'default', 100,
                       '{0}', FALSE, 'all', 0, 'ISOLATED', 'forever', 'PENDING',
                       FALSE, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn workflow_state(pool: &TestPool, id: Uuid) -> (String, bool) {
        sql::query_as(
            "SELECT status, completed_at IS NOT NULL FROM horsies_workflows WHERE id = $1",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn node_state(pool: &TestPool, workflow_id: Uuid) -> (String, bool) {
        sql::query_as(
            "SELECT status, completed_at IS NOT NULL FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 1",
        )
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn a_tree_cancel_sets_completed_at_and_skips_nodes_with_completed_at() {
        let pool = crate::test_support::migrated_pool().await;
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        seed_workflow(&pool, parent_id, "RUNNING", None).await;
        seed_workflow(&pool, child_id, "PAUSED", Some(parent_id)).await;
        seed_pending_node(&pool, parent_id).await;
        seed_pending_node(&pool, child_id).await;

        assert!(cancel_workflow(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            parent_id
        )
        .await
        .unwrap());

        for workflow_id in [parent_id, child_id] {
            assert_eq!(
                workflow_state(&pool, workflow_id).await,
                ("CANCELLED".to_owned(), true)
            );
            assert_eq!(
                node_state(&pool, workflow_id).await,
                ("SKIPPED".to_owned(), true)
            );
        }
    }

    #[tokio::test]
    #[serial]
    async fn a_failed_descendant_cancel_rolls_back_the_whole_tree() {
        let pool = crate::test_support::migrated_pool().await;
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        seed_workflow(&pool, parent_id, "RUNNING", None).await;
        seed_workflow(&pool, child_id, "RUNNING", Some(parent_id)).await;
        seed_pending_node(&pool, parent_id).await;
        sql::query(&format!(
            "CREATE FUNCTION cw_refuse_child_cancel() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.id = '{child_id}'::uuid AND NEW.status = 'CANCELLED' THEN
                     RAISE EXCEPTION 'child cancel refused';
                 END IF;
                 RETURN NEW;
             END
             $$"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sql::query(
            "CREATE TRIGGER cw_refuse_child_cancel BEFORE UPDATE ON horsies_workflows
             FOR EACH ROW EXECUTE FUNCTION cw_refuse_child_cancel()",
        )
        .execute(&pool)
        .await
        .unwrap();

        let outcome = cancel_workflow(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            parent_id,
        )
        .await;

        sql::query("DROP TRIGGER cw_refuse_child_cancel ON horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sql::query("DROP FUNCTION cw_refuse_child_cancel()")
            .execute(&pool)
            .await
            .unwrap();

        let error = outcome.unwrap_err();
        assert_eq!(error.code(), HandleErrorCode::DbOperationFailed);
        // RAISE EXCEPTION is SQLSTATE P0001, which is not a retryable class.
        assert!(!error.retryable());
        assert_eq!(
            workflow_state(&pool, parent_id).await,
            ("RUNNING".to_owned(), false)
        );
        assert_eq!(
            node_state(&pool, parent_id).await,
            ("PENDING".to_owned(), false)
        );
        assert_eq!(
            workflow_state(&pool, child_id).await,
            ("RUNNING".to_owned(), false)
        );
    }

    #[test]
    fn handle_errors_follow_the_python_catalog() {
        let workflow_id = Uuid::new_v4();
        let transient = handle_operation_error(
            HandleOperation::Status,
            WorkflowError::Database(sqlx::Error::PoolTimedOut),
            workflow_id,
        );
        assert_eq!(transient.code(), HandleErrorCode::DbOperationFailed);
        assert!(transient.retryable());
        let persistent = handle_operation_error(
            HandleOperation::Status,
            WorkflowError::Database(sqlx::Error::RowNotFound),
            workflow_id,
        );
        assert_eq!(persistent.code(), HandleErrorCode::DbOperationFailed);
        assert!(!persistent.retryable());
        let stored = handle_operation_error(
            HandleOperation::Status,
            WorkflowError::InvalidStatus("CORRUPT".to_owned()),
            workflow_id,
        );
        assert_eq!(stored.code(), HandleErrorCode::StoredDataInvalid);
        assert!(!stored.retryable());
        let missing = handle_operation_error(
            HandleOperation::Status,
            WorkflowError::WorkflowNotFound { workflow_id },
            workflow_id,
        );
        assert_eq!(missing.code(), HandleErrorCode::WorkflowNotFound);
    }

    /// Python's messages (the Python facade before the core built them): the
    /// operation's label, the workflow id and the error's text; the error
    /// stays the cause. An absent workflow has Python's
    /// `HandleWorkflowNotFound` message.
    #[test]
    fn each_handle_error_has_python_s_message_and_keeps_its_cause() {
        let workflow_id = Uuid::from_u128(0x3f9c_1a2b_4d5e_4f60_8a71_b2c3_d4e5_f607);
        let failed = handle_operation_error(
            HandleOperation::Pause,
            WorkflowError::Broker(crate::broker::BrokerError::ConnectionFailed(
                "refused".to_owned(),
            )),
            workflow_id,
        );
        assert_eq!(
            failed.message(),
            "Pause failed for workflow 3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607: \
             broker error: connection failed: refused"
        );
        assert!(failed.retryable());
        assert!(matches!(
            failed,
            HandleOperationError::DbOperationFailed {
                cause: WorkflowError::Broker(crate::broker::BrokerError::ConnectionFailed(_)),
                ..
            }
        ));

        let stored = handle_operation_error(
            HandleOperation::Status,
            WorkflowError::InvalidStatus("CORRUPT".to_owned()),
            workflow_id,
        );
        assert_eq!(
            stored.message(),
            "Status failed for workflow 3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607: \
             invalid status: CORRUPT"
        );
        assert!(matches!(
            stored,
            HandleOperationError::StoredDataInvalid {
                cause: WorkflowError::InvalidStatus(ref status),
                ..
            } if status == "CORRUPT"
        ));

        let missing = handle_operation_error(
            HandleOperation::Cancel,
            WorkflowError::WorkflowNotFound { workflow_id },
            workflow_id,
        );
        assert_eq!(
            missing.message(),
            "Workflow 3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607 not found"
        );
    }

    /// A resume that committed and whose recovery pass failed keeps
    /// Python's message, and the failure of the pass is its cause.
    #[test]
    fn a_failed_recovery_pass_keeps_the_message_and_its_cause() {
        let workflow_id = Uuid::from_u128(0x3f9c_1a2b_4d5e_4f60_8a71_b2c3_d4e5_f607);
        let failed = resume_recovery_failed(
            workflow_id,
            AfterResumeCommit::TreeRecovery,
            WorkflowError::Broker(crate::broker::BrokerError::ConnectionFailed(
                "refused".to_owned(),
            )),
        );
        assert_eq!(
            failed.message(),
            "Workflow 3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607 resumed; the recovery pass for its \
             tree failed: broker error: connection failed: refused"
        );
        assert!(!failed.retryable());
        assert_eq!(failed.code(), HandleErrorCode::ResumeRecoveryFailed);
        assert_eq!(failed.failure_cause(), crate::FailureCause::Connection);
    }
}

#[cfg(test)]
mod client_family_tests {
    //! A workflow operation of a handle checks the client family before its first write.
    use super::*;
    use crate::broker::client_family::ClientFamily;
    use crate::core::workflow::handle_types::HandleErrorCode;
    use crate::test_support::sql::{self, TestPool};
    use serial_test::serial;

    async fn workflow_status(pool: &TestPool, workflow_id: Uuid) -> String {
        sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(pool)
            .await
            .expect("workflow status")
    }

    /// Pause, resume and cancel through a handle of the other client family are refused as
    /// `DB_OPERATION_FAILED`, not retryable, and write nothing. A handle of the recorded family
    /// passes the same check.
    #[tokio::test]
    #[serial]
    async fn pause_resume_and_cancel_of_another_family_write_nothing() {
        let pool = crate::test_support::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        sql::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'family_check', 'RUNNING', 'fail', 'tests.family.v1', 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("insert workflow");
        let python = PostgresBroker::from_test_url(pool.url(), ClientFamily::Python);

        let refusals = [
            pause_workflow(&python, workflow_id)
                .await
                .expect_err("pause of another family"),
            resume_client_workflow(&python, workflow_id, &TestChildSpecs::empty())
                .await
                .expect_err("resume of another family"),
            cancel_workflow(&python, workflow_id)
                .await
                .expect_err("cancel of another family"),
        ];
        for refused in refusals {
            assert_eq!(refused.code(), HandleErrorCode::DbOperationFailed);
            assert!(!refused.retryable());
            assert!(
                refused.message().contains("its own family"),
                "{}",
                refused.message()
            );
        }
        assert_eq!(workflow_status(&pool, workflow_id).await, "RUNNING");

        let rust = PostgresBroker::from_test_url(pool.url(), ClientFamily::Rust);
        assert!(pause_workflow(&rust, workflow_id)
            .await
            .expect("pause of the recorded family"));
        assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");

        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }
}
