//! Administrative task cancellation with exact live-miss diagnosis.

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::broker::error::is_retryable_sqlx_error;
use crate::broker::terminalization::terminalize_in_tx;
use crate::broker::PostgresBroker;
use crate::core::history::errors::HistoryError;
use crate::core::history::reads::detail::{
    read_task_detail, staged_detail_published, TaskDetailResult,
};
use crate::core::lifecycle::{CallerHoldsRowLock, TerminalizationCommand, TerminalizationOutcome};
use crate::core::types::status::TaskStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskActionErrorCode {
    TaskNotFound,
    TaskNotCancellable,
    TaskIsWorkflowTask,
    DbOperationFailed,
    /// Stored task data does not parse: a history status that is not a task
    /// status, or a cancel outcome row that does not decode. Nothing changed:
    /// the transaction did not commit. Not retryable: a repeat gives the
    /// same result until the stored row changes (Python
    /// `horsies/monitoring/task_actions.py:69-197`).
    StoredDataInvalid,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct TaskActionError {
    pub code: TaskActionErrorCode,
    pub message: String,
    pub retryable: bool,
    pub task_id: Uuid,
    pub current_status: Option<TaskStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancelled {
    pub task_id: Uuid,
    pub was_status: TaskStatus,
}

pub type TaskActionResult<T> = Result<T, TaskActionError>;

#[derive(Debug, FromRow)]
struct LockedTask {
    status: String,
    is_workflow_task: bool,
    #[allow(dead_code)]
    expiry_passed: bool,
}

/// Seed of the task advisory lock. Every move function of the baseline
/// takes `pg_advisory_xact_lock(hashtextextended(<task id>::text, 731))`
/// before its row lock (for example `horsies_cancel_locked_task`), as
/// Python's `TASK_LOCK_SEED`.
const TASK_LOCK_SEED: i64 = 731;

/// The task advisory lock, by the key every move function takes. It runs
/// before the row lock of `LOCK_TASK_SQL`, in the same transaction, so the
/// cancel takes the two locks in the order of the move functions and cannot
/// deadlock with a finalize (Python `horsies/core/lifecycle/persistence.py:
/// 245-256`, `horsies/monitoring/task_actions.py:377-378`).
const LOCK_TASK_ADVISORY_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1::text, $2))";

const LOCK_TASK_SQL: &str = "SELECT status,
            is_workflow_task,
            (good_until IS NOT NULL AND good_until <= NOW()) AS expiry_passed
     FROM horsies_tasks
     WHERE id = $1
     FOR UPDATE";

fn not_found(task_id: Uuid) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::TaskNotFound,
        message: format!("Task {task_id} does not exist."),
        retryable: false,
        task_id,
        current_status: None,
    }
}

fn workflow_bound(task_id: Uuid, current_status: Option<TaskStatus>) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::TaskIsWorkflowTask,
        message: format!("Task {task_id} belongs to a workflow; use the workflow actions instead."),
        retryable: false,
        task_id,
        current_status,
    }
}

fn state_conflict(
    task_id: Uuid,
    status_value: &str,
    current_status: Option<TaskStatus>,
) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::TaskNotCancellable,
        message: format!("Task {task_id} cannot be cancelled from status {status_value}."),
        retryable: false,
        task_id,
        current_status,
    }
}

fn sqlx_failure(task_id: Uuid, error: sqlx::Error) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::DbOperationFailed,
        message: format!("Task cancel failed: {error}"),
        retryable: is_retryable_sqlx_error(&error),
        task_id,
        current_status: None,
    }
}

fn history_failure(task_id: Uuid, error: HistoryError) -> TaskActionError {
    match error {
        HistoryError::Database(database) => sqlx_failure(task_id, database),
        HistoryError::ArchiveDecode(_)
        | HistoryError::Contract(_)
        | HistoryError::LeafLockNotHeld
        | HistoryError::LeafLockBusy { .. }
        | HistoryError::HistoryParentAbsent(_) => stored_data_invalid(task_id, error.to_string()),
    }
}

fn broker_failure(task_id: Uuid, error: crate::broker::BrokerError) -> TaskActionError {
    match error {
        crate::broker::BrokerError::TerminalizationContract(detail) => {
            stored_data_invalid(task_id, detail)
        }
        other => TaskActionError {
            code: TaskActionErrorCode::DbOperationFailed,
            message: format!("Task cancel failed: {other}"),
            retryable: other.is_retryable(),
            task_id,
            current_status: None,
        },
    }
}

/// Stored data that does not parse. Nothing changed: the transaction rolls
/// back.
fn stored_data_invalid(task_id: Uuid, detail: impl Into<String>) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::StoredDataInvalid,
        message: format!(
            "Task cancel failed: stored data does not parse: {}",
            detail.into()
        ),
        retryable: false,
        task_id,
        current_status: None,
    }
}

/// Parse a stored status; a value that is not a task status is
/// [`TaskActionErrorCode::StoredDataInvalid`].
fn parse_stored_status(task_id: Uuid, stored: &str) -> Result<TaskStatus, TaskActionError> {
    TaskStatus::from_str(stored).map_err(|reason| {
        stored_data_invalid(task_id, format!("status of task {task_id}: {reason}"))
    })
}

fn contract_failure(task_id: Uuid, detail: impl Into<String>) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::DbOperationFailed,
        message: format!("Task cancel failed: {}", detail.into()),
        retryable: false,
        task_id,
        current_status: None,
    }
}

async fn diagnose_live_miss<T>(
    connection: &mut sqlx::PgConnection,
    task_id: Uuid,
) -> TaskActionResult<T> {
    if !staged_detail_published(&mut *connection)
        .await
        .map_err(|error| history_failure(task_id, error))?
    {
        return Err(not_found(task_id));
    }
    match read_task_detail(connection, task_id)
        .await
        .map_err(|error| history_failure(task_id, error))?
    {
        TaskDetailResult::History(detail) => match detail.is_workflow_task {
            true => Err(workflow_bound(task_id, Some(detail.status))),
            false => Err(state_conflict(
                task_id,
                &detail.status.to_string(),
                Some(detail.status),
            )),
        },
        TaskDetailResult::Live { .. } | TaskDetailResult::Absent { .. } => Err(not_found(task_id)),
    }
}

/// Move a non-workflow live task to CANCELLED.
///
/// PENDING and CLAIMED are eligible. RUNNING is eligible only when
/// `include_running` is true. A retained terminal row is diagnosed from the
/// staged history reader and returned as a state conflict.
pub async fn cancel_task(
    broker: &PostgresBroker,
    task_id: Uuid,
    include_running: bool,
) -> TaskActionResult<TaskCancelled> {
    let mut transaction = broker
        .pool()
        .begin()
        .await
        .map_err(|error| sqlx_failure(task_id, error))?;
    let result = cancel_task_in_tx(&mut transaction, task_id, include_running).await;
    match result {
        Ok(cancelled) => {
            transaction
                .commit()
                .await
                .map_err(|error| sqlx_failure(task_id, error))?;
            Ok(cancelled)
        }
        Err(action_error) => {
            transaction
                .rollback()
                .await
                .map_err(|error| sqlx_failure(task_id, error))?;
            Err(action_error)
        }
    }
}

pub(super) async fn cancel_task_in_tx(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task_id: Uuid,
    include_running: bool,
) -> TaskActionResult<TaskCancelled> {
    sqlx::query(LOCK_TASK_ADVISORY_SQL)
        .bind(task_id)
        .bind(TASK_LOCK_SEED)
        .execute(transaction.as_mut())
        .await
        .map_err(|error| sqlx_failure(task_id, error))?;
    let locked: Option<LockedTask> = sqlx::query_as(LOCK_TASK_SQL)
        .bind(task_id)
        .fetch_optional(transaction.as_mut())
        .await
        .map_err(|error| sqlx_failure(task_id, error))?;
    let Some(locked) = locked else {
        return diagnose_live_miss(transaction.as_mut(), task_id).await;
    };

    let current_status = parse_stored_status(task_id, &locked.status)?;
    let mut permitted_source_statuses = vec![TaskStatus::Pending, TaskStatus::Claimed];
    if include_running {
        permitted_source_statuses.push(TaskStatus::Running);
    }
    let outcomes = terminalize_in_tx(
        transaction,
        &TerminalizationCommand::CancelLockedTask {
            task_id,
            fence: CallerHoldsRowLock,
            permitted_source_statuses,
        },
    )
    .await
    .map_err(|error| broker_failure(task_id, error))?;
    let outcome = outcomes.into_iter().next().ok_or_else(|| {
        contract_failure(
            task_id,
            "cancel operation returned no terminalization outcome",
        )
    })?;

    match outcome {
        TerminalizationOutcome::Applied { .. } => Ok(TaskCancelled {
            task_id,
            was_status: current_status,
        }),
        TerminalizationOutcome::AlreadyApplied { .. }
        | TerminalizationOutcome::SourceStateConflict { .. } => match locked.is_workflow_task {
            true => Err(workflow_bound(task_id, Some(current_status))),
            false => Err(state_conflict(
                task_id,
                &locked.status,
                Some(current_status),
            )),
        },
        TerminalizationOutcome::LostClaim { .. } | TerminalizationOutcome::TaskAbsent { .. } => {
            Err(contract_failure(
                task_id,
                "administrative cancellation contradicted its caller-held row lock",
            ))
        }
    }
}
