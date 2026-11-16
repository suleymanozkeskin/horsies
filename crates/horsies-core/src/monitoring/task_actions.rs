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

fn workflow_bound(task_id: Uuid) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::TaskIsWorkflowTask,
        message: format!("Task {task_id} belongs to a workflow; use the workflow actions instead."),
        retryable: false,
        task_id,
        current_status: None,
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
    let retryable = match &error {
        HistoryError::Database(database) => is_retryable_sqlx_error(database),
        _ => false,
    };
    TaskActionError {
        code: TaskActionErrorCode::DbOperationFailed,
        message: format!("Task cancel failed: {error}"),
        retryable,
        task_id,
        current_status: None,
    }
}

fn broker_failure(task_id: Uuid, error: crate::broker::BrokerError) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::DbOperationFailed,
        message: format!("Task cancel failed: {error}"),
        retryable: error.is_retryable(),
        task_id,
        current_status: None,
    }
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
        TaskDetailResult::History(detail) if detail.is_workflow_task => {
            Err(workflow_bound(task_id))
        }
        TaskDetailResult::History(detail) => Err(state_conflict(
            task_id,
            &detail.status,
            TaskStatus::from_str(&detail.status).ok(),
        )),
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
    let locked: Option<LockedTask> = sqlx::query_as(LOCK_TASK_SQL)
        .bind(task_id)
        .fetch_optional(transaction.as_mut())
        .await
        .map_err(|error| sqlx_failure(task_id, error))?;
    let Some(locked) = locked else {
        return diagnose_live_miss(transaction.as_mut(), task_id).await;
    };

    let current_status = TaskStatus::from_str(&locked.status).ok();
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
        TerminalizationOutcome::Applied { .. } => {
            let Some(was_status) = current_status else {
                return Err(contract_failure(
                    task_id,
                    format!(
                        "administrative cancellation applied from unknown status {:?}",
                        locked.status
                    ),
                ));
            };
            Ok(TaskCancelled {
                task_id,
                was_status,
            })
        }
        TerminalizationOutcome::AlreadyApplied { .. }
        | TerminalizationOutcome::SourceStateConflict { .. } => {
            if locked.is_workflow_task {
                Err(workflow_bound(task_id))
            } else {
                Err(state_conflict(task_id, &locked.status, current_status))
            }
        }
        TerminalizationOutcome::LostClaim { .. } | TerminalizationOutcome::TaskAbsent { .. } => {
            Err(contract_failure(
                task_id,
                "administrative cancellation contradicted its caller-held row lock",
            ))
        }
    }
}
