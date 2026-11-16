//! Transport-free decisions for monitoring actions.

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::workflow::handle_types::{HandleErrorCode, HandleOperationError};
use crate::core::workflow::status::WorkflowStatus;
use crate::workflow_engine::error::WorkflowError;

use super::models::ActionResponse;
use super::task_actions::{
    cancel_task, TaskActionError, TaskActionErrorCode, TaskActionResult, TaskCancelled,
};

pub const STATE_CONFLICT: &str = "STATE_CONFLICT";
pub const RESUME_RECOVERY_WARNING: &str = "post_resume_recovery_failed";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActionConflictCode {
    TaskNotCancellable,
    StateConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ActionBody {
    Success(ActionResponse),
    Conflict {
        code: ActionConflictCode,
        current_status: Option<String>,
    },
    Code {
        code: TaskActionErrorCode,
    },
    Detail {
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionOutcome {
    status_code: u16,
    body: ActionBody,
}

impl ActionOutcome {
    pub const fn status_code(&self) -> u16 {
        self.status_code
    }

    pub const fn body(&self) -> &ActionBody {
        &self.body
    }

    pub fn into_body(self) -> ActionBody {
        self.body
    }
}

fn succeeded(outcome: &str, was_status: Option<String>, warning: Option<String>) -> ActionOutcome {
    ActionOutcome {
        status_code: 200,
        body: ActionBody::Success(ActionResponse {
            outcome: outcome.to_owned(),
            was_status,
            next_attempt_number: None,
            warning,
        }),
    }
}

fn conflict(code: ActionConflictCode, current_status: Option<String>) -> ActionOutcome {
    ActionOutcome {
        status_code: 409,
        body: ActionBody::Conflict {
            code,
            current_status,
        },
    }
}

fn detail(status_code: u16, message: impl Into<String>) -> ActionOutcome {
    ActionOutcome {
        status_code,
        body: ActionBody::Detail {
            detail: message.into(),
        },
    }
}

fn workflow_not_found(workflow_id: Uuid) -> ActionOutcome {
    detail(404, format!("Workflow {workflow_id} not found"))
}

fn task_action_failed(error: TaskActionError) -> ActionOutcome {
    match error.code {
        TaskActionErrorCode::TaskNotFound => detail(404, error.message),
        TaskActionErrorCode::TaskIsWorkflowTask => ActionOutcome {
            status_code: 400,
            body: ActionBody::Code {
                code: TaskActionErrorCode::TaskIsWorkflowTask,
            },
        },
        TaskActionErrorCode::TaskNotCancellable => conflict(
            ActionConflictCode::TaskNotCancellable,
            error.current_status.map(|status| status.to_string()),
        ),
        TaskActionErrorCode::DbOperationFailed => detail(503, error.message),
    }
}

fn handle_failed(error: HandleOperationError) -> ActionOutcome {
    match error.code {
        HandleErrorCode::WorkflowNotFound => workflow_not_found(error.workflow_id),
        HandleErrorCode::DbOperationFailed
        | HandleErrorCode::LoopRunnerFailed
        | HandleErrorCode::InternalFailed => detail(503, error.message),
    }
}

fn status_failed(workflow_id: Uuid, error: WorkflowError) -> ActionOutcome {
    match error {
        WorkflowError::WorkflowNotFound { .. } => workflow_not_found(workflow_id),
        WorkflowError::Database(_)
        | WorkflowError::Broker(_)
        | WorkflowError::Serialization(_)
        | WorkflowError::WorkflowTimeout { .. }
        | WorkflowError::WorkflowError(_)
        | WorkflowError::InvalidStatus(_)
        | WorkflowError::Validation(_) => detail(503, error.to_string()),
    }
}

async fn current_status(pool: &PgPool, workflow_id: Uuid) -> Result<WorkflowStatus, ActionOutcome> {
    crate::workflow_engine::query::get_workflow_status(pool, workflow_id)
        .await
        .map_err(|error| status_failed(workflow_id, error))
}

pub fn task_action_outcome(result: TaskActionResult<TaskCancelled>) -> ActionOutcome {
    match result {
        Ok(cancelled) => succeeded("cancelled", Some(cancelled.was_status.to_string()), None),
        Err(error) => task_action_failed(error),
    }
}

pub async fn cancel_task_action(
    broker: &PostgresBroker,
    task_id: Uuid,
    include_running: bool,
) -> ActionOutcome {
    task_action_outcome(cancel_task(broker, task_id, include_running).await)
}

pub async fn pause_workflow_action(pool: &PgPool, workflow_id: Uuid) -> ActionOutcome {
    match crate::workflow_engine::lifecycle::pause_workflow(pool, workflow_id).await {
        Ok(true) => succeeded("paused", None, None),
        Ok(false) => match current_status(pool, workflow_id).await {
            Ok(status) => conflict(ActionConflictCode::StateConflict, Some(status.to_string())),
            Err(outcome) => outcome,
        },
        Err(error) => handle_failed(error),
    }
}

async fn resume_failure(
    pool: &PgPool,
    workflow_id: Uuid,
    error: HandleOperationError,
) -> ActionOutcome {
    if error.code == HandleErrorCode::WorkflowNotFound {
        return workflow_not_found(workflow_id);
    }
    resolve_resume_failure(error, current_status(pool, workflow_id).await)
}

fn resolve_resume_failure(
    error: HandleOperationError,
    status: Result<WorkflowStatus, ActionOutcome>,
) -> ActionOutcome {
    match status {
        Ok(WorkflowStatus::Running) => {
            succeeded("resumed", None, Some(RESUME_RECOVERY_WARNING.to_owned()))
        }
        Ok(
            WorkflowStatus::Pending
            | WorkflowStatus::Completed
            | WorkflowStatus::Failed
            | WorkflowStatus::Paused
            | WorkflowStatus::Cancelled
            | WorkflowStatus::Expired,
        ) => detail(503, error.message),
        Err(outcome) => outcome,
    }
}

pub async fn resume_workflow_action(
    pool: &PgPool,
    workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> ActionOutcome {
    match crate::workflow_engine::lifecycle::resume_workflow(
        pool,
        workflow_id,
        registry,
        payload,
        retention,
    )
    .await
    {
        Ok(true) => succeeded("resumed", None, None),
        Ok(false) => match current_status(pool, workflow_id).await {
            Ok(status) => conflict(ActionConflictCode::StateConflict, Some(status.to_string())),
            Err(outcome) => outcome,
        },
        Err(error) => resume_failure(pool, workflow_id, error).await,
    }
}

pub async fn cancel_workflow_action(pool: &PgPool, workflow_id: Uuid) -> ActionOutcome {
    match crate::workflow_engine::lifecycle::cancel_workflow(pool, workflow_id).await {
        Ok(_) => match current_status(pool, workflow_id).await {
            Ok(WorkflowStatus::Cancelled) => succeeded("cancelled", None, None),
            Ok(status) => conflict(ActionConflictCode::StateConflict, Some(status.to_string())),
            Err(outcome) => outcome,
        },
        Err(error) => handle_failed(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_failure_mapping_is_exhaustive() {
        let error = |code| HandleOperationError {
            code,
            message: "recovery failed".to_owned(),
            retryable: true,
            workflow_id: Uuid::nil(),
        };
        assert_eq!(
            handle_failed(error(HandleErrorCode::WorkflowNotFound)).status_code(),
            404
        );
        for code in [
            HandleErrorCode::DbOperationFailed,
            HandleErrorCode::LoopRunnerFailed,
            HandleErrorCode::InternalFailed,
        ] {
            assert_eq!(handle_failed(error(code)).status_code(), 503);
        }

        let outcome = resolve_resume_failure(
            error(HandleErrorCode::DbOperationFailed),
            Ok(WorkflowStatus::Running),
        );
        assert_eq!(outcome.status_code(), 200);
        assert_eq!(
            serde_json::to_value(outcome.body()).unwrap(),
            serde_json::json!({
                "outcome": "resumed",
                "was_status": null,
                "next_attempt_number": null,
                "warning": "post_resume_recovery_failed",
            })
        );

        for status in [
            WorkflowStatus::Pending,
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Paused,
            WorkflowStatus::Cancelled,
            WorkflowStatus::Expired,
        ] {
            let outcome =
                resolve_resume_failure(error(HandleErrorCode::DbOperationFailed), Ok(status));
            assert_eq!(outcome.status_code(), 503);
            assert_eq!(
                serde_json::to_value(outcome.body()).unwrap(),
                serde_json::json!({"detail": "recovery failed"})
            );
        }

        let status_failure = detail(503, "status read failed");
        let outcome = resolve_resume_failure(
            error(HandleErrorCode::DbOperationFailed),
            Err(status_failure.clone()),
        );
        assert_eq!(outcome, status_failure);
    }
}
