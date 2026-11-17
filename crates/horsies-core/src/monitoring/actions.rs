//! Transport-free decisions for monitoring actions.

use serde::{Deserialize, Serialize};
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
        // Python answers it with 503 too; the core never produces it.
        TaskActionErrorCode::EventLoopLimit => detail(503, error.message),
        // The request was valid, and a repeat gives the same answer until the
        // stored row changes (Python `python/horsies/web/routes/actions.py:110-130`).
        TaskActionErrorCode::StoredDataInvalid => detail(500, error.message),
    }
}

fn handle_failed(error: HandleOperationError) -> ActionOutcome {
    match error.code {
        HandleErrorCode::WorkflowNotFound => workflow_not_found(error.workflow_id),
        HandleErrorCode::DbOperationFailed
        | HandleErrorCode::LoopRunnerFailed
        | HandleErrorCode::EventLoopLimit
        | HandleErrorCode::ResumeRecoveryFailed => detail(503, error.message),
        // Stored data that does not parse: the request was valid, and a repeat gets the same answer
        // (Python `_handle_failed`, `web/routes/actions.py`).
        HandleErrorCode::StoredDataInvalid => detail(500, error.message),
        // Only a sync facade of the Python client returns it. The web actions call the core's async
        // workflow functions, which never do, so reaching it is a defect (Python raises
        // `AssertionError` in `_handle_failed`).
        HandleErrorCode::AsyncContext => panic!(
            "web workflow action for workflow {} got ASYNC_CONTEXT; the core's workflow functions \
             never return it",
            error.workflow_id
        ),
    }
}

fn status_failed(workflow_id: Uuid, error: WorkflowError) -> ActionOutcome {
    match error {
        WorkflowError::WorkflowNotFound { .. } => workflow_not_found(workflow_id),
        WorkflowError::Database(_)
        | WorkflowError::Broker(_)
        | WorkflowError::Serialization(_)
        | WorkflowError::InvalidStatus(_)
        | WorkflowError::Validation(_)
        | WorkflowError::StepContract(_)
        | WorkflowError::ChildSpecSource(_) => detail(503, error.to_string()),
    }
}

/// The current status of a workflow, read on the main core pool of `broker`.
async fn current_status(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<WorkflowStatus, ActionOutcome> {
    crate::workflow_engine::query::get_workflow_status(broker, workflow_id)
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

pub async fn pause_workflow_action(broker: &PostgresBroker, workflow_id: Uuid) -> ActionOutcome {
    match crate::workflow_engine::lifecycle::pause_workflow(broker, workflow_id).await {
        Ok(true) => succeeded("paused", None, None),
        Ok(false) => match current_status(broker, workflow_id).await {
            Ok(status) => conflict(ActionConflictCode::StateConflict, Some(status.to_string())),
            Err(outcome) => outcome,
        },
        Err(error) => handle_failed(error),
    }
}

/// The answer of a failed resume, from the error alone, as Python's resume
/// route (`python/horsies/web/routes/actions.py:234-258`): `RESUME_RECOVERY_FAILED`
/// means the resume committed and only its recovery pass failed, so the
/// workflow was resumed and the answer says so with a warning. Every other
/// error is answered as for the other workflow actions. No second status read
/// guesses whether a failed resume committed.
fn resume_failure(error: HandleOperationError) -> ActionOutcome {
    match error.code {
        HandleErrorCode::ResumeRecoveryFailed => {
            succeeded("resumed", None, Some(RESUME_RECOVERY_WARNING.to_owned()))
        }
        HandleErrorCode::WorkflowNotFound
        | HandleErrorCode::DbOperationFailed
        | HandleErrorCode::StoredDataInvalid
        | HandleErrorCode::LoopRunnerFailed
        | HandleErrorCode::EventLoopLimit
        | HandleErrorCode::AsyncContext => handle_failed(error),
    }
}

pub async fn resume_workflow_action(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> ActionOutcome {
    match crate::workflow_engine::lifecycle::resume_workflow(
        broker,
        workflow_id,
        registry,
        payload,
        retention,
    )
    .await
    {
        Ok(true) => succeeded("resumed", None, None),
        Ok(false) => match current_status(broker, workflow_id).await {
            Ok(status) => conflict(ActionConflictCode::StateConflict, Some(status.to_string())),
            Err(outcome) => outcome,
        },
        Err(error) => resume_failure(error),
    }
}

pub async fn cancel_workflow_action(broker: &PostgresBroker, workflow_id: Uuid) -> ActionOutcome {
    match crate::workflow_engine::lifecycle::cancel_workflow(broker, workflow_id).await {
        Ok(_) => match current_status(broker, workflow_id).await {
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
            HandleErrorCode::EventLoopLimit,
            HandleErrorCode::ResumeRecoveryFailed,
        ] {
            assert_eq!(handle_failed(error(code)).status_code(), 503);
        }
        assert_eq!(
            handle_failed(error(HandleErrorCode::StoredDataInvalid)).status_code(),
            500
        );

        // Python `test_web_api.py::TestResumeCommittedThenFailed`: the resume
        // committed and its recovery pass failed, so the workflow was resumed.
        let outcome = resume_failure(error(HandleErrorCode::ResumeRecoveryFailed));
        assert_eq!(outcome.status_code(), 200);
        assert_eq!(
            serde_json::to_value(outcome.body()).unwrap(),
            serde_json::json!({
                "outcome": "resumed",
                "was_status": null,
                "warning": "post_resume_recovery_failed",
            })
        );

        // Any other failure of a resume is not a resume, whatever another actor
        // did to the workflow meanwhile.
        for code in [
            HandleErrorCode::DbOperationFailed,
            HandleErrorCode::LoopRunnerFailed,
            HandleErrorCode::EventLoopLimit,
        ] {
            let outcome = resume_failure(error(code));
            assert_eq!(outcome.status_code(), 503);
            assert_eq!(
                serde_json::to_value(outcome.body()).unwrap(),
                serde_json::json!({"detail": "recovery failed"})
            );
        }
        assert_eq!(
            resume_failure(error(HandleErrorCode::WorkflowNotFound)).status_code(),
            404
        );
        assert_eq!(
            resume_failure(error(HandleErrorCode::StoredDataInvalid)).status_code(),
            500
        );
    }

    #[test]
    #[should_panic(expected = "got ASYNC_CONTEXT")]
    fn async_context_is_a_defect_of_the_web_workflow_actions() {
        let _outcome = handle_failed(HandleOperationError {
            code: HandleErrorCode::AsyncContext,
            message: "sync call inside a loop".to_owned(),
            retryable: false,
            workflow_id: Uuid::nil(),
        });
    }
}
