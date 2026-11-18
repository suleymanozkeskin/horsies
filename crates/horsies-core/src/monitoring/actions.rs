//! Transport-free decisions for monitoring actions.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::catalog::CodeEnum;
use crate::core::workflow::handle_types::HandleOperationError;
use crate::core::workflow::status::WorkflowStatus;
use crate::workflow_engine::child_spec::{ChildSpecSource, ChildSpecUnavailable};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::lifecycle::ResumeFailure;

use super::models::ActionResponse;
use super::task_actions::{
    cancel_task, TaskActionError, TaskActionErrorCode, TaskActionResult, TaskCancelled,
};

pub const STATE_CONFLICT: &str = "STATE_CONFLICT";
pub const RESUME_RECOVERY_WARNING: &str = "post_resume_recovery_failed";

/// The body of [`ActionBody::InternalServerError`], sent as
/// `text/plain; charset=utf-8`: the answer of Starlette's
/// `ServerErrorMiddleware` to an exception that Python's route did not
/// handle.
pub const INTERNAL_SERVER_ERROR_TEXT: &str = "Internal Server Error";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActionConflictCode {
    TaskNotCancellable,
    StateConflict,
}

impl ActionConflictCode {
    /// The code as the response body writes it. Pure.
    pub const fn wire_value(self) -> &'static str {
        match self {
            Self::TaskNotCancellable => "TASK_NOT_CANCELLABLE",
            Self::StateConflict => STATE_CONFLICT,
        }
    }
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
    /// An exception of the client's code ended the action, and nothing of
    /// it committed: the Python client's child build that raised during a
    /// resume. The answer is Python's answer to an exception that its route
    /// did not handle: status 500 with [`INTERNAL_SERVER_ERROR_TEXT`] as
    /// `text/plain`, and nothing of the exception, whose text can hold user
    /// data. It is not JSON: serializing it is an error, and the web route
    /// sends the text.
    #[serde(skip)]
    InternalServerError,
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

    /// The result word of the action log line, as Python's action routes
    /// write it (`result=` of `monitoring action route=...`): the outcome of
    /// a success, else the code of a refusal, else `error`. Pure.
    pub fn result_word(&self) -> &str {
        match &self.body {
            ActionBody::Success(response) => response.outcome.as_str(),
            ActionBody::Conflict { code, .. } => code.wire_value(),
            ActionBody::Code { code } => code.wire_value(),
            ActionBody::Detail { .. } | ActionBody::InternalServerError => "error",
        }
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

/// The answer of an action that did not run because its client could not
/// connect its broker: 503 with the cause, as a failed statement is
/// answered (Python `_handle_failed` for `DB_OPERATION_FAILED`). Pure.
pub fn connect_failed_action(cause: &str) -> ActionOutcome {
    detail(503, cause)
}

fn workflow_not_found(workflow_id: Uuid) -> ActionOutcome {
    detail(404, format!("Workflow {workflow_id} not found"))
}

fn task_action_failed(error: TaskActionError) -> ActionOutcome {
    match error {
        TaskActionError::NotFound { message, .. } => detail(404, message),
        TaskActionError::WorkflowTask { .. } => ActionOutcome {
            status_code: 400,
            body: ActionBody::Code {
                code: TaskActionErrorCode::TaskIsWorkflowTask,
            },
        },
        TaskActionError::NotCancellable { current_status, .. } => conflict(
            ActionConflictCode::TaskNotCancellable,
            Some(current_status.to_string()),
        ),
        TaskActionError::DbOperationFailed { message, .. } => detail(503, message),
        // The request was valid, and a repeat gives the same answer until the
        // stored row changes (Python `tests/dualrun/reference/web_routes/actions.py:110-130`).
        TaskActionError::StoredDataInvalid { message, .. } => detail(500, message),
    }
}

fn handle_failed(error: HandleOperationError) -> ActionOutcome {
    match error {
        HandleOperationError::WorkflowNotFound { workflow_id, .. } => {
            workflow_not_found(workflow_id)
        }
        HandleOperationError::DbOperationFailed { message, .. }
        | HandleOperationError::ResumeRecoveryFailed { message, .. } => detail(503, message),
        // Stored data that does not parse: the request was valid, and a repeat gets the same answer
        // (Python `_handle_failed`, `web/routes/actions.py`).
        HandleOperationError::StoredDataInvalid { message, .. } => detail(500, message),
        // Only a sync or blocking form returns it. The web actions call the core's async
        // workflow functions, which never do, so reaching it is a defect (Python raises
        // `AssertionError` in `_handle_failed`).
        HandleOperationError::AsyncContext { workflow_id, .. } => panic!(
            "web workflow action for workflow {workflow_id} got ASYNC_CONTEXT; the core's workflow \
             functions never return it"
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
    match Box::pin(crate::workflow_engine::lifecycle::pause_workflow(
        broker,
        workflow_id,
    ))
    .await
    {
        Ok(true) => succeeded("paused", None, None),
        Ok(false) => match current_status(broker, workflow_id).await {
            Ok(status) => conflict(ActionConflictCode::StateConflict, Some(status.to_string())),
            Err(outcome) => outcome,
        },
        Err(error) => handle_failed(error),
    }
}

/// The answer of a failed resume, from the error alone, as Python's resume
/// route (`tests/dualrun/reference/web_routes/actions.py:234-258`): `RESUME_RECOVERY_FAILED`
/// means the resume committed and only its recovery pass failed, so the
/// workflow was resumed and the answer says so with a warning. Every other
/// error is answered as for the other workflow actions. No second status read
/// guesses whether a failed resume committed.
fn resume_failure(error: HandleOperationError) -> ActionOutcome {
    match error {
        HandleOperationError::ResumeRecoveryFailed { .. } => {
            succeeded("resumed", None, Some(RESUME_RECOVERY_WARNING.to_owned()))
        }
        HandleOperationError::WorkflowNotFound { .. }
        | HandleOperationError::DbOperationFailed { .. }
        | HandleOperationError::StoredDataInvalid { .. }
        | HandleOperationError::AsyncContext { .. } => handle_failed(error),
    }
}

/// The answer of a resume whose owed child build raised an exception of the
/// client (`ChildSpecUnavailable::Raised`): the step rolled back, and
/// nothing committed. Python's resume route let the exception propagate, so
/// the answer is Python's for an exception that its route did not handle
/// ([`ActionBody::InternalServerError`]). The response holds nothing of the
/// exception; the log holds its type and text, with the resume and the
/// build: one ERROR record.
fn child_build_raised(workflow_id: Uuid, subject: &str, cause: &str) -> ActionOutcome {
    tracing::error!(
        workflow_id = %workflow_id,
        "monitoring resume of workflow {workflow_id} failed and nothing committed: {subject} \
         raised {cause}",
    );
    ActionOutcome {
        status_code: 500,
        body: ActionBody::InternalServerError,
    }
}

/// Resume a workflow tree; each owed sub-workflow child start builds its
/// child through `children`, the client's child spec source (the
/// `build_with` of the Python client, the registry of the Rust client). A resume that committed and whose recovery
/// pass failed answers success with a warning ([`resume_failure`]). A child
/// build of the client that raised answers 500 ([`child_build_raised`]).
pub async fn resume_workflow_action(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    children: &dyn ChildSpecSource,
) -> ActionOutcome {
    match Box::pin(crate::workflow_engine::lifecycle::resume_with(
        broker,
        workflow_id,
        children,
    ))
    .await
    {
        Ok(true) => succeeded("resumed", None, None),
        Ok(false) => match current_status(broker, workflow_id).await {
            Ok(status) => conflict(ActionConflictCode::StateConflict, Some(status.to_string())),
            Err(outcome) => outcome,
        },
        Err(ResumeFailure::Step(WorkflowError::ChildSpecSource(
            ChildSpecUnavailable::Raised { subject, cause },
        ))) => child_build_raised(workflow_id, &subject, &cause),
        Err(failure) => resume_failure(failure.into_handle_error(workflow_id)),
    }
}

pub async fn cancel_workflow_action(broker: &PostgresBroker, workflow_id: Uuid) -> ActionOutcome {
    match Box::pin(crate::workflow_engine::lifecycle::cancel_workflow(
        broker,
        workflow_id,
    ))
    .await
    {
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
    use crate::core::workflow::handle_types::HandleErrorCode;

    /// The result word of the log line is Python's: the outcome, else the
    /// code, else `error`.
    #[test]
    fn the_result_word_is_the_outcome_then_the_code() {
        assert_eq!(succeeded("paused", None, None).result_word(), "paused");
        assert_eq!(
            conflict(ActionConflictCode::StateConflict, None).result_word(),
            "STATE_CONFLICT"
        );
        assert_eq!(
            conflict(ActionConflictCode::TaskNotCancellable, None).result_word(),
            "TASK_NOT_CANCELLABLE"
        );
        let workflow_task = TaskActionError::WorkflowTask {
            message: "a workflow task".to_owned(),
            task_id: Uuid::nil(),
            current_status: crate::TaskStatus::Pending,
        };
        assert_eq!(
            task_action_failed(workflow_task).result_word(),
            "TASK_IS_WORKFLOW_TASK"
        );
        assert_eq!(detail(503, "down").result_word(), "error");
        for code in [
            ActionConflictCode::TaskNotCancellable,
            ActionConflictCode::StateConflict,
        ] {
            assert_eq!(
                serde_json::to_value(code).expect("a code serializes"),
                serde_json::Value::from(code.wire_value())
            );
        }
    }

    #[test]
    fn a_connect_failure_is_503_with_its_cause() {
        let outcome = connect_failed_action("workflow pause failed: connection refused");
        assert_eq!(outcome.status_code(), 503);
        assert_eq!(
            outcome.into_body(),
            ActionBody::Detail {
                detail: "workflow pause failed: connection refused".to_owned()
            }
        );
    }

    #[test]
    fn resume_failure_mapping_is_exhaustive() {
        let error = |code: HandleErrorCode| {
            let message = "recovery failed".to_owned();
            let workflow_id = Uuid::nil();
            let cause = || {
                WorkflowError::Broker(crate::broker::BrokerError::ConnectionFailed(
                    "refused".to_owned(),
                ))
            };
            match code {
                HandleErrorCode::WorkflowNotFound => HandleOperationError::WorkflowNotFound {
                    message,
                    workflow_id,
                },
                HandleErrorCode::DbOperationFailed => HandleOperationError::DbOperationFailed {
                    message,
                    retryable: true,
                    workflow_id,
                    cause: cause(),
                },
                HandleErrorCode::StoredDataInvalid => HandleOperationError::StoredDataInvalid {
                    message,
                    workflow_id,
                    cause: cause(),
                },
                HandleErrorCode::ResumeRecoveryFailed => {
                    HandleOperationError::ResumeRecoveryFailed {
                        message,
                        workflow_id,
                        cause: cause(),
                    }
                }
                HandleErrorCode::AsyncContext => HandleOperationError::AsyncContext {
                    message,
                    workflow_id,
                },
                HandleErrorCode::LoopRunnerFailed | HandleErrorCode::EventLoopLimit => {
                    unreachable!("{code} is a code of the Python client only")
                }
            }
        };
        assert_eq!(
            handle_failed(error(HandleErrorCode::WorkflowNotFound)).status_code(),
            404
        );
        for code in [
            HandleErrorCode::DbOperationFailed,
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
        for code in [HandleErrorCode::DbOperationFailed] {
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

    /// Python's resume route let an exception of a child build propagate,
    /// and Starlette answered 500 with its text: the answer holds nothing of
    /// the exception, and its body is not JSON.
    #[test]
    fn a_child_build_that_raised_is_python_s_internal_server_error() {
        let outcome = child_build_raised(
            Uuid::nil(),
            "build of the child of nightly:0 (definition report.v1)",
            "SerializationError: options of user@example.com",
        );

        assert_eq!(outcome.status_code(), 500);
        assert_eq!(outcome.body(), &ActionBody::InternalServerError);
        assert_eq!(outcome.result_word(), "error");
        assert!(serde_json::to_value(outcome.body()).is_err());
        assert_eq!(INTERNAL_SERVER_ERROR_TEXT, "Internal Server Error");
    }

    #[test]
    #[should_panic(expected = "got ASYNC_CONTEXT")]
    fn async_context_is_a_defect_of_the_web_workflow_actions() {
        let _outcome = handle_failed(HandleOperationError::AsyncContext {
            message: "sync call inside a loop".to_owned(),
            workflow_id: Uuid::nil(),
        });
    }
}
