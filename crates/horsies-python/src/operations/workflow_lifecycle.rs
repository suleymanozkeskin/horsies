//! The pause and the cancel of a workflow through the core:
//! `horsies_workflow_pause` (`PostgresBroker::pause_workflow`) and
//! `horsies_workflow_cancel` (`PostgresBroker::cancel_workflow`).
//!
//! The outcome is one class per answer. Pause: `WorkflowPaused`,
//! `WorkflowNotRunning`, `WorkflowAbsent`, or `HandleFailed`. Cancel:
//! `WorkflowCancelled`, `WorkflowNotCancellable`, `WorkflowAbsent`, or
//! `HandleFailed`. The Python client maps them to the `HandleResult` of
//! `pause_workflow` and `WorkflowHandle.cancel_async`. A row outside a
//! function's contract panics in the core and raises `PanicException`, as
//! Python raises `OutcomeDecodeError`.

use horsies_core::catalog::CodeEnum;
use horsies_core::{HandleErrorCode, HandleOperationError, HandleResult};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// The workflow was RUNNING; it and its RUNNING descendants are PAUSED, and
/// the claimed tasks of the paused tree are abandoned.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowPaused {
    pub workflow_id: Uuid,
}

/// The workflow exists and is not RUNNING. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowNotRunning {
    pub workflow_id: Uuid,
}

/// The workflow was PENDING, RUNNING or PAUSED; its tree is CANCELLED.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowCancelled {
    pub workflow_id: Uuid,
}

/// The workflow is COMPLETED, FAILED, CANCELLED or EXPIRED. Nothing was
/// written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowNotCancellable {
    pub workflow_id: Uuid,
}

/// No workflow has this id. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowAbsent {
    pub workflow_id: Uuid,
}

/// The connect, the client family check or the statement failed
/// (`DB_OPERATION_FAILED`), or a stored value of the workflow does not parse
/// (`STORED_DATA_INVALID`, not retryable). After `DB_OPERATION_FAILED` the
/// statement wrote nothing, or it committed and its reply was lost.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct HandleFailed {
    pub workflow_id: Uuid,
    /// `DB_OPERATION_FAILED` or `STORED_DATA_INVALID`, a wire value of
    /// `HandleErrorCode`.
    pub code: String,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the handle operation as its subject (for example
    /// `pause_workflow`).
    pub exception: FailureException,
}

impl HandleFailed {
    /// The record of a handle error of the operation `subject`. Pure.
    pub fn of_handle_error(
        subject: &'static str,
        workflow_id: Uuid,
        error: &HandleOperationError,
    ) -> Self {
        Self {
            workflow_id,
            code: error.code.wire_value().to_owned(),
            message: error.message.clone(),
            retryable: error.retryable,
            exception: FailureException::of_handle_error(subject, error),
        }
    }

    /// The record of a failed connect of the operation `subject`: no
    /// statement ran. Pure.
    pub fn of_connect_error(
        subject: &'static str,
        workflow_id: Uuid,
        error: &horsies_core::BrokerError,
    ) -> Self {
        Self {
            workflow_id,
            code: HandleErrorCode::DbOperationFailed.wire_value().to_owned(),
            message: error.to_string(),
            retryable: error.is_retryable(),
            exception: FailureException::of(subject, error),
        }
    }
}

/// The operation whose answer a record reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transition {
    Pause,
    Cancel,
}

impl Transition {
    /// The subject of the exception of a failed transition.
    const fn subject(self) -> &'static str {
        match self {
            Self::Pause => "pause_workflow",
            Self::Cancel => "cancel_workflow",
        }
    }
}

/// The outcome of one pause or cancel.
pub enum WorkflowTransitionOutcome {
    Paused(WorkflowPaused),
    NotRunning(WorkflowNotRunning),
    Cancelled(WorkflowCancelled),
    NotCancellable(WorkflowNotCancellable),
    Absent(WorkflowAbsent),
    Failed(HandleFailed),
}

impl<'py> IntoPyObject<'py> for WorkflowTransitionOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Paused(paused) => paused.into_bound_py_any(py),
            Self::NotRunning(not_running) => not_running.into_bound_py_any(py),
            Self::Cancelled(cancelled) => cancelled.into_bound_py_any(py),
            Self::NotCancellable(not_cancellable) => not_cancellable.into_bound_py_any(py),
            Self::Absent(absent) => absent.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The record of a handle error of a pause or a cancel. Pure.
///
/// Panics: a code that the core gives only for another operation
/// (`RESUME_RECOVERY_FAILED`) or only in Python (`LOOP_RUNNER_FAILED`,
/// `EVENT_LOOP_LIMIT`, `ASYNC_CONTEXT`): a defect of the core.
fn failed_record(
    transition: Transition,
    workflow_id: Uuid,
    error: &HandleOperationError,
) -> WorkflowTransitionOutcome {
    match error.code {
        HandleErrorCode::WorkflowNotFound => {
            WorkflowTransitionOutcome::Absent(WorkflowAbsent { workflow_id })
        }
        HandleErrorCode::DbOperationFailed | HandleErrorCode::StoredDataInvalid => {
            WorkflowTransitionOutcome::Failed(HandleFailed::of_handle_error(
                transition.subject(),
                workflow_id,
                error,
            ))
        }
        HandleErrorCode::ResumeRecoveryFailed
        | HandleErrorCode::LoopRunnerFailed
        | HandleErrorCode::EventLoopLimit
        | HandleErrorCode::AsyncContext => panic!(
            "a pause or cancel of workflow {workflow_id} answered {} (a defect): {}",
            error.code, error.message
        ),
    }
}

/// The outcome of a pause or a cancel. Pure.
fn transition_outcome(
    transition: Transition,
    workflow_id: Uuid,
    answer: HandleResult<bool>,
) -> WorkflowTransitionOutcome {
    match (transition, answer) {
        (Transition::Pause, Ok(true)) => {
            WorkflowTransitionOutcome::Paused(WorkflowPaused { workflow_id })
        }
        (Transition::Pause, Ok(false)) => {
            WorkflowTransitionOutcome::NotRunning(WorkflowNotRunning { workflow_id })
        }
        (Transition::Cancel, Ok(true)) => {
            WorkflowTransitionOutcome::Cancelled(WorkflowCancelled { workflow_id })
        }
        (Transition::Cancel, Ok(false)) => {
            WorkflowTransitionOutcome::NotCancellable(WorkflowNotCancellable { workflow_id })
        }
        (_, Err(error)) => failed_record(transition, workflow_id, &error),
    }
}

/// The record of a failed connect. Pure.
fn connect_failed(
    transition: Transition,
    workflow_id: Uuid,
    error: &horsies_core::BrokerError,
) -> WorkflowTransitionOutcome {
    WorkflowTransitionOutcome::Failed(HandleFailed::of_connect_error(
        transition.subject(),
        workflow_id,
        error,
    ))
}

/// Pause one workflow tree. Writes, in one statement; a caller that leaves
/// early does not stop it, and sees its outcome as unknown.
pub async fn pause_workflow_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
) -> WorkflowTransitionOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return connect_failed(Transition::Pause, workflow_id, &error),
    };
    transition_outcome(
        Transition::Pause,
        workflow_id,
        broker.pause_workflow(workflow_id).await,
    )
}

/// Cancel one workflow tree. Writes, in one statement; a caller that leaves
/// early does not stop it, and sees its outcome as unknown.
pub async fn cancel_workflow_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
) -> WorkflowTransitionOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return connect_failed(Transition::Cancel, workflow_id, &error),
    };
    transition_outcome(
        Transition::Cancel,
        workflow_id,
        broker.cancel_workflow(workflow_id).await,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workflow() -> Uuid {
        Uuid::from_u128(7)
    }

    fn handle_error(code: HandleErrorCode, retryable: bool) -> HandleOperationError {
        HandleOperationError {
            code,
            message: format!("planned {code}"),
            retryable,
            workflow_id: workflow(),
            cause: horsies_core::FailureCause::Connection,
        }
    }

    #[test]
    fn each_pause_answer_has_its_own_record() {
        assert!(matches!(
            transition_outcome(Transition::Pause, workflow(), Ok(true)),
            WorkflowTransitionOutcome::Paused(WorkflowPaused { workflow_id }) if workflow_id == workflow()
        ));
        assert!(matches!(
            transition_outcome(Transition::Pause, workflow(), Ok(false)),
            WorkflowTransitionOutcome::NotRunning(_)
        ));
    }

    #[test]
    fn each_cancel_answer_has_its_own_record() {
        assert!(matches!(
            transition_outcome(Transition::Cancel, workflow(), Ok(true)),
            WorkflowTransitionOutcome::Cancelled(_)
        ));
        assert!(matches!(
            transition_outcome(Transition::Cancel, workflow(), Ok(false)),
            WorkflowTransitionOutcome::NotCancellable(_)
        ));
    }

    #[test]
    fn an_unknown_workflow_is_absent_for_both_transitions() {
        for transition in [Transition::Pause, Transition::Cancel] {
            assert!(matches!(
                transition_outcome(
                    transition,
                    workflow(),
                    Err(handle_error(HandleErrorCode::WorkflowNotFound, false))
                ),
                WorkflowTransitionOutcome::Absent(_)
            ));
        }
    }

    #[test]
    fn a_failure_keeps_its_code_cause_and_retry_rule() {
        let WorkflowTransitionOutcome::Failed(failed) = transition_outcome(
            Transition::Pause,
            workflow(),
            Err(handle_error(HandleErrorCode::DbOperationFailed, true)),
        ) else {
            panic!("a database failure gives HandleFailed");
        };
        assert_eq!(failed.code, "DB_OPERATION_FAILED");
        assert!(failed.message.contains("planned"), "{}", failed.message);
        assert!(failed.retryable);

        let WorkflowTransitionOutcome::Failed(invalid) = transition_outcome(
            Transition::Cancel,
            workflow(),
            Err(handle_error(HandleErrorCode::StoredDataInvalid, false)),
        ) else {
            panic!("stored data that does not parse gives HandleFailed");
        };
        assert_eq!(invalid.code, "STORED_DATA_INVALID");
        assert!(!invalid.retryable);
    }

    #[test]
    #[should_panic(expected = "a defect")]
    fn a_code_of_another_operation_is_a_defect() {
        let _ = transition_outcome(
            Transition::Pause,
            workflow(),
            Err(handle_error(HandleErrorCode::ResumeRecoveryFailed, false)),
        );
    }
}
