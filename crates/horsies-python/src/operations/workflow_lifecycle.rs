//! The pause and the cancel of a workflow through the core:
//! `horsies_workflow_pause` (`PostgresBroker::pause_workflow`) and
//! `horsies_workflow_cancel` (`PostgresBroker::cancel_workflow`).
//!
//! The outcome is one class per answer. Pause: `WorkflowPaused`,
//! `WorkflowNotRunning`, `WorkflowAbsent`, or a failure. Cancel:
//! `WorkflowCancelled`, `WorkflowNotCancellable`, `WorkflowAbsent`, or a
//! failure. A failure is the generated class of the core's
//! `HandleOperationError` (`horsies._catalog.workflow_handle_errors`:
//! `HandleDbOperationFailed` or `HandleStoredDataInvalid`), built in Rust
//! (`catalog_errors`) with the core's message. The Python client maps the
//! records to the `HandleResult` of `pause_workflow` and
//! `WorkflowHandle.cancel_async`, and returns a failure as it is. A row
//! outside a function's contract panics in the core and raises
//! `PanicException`, as Python raises `OutcomeDecodeError`.

use horsies_core::{HandleOperation, HandleOperationError, HandleResult};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::catalog_errors::catalog_error;
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

/// A failed handle operation: `DB_OPERATION_FAILED` (the connect, the
/// client family check or the statement failed; the statement wrote
/// nothing, or it committed and its reply was lost) or
/// `STORED_DATA_INVALID` (a stored value of the workflow does not parse,
/// not retryable). Python reads it as the generated class of `error`, with
/// the subject of `operation` as the subject of its exception.
#[derive(Debug)]
pub struct HandleFailure {
    pub operation: HandleOperation,
    pub error: HandleOperationError,
}

impl HandleFailure {
    /// A failed connect of `operation`: no statement ran. Pure.
    pub fn connect_failed(
        operation: HandleOperation,
        workflow_id: Uuid,
        error: horsies_core::BrokerError,
    ) -> Self {
        Self {
            error: HandleOperationError::connect_failed(operation.clone(), workflow_id, error),
            operation,
        }
    }
}

impl<'py> IntoPyObject<'py> for HandleFailure {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        catalog_error(py, self.operation.subject(), &self.error)
    }
}

/// The operation whose answer a record reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transition {
    Pause,
    Cancel,
}

impl Transition {
    /// The handle operation of the transition: its label and the subject
    /// of the exception of a failure.
    fn operation(self) -> HandleOperation {
        match self {
            Self::Pause => HandleOperation::Pause,
            Self::Cancel => HandleOperation::Cancel,
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
    Failed(HandleFailure),
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
            Self::Failed(failed) => failed.into_pyobject(py),
        }
    }
}

/// The record of a handle error of a pause or a cancel. Pure.
///
/// Panics: a code that the core gives only for another operation
/// (`RESUME_RECOVERY_FAILED`) or only to a sync or blocking form
/// (`ASYNC_CONTEXT`): a defect of the core.
fn failed_record(
    transition: Transition,
    workflow_id: Uuid,
    error: HandleOperationError,
) -> WorkflowTransitionOutcome {
    match error {
        HandleOperationError::WorkflowNotFound { .. } => {
            WorkflowTransitionOutcome::Absent(WorkflowAbsent { workflow_id })
        }
        HandleOperationError::DbOperationFailed { .. }
        | HandleOperationError::StoredDataInvalid { .. } => {
            WorkflowTransitionOutcome::Failed(HandleFailure {
                operation: transition.operation(),
                error,
            })
        }
        HandleOperationError::ResumeRecoveryFailed { .. }
        | HandleOperationError::AsyncContext { .. } => panic!(
            "a pause or cancel of workflow {workflow_id} answered {} (a defect): {}",
            error.code(),
            error.message()
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
        (_, Err(error)) => failed_record(transition, workflow_id, error),
    }
}

/// The record of a failed connect. Pure.
fn connect_failed(
    transition: Transition,
    workflow_id: Uuid,
    error: horsies_core::BrokerError,
) -> WorkflowTransitionOutcome {
    WorkflowTransitionOutcome::Failed(HandleFailure::connect_failed(
        transition.operation(),
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
        Err(error) => return connect_failed(Transition::Pause, workflow_id, error),
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
        Err(error) => return connect_failed(Transition::Cancel, workflow_id, error),
    };
    transition_outcome(
        Transition::Cancel,
        workflow_id,
        broker.cancel_workflow(workflow_id).await,
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerError, HandleErrorCode, WorkflowError};

    use super::*;

    fn workflow() -> Uuid {
        Uuid::from_u128(7)
    }

    fn handle_error(code: HandleErrorCode, retryable: bool) -> HandleOperationError {
        let message = format!("planned {code}");
        let workflow_id = workflow();
        let cause = || WorkflowError::Broker(BrokerError::ConnectionFailed("refused".to_owned()));
        match code {
            HandleErrorCode::WorkflowNotFound => HandleOperationError::WorkflowNotFound {
                message,
                workflow_id,
            },
            HandleErrorCode::DbOperationFailed => HandleOperationError::DbOperationFailed {
                message,
                retryable,
                workflow_id,
                cause: cause(),
            },
            HandleErrorCode::StoredDataInvalid => HandleOperationError::StoredDataInvalid {
                message,
                workflow_id,
                cause: WorkflowError::InvalidStatus("BOGUS".to_owned()),
            },
            HandleErrorCode::ResumeRecoveryFailed => HandleOperationError::ResumeRecoveryFailed {
                message,
                workflow_id,
                cause: cause(),
            },
            HandleErrorCode::AsyncContext => HandleOperationError::AsyncContext {
                message,
                workflow_id,
            },
            HandleErrorCode::LoopRunnerFailed | HandleErrorCode::EventLoopLimit => {
                unreachable!("{code} is a code of the Python client only")
            }
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

    /// A failure stays the core's value, with the transition's operation:
    /// Python reads its generated class with the operation's subject.
    #[test]
    fn a_failure_keeps_the_core_s_value_and_its_operation() {
        let WorkflowTransitionOutcome::Failed(failed) = transition_outcome(
            Transition::Pause,
            workflow(),
            Err(handle_error(HandleErrorCode::DbOperationFailed, true)),
        ) else {
            panic!("a database failure gives a failure");
        };
        assert_eq!(failed.operation, HandleOperation::Pause);
        assert_eq!(failed.error.code(), HandleErrorCode::DbOperationFailed);
        assert_eq!(failed.error.message(), "planned DB_OPERATION_FAILED");
        assert!(failed.error.retryable());

        let WorkflowTransitionOutcome::Failed(invalid) = transition_outcome(
            Transition::Cancel,
            workflow(),
            Err(handle_error(HandleErrorCode::StoredDataInvalid, false)),
        ) else {
            panic!("stored data that does not parse gives a failure");
        };
        assert_eq!(invalid.operation, HandleOperation::Cancel);
        assert_eq!(invalid.error.code(), HandleErrorCode::StoredDataInvalid);
        assert!(!invalid.error.retryable());
    }

    /// A failed connect names the transition's operation and keeps the
    /// broker error as its cause.
    #[test]
    fn a_failed_connect_names_the_operation_and_keeps_the_broker_error() {
        let WorkflowTransitionOutcome::Failed(failed) = connect_failed(
            Transition::Cancel,
            workflow(),
            BrokerError::ConnectionFailed("refused".to_owned()),
        ) else {
            panic!("a failed connect gives a failure");
        };
        assert_eq!(failed.operation, HandleOperation::Cancel);
        assert_eq!(
            failed.error.message(),
            format!(
                "Cancel failed for workflow {}: connection failed: refused",
                workflow()
            )
        );
        assert!(failed.error.retryable());
        assert!(matches!(
            failed.error,
            HandleOperationError::DbOperationFailed {
                cause: WorkflowError::Broker(BrokerError::ConnectionFailed(_)),
                ..
            }
        ));
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
