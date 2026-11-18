//! The resume of a workflow through the core
//! (`PostgresBroker::resume_client_workflow`): `horsies_workflow_resume` with
//! its child starts from the handle's child spec source, the parent advances
//! after the commit, and the recovery pass of the tree.
//!
//! The outcome is one class per answer: `WorkflowResumed`,
//! `WorkflowNotPaused`, `WorkflowAbsent`, or a failure as the generated
//! class of the core's `HandleOperationError`
//! (`horsies._catalog.workflow_handle_errors`: `HandleDbOperationFailed`,
//! `HandleStoredDataInvalid` or `HandleResumeRecoveryFailed`), built in Rust
//! (`catalog_errors`) with the core's message. The Python client maps the
//! records to the `HandleResult` of `resume_workflow`, and returns a failure
//! as it is. A child build that raised `KeyboardInterrupt` or
//! `SystemExit` rolls the resume back and the same object is raised again. A
//! child build that raised an `Exception` (such as `SerializationError`) is
//! raised again when the resume failed with it, as Python's resume lets
//! every exception other than a database error propagate; one that the
//! recovery pass of the tree after the commit logged as a failed candidate
//! leaves the answer as it is, as Python's pass logs it. A row outside a
//! function's contract raises `PanicException`, as Python raises
//! `OutcomeDecodeError`.

use horsies_core::{HandleOperation, HandleOperationError, HandleResult};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::child_spec::PythonChildSpecSource;
use crate::handle::BrokerConnection;
use crate::operations::workflow_lifecycle::{HandleFailure, WorkflowAbsent};

/// The workflow was PAUSED; its tree is RUNNING again.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowResumed {
    pub workflow_id: Uuid,
}

/// The workflow exists and is not PAUSED. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowNotPaused {
    pub workflow_id: Uuid,
}

/// The outcome of one resume.
pub enum WorkflowResumeOutcome {
    Resumed(WorkflowResumed),
    NotPaused(WorkflowNotPaused),
    Absent(WorkflowAbsent),
    /// `DB_OPERATION_FAILED`, `STORED_DATA_INVALID` or
    /// `RESUME_RECOVERY_FAILED` (the resume committed; the parent advance or
    /// the recovery pass of the tree after it failed, and the workflow is
    /// RUNNING).
    Failed(HandleFailure),
    /// A child build raised this `BaseException` (an interrupt, or an
    /// `Exception` that failed the resume).
    BuildRaised(PyErr),
}

impl<'py> IntoPyObject<'py> for WorkflowResumeOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Resumed(resumed) => resumed.into_bound_py_any(py),
            Self::NotPaused(not_paused) => not_paused.into_bound_py_any(py),
            Self::Absent(absent) => absent.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_pyobject(py),
            Self::BuildRaised(exception) => Err(exception),
        }
    }
}

/// The record of a resume's handle error. Pure.
///
/// Panics: `ASYNC_CONTEXT`, which only a sync or blocking form gives: a
/// defect of the core.
fn failed_record(workflow_id: Uuid, error: HandleOperationError) -> WorkflowResumeOutcome {
    match error {
        HandleOperationError::WorkflowNotFound { .. } => {
            WorkflowResumeOutcome::Absent(WorkflowAbsent { workflow_id })
        }
        HandleOperationError::DbOperationFailed { .. }
        | HandleOperationError::StoredDataInvalid { .. }
        | HandleOperationError::ResumeRecoveryFailed { .. } => {
            WorkflowResumeOutcome::Failed(HandleFailure {
                operation: HandleOperation::Resume,
                error,
            })
        }
        HandleOperationError::AsyncContext { .. } => panic!(
            "a resume of workflow {workflow_id} answered {} (a defect): {}",
            error.code(),
            error.message()
        ),
    }
}

/// The outcome of a resume's answer. An interrupt that a child build raised
/// comes first, whatever the record says. An `Exception` that a child build
/// raised comes next when the resume failed: the step that the build failed
/// rolled back. With a resume that succeeded, the build raised in the
/// recovery pass of the tree after the commit, which logged it as a failed
/// candidate; the exception is dropped. Pure apart from `interrupt` and
/// `exception`.
fn resume_outcome(
    workflow_id: Uuid,
    answer: HandleResult<bool>,
    interrupt: Option<PyErr>,
    exception: Option<PyErr>,
) -> WorkflowResumeOutcome {
    match (interrupt, exception, answer) {
        (Some(raised), _, _) => WorkflowResumeOutcome::BuildRaised(raised),
        (None, Some(raised), Err(_)) => WorkflowResumeOutcome::BuildRaised(raised),
        (None, _, Ok(true)) => WorkflowResumeOutcome::Resumed(WorkflowResumed { workflow_id }),
        (None, _, Ok(false)) => WorkflowResumeOutcome::NotPaused(WorkflowNotPaused { workflow_id }),
        (None, None, Err(error)) => failed_record(workflow_id, error),
    }
}

/// Resume one workflow tree. Writes; a caller that leaves early does not
/// stop it, and sees its outcome as unknown.
pub async fn resume_workflow_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
    source: PythonChildSpecSource,
) -> WorkflowResumeOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return WorkflowResumeOutcome::Failed(HandleFailure::connect_failed(
                HandleOperation::Resume,
                workflow_id,
                error,
            ))
        }
    };
    let answer = broker.resume_client_workflow(workflow_id, &source).await;
    resume_outcome(
        workflow_id,
        answer,
        source.take_interrupt(),
        source.take_exception(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use horsies_core::HandleErrorCode;

    fn handle_error(code: HandleErrorCode, retryable: bool) -> HandleOperationError {
        let message = "planned".to_owned();
        let workflow_id = Uuid::nil();
        let cause = || {
            horsies_core::WorkflowError::Broker(horsies_core::BrokerError::ConnectionFailed(
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
                retryable,
                workflow_id,
                cause: cause(),
            },
            HandleErrorCode::StoredDataInvalid => HandleOperationError::StoredDataInvalid {
                message,
                workflow_id,
                cause: horsies_core::WorkflowError::InvalidStatus("BOGUS".to_owned()),
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
        }
    }

    #[test]
    fn each_answer_has_its_record() {
        let id = Uuid::nil();
        assert!(matches!(
            resume_outcome(id, Ok(true), None, None),
            WorkflowResumeOutcome::Resumed(_)
        ));
        assert!(matches!(
            resume_outcome(id, Ok(false), None, None),
            WorkflowResumeOutcome::NotPaused(_)
        ));
        assert!(matches!(
            resume_outcome(
                id,
                Err(handle_error(HandleErrorCode::WorkflowNotFound, false)),
                None,
                None
            ),
            WorkflowResumeOutcome::Absent(_)
        ));
        match resume_outcome(
            id,
            Err(handle_error(HandleErrorCode::DbOperationFailed, true)),
            None,
            None,
        ) {
            WorkflowResumeOutcome::Failed(failed) => {
                assert_eq!(failed.operation, HandleOperation::Resume);
                assert_eq!(failed.error.code(), HandleErrorCode::DbOperationFailed);
                assert!(failed.error.retryable());
            }
            _ => panic!("a database failure is a failure"),
        }
        match resume_outcome(
            id,
            Err(handle_error(HandleErrorCode::ResumeRecoveryFailed, false)),
            None,
            None,
        ) {
            WorkflowResumeOutcome::Failed(failed) => {
                assert_eq!(failed.operation, HandleOperation::Resume);
                assert_eq!(failed.error.code(), HandleErrorCode::ResumeRecoveryFailed);
                assert_eq!(failed.error.message(), "planned");
            }
            _ => panic!("a failed recovery pass is a failure"),
        }
    }

    #[test]
    fn an_interrupt_comes_before_the_record() {
        Python::initialize();
        let interrupt = Some(pyo3::exceptions::PyKeyboardInterrupt::new_err(()));
        let outcome = resume_outcome(
            Uuid::nil(),
            Err(handle_error(HandleErrorCode::DbOperationFailed, false)),
            interrupt,
            None,
        );
        assert!(matches!(outcome, WorkflowResumeOutcome::BuildRaised(_)));
    }

    /// Python's resume lets the exception of a child build propagate: a
    /// resume that failed with it raises the same object again.
    #[test]
    fn an_exception_of_a_failed_resume_comes_before_the_record() {
        Python::initialize();
        let exception = Some(pyo3::exceptions::PyValueError::new_err(
            "Corrupt task_options",
        ));
        let outcome = resume_outcome(
            Uuid::nil(),
            Err(handle_error(HandleErrorCode::DbOperationFailed, false)),
            None,
            exception,
        );
        assert!(matches!(outcome, WorkflowResumeOutcome::BuildRaised(_)));
    }

    /// A resume that succeeded keeps its record: the exception came from a
    /// candidate of the recovery pass of the tree, which logged it.
    #[test]
    fn an_exception_of_a_resume_that_succeeded_is_dropped() {
        Python::initialize();
        let exception = Some(pyo3::exceptions::PyValueError::new_err(
            "Corrupt task_options",
        ));
        let outcome = resume_outcome(Uuid::nil(), Ok(true), None, exception);
        assert!(matches!(outcome, WorkflowResumeOutcome::Resumed(_)));
    }

    #[test]
    #[should_panic(expected = "a defect")]
    fn a_python_only_code_panics() {
        let _outcome = resume_outcome(
            Uuid::nil(),
            Err(handle_error(HandleErrorCode::AsyncContext, false)),
            None,
            None,
        );
    }
}
