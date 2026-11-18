//! The resume of a workflow through the core
//! (`PostgresBroker::resume_client_workflow`): `horsies_workflow_resume` with
//! its child starts from the handle's child spec source, the parent advances
//! after the commit, and the recovery pass of the tree.
//!
//! The outcome is one class per answer: `WorkflowResumed`,
//! `WorkflowNotPaused`, `WorkflowResumeRecoveryFailed`, `WorkflowAbsent` or
//! `HandleFailed`. The Python client maps them to the `HandleResult` of
//! `resume_workflow`. A child build that raised `KeyboardInterrupt` or
//! `SystemExit` rolls the resume back and the same object is raised again. A
//! child build that raised an `Exception` (such as `SerializationError`) is
//! raised again when the resume failed with it, as Python's resume lets
//! every exception other than a database error propagate; one that the
//! recovery pass of the tree after the commit logged as a failed candidate
//! leaves the answer as it is, as Python's pass logs it. A row outside a
//! function's contract raises `PanicException`, as Python raises
//! `OutcomeDecodeError`.

use horsies_core::{HandleErrorCode, HandleOperationError, HandleResult};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::child_spec::PythonChildSpecSource;
use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;
use crate::operations::workflow_lifecycle::{HandleFailed, WorkflowAbsent};

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

/// The resume committed; the parent advance or the recovery pass of the tree
/// after it failed. The workflow is RUNNING, and workflow recovery finishes
/// the work. Not retryable.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowResumeRecoveryFailed {
    pub workflow_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    /// The cause of the failed work after the commit, with the subject
    /// `resume_workflow`, not retryable.
    pub exception: FailureException,
}

/// The subject of the exception of a failed resume.
const SUBJECT: &str = "resume_workflow";

/// The outcome of one resume.
pub enum WorkflowResumeOutcome {
    Resumed(WorkflowResumed),
    NotPaused(WorkflowNotPaused),
    RecoveryFailed(WorkflowResumeRecoveryFailed),
    Absent(WorkflowAbsent),
    Failed(HandleFailed),
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
            Self::RecoveryFailed(failed) => failed.into_bound_py_any(py),
            Self::Absent(absent) => absent.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
            Self::BuildRaised(exception) => Err(exception),
        }
    }
}

/// The record of a resume's handle error. Pure.
///
/// Panics: a code that the core gives only in Python (`LOOP_RUNNER_FAILED`,
/// `EVENT_LOOP_LIMIT`, `ASYNC_CONTEXT`): a defect of the core.
fn failed_record(workflow_id: Uuid, error: &HandleOperationError) -> WorkflowResumeOutcome {
    match error.code {
        HandleErrorCode::WorkflowNotFound => {
            WorkflowResumeOutcome::Absent(WorkflowAbsent { workflow_id })
        }
        HandleErrorCode::DbOperationFailed | HandleErrorCode::StoredDataInvalid => {
            WorkflowResumeOutcome::Failed(HandleFailed::of_handle_error(
                SUBJECT,
                workflow_id,
                error,
            ))
        }
        HandleErrorCode::ResumeRecoveryFailed => {
            WorkflowResumeOutcome::RecoveryFailed(WorkflowResumeRecoveryFailed {
                workflow_id,
                message: error.message.clone(),
                exception: FailureException::of_handle_error(SUBJECT, error),
            })
        }
        HandleErrorCode::LoopRunnerFailed
        | HandleErrorCode::EventLoopLimit
        | HandleErrorCode::AsyncContext => panic!(
            "a resume of workflow {workflow_id} answered {} (a defect): {}",
            error.code, error.message
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
        (None, None, Err(error)) => failed_record(workflow_id, &error),
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
            return WorkflowResumeOutcome::Failed(HandleFailed::of_connect_error(
                SUBJECT,
                workflow_id,
                &error,
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

    fn handle_error(code: HandleErrorCode, retryable: bool) -> HandleOperationError {
        HandleOperationError {
            code,
            message: "planned".to_owned(),
            retryable,
            workflow_id: Uuid::nil(),
            cause: horsies_core::FailureCause::Connection,
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
                assert_eq!(failed.code, "DB_OPERATION_FAILED");
                assert!(failed.retryable);
            }
            _ => panic!("a database failure is HandleFailed"),
        }
        assert!(matches!(
            resume_outcome(
                id,
                Err(handle_error(HandleErrorCode::ResumeRecoveryFailed, false)),
                None,
                None
            ),
            WorkflowResumeOutcome::RecoveryFailed(_)
        ));
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
            Err(handle_error(HandleErrorCode::EventLoopLimit, false)),
            None,
            None,
        );
    }
}
