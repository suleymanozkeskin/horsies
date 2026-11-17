//! The resume of a workflow through the core
//! (`PostgresBroker::resume_client_workflow`): `horsies_workflow_resume` with
//! its child starts from the handle's child spec source, the parent advances
//! after the commit, and the recovery pass of the tree.
//!
//! The outcome is one class per answer: `WorkflowResumed`,
//! `WorkflowNotPaused`, `WorkflowResumeRecoveryFailed`, `WorkflowAbsent` or
//! `HandleFailed`. The Python client maps them to the `HandleResult` of
//! `resume_workflow`. A child build that raised `KeyboardInterrupt` or
//! `SystemExit` rolls the resume back and the same object is raised again; a
//! row outside a function's contract raises `PanicException`, as Python
//! raises `OutcomeDecodeError`.

use horsies_core::catalog::CodeEnum;
use horsies_core::{HandleErrorCode, HandleOperationError, HandleResult};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::child_spec::PythonChildSpecSource;
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
}

/// The outcome of one resume.
pub enum WorkflowResumeOutcome {
    Resumed(WorkflowResumed),
    NotPaused(WorkflowNotPaused),
    RecoveryFailed(WorkflowResumeRecoveryFailed),
    Absent(WorkflowAbsent),
    Failed(HandleFailed),
    /// A child build raised this `BaseException`; the resume rolled back.
    Interrupted(PyErr),
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
            Self::Interrupted(exception) => Err(exception),
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
            WorkflowResumeOutcome::Failed(HandleFailed {
                workflow_id,
                code: error.code.wire_value().to_owned(),
                message: error.message.clone(),
                retryable: error.retryable,
            })
        }
        HandleErrorCode::ResumeRecoveryFailed => {
            WorkflowResumeOutcome::RecoveryFailed(WorkflowResumeRecoveryFailed {
                workflow_id,
                message: error.message.clone(),
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
/// comes first: the resume rolled back, whatever the record says. Pure apart
/// from `interrupt`.
fn resume_outcome(
    workflow_id: Uuid,
    answer: HandleResult<bool>,
    interrupt: Option<PyErr>,
) -> WorkflowResumeOutcome {
    match (interrupt, answer) {
        (Some(exception), _) => WorkflowResumeOutcome::Interrupted(exception),
        (None, Ok(true)) => WorkflowResumeOutcome::Resumed(WorkflowResumed { workflow_id }),
        (None, Ok(false)) => WorkflowResumeOutcome::NotPaused(WorkflowNotPaused { workflow_id }),
        (None, Err(error)) => failed_record(workflow_id, &error),
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
            return WorkflowResumeOutcome::Failed(HandleFailed {
                workflow_id,
                code: HandleErrorCode::DbOperationFailed.wire_value().to_owned(),
                message: error.to_string(),
                retryable: error.is_retryable(),
            })
        }
    };
    let answer = broker.resume_client_workflow(workflow_id, &source).await;
    resume_outcome(workflow_id, answer, source.take_interrupt())
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
        }
    }

    #[test]
    fn each_answer_has_its_record() {
        let id = Uuid::nil();
        assert!(matches!(
            resume_outcome(id, Ok(true), None),
            WorkflowResumeOutcome::Resumed(_)
        ));
        assert!(matches!(
            resume_outcome(id, Ok(false), None),
            WorkflowResumeOutcome::NotPaused(_)
        ));
        assert!(matches!(
            resume_outcome(
                id,
                Err(handle_error(HandleErrorCode::WorkflowNotFound, false)),
                None
            ),
            WorkflowResumeOutcome::Absent(_)
        ));
        match resume_outcome(
            id,
            Err(handle_error(HandleErrorCode::DbOperationFailed, true)),
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
        );
        assert!(matches!(outcome, WorkflowResumeOutcome::Interrupted(_)));
    }

    #[test]
    #[should_panic(expected = "a defect")]
    fn a_python_only_code_panics() {
        let _outcome = resume_outcome(
            Uuid::nil(),
            Err(handle_error(HandleErrorCode::EventLoopLimit, false)),
            None,
        );
    }
}
