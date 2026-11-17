//! The administrative cancel of one plain task through the core:
//! `horsies_cancel_task` (`PostgresBroker::cancel_task`).
//!
//! The outcome is one class per answer: `TaskCancelledRecord`,
//! `CancelRefusedWorkflowTask`, `CancelRefusedStatus`, `CancelTaskAbsent`,
//! `CancelFailed`, or `CancelStoredDataInvalid`. The Python client maps them
//! to the result of `horsies.monitoring.task_actions.cancel_task`. An
//! outcome that contradicts the function panics in the core and raises
//! `PanicException`, as Python raises `RuntimeError`.

use horsies_core::catalog::CodeEnum;
use horsies_core::{BrokerError, CancelAnswer, CancelTaskError, TaskStatus};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// The plain task moved to history as CANCELLED. `was_status` is the status
/// it left: `PENDING`, `CLAIMED`, or `RUNNING` with the opt-in.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct TaskCancelledRecord {
    pub task_id: Uuid,
    /// A member of `TaskStatus`, as its wire value.
    pub was_status: String,
}

/// The task belongs to a workflow, live or finished. Nothing changed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CancelRefusedWorkflowTask {
    pub task_id: Uuid,
    /// A member of `TaskStatus`, as its wire value.
    pub current_status: String,
}

/// The task's status does not admit the cancel: a terminal status (an
/// earlier cancel included), or `RUNNING` without the opt-in. Nothing
/// changed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CancelRefusedStatus {
    pub task_id: Uuid,
    /// A member of `TaskStatus`, as its wire value.
    pub current_status: String,
}

/// Neither the live table nor history holds the task. Nothing changed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CancelTaskAbsent {
    pub task_id: Uuid,
}

/// The connect, the client family check or the statement failed. The
/// statement wrote nothing, or it committed and its reply was lost; a
/// repeated cancel tells which, because a committed cancel answers
/// `CancelRefusedStatus` with `CANCELLED`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CancelFailed {
    pub task_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `cancel_task`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed cancel.
const SUBJECT: &str = "cancel_task";

/// Stored task data does not parse, for example a history status that is
/// not a task status. The cancel refused the task, so nothing changed; a
/// repeat gives the same answer until the stored row changes.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CancelStoredDataInvalid {
    pub task_id: Uuid,
    /// What does not parse, with its subject.
    pub message: String,
}

/// The outcome of one cancel.
pub enum CancelOutcome {
    Cancelled(TaskCancelledRecord),
    WorkflowTask(CancelRefusedWorkflowTask),
    NotCancellable(CancelRefusedStatus),
    Absent(CancelTaskAbsent),
    Failed(CancelFailed),
    StoredDataInvalid(CancelStoredDataInvalid),
}

impl<'py> IntoPyObject<'py> for CancelOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Cancelled(cancelled) => cancelled.into_bound_py_any(py),
            Self::WorkflowTask(refused) => refused.into_bound_py_any(py),
            Self::NotCancellable(refused) => refused.into_bound_py_any(py),
            Self::Absent(absent) => absent.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
            Self::StoredDataInvalid(invalid) => invalid.into_bound_py_any(py),
        }
    }
}

fn wire_status(status: TaskStatus) -> String {
    status.wire_value().to_owned()
}

fn failed(task_id: Uuid, error: &BrokerError) -> CancelOutcome {
    CancelOutcome::Failed(CancelFailed {
        task_id,
        message: error.to_string(),
        retryable: error.is_retryable(),
        exception: FailureException::of(SUBJECT, error),
    })
}

/// The outcome of a cancel. Pure.
fn cancel_outcome(task_id: Uuid, answer: Result<CancelAnswer, CancelTaskError>) -> CancelOutcome {
    match answer {
        Ok(CancelAnswer::Cancelled { was_status }) => {
            CancelOutcome::Cancelled(TaskCancelledRecord {
                task_id,
                was_status: wire_status(was_status),
            })
        }
        Ok(CancelAnswer::WorkflowTask { current_status }) => {
            CancelOutcome::WorkflowTask(CancelRefusedWorkflowTask {
                task_id,
                current_status: wire_status(current_status),
            })
        }
        Ok(CancelAnswer::NotCancellable { current_status }) => {
            CancelOutcome::NotCancellable(CancelRefusedStatus {
                task_id,
                current_status: wire_status(current_status),
            })
        }
        Ok(CancelAnswer::TaskAbsent) => CancelOutcome::Absent(CancelTaskAbsent { task_id }),
        Err(CancelTaskError::Broker(error)) => failed(task_id, &error),
        Err(CancelTaskError::StoredDataInvalid(error)) => {
            CancelOutcome::StoredDataInvalid(CancelStoredDataInvalid {
                task_id,
                message: error.to_string(),
            })
        }
    }
}

/// Cancel one plain task. Writes, in one statement; a caller that leaves
/// early does not stop the statement, and sees its outcome as unknown.
pub async fn cancel_task_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    include_running: bool,
) -> CancelOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed(task_id, &error),
    };
    cancel_outcome(task_id, broker.cancel_task(task_id, include_running).await)
}

#[cfg(test)]
mod tests {
    use horsies_core::core::lifecycle::OutcomeDecodeError;

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(3)
    }

    #[test]
    fn each_answer_has_its_own_record_with_the_status_as_its_wire_value() {
        assert!(matches!(
            cancel_outcome(
                task(),
                Ok(CancelAnswer::Cancelled {
                    was_status: TaskStatus::Running
                })
            ),
            CancelOutcome::Cancelled(TaskCancelledRecord { ref was_status, .. })
                if was_status == "RUNNING"
        ));
        assert!(matches!(
            cancel_outcome(
                task(),
                Ok(CancelAnswer::WorkflowTask {
                    current_status: TaskStatus::Pending
                })
            ),
            CancelOutcome::WorkflowTask(CancelRefusedWorkflowTask { ref current_status, .. })
                if current_status == "PENDING"
        ));
        assert!(matches!(
            cancel_outcome(
                task(),
                Ok(CancelAnswer::NotCancellable {
                    current_status: TaskStatus::Cancelled
                })
            ),
            CancelOutcome::NotCancellable(CancelRefusedStatus { ref current_status, .. })
                if current_status == "CANCELLED"
        ));
        assert!(matches!(
            cancel_outcome(task(), Ok(CancelAnswer::TaskAbsent)),
            CancelOutcome::Absent(CancelTaskAbsent { task_id }) if task_id == task()
        ));
    }

    #[test]
    fn a_failure_keeps_its_cause_and_retry_rule_and_stored_data_keeps_its_cause() {
        let CancelOutcome::Failed(failed) = cancel_outcome(
            task(),
            Err(CancelTaskError::Broker(BrokerError::ClaimContract(
                "planned failure".to_owned(),
            ))),
        ) else {
            panic!("a broker failure gives CancelFailed");
        };
        assert!(
            failed.message.contains("planned failure"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);

        let CancelOutcome::StoredDataInvalid(invalid) = cancel_outcome(
            task(),
            Err(CancelTaskError::StoredDataInvalid(OutcomeDecodeError(
                "observed_status NOT_KNOWN".to_owned(),
            ))),
        ) else {
            panic!("a decode failure gives CancelStoredDataInvalid");
        };
        assert!(invalid.message.contains("NOT_KNOWN"), "{}", invalid.message);
    }
}
