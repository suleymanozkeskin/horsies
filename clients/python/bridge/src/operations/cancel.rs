//! The administrative cancel of one plain task through the core:
//! `horsies_cancel_task` (`horsies_core::monitoring::cancel_task`).
//!
//! The outcome is `TaskCancelledRecord`, or the refusal or failure as the
//! generated class of the core's `TaskActionError`
//! (`horsies._catalog.monitoring_errors`: `TaskActionNotFound`,
//! `TaskActionNotCancellable`, `TaskActionWorkflowTask`,
//! `TaskActionDbOperationFailed` or `TaskActionStoredDataInvalid`), built in
//! Rust (`catalog_errors`). The core gives each class its message, so the
//! Python client returns it as it is. An outcome that contradicts the
//! function panics in the core and raises `PanicException`, as Python
//! raises `RuntimeError`.

use horsies_core::catalog::CodeEnum;
use horsies_core::monitoring::{cancel_task, TaskActionError, TaskActionResult, TaskCancelled};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::catalog_errors::catalog_error;
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

/// The subject of the exception of a failed cancel.
const SUBJECT: &str = "cancel_task";

/// The outcome of one cancel.
#[derive(Debug)]
pub enum CancelOutcome {
    Cancelled(TaskCancelledRecord),
    /// Refused or failed: Python reads it as the generated class of the
    /// error, with `SUBJECT` as the subject of its exception.
    Refused(TaskActionError),
}

impl<'py> IntoPyObject<'py> for CancelOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Cancelled(cancelled) => cancelled.into_bound_py_any(py),
            Self::Refused(error) => catalog_error(py, SUBJECT, &error),
        }
    }
}

/// The outcome of the core's answer to a cancel. Pure.
fn cancel_outcome(answer: TaskActionResult<TaskCancelled>) -> CancelOutcome {
    match answer {
        Ok(cancelled) => CancelOutcome::Cancelled(TaskCancelledRecord {
            task_id: cancelled.task_id,
            was_status: cancelled.was_status.wire_value().to_owned(),
        }),
        Err(error) => CancelOutcome::Refused(error),
    }
}

/// Cancel one plain task. Writes, in one statement; a caller that leaves
/// early does not stop the statement, and sees its outcome as unknown. A
/// failed connect is `TaskActionDbOperationFailed`: nothing was sent.
pub async fn cancel_task_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    include_running: bool,
) -> CancelOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return CancelOutcome::Refused(TaskActionError::cancel_failed(task_id, error))
        }
    };
    cancel_outcome(cancel_task(&broker, task_id, include_running).await)
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerError, TaskStatus};

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(3)
    }

    #[test]
    fn a_cancel_is_a_record_with_the_status_as_its_wire_value() {
        let outcome = cancel_outcome(Ok(TaskCancelled {
            task_id: task(),
            was_status: TaskStatus::Running,
        }));
        assert!(matches!(
            outcome,
            CancelOutcome::Cancelled(TaskCancelledRecord { task_id, ref was_status })
                if task_id == task() && was_status == "RUNNING"
        ));
    }

    /// A refusal and a failure stay the core's value: the message, the
    /// retry rule and the cause that Python's class gets.
    #[test]
    fn a_refusal_or_a_failure_keeps_the_core_s_value() {
        let outcome = cancel_outcome(Err(TaskActionError::cancel_failed(
            task(),
            BrokerError::ClaimContract("planned failure".to_owned()),
        )));
        let CancelOutcome::Refused(TaskActionError::DbOperationFailed {
            message,
            retryable,
            task_id,
            cause,
        }) = outcome
        else {
            panic!("a broker failure stays DbOperationFailed");
        };
        assert_eq!(
            message,
            "Task cancel failed: claim contract violation: planned failure"
        );
        assert!(!retryable);
        assert_eq!(task_id, task());
        assert!(matches!(cause, BrokerError::ClaimContract(ref text) if text == "planned failure"));

        let refused = cancel_outcome(Err(TaskActionError::NotFound {
            message: format!("Task {} does not exist.", task()),
            task_id: task(),
        }));
        assert!(matches!(
            refused,
            CancelOutcome::Refused(TaskActionError::NotFound { task_id, .. }) if task_id == task()
        ));
    }
}
