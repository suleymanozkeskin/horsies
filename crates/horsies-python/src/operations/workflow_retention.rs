//! The workflow retention delete of the reaper through the core
//! (`PostgresBroker::delete_expired_workflows`): batches of terminal
//! workflows past their retention, with their node rows, until a batch
//! deletes none or the pass budget has passed.
//!
//! The outcome is `WorkflowsDeleted` or `WorkflowDeleteFailed`. The Python
//! reaper logs a failure as its retention cleanup error, as it logs the
//! driver's exception today.

use std::num::NonZeroU32;
use std::time::Duration;

use horsies_core::{BrokerError, WorkflowRetentionFailed, WorkflowRetentionPass};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;

/// The pass ended: a batch deleted no workflow, or the budget passed after
/// a batch. `deleted` counts the workflows deleted.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowsDeleted {
    pub deleted: u64,
}

/// The pass failed: an hour count or batch size of 0 (not retryable), a
/// failed connect, a refused or failed client family check, or a failed
/// batch. `deleted_before` counts the workflows of the batches that
/// committed first; a batch whose reply was lost may have committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowDeleteFailed {
    pub deleted_before: u64,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one workflow retention pass.
pub enum WorkflowRetentionOutcome {
    Deleted(WorkflowsDeleted),
    Failed(WorkflowDeleteFailed),
}

impl<'py> IntoPyObject<'py> for WorkflowRetentionOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Deleted(deleted) => deleted.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The bounds of a pass, or the refusal as text. Pure.
fn retention_pass(
    older_than_hours: u32,
    batch_size: u32,
    budget_ms: u64,
) -> Result<WorkflowRetentionPass, String> {
    let retention_hours = NonZeroU32::new(older_than_hours).ok_or_else(|| {
        "the workflow retention delete needs a retention of 1 hour or more, not 0".to_owned()
    })?;
    let batch_size = NonZeroU32::new(batch_size).ok_or_else(|| {
        "the workflow retention delete needs a batch size of 1 or more, not 0".to_owned()
    })?;
    Ok(WorkflowRetentionPass {
        retention_hours,
        batch_size,
        budget: Duration::from_millis(budget_ms),
    })
}

fn failed_before_start(error: &BrokerError) -> WorkflowRetentionOutcome {
    WorkflowRetentionOutcome::Failed(WorkflowDeleteFailed {
        deleted_before: 0,
        message: error.to_string(),
        retryable: error.is_retryable(),
    })
}

/// The outcome of a pass. Pure.
fn pass_outcome(pass: Result<u64, WorkflowRetentionFailed>) -> WorkflowRetentionOutcome {
    match pass {
        Ok(deleted) => WorkflowRetentionOutcome::Deleted(WorkflowsDeleted { deleted }),
        Err(failed) => WorkflowRetentionOutcome::Failed(WorkflowDeleteFailed {
            deleted_before: failed.deleted_before,
            message: failed.error.to_string(),
            retryable: failed.error.is_retryable(),
        }),
    }
}

/// Run one workflow retention pass. Writes; each batch commits alone, and
/// a caller that leaves early does not stop the pass.
pub async fn delete_expired_workflows_operation(
    connection: BrokerConnection,
    older_than_hours: u32,
    batch_size: u32,
    budget_ms: u64,
) -> WorkflowRetentionOutcome {
    let pass = match retention_pass(older_than_hours, batch_size, budget_ms) {
        Ok(pass) => pass,
        Err(message) => {
            return WorkflowRetentionOutcome::Failed(WorkflowDeleteFailed {
                deleted_before: 0,
                message,
                retryable: false,
            })
        }
    };
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed_before_start(&error),
    };
    pass_outcome(broker.delete_expired_workflows(pass).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bounds_are_kept_and_a_zero_is_refused_with_its_name() {
        let pass = retention_pass(720, 500, 60_000).expect("bounds in range");
        assert_eq!(pass.retention_hours.get(), 720);
        assert_eq!(pass.batch_size.get(), 500);
        assert_eq!(pass.budget, Duration::from_secs(60));

        let hours = retention_pass(0, 500, 60_000).expect_err("a retention of 0 hours");
        assert!(hours.contains("retention of 1 hour"), "{hours}");
        let batch = retention_pass(720, 0, 60_000).expect_err("a batch of 0");
        assert!(batch.contains("batch size"), "{batch}");
    }

    #[test]
    fn a_pass_that_ended_counts_its_workflows() {
        assert!(matches!(
            pass_outcome(Ok(42)),
            WorkflowRetentionOutcome::Deleted(WorkflowsDeleted { deleted: 42 })
        ));
    }

    #[test]
    fn a_failed_pass_keeps_its_count_cause_and_retry_rule() {
        let WorkflowRetentionOutcome::Failed(failed) = pass_outcome(Err(WorkflowRetentionFailed {
            deleted_before: 7,
            error: BrokerError::ClaimContract("planned failure".to_owned()),
        })) else {
            panic!("a failed pass gives WorkflowDeleteFailed");
        };
        assert_eq!(failed.deleted_before, 7);
        assert!(
            failed.message.contains("planned failure"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}
