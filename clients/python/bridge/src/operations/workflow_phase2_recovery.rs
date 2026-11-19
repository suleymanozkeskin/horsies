//! The phase-2 recovery pass of the reaper through the core
//! (`PostgresBroker::recover_workflow_phase2`), with the children of the
//! owed child starts from the handle's child spec source.
//!
//! The outcome is `Phase2RecoveryRecord` (the summary of the pass, field by
//! field) or `Phase2RecoveryFailed`. A child build that raised
//! `KeyboardInterrupt` or `SystemExit` is raised again after the pass; its
//! record was not counted toward the bound.

use horsies_core::{Phase2RecoverySummary, WorkflowError};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::child_spec::PythonChildSpecSource;
use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// What one pass did. `pending_absent` counts the records that were gone
/// while their node is not terminal (nothing written).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct Phase2RecoveryRecord {
    pub considered: u64,
    pub applied: u64,
    pub already_applied: u64,
    pub superseded: u64,
    pub pending_absent: u64,
    pub retained: u64,
    pub failed: u64,
    pub quarantined: u64,
    pub raised_counted: u64,
    pub over_attempt_bound: i64,
    pub retained_details: Vec<String>,
    pub quarantine_refusals: Vec<String>,
}

impl Phase2RecoveryRecord {
    fn of(summary: Phase2RecoverySummary) -> Self {
        Self {
            considered: summary.considered,
            applied: summary.applied,
            already_applied: summary.already_applied,
            superseded: summary.superseded,
            pending_absent: summary.pending_absent,
            retained: summary.retained,
            failed: summary.failed,
            quarantined: summary.quarantined,
            raised_counted: summary.raised_counted,
            over_attempt_bound: summary.over_attempt_bound,
            retained_details: summary.retained_details,
            quarantine_refusals: summary.quarantine_refusals,
        }
    }
}

/// The pass failed before it recovered a record: the client family check,
/// the discovery or the count. Nothing was recovered.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct Phase2RecoveryFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `recover_workflow_phase2`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed pass.
const SUBJECT: &str = "recover_workflow_phase2";

/// The outcome of one pass.
pub enum Phase2RecoveryOutcome {
    Recovered(Phase2RecoveryRecord),
    Failed(Phase2RecoveryFailed),
    /// A child build raised this `BaseException` during the pass.
    Interrupted(PyErr),
}

impl<'py> IntoPyObject<'py> for Phase2RecoveryOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Recovered(record) => record.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
            Self::Interrupted(exception) => Err(exception),
        }
    }
}

/// The outcome of a pass's answer. An interrupt comes first. Pure.
fn recovery_outcome(
    answer: Result<Phase2RecoverySummary, WorkflowError>,
    interrupt: Option<PyErr>,
) -> Phase2RecoveryOutcome {
    match (interrupt, answer) {
        (Some(exception), _) => Phase2RecoveryOutcome::Interrupted(exception),
        (None, Ok(summary)) => Phase2RecoveryOutcome::Recovered(Phase2RecoveryRecord::of(summary)),
        (None, Err(error)) => Phase2RecoveryOutcome::Failed(Phase2RecoveryFailed {
            message: format!("phase-2 recovery pass failed: {error}"),
            retryable: error.is_retryable(),
            exception: FailureException::of(SUBJECT, &error),
        }),
    }
}

/// One phase-2 recovery pass. Writes; each record commits alone. A caller
/// that leaves early does not stop it.
pub async fn recover_workflow_phase2_operation(
    connection: BrokerConnection,
    grace_ms: u64,
    max_rows: u32,
    quarantine_after_attempts: u32,
    source: PythonChildSpecSource,
) -> Phase2RecoveryOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return Phase2RecoveryOutcome::Failed(Phase2RecoveryFailed {
                message: format!("phase-2 recovery pass failed: {error}"),
                retryable: error.is_retryable(),
                exception: FailureException::of(SUBJECT, &error),
            })
        }
    };
    let answer = broker
        .recover_workflow_phase2(
            grace_ms,
            i64::from(max_rows),
            quarantine_after_attempts,
            &source,
        )
        .await;
    recovery_outcome(answer, source.take_interrupt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_is_its_record_field_by_field() {
        let summary = Phase2RecoverySummary {
            considered: 4,
            applied: 1,
            already_applied: 1,
            pending_absent: 1,
            retained: 1,
            retained_details: vec!["t: SOURCE_ABSENT".to_owned()],
            over_attempt_bound: 2,
            ..Phase2RecoverySummary::default()
        };
        match recovery_outcome(Ok(summary), None) {
            Phase2RecoveryOutcome::Recovered(record) => {
                assert_eq!(record.considered, 4);
                assert_eq!(record.pending_absent, 1);
                assert_eq!(record.over_attempt_bound, 2);
                assert_eq!(record.retained_details, vec!["t: SOURCE_ABSENT".to_owned()]);
            }
            _ => panic!("a pass is Phase2RecoveryRecord"),
        }
    }

    #[test]
    fn a_failed_pass_keeps_its_retry_rule() {
        match recovery_outcome(Err(WorkflowError::Validation("refused".to_owned())), None) {
            Phase2RecoveryOutcome::Failed(failed) => assert!(!failed.retryable),
            _ => panic!("a failed pass is Phase2RecoveryFailed"),
        }
    }
}
