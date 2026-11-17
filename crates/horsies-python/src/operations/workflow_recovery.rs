//! The global workflow recovery pass of the reaper through the core
//! (`PostgresBroker::recover_workflows`), with the children of the owed
//! child starts from the handle's child spec source.
//!
//! The outcome is `RecoveryPassRecord` (the counts of each case) or
//! `RecoveryPassFailed` with the partial record. The Python reaper builds
//! its `RecoveryReport` and its health from them. A child build that raised
//! `KeyboardInterrupt` or `SystemExit` is raised again after the pass; its
//! candidate was not counted toward the limit.

use horsies_core::{RecoveryCaseMetrics, RecoveryPassFailure, RecoveryReport};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::child_spec::PythonChildSpecSource;
use crate::handle::BrokerConnection;

/// Bounded work and outcome counts of one recovery case.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RecoveryCaseRecord {
    pub rows_selected: u32,
    pub candidates_returned: u32,
    pub duration_ms: u64,
    pub refusals: u32,
    pub errors: u32,
    pub failures_counted: u32,
    pub limit_reached: u32,
}

impl RecoveryCaseRecord {
    fn of(metrics: &RecoveryCaseMetrics) -> Self {
        Self {
            rows_selected: metrics.rows_selected,
            candidates_returned: metrics.candidates_returned,
            duration_ms: metrics.duration_ms,
            refusals: metrics.refusals,
            errors: metrics.errors,
            failures_counted: metrics.failures_counted,
            limit_reached: metrics.limit_reached,
        }
    }
}

/// What one pass did: the candidates recovered over every case, the
/// failures that no case counted, the workflows over the failure limit, and
/// the counts of each case. The audit page of cases 2/3 and 4 scanned
/// `case_2_3.rows_selected` workflows and returned
/// `case_2_3.candidates_returned` completion candidates.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RecoveryPassRecord {
    pub recovered: u32,
    pub pass_errors: u32,
    pub over_failure_limit: i64,
    pub case_0: RecoveryCaseRecord,
    pub case_1: RecoveryCaseRecord,
    pub case_1_5: RecoveryCaseRecord,
    pub case_1_6: RecoveryCaseRecord,
    pub case_2_3: RecoveryCaseRecord,
    pub case_4: RecoveryCaseRecord,
}

impl RecoveryPassRecord {
    fn of(report: &RecoveryReport) -> Self {
        Self {
            recovered: report.total(),
            pass_errors: report.pass_errors,
            over_failure_limit: report.over_failure_limit,
            case_0: RecoveryCaseRecord::of(&report.metrics.case0),
            case_1: RecoveryCaseRecord::of(&report.metrics.case1),
            case_1_5: RecoveryCaseRecord::of(&report.metrics.case1_5),
            case_1_6: RecoveryCaseRecord::of(&report.metrics.case1_6),
            case_2_3: RecoveryCaseRecord::of(&report.metrics.case2_3),
            case_4: RecoveryCaseRecord::of(&report.metrics.case4),
        }
    }
}

/// The pass failed: the client family check, a discovery statement, the
/// audit page or the count of workflows over the limit. `report` holds the
/// counts of the cases before it.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RecoveryPassFailed {
    pub report: RecoveryPassRecord,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one pass.
pub enum RecoveryPassOutcome {
    Recovered(RecoveryPassRecord),
    Failed(RecoveryPassFailed),
    /// A child build raised this `BaseException` during the pass.
    Interrupted(PyErr),
}

impl<'py> IntoPyObject<'py> for RecoveryPassOutcome {
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
fn pass_outcome(
    answer: Result<RecoveryReport, RecoveryPassFailure>,
    interrupt: Option<PyErr>,
) -> RecoveryPassOutcome {
    match (interrupt, answer) {
        (Some(exception), _) => RecoveryPassOutcome::Interrupted(exception),
        (None, Ok(report)) => RecoveryPassOutcome::Recovered(RecoveryPassRecord::of(&report)),
        (None, Err(failure)) => RecoveryPassOutcome::Failed(RecoveryPassFailed {
            report: RecoveryPassRecord::of(&failure.report),
            message: format!("workflow recovery pass failed: {}", failure.error),
            retryable: failure.error.is_retryable(),
        }),
    }
}

/// One global workflow recovery pass. Writes; each candidate commits alone.
/// A caller that leaves early does not stop it.
pub async fn recover_workflows_operation(
    connection: BrokerConnection,
    failure_limit: u32,
    source: PythonChildSpecSource,
) -> RecoveryPassOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return RecoveryPassOutcome::Failed(RecoveryPassFailed {
                report: RecoveryPassRecord::of(&RecoveryReport {
                    errors: 1,
                    pass_errors: 1,
                    ..RecoveryReport::default()
                }),
                message: format!("workflow recovery pass failed: {error}"),
                retryable: error.is_retryable(),
            })
        }
    };
    let answer = broker.recover_workflows(failure_limit, &source).await;
    pass_outcome(answer, source.take_interrupt())
}

#[cfg(test)]
mod tests {
    use horsies_core::WorkflowError;

    use super::*;

    #[test]
    fn a_pass_is_its_record_case_by_case() {
        let mut report = RecoveryReport {
            case1_ready_enqueued: 2,
            case4_orphaned_failed: 1,
            over_failure_limit: 3,
            ..RecoveryReport::default()
        };
        report.metrics.case2_3.rows_selected = 10;
        report.metrics.case2_3.candidates_returned = 2;
        match pass_outcome(Ok(report), None) {
            RecoveryPassOutcome::Recovered(record) => {
                assert_eq!(record.recovered, 3);
                assert_eq!(record.over_failure_limit, 3);
                assert_eq!(record.case_2_3.rows_selected, 10);
                assert_eq!(record.case_2_3.candidates_returned, 2);
                assert_eq!(record.pass_errors, 0);
            }
            _ => panic!("a pass is RecoveryPassRecord"),
        }
    }

    #[test]
    fn a_failed_pass_keeps_its_partial_report() {
        let mut report = RecoveryReport {
            errors: 1,
            pass_errors: 1,
            ..RecoveryReport::default()
        };
        report.metrics.case0.errors = 1;
        let failure = RecoveryPassFailure {
            report,
            error: WorkflowError::Validation("count failed".to_owned()),
        };
        match pass_outcome(Err(failure), None) {
            RecoveryPassOutcome::Failed(failed) => {
                assert_eq!(failed.report.pass_errors, 1);
                assert_eq!(failed.report.case_0.errors, 1);
                assert!(!failed.retryable);
            }
            _ => panic!("a failed pass is RecoveryPassFailed"),
        }
    }
}
