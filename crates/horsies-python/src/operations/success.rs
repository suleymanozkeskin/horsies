//! The success writes through the core:
//! - the plain successes of one worker in one statement,
//!   `horsies_complete_tasks_fused` (`PostgresBroker::complete_successes`);
//! - phase 1 of a workflow task's success, `horsies_finish_workflow_success`
//!   (`PostgresBroker::finish_workflow_success`). Phase 2 stays with the
//!   caller.
//!
//! A batch answers `SuccessesWritten` with one row record per success in the
//! request order (`SuccessApplied` or a refusal of `attempt_records`), or
//! the batch failure: `BatchNotSent` or `BatchRefused` (no row changed; the
//! two causes that Python's batcher keeps apart, because only a refused
//! statement of several rows is written again row by row) or
//! `BatchCommitUnknown` (each row may have committed). Rows outside the
//! function's contract panic, as Python raises.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::core::lifecycle::{
    AttemptWriteRefusal, OwnedClaim, TerminalizationOutcome, WorkflowSuccessOutcome,
};
use horsies_core::db::Transience;
use horsies_core::{
    BatchOutcomes, BatchWriteError, BrokerError, FinishWorkflowSuccess, PlainSuccess, SuccessBatch,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::utc_instant;
use super::attempt_records::{
    observed_fields, AttemptWriteFailed, PhaseTwoOwedRecord, RefusalRecord,
};
use crate::handle::BrokerConnection;

/// The plain successes of one worker for one statement: 1 to 64 rows of
/// `(task_id, claimed_at, result_json)`, each task once, in the order of
/// their outcomes. A naive `claimed_at` raises `TypeError`; no row, more
/// rows or a repeated task id raises `ValueError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct SuccessBatchRequest {
    worker_id: Arc<str>,
    batch: Arc<SuccessBatch>,
}

impl SuccessBatchRequest {
    pub fn parts(&self) -> (Arc<str>, Arc<SuccessBatch>) {
        (Arc::clone(&self.worker_id), Arc::clone(&self.batch))
    }
}

#[pymethods]
impl SuccessBatchRequest {
    #[new]
    #[pyo3(signature = (*, worker_id, rows))]
    fn new(worker_id: String, rows: Vec<(Uuid, Bound<'_, PyAny>, String)>) -> PyResult<Self> {
        let rows = rows
            .into_iter()
            .map(|(task_id, claimed_at, result_json)| {
                Ok(PlainSuccess {
                    task_id,
                    claimed_at: utc_instant(&claimed_at)?,
                    result_json,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let batch = SuccessBatch::new(rows)
            .map_err(|refused| PyValueError::new_err(refused.to_string()))?;
        Ok(Self {
            worker_id: Arc::from(worker_id),
            batch: Arc::new(batch),
        })
    }
}

/// The success of this row committed with the statement: the row is in
/// history as COMPLETED, and its queue got its capacity wake.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SuccessApplied {
    pub task_id: Uuid,
    pub terminal_at: DateTime<Utc>,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
}

/// What the statement did with one row.
#[derive(Clone, Debug)]
pub enum SuccessRowRecord {
    Applied(SuccessApplied),
    Refused(RefusalRecord),
}

impl<'py> IntoPyObject<'py> for SuccessRowRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Applied(applied) => applied.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// The batch statement ran: one row record per success, in the request
/// order. `SuccessApplied` rows committed with it; a refused row wrote
/// nothing.
#[pyclass(module = "horsies._core", frozen)]
pub struct SuccessesWritten {
    rows: Vec<SuccessRowRecord>,
}

#[pymethods]
impl SuccessesWritten {
    /// `SuccessApplied` or a refusal, per success, in the request order.
    #[getter]
    fn rows<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        self.rows
            .iter()
            .cloned()
            .map(|row| row.into_bound_py_any(py))
            .collect()
    }
}

/// No row of the batch changed, and the statement was not sent: the pools,
/// the client family check or the checkout failed (Python
/// `BatchRolledBack(cause=NotSent)`). The cause is not about one row.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct BatchNotSent {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// No row of the batch changed: the server refused the statement (Python
/// `BatchRolledBack(cause=StatementRefused)`). One row can be the cause, as
/// a row that raises refuses the whole statement, so each row of a batch of
/// several can be written alone.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct BatchRefused {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The statement was sent and its reply was lost: each row may have
/// committed or not. A replay of a row classifies it (`AttemptAlreadyApplied`
/// when it committed).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct BatchCommitUnknown {
    /// The cause, with its subject.
    pub message: String,
}

/// The outcome of one batch.
pub enum BatchRecord {
    Written(SuccessesWritten),
    NotSent(BatchNotSent),
    Refused(BatchRefused),
    CommitUnknown(BatchCommitUnknown),
}

impl<'py> IntoPyObject<'py> for BatchRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Written(written) => written.into_bound_py_any(py),
            Self::NotSent(not_sent) => not_sent.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
            Self::CommitUnknown(unknown) => unknown.into_bound_py_any(py),
        }
    }
}

/// The record of one row's outcome. Pure.
fn success_row(outcome: &TerminalizationOutcome) -> SuccessRowRecord {
    match outcome {
        TerminalizationOutcome::Applied {
            task_id,
            terminal_at,
            observed,
            ..
        } => {
            let (observed_status, observed_worker_id, observed_claimed_at) =
                observed_fields(observed);
            SuccessRowRecord::Applied(SuccessApplied {
                task_id: *task_id,
                terminal_at: *terminal_at,
                observed_status,
                observed_worker_id,
                observed_claimed_at,
            })
        }
        TerminalizationOutcome::AlreadyApplied {
            task_id,
            terminal_at,
            kind,
            observed,
            ..
        } => SuccessRowRecord::Refused(RefusalRecord::from(&AttemptWriteRefusal::AlreadyApplied {
            task_id: *task_id,
            terminal_at: *terminal_at,
            kind: *kind,
            observed: observed.clone(),
        })),
        TerminalizationOutcome::LostClaim {
            task_id, observed, ..
        } => SuccessRowRecord::Refused(RefusalRecord::from(&AttemptWriteRefusal::LostClaim {
            task_id: *task_id,
            observed: observed.clone(),
        })),
        TerminalizationOutcome::SourceStateConflict {
            task_id,
            observed,
            evidence,
            ..
        } => SuccessRowRecord::Refused(RefusalRecord::from(
            &AttemptWriteRefusal::SourceStateConflict {
                task_id: *task_id,
                observed: observed.clone(),
                evidence: evidence.clone(),
            },
        )),
        TerminalizationOutcome::TaskAbsent { task_id, .. } => {
            SuccessRowRecord::Refused(RefusalRecord::from(&AttemptWriteRefusal::TaskAbsent {
                task_id: *task_id,
            }))
        }
    }
}

/// The record of a batch. Pure.
///
/// Panics: rows outside the function's contract
/// ([`BatchWriteError::ContractBroken`]), as Python raises
/// `OutcomeDecodeError` or `RuntimeError`: the statement completed, so what
/// it wrote is committed, and its rows do not say which.
fn batch_record(written: Result<BatchOutcomes, BatchWriteError>) -> BatchRecord {
    match written {
        Ok(outcomes) => BatchRecord::Written(SuccessesWritten {
            rows: outcomes.into_outcomes().iter().map(success_row).collect(),
        }),
        Err(error @ BatchWriteError::NotSent { .. }) => BatchRecord::NotSent(BatchNotSent {
            message: error.to_string(),
            retryable: matches!(error.transience(), Transience::Transient),
        }),
        Err(error @ BatchWriteError::Refused { .. }) => BatchRecord::Refused(BatchRefused {
            message: error.to_string(),
            retryable: matches!(error.transience(), Transience::Transient),
        }),
        Err(error @ BatchWriteError::CommitUnknown { .. }) => {
            BatchRecord::CommitUnknown(BatchCommitUnknown {
                message: error.to_string(),
            })
        }
        Err(error @ BatchWriteError::ContractBroken { .. }) => panic!("{error}"),
    }
}

/// Write the plain successes of one request. Writes, in one statement; a
/// caller that leaves early does not stop it, and sees its outcome as
/// unknown.
pub async fn complete_successes_operation(
    connection: BrokerConnection,
    worker_id: Arc<str>,
    batch: Arc<SuccessBatch>,
) -> BatchRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return BatchRecord::NotSent(BatchNotSent {
                message: error.to_string(),
                retryable: error.is_retryable(),
            })
        }
    };
    batch_record(broker.complete_successes(&worker_id, &batch).await)
}

/// Phase 1 of the workflow task's success committed: the attempt row, the
/// history row and the phase-2 record of the node. `phase_two` is the phase
/// 2 that the caller owes.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct WorkflowSuccessApplied {
    #[pyo3(get)]
    pub task_id: Uuid,
    #[pyo3(get)]
    pub terminal_at: DateTime<Utc>,
    #[pyo3(get)]
    pub observed_status: Option<String>,
    #[pyo3(get)]
    pub observed_worker_id: Option<String>,
    #[pyo3(get)]
    pub observed_claimed_at: Option<DateTime<Utc>>,
    phase_two: PhaseTwoOwedRecord,
}

#[pymethods]
impl WorkflowSuccessApplied {
    /// The phase 2 that the caller owes.
    #[getter]
    fn phase_two(&self) -> PhaseTwoOwedRecord {
        self.phase_two.clone()
    }
}

/// The outcome of one workflow success.
pub enum WorkflowSuccessRecord {
    Applied(WorkflowSuccessApplied),
    Refused(RefusalRecord),
    Failed(AttemptWriteFailed),
}

impl<'py> IntoPyObject<'py> for WorkflowSuccessRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Applied(applied) => applied.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The record of a workflow success. Pure.
fn workflow_success_record(
    task_id: Uuid,
    finished: Result<WorkflowSuccessOutcome, BrokerError>,
) -> WorkflowSuccessRecord {
    match finished {
        Ok(WorkflowSuccessOutcome::SuccessApplied {
            task_id,
            terminal_at,
            observed,
            phase_two,
        }) => {
            let (observed_status, observed_worker_id, observed_claimed_at) =
                observed_fields(&observed);
            WorkflowSuccessRecord::Applied(WorkflowSuccessApplied {
                task_id,
                terminal_at,
                observed_status,
                observed_worker_id,
                observed_claimed_at,
                phase_two: PhaseTwoOwedRecord::from(&phase_two),
            })
        }
        Ok(WorkflowSuccessOutcome::Refused(refusal)) => {
            WorkflowSuccessRecord::Refused(RefusalRecord::from(&refusal))
        }
        Err(error) => WorkflowSuccessRecord::Failed(AttemptWriteFailed::new(task_id, &error)),
    }
}

/// Write phase 1 of a workflow task's success. Writes, in one statement; a
/// caller that leaves early does not stop it, and sees its outcome as
/// unknown.
pub async fn finish_workflow_success_operation(
    connection: BrokerConnection,
    command: FinishWorkflowSuccess,
) -> WorkflowSuccessRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return WorkflowSuccessRecord::Failed(AttemptWriteFailed::new(command.task_id, &error))
        }
    };
    workflow_success_record(
        command.task_id,
        broker.finish_workflow_success(&command).await,
    )
}

/// The command of a workflow success from the call's arguments.
///
/// Errors: `TypeError` for a naive `claimed_at`.
pub fn workflow_success_command(
    task_id: Uuid,
    worker_id: String,
    claimed_at: &Bound<'_, PyAny>,
    result_json: String,
) -> PyResult<FinishWorkflowSuccess> {
    Ok(FinishWorkflowSuccess {
        task_id,
        fence: OwnedClaim {
            worker_id,
            claimed_at: utc_instant(claimed_at)?,
        },
        result_json,
    })
}

#[cfg(test)]
mod tests {
    use horsies_core::core::lifecycle::outcomes::ObservedClaim;
    use horsies_core::core::lifecycle::{
        GuardEvidence, ObservedTaskState, PhaseTwoOwed, PhaseTwoStep, TerminalizationKind,
    };
    use horsies_core::TaskStatus;

    use super::*;
    use crate::operations::attempt_records::{
        AttemptAlreadyApplied, AttemptSourceStateConflict, AttemptTaskAbsent,
    };

    fn task(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn observed() -> ObservedTaskState {
        ObservedTaskState {
            status: Some(TaskStatus::Running),
            worker_id: Some("worker-a".to_owned()),
            claimed_at: Some(Utc::now()),
        }
    }

    #[test]
    fn each_row_outcome_has_its_own_record() {
        let now = Utc::now();
        assert!(matches!(
            success_row(&TerminalizationOutcome::Applied {
                task_id: task(1),
                ordinality: Some(1),
                terminal_at: now,
                kind: TerminalizationKind::CompleteFused,
                observed: observed(),
            }),
            SuccessRowRecord::Applied(SuccessApplied { task_id, .. }) if task_id == task(1)
        ));
        assert!(matches!(
            success_row(&TerminalizationOutcome::AlreadyApplied {
                task_id: task(2),
                ordinality: Some(2),
                terminal_at: now,
                kind: TerminalizationKind::CompleteFused,
                observed: observed(),
            }),
            SuccessRowRecord::Refused(RefusalRecord::AlreadyApplied(AttemptAlreadyApplied { ref kind, .. }))
                if kind == "COMPLETE_FUSED"
        ));
        assert!(matches!(
            success_row(&TerminalizationOutcome::SourceStateConflict {
                task_id: task(3),
                ordinality: Some(3),
                observed: observed(),
                evidence: GuardEvidence::Claim(ObservedClaim {
                    worker_id: None,
                    claimed_at: None,
                }),
            }),
            SuccessRowRecord::Refused(RefusalRecord::SourceStateConflict(AttemptSourceStateConflict { ref guard, .. }))
                if guard == "CLAIM"
        ));
        assert!(matches!(
            success_row(&TerminalizationOutcome::TaskAbsent {
                task_id: task(4),
                ordinality: Some(4),
            }),
            SuccessRowRecord::Refused(RefusalRecord::TaskAbsent(AttemptTaskAbsent { task_id }))
                if task_id == task(4)
        ));
    }

    #[test]
    fn a_failed_batch_keeps_python_s_three_states() {
        let BatchRecord::NotSent(not_sent) = batch_record(Err(BatchWriteError::NotSent {
            transience: Transience::Transient,
            detail: "pool timeout".to_owned(),
            cause: horsies_core::FailureCause::Connection,
        })) else {
            panic!("a batch that was not sent is BatchNotSent");
        };
        assert!(not_sent.retryable);
        assert!(
            not_sent.message.contains("was not sent: pool timeout"),
            "{}",
            not_sent.message
        );
        let BatchRecord::Refused(refused) = batch_record(Err(BatchWriteError::Refused {
            transience: Transience::Permanent,
            detail: "22023".to_owned(),
            server_error: horsies_core::ServerErrorFacts::of_code(
                "22023",
                "invalid parameter".to_owned(),
            ),
        })) else {
            panic!("a refused batch is BatchRefused");
        };
        assert!(!refused.retryable);
        assert!(
            refused.message.contains("was refused: 22023"),
            "{}",
            refused.message
        );
        assert!(matches!(
            batch_record(Err(BatchWriteError::CommitUnknown {
                detail: "connection lost".to_owned(),
                cause: horsies_core::FailureCause::Connection,
            })),
            BatchRecord::CommitUnknown(_)
        ));
    }

    #[test]
    #[should_panic(expected = "broke its row contract")]
    fn rows_outside_the_contract_panic() {
        let _never = batch_record(Err(BatchWriteError::ContractBroken {
            detail: "ordinality 2 returned twice".to_owned(),
        }));
    }

    #[test]
    fn a_workflow_success_keeps_the_phase_two_it_owes() {
        let record = workflow_success_record(
            task(5),
            Ok(WorkflowSuccessOutcome::SuccessApplied {
                task_id: task(5),
                terminal_at: Utc::now(),
                observed: observed(),
                phase_two: PhaseTwoOwed {
                    task_name: "t".to_owned(),
                    queue_name: "q".to_owned(),
                    step: PhaseTwoStep::DependentsWithoutSubworkflow,
                },
            }),
        );
        let WorkflowSuccessRecord::Applied(applied) = record else {
            panic!("an applied success gives WorkflowSuccessApplied");
        };
        assert_eq!(applied.phase_two.step, "DEPENDENTS_WITHOUT_SUBWORKFLOW");
        assert!(matches!(
            workflow_success_record(
                task(5),
                Err(BrokerError::TerminalizationContract("planned".to_owned()))
            ),
            WorkflowSuccessRecord::Failed(AttemptWriteFailed {
                retryable: false,
                ..
            })
        ));
    }
}
