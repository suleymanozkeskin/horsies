//! The records that the attempt writes share: the refusals of a writer that
//! wrote nothing, the phase 2 that a workflow task owes, and the failure of
//! the statement.
//!
//! Each record names the task and carries the row as the statement saw it
//! under its locks (`observed_status`, `observed_worker_id`,
//! `observed_claimed_at`). The guard evidence of a source-state conflict is
//! named by `guard`; the core logs the full evidence with the refusal.

use chrono::{DateTime, Utc};
use horsies_core::catalog::CodeEnum;
use horsies_core::core::lifecycle::{
    AttemptWriteRefusal, GuardEvidence, GuardKind, ObservedTaskState, PhaseTwoOwed, PhaseTwoStep,
};
use horsies_core::BrokerError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::core_failures::FailureException;

/// The row as a statement saw it under its locks.
#[derive(Clone, Debug)]
struct Observed {
    status: Option<String>,
    worker_id: Option<String>,
    claimed_at: Option<DateTime<Utc>>,
}

impl From<&ObservedTaskState> for Observed {
    fn from(observed: &ObservedTaskState) -> Self {
        Self {
            status: observed.status.map(|status| status.wire_value().to_owned()),
            worker_id: observed.worker_id.clone(),
            claimed_at: observed.claimed_at,
        }
    }
}

/// The writer's own effect was already committed (an equivalent kind): a
/// replay after a lost reply. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptAlreadyApplied {
    pub task_id: Uuid,
    pub terminal_at: DateTime<Utc>,
    /// The terminalization kind of the committed row.
    pub kind: String,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
}

/// The row is live, and the fence (worker, claim generation, retry count)
/// does not match it. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptLostClaim {
    pub task_id: Uuid,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
}

/// The row exists and is not this writer's to end. `guard` names the guard
/// that refused it: `CLAIM` (its claim fields), or the guard kind of the
/// function. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptSourceStateConflict {
    pub task_id: Uuid,
    pub guard: String,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
}

/// No live or history row has this id. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptTaskAbsent {
    pub task_id: Uuid,
}

/// The statement failed. It wrote nothing, or it committed and its reply
/// was lost (the same write sent again reports a committed effect as a
/// replay or a refusal). `exception` is the cause, with the write as its
/// subject.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptWriteFailed {
    pub task_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    pub exception: FailureException,
}

impl AttemptWriteFailed {
    /// The failure of the write `subject` of `task_id` that met `error`.
    pub fn new(subject: &'static str, task_id: Uuid, error: &BrokerError) -> Self {
        Self {
            task_id,
            message: error.to_string(),
            retryable: error.is_retryable(),
            exception: FailureException::of(subject, error),
        }
    }
}

/// Phase 2 of a workflow task is owed: it advances the node and wakes the
/// queue. `step` is `NO_DEPENDENTS`, `DEPENDENTS_WITHOUT_SUBWORKFLOW` or
/// `SUBWORKFLOW_REACHABLE`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PhaseTwoOwedRecord {
    pub task_name: String,
    pub queue_name: String,
    pub step: String,
}

impl From<&PhaseTwoOwed> for PhaseTwoOwedRecord {
    fn from(owed: &PhaseTwoOwed) -> Self {
        Self {
            task_name: owed.task_name.clone(),
            queue_name: owed.queue_name.clone(),
            step: phase_two_step(owed.step).to_owned(),
        }
    }
}

/// The wire value of a phase-2 step (Python `PhaseTwoStep`). Pure.
fn phase_two_step(step: PhaseTwoStep) -> &'static str {
    match step {
        PhaseTwoStep::NoDependents => "NO_DEPENDENTS",
        PhaseTwoStep::DependentsWithoutSubworkflow => "DEPENDENTS_WITHOUT_SUBWORKFLOW",
        PhaseTwoStep::SubworkflowReachable => "SUBWORKFLOW_REACHABLE",
    }
}

/// The guard that refused a row, as its wire value; `CLAIM` for evidence in
/// the claim fields. Pure.
fn guard_of(evidence: &GuardEvidence) -> &'static str {
    match evidence {
        GuardEvidence::Claim(_) => "CLAIM",
        GuardEvidence::Deadline(_) => GuardKind::Deadline.as_str(),
        GuardEvidence::Staleness(_) => GuardKind::Staleness.as_str(),
        GuardEvidence::RetryCount(_) => GuardKind::RetryCount.as_str(),
        GuardEvidence::WorkflowState(_) => GuardKind::WorkflowStatus.as_str(),
        GuardEvidence::WorkflowLink(link) => match link.node_status {
            None => GuardKind::WorkflowLinkAbsent.as_str(),
            Some(_) => GuardKind::WorkflowLinkState.as_str(),
        },
        GuardEvidence::ForeignTerminalization(_) => GuardKind::ForeignTerminalization.as_str(),
        GuardEvidence::WorkflowTask(_) => GuardKind::WorkflowTask.as_str(),
    }
}

/// A writer that wrote nothing.
#[derive(Clone, Debug)]
pub enum RefusalRecord {
    AlreadyApplied(AttemptAlreadyApplied),
    LostClaim(AttemptLostClaim),
    SourceStateConflict(AttemptSourceStateConflict),
    TaskAbsent(AttemptTaskAbsent),
}

impl From<&AttemptWriteRefusal> for RefusalRecord {
    fn from(refusal: &AttemptWriteRefusal) -> Self {
        match refusal {
            AttemptWriteRefusal::AlreadyApplied {
                task_id,
                terminal_at,
                kind,
                observed,
            } => {
                let observed = Observed::from(observed);
                Self::AlreadyApplied(AttemptAlreadyApplied {
                    task_id: *task_id,
                    terminal_at: *terminal_at,
                    kind: kind.as_str().to_owned(),
                    observed_status: observed.status,
                    observed_worker_id: observed.worker_id,
                    observed_claimed_at: observed.claimed_at,
                })
            }
            AttemptWriteRefusal::LostClaim { task_id, observed } => {
                let observed = Observed::from(observed);
                Self::LostClaim(AttemptLostClaim {
                    task_id: *task_id,
                    observed_status: observed.status,
                    observed_worker_id: observed.worker_id,
                    observed_claimed_at: observed.claimed_at,
                })
            }
            AttemptWriteRefusal::SourceStateConflict {
                task_id,
                observed,
                evidence,
            } => {
                let observed = Observed::from(observed);
                Self::SourceStateConflict(AttemptSourceStateConflict {
                    task_id: *task_id,
                    guard: guard_of(evidence).to_owned(),
                    observed_status: observed.status,
                    observed_worker_id: observed.worker_id,
                    observed_claimed_at: observed.claimed_at,
                })
            }
            AttemptWriteRefusal::TaskAbsent { task_id } => {
                Self::TaskAbsent(AttemptTaskAbsent { task_id: *task_id })
            }
        }
    }
}

impl<'py> IntoPyObject<'py> for RefusalRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::AlreadyApplied(replay) => replay.into_bound_py_any(py),
            Self::LostClaim(lost) => lost.into_bound_py_any(py),
            Self::SourceStateConflict(conflict) => conflict.into_bound_py_any(py),
            Self::TaskAbsent(absent) => absent.into_bound_py_any(py),
        }
    }
}

/// The observed fields of a record, from the core's observed state.
pub fn observed_fields(
    observed: &ObservedTaskState,
) -> (Option<String>, Option<String>, Option<DateTime<Utc>>) {
    let observed = Observed::from(observed);
    (observed.status, observed.worker_id, observed.claimed_at)
}

#[cfg(test)]
mod tests {
    use horsies_core::core::lifecycle::outcomes::{ObservedClaim, ObservedWorkflowLink};
    use horsies_core::core::lifecycle::TerminalizationKind;
    use horsies_core::TaskStatus;

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(5)
    }

    fn observed() -> ObservedTaskState {
        ObservedTaskState {
            status: Some(TaskStatus::Running),
            worker_id: Some("worker-a".to_owned()),
            claimed_at: Some(Utc::now()),
        }
    }

    #[test]
    fn each_refusal_has_its_own_record_with_the_observed_row() {
        let replay = RefusalRecord::from(&AttemptWriteRefusal::AlreadyApplied {
            task_id: task(),
            terminal_at: Utc::now(),
            kind: TerminalizationKind::FailRunning,
            observed: observed(),
        });
        assert!(matches!(
            replay,
            RefusalRecord::AlreadyApplied(AttemptAlreadyApplied { ref kind, ref observed_status, .. })
                if kind == "FAIL_RUNNING" && observed_status.as_deref() == Some("RUNNING")
        ));
        let lost = RefusalRecord::from(&AttemptWriteRefusal::LostClaim {
            task_id: task(),
            observed: observed(),
        });
        assert!(matches!(
            lost,
            RefusalRecord::LostClaim(AttemptLostClaim { ref observed_worker_id, .. })
                if observed_worker_id.as_deref() == Some("worker-a")
        ));
        let absent = RefusalRecord::from(&AttemptWriteRefusal::TaskAbsent { task_id: task() });
        assert!(matches!(
            absent,
            RefusalRecord::TaskAbsent(AttemptTaskAbsent { task_id }) if task_id == task()
        ));
    }

    #[test]
    fn a_conflict_names_its_guard_and_claim_evidence_is_the_claim_guard() {
        let conflict = |evidence: GuardEvidence| {
            RefusalRecord::from(&AttemptWriteRefusal::SourceStateConflict {
                task_id: task(),
                observed: observed(),
                evidence,
            })
        };
        let guard = |record: RefusalRecord| match record {
            RefusalRecord::SourceStateConflict(conflict) => conflict.guard,
            other => panic!("expected a conflict, got {other:?}"),
        };
        assert_eq!(
            guard(conflict(GuardEvidence::Claim(ObservedClaim {
                worker_id: None,
                claimed_at: None,
            }))),
            "CLAIM"
        );
        assert_eq!(
            guard(conflict(GuardEvidence::WorkflowLink(
                ObservedWorkflowLink { node_status: None }
            ))),
            "WORKFLOW_LINK_ABSENT"
        );
        assert_eq!(
            guard(conflict(GuardEvidence::WorkflowLink(
                ObservedWorkflowLink {
                    node_status: Some("READY".to_owned()),
                }
            ))),
            "WORKFLOW_LINK_STATE"
        );
    }

    #[test]
    fn a_phase_two_step_is_its_wire_value() {
        let owed = PhaseTwoOwedRecord::from(&PhaseTwoOwed {
            task_name: "t".to_owned(),
            queue_name: "q".to_owned(),
            step: PhaseTwoStep::SubworkflowReachable,
        });
        assert_eq!(owed.step, "SUBWORKFLOW_REACHABLE");
        assert_eq!(phase_two_step(PhaseTwoStep::NoDependents), "NO_DEPENDENTS");
        assert_eq!(
            phase_two_step(PhaseTwoStep::DependentsWithoutSubworkflow),
            "DEPENDENTS_WITHOUT_SUBWORKFLOW"
        );
    }
}
