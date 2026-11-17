//! Phase 2 of a finished workflow task and the parent advance after it,
//! through the core (`PostgresBroker::finish_workflow_node`,
//! `PostgresBroker::advance_workflow_parents`), with the children of the
//! owed child starts from the handle's child spec source.
//!
//! Outcomes: `NodeFinished` or `NodeFinishFailed`; `ParentsAdvanced` or
//! `ParentAdvanceStopped`. The Python worker maps them to its phase-2 and
//! parent-propagation errors. Every failure of the step is a record, as
//! Python's finalize catches every `Exception` of the step. A child build
//! that raised `KeyboardInterrupt` or `SystemExit` rolls the step back and
//! the same object is raised again.

use horsies_core::{NodeFinishForm, NodeFinished, ParentAdvanceStopped, WorkflowError};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::child_spec::PythonChildSpecSource;
use crate::handle::BrokerConnection;

/// One frozen phase 2. `facts`: `NO_DEPENDENTS`,
/// `DEPENDENTS_WITHOUT_SUBWORKFLOW`, `SUBWORKFLOW_REACHABLE` or `UNKNOWN`;
/// `wake`: `SEND` or `NONE`. Another value raises `ValueError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct NodeFinishRequest {
    pub task_id: Uuid,
    pub form: NodeFinishForm,
    pub wake: bool,
}

/// The form of a phase 2 from its text. Pure.
fn node_finish_form(facts: &str) -> Result<NodeFinishForm, String> {
    match facts {
        "NO_DEPENDENTS" => Ok(NodeFinishForm::NoDependents),
        "DEPENDENTS_WITHOUT_SUBWORKFLOW" => Ok(NodeFinishForm::DependentsWithoutSubworkflow),
        "SUBWORKFLOW_REACHABLE" => Ok(NodeFinishForm::SubworkflowReachable),
        "UNKNOWN" => Ok(NodeFinishForm::FactsUnknown),
        other => Err(format!(
            "facts must be 'NO_DEPENDENTS', 'DEPENDENTS_WITHOUT_SUBWORKFLOW', \
             'SUBWORKFLOW_REACHABLE' or 'UNKNOWN', not {other:?}"
        )),
    }
}

/// The wake choice of a phase 2 from its text. Pure.
fn capacity_wake(wake: &str) -> Result<bool, String> {
    match wake {
        "SEND" => Ok(true),
        "NONE" => Ok(false),
        other => Err(format!("wake must be 'SEND' or 'NONE', not {other:?}")),
    }
}

#[pymethods]
impl NodeFinishRequest {
    #[new]
    #[pyo3(signature = (*, task_id, facts, wake))]
    fn new(task_id: Uuid, facts: &str, wake: &str) -> PyResult<Self> {
        Ok(Self {
            task_id,
            form: node_finish_form(facts).map_err(PyValueError::new_err)?,
            wake: capacity_wake(wake).map_err(PyValueError::new_err)?,
        })
    }
}

/// The phase 2 committed, or found nothing to write. `disposition` is the
/// function's disposition; a `SOURCE_*` disposition kept the phase-2 record
/// and wrote nothing. `parent_advances` are the child workflows whose parent
/// nodes advance after the commit.
#[pyclass(module = "horsies._core", name = "NodeFinished", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct NodeFinishedRecord {
    pub task_id: Uuid,
    pub disposition: String,
    pub workflow_status: Option<String>,
    pub parent_advances: Vec<Uuid>,
}

/// The phase 2 failed: the client family check, a statement, a child build
/// that did not run, a child tree that the core refuses, or a row outside
/// the function's contract (not retryable). Nothing committed, or the
/// outcome is unknown; a row outside the contract of the autocommit form
/// came after its commit.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct NodeFinishFailed {
    pub task_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// Every parent advance committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ParentsAdvanced {
    pub child_workflow_ids: Vec<Uuid>,
}

/// The parent advance stopped at the level of `failed_child_workflow_id`;
/// the levels before it committed. `still_owed` holds every advance a retry
/// runs, the failed level first.
#[pyclass(
    module = "horsies._core",
    name = "ParentAdvanceStopped",
    frozen,
    get_all
)]
#[derive(Clone, Debug)]
pub struct ParentAdvanceStoppedRecord {
    pub failed_child_workflow_id: Uuid,
    pub still_owed: Vec<Uuid>,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one phase 2.
pub enum NodeFinishOutcome {
    Finished(NodeFinishedRecord),
    Failed(NodeFinishFailed),
    /// A child build raised this `BaseException`; the step rolled back.
    Interrupted(PyErr),
}

impl<'py> IntoPyObject<'py> for NodeFinishOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Finished(finished) => finished.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
            Self::Interrupted(exception) => Err(exception),
        }
    }
}

/// The outcome of one parent advance drive.
pub enum ParentAdvanceOutcome {
    Advanced(ParentsAdvanced),
    Stopped(ParentAdvanceStoppedRecord),
    /// A child build raised this `BaseException`; the level rolled back.
    Interrupted(PyErr),
}

impl<'py> IntoPyObject<'py> for ParentAdvanceOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Advanced(advanced) => advanced.into_bound_py_any(py),
            Self::Stopped(stopped) => stopped.into_bound_py_any(py),
            Self::Interrupted(exception) => Err(exception),
        }
    }
}

/// The outcome of a phase 2's answer. An interrupt comes first. Pure.
fn node_finish_outcome(
    task_id: Uuid,
    answer: Result<NodeFinished, WorkflowError>,
    interrupt: Option<PyErr>,
) -> NodeFinishOutcome {
    match (interrupt, answer) {
        (Some(exception), _) => NodeFinishOutcome::Interrupted(exception),
        (None, Ok(finished)) => NodeFinishOutcome::Finished(NodeFinishedRecord {
            task_id,
            disposition: finished.disposition.to_owned(),
            workflow_status: finished.workflow_status,
            parent_advances: finished.parent_advances,
        }),
        (None, Err(error)) => NodeFinishOutcome::Failed(NodeFinishFailed {
            task_id,
            message: format!("phase 2 of workflow task {task_id} failed: {error}"),
            retryable: error.is_retryable(),
        }),
    }
}

/// The outcome of a parent advance drive's answer. An interrupt comes
/// first. Pure.
fn parent_advance_outcome(
    child_workflow_ids: Vec<Uuid>,
    answer: Result<(), ParentAdvanceStopped>,
    interrupt: Option<PyErr>,
) -> ParentAdvanceOutcome {
    match (interrupt, answer) {
        (Some(exception), _) => ParentAdvanceOutcome::Interrupted(exception),
        (None, Ok(())) => ParentAdvanceOutcome::Advanced(ParentsAdvanced { child_workflow_ids }),
        (None, Err(stopped)) => ParentAdvanceOutcome::Stopped(ParentAdvanceStoppedRecord {
            failed_child_workflow_id: stopped.failed_child_workflow_id,
            still_owed: stopped.still_owed,
            message: format!(
                "parent advance of child workflow {} failed: {}",
                stopped.failed_child_workflow_id, stopped.error
            ),
            retryable: stopped.error.is_retryable(),
        }),
    }
}

/// Phase 2 of one workflow task. Writes; a caller that leaves early does not
/// stop it, and sees its outcome as unknown.
pub async fn finish_workflow_node_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    form: NodeFinishForm,
    wake: bool,
    source: PythonChildSpecSource,
) -> NodeFinishOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return NodeFinishOutcome::Failed(NodeFinishFailed {
                task_id,
                message: format!("phase 2 of workflow task {task_id} failed: {error}"),
                retryable: error.is_retryable(),
            })
        }
    };
    let answer = broker
        .finish_workflow_node(task_id, form, wake, &source)
        .await;
    node_finish_outcome(task_id, answer, source.take_interrupt())
}

/// Advance the parent nodes above `child_workflow_ids`, one transaction per
/// level; an empty list advances nothing. Writes; a caller that leaves early
/// does not stop it, and sees its outcome as unknown.
pub async fn advance_workflow_parents_operation(
    connection: BrokerConnection,
    child_workflow_ids: Vec<Uuid>,
    source: PythonChildSpecSource,
) -> ParentAdvanceOutcome {
    let Some(first) = child_workflow_ids.first().copied() else {
        return ParentAdvanceOutcome::Advanced(ParentsAdvanced { child_workflow_ids });
    };
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return ParentAdvanceOutcome::Stopped(ParentAdvanceStoppedRecord {
                failed_child_workflow_id: first,
                still_owed: child_workflow_ids,
                message: format!("parent advance of child workflow {first} failed: {error}"),
                retryable: error.is_retryable(),
            });
        }
    };
    let answer = broker
        .advance_workflow_parents(&child_workflow_ids, &source)
        .await;
    parent_advance_outcome(child_workflow_ids, answer, source.take_interrupt())
}

#[cfg(test)]
mod tests {
    use horsies_core::{ChildSpecUnavailable, WorkflowError};

    use super::*;

    #[test]
    fn the_facts_and_wake_texts_parse_and_others_are_refused() {
        assert_eq!(
            node_finish_form("NO_DEPENDENTS"),
            Ok(NodeFinishForm::NoDependents)
        );
        assert_eq!(
            node_finish_form("DEPENDENTS_WITHOUT_SUBWORKFLOW"),
            Ok(NodeFinishForm::DependentsWithoutSubworkflow)
        );
        assert_eq!(
            node_finish_form("SUBWORKFLOW_REACHABLE"),
            Ok(NodeFinishForm::SubworkflowReachable)
        );
        assert_eq!(
            node_finish_form("UNKNOWN"),
            Ok(NodeFinishForm::FactsUnknown)
        );
        assert!(node_finish_form("READ").is_err());
        assert_eq!(capacity_wake("SEND"), Ok(true));
        assert_eq!(capacity_wake("NONE"), Ok(false));
        assert!(capacity_wake("send").is_err());
    }

    #[test]
    fn a_finished_step_is_its_record_and_a_failure_keeps_its_retry_rule() {
        let id = Uuid::nil();
        let finished = node_finish_outcome(
            id,
            Ok(NodeFinished {
                disposition: "APPLIED",
                workflow_status: Some("RUNNING".to_owned()),
                parent_advances: vec![Uuid::from_u128(1)],
            }),
            None,
        );
        match finished {
            NodeFinishOutcome::Finished(record) => {
                assert_eq!(record.disposition, "APPLIED");
                assert_eq!(record.parent_advances, vec![Uuid::from_u128(1)]);
            }
            _ => panic!("a committed step is NodeFinished"),
        }
        let failed = node_finish_outcome(
            id,
            Err(WorkflowError::ChildSpecSource(
                ChildSpecUnavailable::NotRun {
                    subject: "build".to_owned(),
                    cause: "queue full".to_owned(),
                },
            )),
            None,
        );
        match failed {
            NodeFinishOutcome::Failed(record) => assert!(record.retryable),
            _ => panic!("a failed step is NodeFinishFailed"),
        }
    }

    #[test]
    fn a_stopped_drive_names_its_level_and_what_is_still_owed() {
        let first = Uuid::from_u128(1);
        let outcome = parent_advance_outcome(
            vec![first],
            Err(ParentAdvanceStopped {
                failed_child_workflow_id: first,
                still_owed: vec![first],
                error: WorkflowError::Validation("refused".to_owned()),
            }),
            None,
        );
        match outcome {
            ParentAdvanceOutcome::Stopped(record) => {
                assert_eq!(record.failed_child_workflow_id, first);
                assert_eq!(record.still_owed, vec![first]);
                assert!(!record.retryable);
            }
            _ => panic!("a stopped drive is ParentAdvanceStopped"),
        }
    }
}
