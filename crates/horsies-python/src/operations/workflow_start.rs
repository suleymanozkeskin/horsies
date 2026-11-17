//! The workflow start through the core
//! (`PostgresBroker::start_client_workflow`): the tree of a spec that the
//! Python client sends as data, with the children of its sub-workflow
//! nodes from the handle's child spec source.
//!
//! The outcome is one class per answer: `WorkflowStarted`,
//! `WorkflowExists`, `WorkflowIdTaken` or `WorkflowStartFailed`. The Python
//! client maps them to the `WorkflowStartResult` of `start_workflow_async`.
//! A build that raised `KeyboardInterrupt` or `SystemExit` rolls the start
//! back and the same object is raised again; a row outside a function's
//! contract and spec data that the core refuses raise `PanicException`, as
//! Python raises `OutcomeDecodeError` and `SerializationError`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::{
    parse_workflow_spec_data, ChildSpecUnavailable, ClientSpec, ClientStartError,
    ClientStartOutcome, WorkflowError,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::utc_instant;
use crate::child_spec::PythonChildSpecSource;
use crate::handle::BrokerConnection;

/// One frozen workflow start. `spec_json` is the workflow spec data of the
/// spec; spec data that the core refuses raises `ValueError` with the field
/// path (a defect of the client). A naive `sent_at` raises `TypeError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct WorkflowStartRequest {
    parts: Arc<StartParts>,
}

/// The values of one start.
pub struct StartParts {
    pub spec: ClientSpec,
    pub workflow_id: Uuid,
    pub sent_at: DateTime<Utc>,
    pub resend_on_transient_err: bool,
}

impl WorkflowStartRequest {
    pub fn parts(&self) -> Arc<StartParts> {
        Arc::clone(&self.parts)
    }
}

#[pymethods]
impl WorkflowStartRequest {
    #[new]
    #[pyo3(signature = (*, spec_json, workflow_id, sent_at, resend_on_transient_err))]
    fn new(
        spec_json: &str,
        workflow_id: Uuid,
        sent_at: Bound<'_, PyAny>,
        resend_on_transient_err: bool,
    ) -> PyResult<Self> {
        let spec = parse_workflow_spec_data(spec_json)
            .map_err(|refused| PyValueError::new_err(refused.to_string()))?;
        Ok(Self {
            parts: Arc::new(StartParts {
                spec,
                workflow_id,
                sent_at: utc_instant(&sent_at)?,
                resend_on_transient_err,
            }),
        })
    }
}

/// The tree was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowStarted {
    pub workflow_id: Uuid,
}

/// A workflow with this id and this name exists, or the row with the id was
/// gone by the read of its name. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowExists {
    pub workflow_id: Uuid,
}

/// A workflow with this id exists under another name. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowIdTaken {
    pub workflow_id: Uuid,
    pub existing_name: String,
}

/// The connect, the schema install, the client family check, the statement
/// or an owed child start failed, or a child build did not run. Nothing
/// committed, or the commit outcome is unknown: a resend with the same id
/// then finds the workflow.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowStartFailed {
    pub workflow_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one start.
pub enum WorkflowStartOutcome {
    Started(WorkflowStarted),
    Exists(WorkflowExists),
    IdTaken(WorkflowIdTaken),
    Failed(WorkflowStartFailed),
    /// A child build raised this `BaseException`; the start rolled back.
    Interrupted(PyErr),
}

impl<'py> IntoPyObject<'py> for WorkflowStartOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Started(started) => started.into_bound_py_any(py),
            Self::Exists(exists) => exists.into_bound_py_any(py),
            Self::IdTaken(taken) => taken.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
            Self::Interrupted(exception) => Err(exception),
        }
    }
}

/// The outcome of a start's answer. `interrupt` takes the interrupt that a
/// child build raised.
///
/// Panics: a row outside a function's contract, spec data that the core
/// refuses at the tree build, and the errors that a start never gives (each
/// a defect).
fn start_outcome(
    workflow_id: Uuid,
    answer: Result<ClientStartOutcome, ClientStartError>,
    interrupt: impl FnOnce() -> Option<PyErr>,
) -> WorkflowStartOutcome {
    let error = match answer {
        Ok(ClientStartOutcome::Started) => {
            return WorkflowStartOutcome::Started(WorkflowStarted { workflow_id })
        }
        Ok(ClientStartOutcome::Exists) => {
            return WorkflowStartOutcome::Exists(WorkflowExists { workflow_id })
        }
        Ok(ClientStartOutcome::IdTaken { existing_name }) => {
            return WorkflowStartOutcome::IdTaken(WorkflowIdTaken {
                workflow_id,
                existing_name,
            })
        }
        Err(error) => error,
    };
    match &error {
        ClientStartError::SchemaInstall(_)
        | ClientStartError::Start(
            WorkflowError::Broker(_)
            | WorkflowError::Database(_)
            | WorkflowError::ChildSpecSource(ChildSpecUnavailable::NotRun { .. }),
        ) => failed(workflow_id, &error),
        ClientStartError::Start(WorkflowError::ChildSpecSource(
            ChildSpecUnavailable::Interrupted { .. },
        )) => match interrupt() {
            Some(exception) => WorkflowStartOutcome::Interrupted(exception),
            None => panic!(
                "the start of workflow {workflow_id} was interrupted by a child build that kept \
                 no interrupt (a defect): {error}"
            ),
        },
        ClientStartError::Start(
            WorkflowError::StepContract(_)
            | WorkflowError::Validation(_)
            | WorkflowError::Serialization(_)
            | WorkflowError::WorkflowNotFound { .. }
            | WorkflowError::InvalidStatus(_),
        ) => panic!("the start of workflow {workflow_id} failed with a defect: {error}"),
    }
}

/// The record of a failure that the caller can see. Pure.
fn failed(workflow_id: Uuid, error: &ClientStartError) -> WorkflowStartOutcome {
    WorkflowStartOutcome::Failed(WorkflowStartFailed {
        workflow_id,
        message: error.to_string(),
        retryable: error.is_retryable(),
    })
}

/// Start one workflow. Writes; a caller that leaves early does not stop it,
/// and sees its outcome as unknown.
pub async fn start_workflow_operation(
    connection: BrokerConnection,
    parts: Arc<StartParts>,
    source: PythonChildSpecSource,
) -> WorkflowStartOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return WorkflowStartOutcome::Failed(WorkflowStartFailed {
                workflow_id: parts.workflow_id,
                message: error.to_string(),
                retryable: error.is_retryable(),
            })
        }
    };
    let answer = broker
        .start_client_workflow(
            &parts.spec,
            parts.workflow_id,
            parts.sent_at,
            parts.resend_on_transient_err,
            &source,
        )
        .await;
    start_outcome(parts.workflow_id, answer, || source.take_interrupt())
}

#[cfg(test)]
mod tests {
    use horsies_core::BrokerError;

    use super::*;

    fn no_interrupt() -> Option<PyErr> {
        None
    }

    #[test]
    fn each_answer_has_its_record() {
        let id = Uuid::nil();
        assert!(matches!(
            start_outcome(id, Ok(ClientStartOutcome::Started), no_interrupt),
            WorkflowStartOutcome::Started(_)
        ));
        assert!(matches!(
            start_outcome(id, Ok(ClientStartOutcome::Exists), no_interrupt),
            WorkflowStartOutcome::Exists(_)
        ));
        match start_outcome(
            id,
            Ok(ClientStartOutcome::IdTaken {
                existing_name: "other".to_owned(),
            }),
            no_interrupt,
        ) {
            WorkflowStartOutcome::IdTaken(taken) => assert_eq!(taken.existing_name, "other"),
            _ => panic!("an id taken under another name is WorkflowIdTaken"),
        }
    }

    #[test]
    fn a_build_that_did_not_run_is_a_retryable_failure() {
        let answer = Err(ClientStartError::Start(WorkflowError::ChildSpecSource(
            ChildSpecUnavailable::NotRun {
                subject: "build".to_owned(),
                cause: "queue full".to_owned(),
            },
        )));
        match start_outcome(Uuid::nil(), answer, no_interrupt) {
            WorkflowStartOutcome::Failed(failed) => {
                assert!(failed.retryable);
                assert!(failed.message.contains("queue full"), "{}", failed.message);
            }
            _ => panic!("a build that did not run fails the start"),
        }
    }

    #[test]
    fn a_failed_install_names_the_install() {
        let answer = Err(ClientStartError::SchemaInstall(
            BrokerError::ConnectionFailed("refused".to_owned()),
        ));
        match start_outcome(Uuid::nil(), answer, no_interrupt) {
            WorkflowStartOutcome::Failed(failed) => {
                assert!(failed.message.starts_with("Schema initialization failed: "));
            }
            _ => panic!("a failed install fails the start"),
        }
    }

    #[test]
    fn an_interrupt_is_raised_again() {
        Python::initialize();
        let answer = Err(ClientStartError::Start(WorkflowError::ChildSpecSource(
            ChildSpecUnavailable::Interrupted {
                subject: "build".to_owned(),
                cause: "KeyboardInterrupt: ".to_owned(),
            },
        )));
        let outcome = start_outcome(Uuid::nil(), answer, || {
            Some(pyo3::exceptions::PyKeyboardInterrupt::new_err(()))
        });
        Python::attach(|py| match outcome.into_pyobject(py) {
            Err(error) => {
                assert!(error.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py));
            }
            Ok(_) => panic!("an interrupt is raised again"),
        });
    }

    #[test]
    #[should_panic(expected = "failed with a defect")]
    fn a_row_outside_the_contract_panics() {
        let answer = Err(ClientStartError::Start(WorkflowError::Validation(
            "refused".to_owned(),
        )));
        let _outcome = start_outcome(Uuid::nil(), answer, no_interrupt);
    }
}
