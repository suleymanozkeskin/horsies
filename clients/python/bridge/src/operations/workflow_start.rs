//! The workflow start through the core
//! (`PostgresBroker::start_client_workflow`): the tree of a spec that the
//! Python client sends as data, with the children of its sub-workflow
//! nodes from the handle's child spec source.
//!
//! The outcome is one class per answer: `WorkflowStarted` or
//! `WorkflowExists`, or the generated class of the core's start catalog
//! (`horsies._catalog.workflow_start_errors`, built in Rust):
//! `WorkflowStartIdConflict`, `WorkflowStartCommandConflict` or
//! `WorkflowStartEnqueueFailed`. The Python client returns the class as the
//! error of the `WorkflowStartResult` of `start_workflow_async`.
//! A build that raised rolls the start back and the same object is raised
//! again: `KeyboardInterrupt` or `SystemExit`, or an `Exception` such as
//! `SerializationError` for task options of a child root that do not parse,
//! as Python's start lets it propagate. A row outside a function's contract
//! and spec data that the core refuses raise `PanicException`, as Python
//! raises `OutcomeDecodeError` and `SerializationError`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::telemetry::trace_carrier::SendTrace;
use horsies_core::{
    parse_workflow_spec_data, BrokerError, ChildSpecUnavailable, ClientSpec, ClientStartError,
    ClientStartOutcome, WorkflowError, WorkflowStartError,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::{utc_instant, TraceArgument};
use crate::catalog_errors::catalog_error;
use crate::child_spec::PythonChildSpecSource;
use crate::handle::BrokerConnection;

/// One frozen workflow start. `spec_json` is the workflow spec data of the
/// spec; spec data that the core refuses raises `ValueError` with the field
/// path (a defect of the client). A naive `sent_at` raises `TypeError`.
/// `trace` is `NoTraceCarrier` or the `TraceCarrier` of the start's span;
/// every workflow row and task row of the tree stores it.
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
    pub trace: SendTrace,
}

impl WorkflowStartRequest {
    pub fn parts(&self) -> Arc<StartParts> {
        Arc::clone(&self.parts)
    }
}

#[pymethods]
impl WorkflowStartRequest {
    #[new]
    #[pyo3(signature = (*, spec_json, workflow_id, sent_at, resend_on_transient_err, trace))]
    fn new(
        spec_json: &str,
        workflow_id: Uuid,
        sent_at: &Bound<'_, PyAny>,
        resend_on_transient_err: bool,
        trace: TraceArgument<'_>,
    ) -> PyResult<Self> {
        let spec = parse_workflow_spec_data(spec_json)
            .map_err(|refused| PyValueError::new_err(refused.to_string()))?;
        Ok(Self {
            parts: Arc::new(StartParts {
                spec,
                workflow_id,
                sent_at: utc_instant(sent_at)?,
                resend_on_transient_err,
                trace: trace.send_trace(),
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

/// A workflow run with this id exists, and the start returns it: the run holds
/// the start command of this start, or it has no stored start command (an
/// earlier horsies version started it; the core logs a warning). Nothing was
/// written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowExists {
    pub workflow_id: Uuid,
}

/// The subject of the exception of a failed start.
const SUBJECT: &str = "start_workflow";

/// The outcome of one start.
pub enum WorkflowStartOutcome {
    Started(WorkflowStarted),
    Exists(WorkflowExists),
    /// The id belongs to another workflow name (`IdConflict`), or to a run
    /// of this name that another start command started, or to a sub-workflow
    /// (`CommandConflict`); or the connect, the schema install, the client
    /// family check, the statement or an owed child start failed, or a child
    /// build did not run (`EnqueueFailed`: nothing committed, or the commit
    /// outcome is unknown, and a resend with the same id then finds the
    /// workflow). Built as its generated class.
    Refused(WorkflowStartError),
    /// A child build raised this `BaseException` (an interrupt or an
    /// `Exception`); the start rolled back.
    BuildRaised(PyErr),
}

impl<'py> IntoPyObject<'py> for WorkflowStartOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Started(started) => started.into_bound_py_any(py),
            Self::Exists(exists) => exists.into_bound_py_any(py),
            Self::Refused(error) => catalog_error(py, SUBJECT, &error),
            Self::BuildRaised(exception) => Err(exception),
        }
    }
}

/// The outcome of a start's answer for the start `workflow_id` of
/// `workflow_name`. `interrupt` takes the interrupt that a child build
/// raised; `exception` takes the `Exception` that it raised.
///
/// Panics: a row outside a function's contract, spec data that the core
/// refuses at the tree build, the errors that a start never gives, and a
/// build that raised with nothing kept (each a defect).
fn start_outcome(
    workflow_name: &str,
    workflow_id: Uuid,
    answer: Result<ClientStartOutcome, ClientStartError>,
    interrupt: impl FnOnce() -> Option<PyErr>,
    exception: impl FnOnce() -> Option<PyErr>,
) -> WorkflowStartOutcome {
    let error = match answer {
        Ok(ClientStartOutcome::Started) => {
            return WorkflowStartOutcome::Started(WorkflowStarted { workflow_id })
        }
        Ok(ClientStartOutcome::Exists(_)) => {
            return WorkflowStartOutcome::Exists(WorkflowExists { workflow_id })
        }
        Ok(ClientStartOutcome::IdHeld(holder)) => {
            return WorkflowStartOutcome::Refused(WorkflowStartError::id_held(
                workflow_name,
                workflow_id,
                holder,
            ))
        }
        Err(error) => error,
    };
    match error {
        failed @ (ClientStartError::SchemaInstall(_)
        | ClientStartError::Start(
            WorkflowError::Broker(_)
            | WorkflowError::ChildSpecSource(ChildSpecUnavailable::NotRun { .. }),
        )) => WorkflowStartOutcome::Refused(WorkflowStartError::start_failed(
            workflow_name,
            workflow_id,
            failed,
        )),
        ref interrupted @ ClientStartError::Start(WorkflowError::ChildSpecSource(
            ChildSpecUnavailable::Interrupted { .. },
        )) => match interrupt() {
            Some(raised) => WorkflowStartOutcome::BuildRaised(raised),
            None => panic!(
                "the start of workflow {workflow_id} was interrupted by a child build that kept \
                 no interrupt (a defect): {interrupted}"
            ),
        },
        ref raised_in_build @ ClientStartError::Start(WorkflowError::ChildSpecSource(
            ChildSpecUnavailable::Raised { .. },
        )) => match exception() {
            Some(raised) => WorkflowStartOutcome::BuildRaised(raised),
            None => panic!(
                "the start of workflow {workflow_id} failed in a child build that kept no \
                 exception (a defect): {raised_in_build}"
            ),
        },
        ref defect @ ClientStartError::Start(
            WorkflowError::StepContract(_)
            | WorkflowError::Validation(_)
            | WorkflowError::Serialization(_)
            | WorkflowError::WorkflowNotFound { .. }
            | WorkflowError::InvalidStatus(_),
        ) => panic!("the start of workflow {workflow_id} failed with a defect: {defect}"),
    }
}

/// Start one workflow. Writes; a caller that leaves early does not stop it,
/// and sees its outcome as unknown.
pub async fn start_workflow_operation(
    connection: BrokerConnection,
    parts: Arc<StartParts>,
    source: PythonChildSpecSource,
) -> WorkflowStartOutcome {
    let workflow_name = parts.spec.name();
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return connect_failed(workflow_name, parts.workflow_id, error),
    };
    let answer = broker
        .start_client_workflow(
            &parts.spec,
            parts.workflow_id,
            parts.sent_at,
            parts.trace.clone(),
            parts.resend_on_transient_err,
            &source,
        )
        .await;
    start_outcome(
        workflow_name,
        parts.workflow_id,
        answer,
        || source.take_interrupt(),
        || source.take_exception(),
    )
}

/// The start `workflow_id` of `workflow_name` could not connect: nothing
/// was sent. The message is the broker error's text, and the error is the
/// cause. Pure.
fn connect_failed(
    workflow_name: &str,
    workflow_id: Uuid,
    error: BrokerError,
) -> WorkflowStartOutcome {
    WorkflowStartOutcome::Refused(WorkflowStartError::EnqueueFailed {
        message: error.to_string(),
        retryable: error.is_retryable(),
        workflow_name: workflow_name.to_owned(),
        workflow_id,
        cause: Box::new(WorkflowError::Broker(error)),
    })
}

#[cfg(test)]
mod tests {
    use horsies_core::error_catalog::{ErrorClassValues, ExceptionValue, SlotData};
    use horsies_core::{ExistingRun, FailureCause, IdHolder};

    use super::*;

    fn no_raise() -> Option<PyErr> {
        None
    }

    /// The workflow name of the starts of these tests.
    const NAME: &str = "nightly";

    /// The value of the slot `name` of `error`'s class.
    fn slot<'e>(error: &'e WorkflowStartError, name: &str) -> Option<SlotData<'e>> {
        error
            .slot_values()
            .into_iter()
            .find(|slot| slot.name == name)
            .map(|slot| slot.value)
    }

    #[test]
    fn each_answer_has_its_record_or_class() {
        let id = Uuid::from_u128(5);
        assert!(matches!(
            start_outcome(
                NAME,
                id,
                Ok(ClientStartOutcome::Started),
                no_raise,
                no_raise
            ),
            WorkflowStartOutcome::Started(_)
        ));
        for existing in [ExistingRun::SameCommand, ExistingRun::FingerprintNotStored] {
            assert!(matches!(
                start_outcome(
                    NAME,
                    id,
                    Ok(ClientStartOutcome::Exists(existing)),
                    no_raise,
                    no_raise
                ),
                WorkflowStartOutcome::Exists(_)
            ));
        }
        match start_outcome(
            NAME,
            id,
            Ok(ClientStartOutcome::IdHeld(IdHolder::OtherWorkflowName {
                existing_name: "other".to_owned(),
            })),
            no_raise,
            no_raise,
        ) {
            WorkflowStartOutcome::Refused(conflict) => {
                assert_eq!(
                    conflict.code(),
                    horsies_core::WorkflowStartErrorCode::WorkflowIdConflict
                );
                assert_eq!(conflict.workflow_name(), NAME);
                assert_eq!(conflict.workflow_id(), id);
                assert_eq!(
                    slot(&conflict, "existing_workflow_name"),
                    Some(SlotData::Str("other"))
                );
                assert_eq!(
                    conflict.message(),
                    format!(
                        "workflow_id '{id}' already exists for workflow name 'other', cannot \
                         start workflow name 'nightly' with the same id"
                    )
                );
            }
            _ => panic!("an id taken under another name is WorkflowStartIdConflict"),
        }
    }

    /// An id that a run of this name with another start command holds, or
    /// that a sub-workflow holds, is `WorkflowStartCommandConflict` with the
    /// id of that run, never retryable.
    #[test]
    fn an_id_held_by_another_command_or_a_sub_workflow_is_a_command_conflict() {
        let id = Uuid::from_u128(6);
        for (holder, message) in [
            (
                IdHolder::OtherCommand,
                format!(
                    "workflow_id '{id}' already exists for workflow name 'nightly' with another \
                     start command; a start with this id must send the same command"
                ),
            ),
            (
                IdHolder::SubWorkflow,
                format!(
                    "workflow_id '{id}' belongs to a sub-workflow that another workflow started; \
                     it has no start command"
                ),
            ),
        ] {
            match start_outcome(
                NAME,
                id,
                Ok(ClientStartOutcome::IdHeld(holder)),
                no_raise,
                no_raise,
            ) {
                WorkflowStartOutcome::Refused(conflict) => {
                    assert_eq!(
                        conflict.code(),
                        horsies_core::WorkflowStartErrorCode::WorkflowCommandConflict
                    );
                    assert_eq!(conflict.workflow_name(), NAME);
                    assert_eq!(conflict.workflow_id(), id);
                    assert!(!conflict.retryable());
                    assert_eq!(conflict.message(), message);
                }
                _ => panic!("an id held by another command is WorkflowStartCommandConflict"),
            }
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
        match start_outcome(NAME, Uuid::nil(), answer, no_raise, no_raise) {
            WorkflowStartOutcome::Refused(failed) => {
                assert_eq!(
                    failed.code(),
                    horsies_core::WorkflowStartErrorCode::EnqueueFailed
                );
                assert!(failed.retryable());
                assert!(
                    failed.message().contains("queue full"),
                    "{}",
                    failed.message()
                );
            }
            _ => panic!("a build that did not run fails the start"),
        }
    }

    /// A failed install names the install in the message; its exception is
    /// the broker error with the error's own text, as Python 0.7's
    /// exception was the driver's error.
    #[test]
    fn a_failed_install_names_the_install_and_its_exception_is_the_broker_error() {
        let refused = || BrokerError::ConnectionFailed("refused".to_owned());
        let answer = Err(ClientStartError::SchemaInstall(refused()));
        match start_outcome(NAME, Uuid::nil(), answer, no_raise, no_raise) {
            WorkflowStartOutcome::Refused(failed) => {
                assert_eq!(
                    failed.message(),
                    format!("Schema initialization failed: {}", refused())
                );
                assert_eq!(
                    slot(&failed, "cause"),
                    Some(SlotData::Exception(ExceptionValue::CoreFailure {
                        text: refused().to_string(),
                        retryable: true,
                        cause: FailureCause::Connection,
                    }))
                );
            }
            _ => panic!("a failed install fails the start"),
        }
    }

    /// A failed connect: nothing was sent; the message is the broker
    /// error's text, and the error is the cause.
    #[test]
    fn a_failed_connect_is_enqueue_failed_with_the_broker_error() {
        let refused = || BrokerError::ConnectionFailed("refused".to_owned());
        match connect_failed(NAME, Uuid::from_u128(9), refused()) {
            WorkflowStartOutcome::Refused(failed) => {
                assert_eq!(
                    failed.code(),
                    horsies_core::WorkflowStartErrorCode::EnqueueFailed
                );
                assert_eq!(failed.message(), refused().to_string());
                assert!(failed.retryable());
                assert_eq!(failed.workflow_id(), Uuid::from_u128(9));
                assert_eq!(failed.failure_cause(), FailureCause::Connection);
            }
            _ => panic!("a failed connect fails the start"),
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
        let outcome = start_outcome(
            NAME,
            Uuid::nil(),
            answer,
            || Some(pyo3::exceptions::PyKeyboardInterrupt::new_err(())),
            no_raise,
        );
        Python::attach(|py| match outcome.into_pyobject(py) {
            Err(error) => {
                assert!(error.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py));
            }
            Ok(_) => panic!("an interrupt is raised again"),
        });
    }

    /// An `Exception` of a child build (a `SerializationError` for task
    /// options of a child root that do not parse) is no defect of the core:
    /// the start rolled back, and the caller gets the same object, as
    /// Python's start lets it propagate.
    #[test]
    fn an_exception_of_a_child_build_is_raised_again() {
        Python::initialize();
        let answer = Err(ClientStartError::Start(WorkflowError::ChildSpecSource(
            ChildSpecUnavailable::Raised {
                subject: "build".to_owned(),
                cause: "SerializationError: Corrupt task_options".to_owned(),
            },
        )));
        let outcome = start_outcome(NAME, Uuid::nil(), answer, no_raise, || {
            Some(pyo3::exceptions::PyValueError::new_err(
                "Corrupt task_options",
            ))
        });
        Python::attach(|py| match outcome.into_pyobject(py) {
            Err(error) => {
                assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            }
            Ok(_) => panic!("the exception of a child build is raised again"),
        });
    }

    #[test]
    #[should_panic(expected = "failed with a defect")]
    fn a_row_outside_the_contract_panics() {
        let answer = Err(ClientStartError::Start(WorkflowError::Validation(
            "refused".to_owned(),
        )));
        let _outcome = start_outcome(NAME, Uuid::nil(), answer, no_raise, no_raise);
    }
}
