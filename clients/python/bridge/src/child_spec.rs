//! The child spec source of the Python client: the `build_with` of a
//! sub-workflow child, run on the callback thread.
//!
//! The Python client registers one callable per `CoreHandle`
//! (`register_child_spec_source`). When a workflow step of the core needs
//! the child of a sub-workflow node, the core passes a `ChildSpecRequest` to
//! that callable on the callback thread. The callable answers:
//!
//! - `ChildSpecBuilt(spec_json)`: the child's spec as workflow spec data
//!   (`_catalog/workflow_spec.py`);
//! - `ChildSpecRefused(message, failure_result)`: the child cannot start, and
//!   its node fails with that stored envelope.
//!
//! Failures of the call:
//! - The callback did not run (a full queue, a callback thread that did not
//!   start, an interpreter that is shutting down): `NotRun`, retryable; the
//!   step rolls back.
//! - The callable raised `KeyboardInterrupt`, `SystemExit` or another
//!   `BaseException` that is not an `Exception`: `Interrupted`; the step
//!   rolls back, and the operation raises the same object again.
//! - The callable raised an `Exception`, such as `SerializationError` for
//!   task options of a child root that do not parse, or gave another type
//!   (`TypeError`): `Raised`, not retryable; the step rolls back. It is not
//!   a defect of the core. Where it goes is the operation's
//!   [`RaisedExceptions`]: a start or a resume raises the same object again
//!   to its caller, as Python lets the exception of a child build propagate
//!   there; every other operation keeps its type and text in its record, as
//!   Python's finalize and recovery catch it.
//! - The callable gave spec data that the core refuses, or panicked: a
//!   defect of the client. The step panics with the subject and the cause
//!   (`PanicException` for the caller).

use std::sync::{Arc, Mutex, PoisonError};

use horsies_core::{
    parse_workflow_spec_data, ChildSpecFuture, ChildSpecRequest, ChildSpecSource,
    ChildSpecUnavailable, ChildStartSource, ClientChildSpec,
};
use pyo3::prelude::*;
use uuid::Uuid;

use crate::bridge::callback_thread::{python_call, CallbackFailure};
use crate::process_core::Core;

/// One dependency of the node whose result an `args_from` member of the
/// child's input takes.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ChildSpecSourceRow {
    /// The index of the dependency node.
    pub task_index: i32,
    /// Its node id; None for a node stored without one.
    pub node_id: Option<String>,
    /// Its status: COMPLETED, FAILED or SKIPPED.
    pub status: String,
    /// Its task name.
    pub task_name: String,
    /// The definition key of a sub-workflow dependency; None for a task.
    pub definition_key: Option<String>,
    /// Its stored result text; None when it stores none.
    pub result: Option<String>,
}

impl ChildSpecSourceRow {
    fn of(source: ChildStartSource) -> Self {
        Self {
            task_index: source.task_index,
            node_id: source.node_id,
            status: source.status,
            task_name: source.task_name,
            definition_key: source.definition_key,
            result: source.result,
        }
    }
}

/// The sub-workflow node whose child the Python client builds.
#[pyclass(module = "horsies._core", name = "ChildSpecRequest", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ChildSpecRequestRecord {
    /// The workflow of the node.
    pub parent_workflow_id: Uuid,
    /// The name of that workflow.
    pub parent_workflow_name: String,
    /// The index of the sub-workflow node.
    pub task_index: i32,
    /// The depth of the child, within the depth limit.
    pub child_depth: i32,
    /// The definition key that resolves the child; None when the node
    /// stores none.
    pub definition_key: Option<String>,
    /// GIVEN when the node gives the child params, NOT_GIVEN when it gives
    /// none.
    pub params: String,
    /// The node's stored input.
    pub node_input: String,
    /// The node's args_from as JSON text; None when it has none.
    pub args_from: Option<String>,
    /// The terminal dependencies of the node.
    pub sources: Vec<ChildSpecSourceRow>,
}

impl ChildSpecRequestRecord {
    fn of(request: ChildSpecRequest) -> Self {
        Self {
            parent_workflow_id: request.parent_workflow_id,
            parent_workflow_name: request.parent_workflow_name,
            task_index: request.task_index,
            child_depth: request.child_depth,
            definition_key: request.definition_key,
            params: request.params.as_stored().to_owned(),
            node_input: request.node_input,
            args_from: request.args_from,
            sources: request
                .sources
                .into_iter()
                .map(ChildSpecSourceRow::of)
                .collect(),
        }
    }
}

/// The child built: its spec as workflow spec data.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ChildSpecBuilt {
    pub spec_json: String,
}

#[pymethods]
impl ChildSpecBuilt {
    #[new]
    #[pyo3(signature = (*, spec_json))]
    fn new(spec_json: String) -> Self {
        Self { spec_json }
    }
}

/// The child cannot start: its node fails. message is the error text for
/// the log; failure_result is the result envelope that the node stores.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ChildSpecRefused {
    pub message: String,
    pub failure_result: String,
}

#[pymethods]
impl ChildSpecRefused {
    #[new]
    #[pyo3(signature = (*, message, failure_result))]
    fn new(message: String, failure_result: String) -> Self {
        Self {
            message,
            failure_result,
        }
    }
}

/// The answer of the Python callable, read on the callback thread.
enum PythonAnswer {
    Built { spec_json: String },
    Refused(ChildSpecRefused),
}

/// Read the answer of the callable. Another type raises `TypeError`, which
/// the source answers as a build that raised.
fn python_answer(answer: &Bound<'_, PyAny>) -> PyResult<PythonAnswer> {
    if let Ok(built) = answer.cast::<ChildSpecBuilt>() {
        return Ok(PythonAnswer::Built {
            spec_json: built.get().spec_json.clone(),
        });
    }
    match answer.cast::<ChildSpecRefused>() {
        Ok(refused) => Ok(PythonAnswer::Refused(refused.get().clone())),
        Err(_) => Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "the child spec source returned {}, not ChildSpecBuilt or ChildSpecRefused",
            answer
                .get_type()
                .name()
                .map(|name| name.to_string())
                .unwrap_or_else(|_unreadable| "an object of unreadable type".to_owned()),
        ))),
    }
}

/// The callable that a `CoreHandle` registered as its child spec source.
pub type RegisteredSource = Arc<Py<PyAny>>;

/// Where an operation sends an `Exception` that a child build raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaisedExceptions {
    /// The source keeps the first one, and the operation raises it again to
    /// its caller once the step rolled back: a start and a resume, where
    /// Python lets the exception of a child build propagate.
    ToCaller,
    /// The source keeps none: the step's record carries the type and the
    /// text (`ChildSpecUnavailable::Raised`). Phase 2, the parent advance,
    /// the recovery passes, the worker and the web handler, where Python's
    /// finalize and recovery catch the exception of a step.
    InRecord,
}

/// What the builds of one operation raised, kept for the operation to raise
/// again: the first interrupt, and, when the operation's exceptions go
/// [`RaisedExceptions::ToCaller`], the first `Exception`. Holds no core, so
/// its rules are tested on their own.
struct KeptRaises {
    raised_exceptions: RaisedExceptions,
    interrupt: Mutex<Option<PyErr>>,
    exception: Mutex<Option<PyErr>>,
}

impl KeptRaises {
    /// Nothing kept yet. Pure.
    fn new(raised_exceptions: RaisedExceptions) -> Self {
        Self {
            raised_exceptions,
            interrupt: Mutex::new(None),
            exception: Mutex::new(None),
        }
    }

    /// The kept interrupt, taken once.
    fn take_interrupt(&self) -> Option<PyErr> {
        self.interrupt
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The kept `Exception`, taken once.
    fn take_exception(&self) -> Option<PyErr> {
        self.exception
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Keep `interrupt` when no interrupt is kept yet.
    fn keep_interrupt(&self, interrupt: PyErr) {
        let mut kept = self
            .interrupt
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if kept.is_none() {
            *kept = Some(interrupt);
        }
    }

    /// Keep `exception` when the exceptions go to the caller and none is kept
    /// yet; drop it when they go in the record.
    fn keep_exception(&self, exception: PyErr) {
        match self.raised_exceptions {
            RaisedExceptions::ToCaller => {
                let mut kept = self
                    .exception
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if kept.is_none() {
                    *kept = Some(exception);
                }
            }
            RaisedExceptions::InRecord => drop(exception),
        }
    }

    /// The failure of a call as the answer of the source; keeps what the
    /// call raised.
    ///
    /// Panics: a panic of the call (a defect of the client).
    fn unavailable(&self, failure: CallbackFailure) -> ChildSpecUnavailable {
        match failure {
            CallbackFailure::QueueFull { ref subject, .. }
            | CallbackFailure::ThreadStartFailed { ref subject, .. }
            | CallbackFailure::InterpreterShutDown { ref subject } => {
                ChildSpecUnavailable::NotRun {
                    subject: subject.clone(),
                    cause: failure.to_string(),
                }
            }
            CallbackFailure::RaisedBaseException {
                subject,
                cause,
                exception,
            } => {
                self.keep_interrupt(*exception);
                ChildSpecUnavailable::Interrupted {
                    subject,
                    cause: cause.to_string(),
                }
            }
            CallbackFailure::RaisedException {
                subject,
                cause,
                exception,
            } => {
                self.keep_exception(*exception);
                ChildSpecUnavailable::Raised {
                    subject,
                    cause: cause.to_string(),
                }
            }
            CallbackFailure::Panicked { .. } => {
                panic!("{failure} (a defect of the Python client's child spec source)")
            }
        }
    }
}

/// The child spec source of one operation: the registered callable, run on
/// the callback thread of `core`. It keeps what a build raised
/// ([`KeptRaises`]), so that the operation raises it again.
pub struct PythonChildSpecSource {
    core: &'static Core,
    callable: RegisteredSource,
    kept: KeptRaises,
}

impl PythonChildSpecSource {
    /// The source of one operation; `raised_exceptions` says where an
    /// `Exception` of a build goes. Starts nothing.
    pub fn new(
        core: &'static Core,
        callable: RegisteredSource,
        raised_exceptions: RaisedExceptions,
    ) -> Self {
        Self {
            core,
            callable,
            kept: KeptRaises::new(raised_exceptions),
        }
    }

    /// The interrupt that a build of this source raised, taken once.
    pub fn take_interrupt(&self) -> Option<PyErr> {
        self.kept.take_interrupt()
    }

    /// The `Exception` that a build of this source raised, taken once.
    /// Always `None` for a source whose exceptions go
    /// [`RaisedExceptions::InRecord`].
    pub fn take_exception(&self) -> Option<PyErr> {
        self.kept.take_exception()
    }
}

/// The subject of one build, as the logs and the failures name it.
fn subject_of(request: &ChildSpecRequest) -> String {
    format!(
        "build of the child of {}:{} (definition {})",
        request.parent_workflow_name,
        request.task_index,
        request.definition_key.as_deref().unwrap_or("<none>"),
    )
}

impl ChildSpecSource for PythonChildSpecSource {
    fn child_spec(&self, request: ChildSpecRequest) -> ChildSpecFuture<'_> {
        let subject = subject_of(&request);
        let callable = Arc::clone(&self.callable);
        let record = ChildSpecRequestRecord::of(request);
        Box::pin(async move {
            let call = python_call(move |py: Python<'_>| -> PyResult<PythonAnswer> {
                let answer = callable.bind(py).call1((record,))?;
                python_answer(&answer)
            });
            match self.core.callbacks().call(subject.clone(), call).await {
                Ok(PythonAnswer::Built { spec_json }) => match parse_workflow_spec_data(&spec_json)
                {
                    Ok(spec) => Ok(ClientChildSpec::Built(spec)),
                    Err(refused) => panic!(
                        "{subject} gave spec data that the core refuses (a defect of the Python \
                         client): {refused}"
                    ),
                },
                Ok(PythonAnswer::Refused(refused)) => Ok(ClientChildSpec::NodeFails {
                    message: refused.message,
                    failure_result: refused.failure_result,
                }),
                Err(failure) => Err(self.kept.unavailable(failure)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::SubWorkflowParams;

    use super::*;

    fn request() -> ChildSpecRequest {
        ChildSpecRequest {
            parent_workflow_id: Uuid::nil(),
            parent_workflow_name: "nightly".to_owned(),
            task_index: 2,
            child_depth: 1,
            definition_key: Some("tests.report.v1".to_owned()),
            params: SubWorkflowParams::Given,
            node_input: "{\"day\":1}".to_owned(),
            args_from: Some("{\"rows\":0}".to_owned()),
            sources: vec![ChildStartSource {
                task_index: 0,
                node_id: Some("fetch".to_owned()),
                status: "COMPLETED".to_owned(),
                task_name: "fetch_rows".to_owned(),
                definition_key: None,
                result: Some("{\"ok\":3}".to_owned()),
            }],
        }
    }

    #[test]
    fn the_request_record_carries_every_fact_of_the_request() {
        let record = ChildSpecRequestRecord::of(request());
        assert_eq!(record.parent_workflow_name, "nightly");
        assert_eq!(record.task_index, 2);
        assert_eq!(record.child_depth, 1);
        assert_eq!(record.params, "GIVEN");
        assert_eq!(record.args_from.as_deref(), Some("{\"rows\":0}"));
        assert_eq!(record.sources.len(), 1);
        assert_eq!(record.sources[0].node_id.as_deref(), Some("fetch"));
        assert_eq!(record.sources[0].result.as_deref(), Some("{\"ok\":3}"));
    }

    #[test]
    fn the_subject_names_the_node_and_the_definition() {
        assert_eq!(
            subject_of(&request()),
            "build of the child of nightly:2 (definition tests.report.v1)"
        );
    }

    fn raised_exception() -> CallbackFailure {
        CallbackFailure::RaisedException {
            subject: "build of the child of bulk_parent:0 (definition bulk-child)".to_owned(),
            cause: crate::bridge::callback_thread::RaisedCause {
                exception_type: "SerializationError".to_owned(),
                message: "Corrupt task_options for root node".to_owned(),
            },
            exception: Box::new(pyo3::exceptions::PyValueError::new_err(
                "Corrupt task_options",
            )),
        }
    }

    /// An `Exception` of a build is no defect: the source answers `Raised`
    /// with its type and text, and keeps the object for an operation that
    /// raises it again to its caller.
    #[test]
    fn an_exception_of_a_build_is_kept_for_the_caller() {
        Python::initialize();
        let kept = KeptRaises::new(RaisedExceptions::ToCaller);

        let answer = kept.unavailable(raised_exception());

        assert_eq!(
            answer,
            ChildSpecUnavailable::Raised {
                subject: "build of the child of bulk_parent:0 (definition bulk-child)".to_owned(),
                cause: "SerializationError: Corrupt task_options for root node".to_owned(),
            }
        );
        assert!(!answer.is_retryable());
        assert!(kept.take_interrupt().is_none());
        let exception = kept.take_exception().expect("the exception is kept");
        Python::attach(|py| {
            assert!(exception.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        });
        assert!(kept.take_exception().is_none(), "taken once");
    }

    /// An operation whose exceptions go in its record keeps none: the answer
    /// carries the type and the text.
    #[test]
    fn an_exception_for_the_record_is_not_kept() {
        let kept = KeptRaises::new(RaisedExceptions::InRecord);

        let answer = kept.unavailable(raised_exception());

        assert!(matches!(answer, ChildSpecUnavailable::Raised { .. }));
        assert!(kept.take_exception().is_none());
    }

    /// An interrupt is kept for every operation, apart from the exceptions.
    #[test]
    fn an_interrupt_is_kept_apart_from_the_exceptions() {
        let kept = KeptRaises::new(RaisedExceptions::InRecord);

        let answer = kept.unavailable(CallbackFailure::RaisedBaseException {
            subject: "build".to_owned(),
            cause: crate::bridge::callback_thread::RaisedCause {
                exception_type: "KeyboardInterrupt".to_owned(),
                message: String::new(),
            },
            exception: Box::new(pyo3::exceptions::PyKeyboardInterrupt::new_err(())),
        });

        assert!(matches!(answer, ChildSpecUnavailable::Interrupted { .. }));
        assert!(kept.take_interrupt().is_some());
        assert!(kept.take_exception().is_none());
    }

    #[test]
    fn an_answer_of_another_type_raises_type_error() {
        Python::initialize();
        Python::attach(|py| {
            let refused = Py::new(
                py,
                ChildSpecRefused {
                    message: "load failed".to_owned(),
                    failure_result: "{}".to_owned(),
                },
            )
            .expect("the record is built");
            assert!(matches!(
                python_answer(refused.bind(py).as_any()),
                Ok(PythonAnswer::Refused(_))
            ));
            let text = "spec".into_pyobject(py).expect("a str");
            let Err(error) = python_answer(text.as_any()) else {
                panic!("a str is no answer");
            };
            assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        });
    }
}
