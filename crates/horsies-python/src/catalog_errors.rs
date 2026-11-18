//! The generated error classes of the core's catalogs, built in Rust.
//!
//! A core error value whose enum derives `ErrorClassValues` becomes an
//! instance of its generated class in `horsies._catalog` (for example
//! `horsies._catalog.monitoring_errors.TaskActionNotFound`): the class is
//! called with the value's instance slots as keyword arguments. A slot that
//! holds a code is the member of its generated code enum. The `exception`
//! slot is built from the variant's typed cause: the core's class of the
//! cause (`core_failures`), with the operation that failed as its subject,
//! or a generated exception class such as `OutcomeDecodeError`.
//!
//! Each class is imported once per process, at its first use, and kept
//! (`PyOnceLock`). The instance is built on the thread that hands the
//! outcome to Python, with the GIL; no runtime thread takes the GIL. An
//! import or a constructor that fails raises its exception to the caller:
//! a generated module that does not match the core is a defect of the
//! build, not an outcome.

use horsies_core::error_catalog::{ErrorClassValues, ErrorEnum, ExceptionValue, SlotData};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

use crate::core_failures::FailureException;

/// The package of the generated modules.
const CATALOG_PACKAGE: &str = "horsies._catalog";

/// The imported classes of `horsies._catalog`, keyed by `(module, class)`.
static CATALOG_CLASSES: PyOnceLock<Py<PyDict>> = PyOnceLock::new();

/// The class `class` of `horsies._catalog.<module>`. The first call for a
/// class imports it and keeps it; later calls read the kept class.
///
/// Failure exits: the `ImportError` or `AttributeError` of a module or a
/// class that does not exist.
fn catalog_class<'py>(
    py: Python<'py>,
    module: &'static str,
    class: &'static str,
) -> PyResult<Bound<'py, PyAny>> {
    let classes = CATALOG_CLASSES
        .get_or_init(py, || PyDict::new(py).unbind())
        .bind(py);
    match classes.get_item((module, class))? {
        Some(kept) => Ok(kept),
        None => {
            let imported = py
                .import(format!("{CATALOG_PACKAGE}.{module}"))?
                .getattr(class)?;
            classes.set_item((module, class), &imported)?;
            Ok(imported)
        }
    }
}

/// The instance of the generated class of `error`. `subject` is the
/// operation that failed, for example `cancel_task`: the subject of a core
/// exception class in the `exception` slot.
///
/// Failure exits: those of the class import, and the exception of a
/// constructor that refuses the values.
pub fn catalog_error<'py, E: ErrorClassValues>(
    py: Python<'py>,
    subject: &'static str,
    error: &E,
) -> PyResult<Bound<'py, PyAny>> {
    let class = catalog_class(
        py,
        <E as ErrorEnum>::ERROR_CATALOG.python_module,
        ErrorEnum::python_class(error),
    )?;
    let fields = PyDict::new(py);
    for slot in error.slot_values() {
        fields.set_item(slot.name, slot_object(py, subject, slot.value)?)?;
    }
    class.call((), Some(&fields))
}

/// The Python value of one instance slot.
fn slot_object<'py>(
    py: Python<'py>,
    subject: &'static str,
    value: SlotData<'_>,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        SlotData::Str(text) => text.into_bound_py_any(py),
        SlotData::Bool(flag) => flag.into_bound_py_any(py),
        SlotData::Uuid(id) => id.into_bound_py_any(py),
        SlotData::UuidText(id) => id.to_string().into_bound_py_any(py),
        SlotData::CodeMember { catalog, value } => {
            catalog_class(py, catalog.python_module, catalog.python_class)?.call1((value,))
        }
        SlotData::Exception(exception) => exception_object(py, subject, exception),
    }
}

/// The exception of the `exception` slot: the core's class of the cause
/// with `subject`, or the generated exception class with its message.
fn exception_object<'py>(
    py: Python<'py>,
    subject: &'static str,
    exception: ExceptionValue,
) -> PyResult<Bound<'py, PyAny>> {
    match exception {
        ExceptionValue::CoreFailure {
            text,
            retryable,
            cause,
        } => FailureException::new(subject, text, retryable, cause).into_pyobject(py),
        ExceptionValue::Generated { class, text } => {
            catalog_class(py, class.python_module, class.python_class)?.call1((text,))
        }
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::{FailureCause, ServerErrorFacts};

    use super::*;
    use crate::core_failures::{CoreConnectionError, CoreServerError};

    /// A cause that the server answered keeps its SQLSTATE and the server's
    /// primary message; any other cause keeps the text of the failure. Both
    /// carry the subject of the operation.
    #[test]
    fn a_core_failure_is_the_class_of_its_cause_with_the_subject() {
        Python::initialize();
        Python::attach(|py| {
            let connection = exception_object(
                py,
                "cancel_task",
                ExceptionValue::CoreFailure {
                    text: "connection failed: refused".to_owned(),
                    retryable: true,
                    cause: FailureCause::Connection,
                },
            )
            .expect("the exception is built");
            assert!(connection.is_instance_of::<CoreConnectionError>());
            assert_eq!(
                connection.str().expect("str").to_string(),
                "cancel_task: connection failed: refused"
            );

            let server = exception_object(
                py,
                "cancel_task",
                ExceptionValue::CoreFailure {
                    text: "database error: canceling statement".to_owned(),
                    retryable: true,
                    cause: FailureCause::Server(ServerErrorFacts::of_code(
                        "57014",
                        "canceling statement due to statement timeout".to_owned(),
                    )),
                },
            )
            .expect("the exception is built");
            assert!(server.is_instance_of::<CoreServerError>());
            assert_eq!(
                server.str().expect("str").to_string(),
                "cancel_task: canceling statement due to statement timeout"
            );
            assert_eq!(
                server
                    .getattr("sqlstate")
                    .and_then(|sqlstate| sqlstate.extract::<String>())
                    .expect("sqlstate"),
                "57014"
            );
        });
    }
}
