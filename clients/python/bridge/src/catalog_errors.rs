//! The generated error classes of the core's catalogs, built in Rust.
//!
//! A core error value whose enum derives `ErrorClassValues` becomes an
//! instance of its generated class in `horsies._catalog` (for example
//! `horsies._catalog.monitoring_errors.TaskActionNotFound`): the class is
//! called with the value's instance slots as keyword arguments. A slot that
//! holds a code is the member of its generated code enum, and a slot that
//! holds the payload of a send is the generated record `TaskSendPayload`,
//! called with the payload's fields. The `exception`
//! slot is built from the variant's typed cause: the core's class of the
//! cause (`core_failures`), with the operation that failed as its subject,
//! a generated exception class such as `OutcomeDecodeError`, or a class
//! that the Python client defines: the built-in `ValueError`, or
//! `SerializationError` of `horsies.core.codec.json_io`.
//!
//! Each class is imported once per process, at its first use, and kept
//! (`PyOnceLock`). The instance is built on the thread that hands the
//! outcome to Python, with the GIL; no runtime thread takes the GIL. An
//! import or a constructor that fails raises its exception to the caller:
//! a generated module that does not match the core is a defect of the
//! build, not an outcome.

use horsies_core::error_catalog::{
    ClientExceptionClass, ErrorClassValues, ErrorEnum, ExceptionValue, RecordValue, SlotData,
    TASK_SEND_PAYLOAD,
};
use horsies_core::TaskSendPayload;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

use crate::core_failures::FailureException;

/// The package of the generated modules.
const CATALOG_PACKAGE: &str = "horsies._catalog";

/// The module and the class of `NoCause()`, the `exception` of a failure
/// that a check of the core found (`horsies._catalog.error_detail`).
const NO_CAUSE_MODULE: &str = "error_detail";
const NO_CAUSE_CLASS: &str = "NoCause";

/// The imported classes of `horsies._catalog`, keyed by `(module, class)`.
static CATALOG_CLASSES: PyOnceLock<Py<PyDict>> = PyOnceLock::new();

/// The module and the class of the client's error of a JSON text that does
/// not read.
const SERIALIZATION_ERROR_MODULE: &str = "horsies.core.codec.json_io";
const SERIALIZATION_ERROR_CLASS: &str = "SerializationError";

/// `SerializationError` of [`SERIALIZATION_ERROR_MODULE`], imported at its
/// first use and kept.
static SERIALIZATION_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

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
        <E as ErrorEnum>::ERROR_CATALOG.module,
        ErrorEnum::type_name(error),
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
        SlotData::Int(number) => number.into_bound_py_any(py),
        SlotData::TaskSendPayload(payload) => payload_object(py, payload),
        SlotData::Uuid(id) => id.into_bound_py_any(py),
        SlotData::UuidText(id) => id.to_string().into_bound_py_any(py),
        SlotData::CodeMember { catalog, value } => {
            catalog_class(py, catalog.module, catalog.type_name)?.call1((value,))
        }
        SlotData::Exception(exception) => exception_object(py, subject, exception),
    }
}

/// The generated record `TaskSendPayload`, called with the payload's fields
/// by name ([`TaskSendPayload::record_values`]).
///
/// Failure exits: those of the class import, and the exception of a
/// constructor that refuses the values.
fn payload_object<'py>(py: Python<'py>, payload: &TaskSendPayload) -> PyResult<Bound<'py, PyAny>> {
    let class = catalog_class(py, TASK_SEND_PAYLOAD.module, TASK_SEND_PAYLOAD.type_name)?;
    let fields = PyDict::new(py);
    for field in payload.record_values() {
        fields.set_item(field.name, record_value_object(py, field.value)?)?;
    }
    class.call((), Some(&fields))
}

/// The Python value of one record field: a `str`, an `int`, an aware
/// `datetime` in UTC, or `None` for an absent optional value. A retention
/// class key is its text; `RetentionClassKey` is a `NewType` of `str`.
fn record_value_object<'py>(
    py: Python<'py>,
    value: RecordValue<'_>,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        RecordValue::Str(text) => text.into_bound_py_any(py),
        RecordValue::Int(number) => number.into_bound_py_any(py),
        RecordValue::DateTime(instant) => instant.into_bound_py_any(py),
        RecordValue::OptionalStr(text) => text.into_bound_py_any(py),
        RecordValue::OptionalInt(number) => number.into_bound_py_any(py),
        RecordValue::OptionalDateTime(instant) => instant.into_bound_py_any(py),
        RecordValue::OptionalRetentionClassKey(key) => key.into_bound_py_any(py),
    }
}

/// The exception of the `exception` slot: the core's class of the cause
/// with `subject`, the generated exception class with its message, the
/// client's class with its text, or `NoCause()` for a check of the core.
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
            catalog_class(py, class.module, class.type_name)?.call1((text,))
        }
        ExceptionValue::ClientClass { class, text } => client_class(py, class)?.call1((text,)),
        ExceptionValue::NoCause => catalog_class(py, NO_CAUSE_MODULE, NO_CAUSE_CLASS)?.call0(),
    }
}

/// The Python class of a class that the client defines: the built-in
/// `ValueError`, or `SerializationError`, imported once and kept.
///
/// Failure exits: the `ImportError` or `AttributeError` of a module or a
/// class that does not exist.
fn client_class<'py>(py: Python<'py>, class: ClientExceptionClass) -> PyResult<Bound<'py, PyAny>> {
    match class {
        ClientExceptionClass::ValueError => Ok(py.get_type::<PyValueError>().into_any()),
        ClientExceptionClass::SerializationError => Ok(SERIALIZATION_ERROR
            .import(py, SERIALIZATION_ERROR_MODULE, SERIALIZATION_ERROR_CLASS)?
            .clone()),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
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

    /// The rule of a refusal is the built-in `ValueError` itself, with the
    /// rule as its one argument and its text, as Python's broker gave it.
    #[test]
    fn a_rule_is_a_value_error_with_the_rule_s_text() {
        Python::initialize();
        Python::attach(|py| {
            let rule = exception_object(
                py,
                "enqueue",
                ExceptionValue::ClientClass {
                    class: ClientExceptionClass::ValueError,
                    text: "priority must be between 1 and 100".to_owned(),
                },
            )
            .expect("the exception is built");
            assert!(rule.get_type().is(py.get_type::<PyValueError>().as_any()));
            assert_eq!(
                rule.str().expect("str").to_string(),
                "priority must be between 1 and 100"
            );
        });
    }

    /// A record value is its Python value: text, an integer, an aware
    /// `datetime` in UTC, and `None` for an absent optional value.
    #[test]
    fn a_record_value_is_its_python_value() {
        Python::initialize();
        Python::attach(|py| {
            let text = record_value_object(py, RecordValue::Str("report")).expect("str");
            assert_eq!(text.extract::<String>().expect("a str"), "report");
            let number = record_value_object(py, RecordValue::Int(-7)).expect("int");
            assert_eq!(number.extract::<i64>().expect("an int"), -7);
            let absent = record_value_object(py, RecordValue::OptionalInt(None)).expect("None");
            assert!(absent.is_none());
            let instant: DateTime<Utc> = Utc
                .with_ymd_and_hms(2026, 11, 18, 12, 0, 0)
                .single()
                .expect("a valid instant");
            let sent_at =
                record_value_object(py, RecordValue::DateTime(instant)).expect("datetime");
            assert_eq!(
                sent_at
                    .extract::<DateTime<Utc>>()
                    .expect("an aware datetime"),
                instant
            );
            let forever = record_value_object(py, RecordValue::OptionalRetentionClassKey(None))
                .expect("None");
            assert!(forever.is_none());
        });
    }

    /// An integer slot of 64 bits is a Python `int` with the same value.
    #[test]
    fn an_int_slot_keeps_its_value() {
        Python::initialize();
        Python::attach(|py| {
            let large = slot_object(py, "send", SlotData::Int(i128::from(u64::MAX))).expect("int");
            assert_eq!(large.extract::<u64>().expect("an int"), u64::MAX);
        });
    }
}
