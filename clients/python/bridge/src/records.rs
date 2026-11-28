//! The records that core operations return to Python.
//!
//! An operation returns its success record or a failure record; expected
//! failures are values, not exceptions. The Python client maps them into its
//! public result types.

use horsies_core::DatabaseOperationError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::catalog_errors::catalog_error;

/// An expected failure of a broker operation: the core's value, with the
/// operation that failed. Python reads it as the generated class of `error`
/// (`horsies._catalog.broker_errors`), with `subject` as the subject of the
/// core exception class in its `exception` slot; a check of the core gives
/// `NoCause()` there.
#[derive(Debug)]
pub struct BrokerFailure {
    /// The operation that failed, for example `ping_database`.
    pub subject: &'static str,
    pub error: DatabaseOperationError,
}

impl BrokerFailure {
    /// The failure of the operation `subject` that a broker call, or a core
    /// constructor of the operation's code, answered with `error`. Pure.
    pub fn of_operation_error(subject: &'static str, error: DatabaseOperationError) -> Self {
        Self { subject, error }
    }
}

impl<'py> IntoPyObject<'py> for BrokerFailure {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        catalog_error(py, self.subject, &self.error)
    }
}

/// The round trip of one database ping.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct DatabasePingRecord {
    pub latency_ms: f64,
}

/// The close of a handle ended: its core pools are closed, or the handle
/// never connected. Writes in flight that did not end within the close bound
/// are logged with their count; their outcome is unknown. The next operation
/// on the handle opens new core pools.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct HandleClosed {}

/// The outcome of one operation: its record, or its failure.
pub enum Outcome<T> {
    Done(T),
    Failed(BrokerFailure),
}

impl<'py, T> IntoPyObject<'py> for Outcome<T>
where
    T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Done(record) => record.into_bound_py_any(py),
            Self::Failed(failure) => failure.into_pyobject(py),
        }
    }
}
