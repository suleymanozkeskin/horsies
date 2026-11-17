//! The records that core operations return to Python.
//!
//! An operation returns its success record or a failure record; expected
//! failures are values, not exceptions. The Python client maps them into its
//! public result types.

use horsies_core::{BrokerError, BrokerErrorCode, BrokerOperationError};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

/// An expected failure of a broker operation: the catalog code, the cause
/// and whether a repeat of the same call can succeed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerFailure {
    /// A member of `BrokerErrorCode`, as its wire value.
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl BrokerFailure {
    /// The failure `code` of an operation that met `error`: a failed
    /// connect, or a failure of a broker call that returns `BrokerError`.
    pub fn from_broker_error(code: BrokerErrorCode, error: &BrokerError) -> Self {
        Self {
            code: code.to_string(),
            message: error.to_string(),
            retryable: error.is_retryable(),
        }
    }
}

impl From<BrokerOperationError> for BrokerFailure {
    fn from(error: BrokerOperationError) -> Self {
        Self {
            code: error.code.to_string(),
            message: error.message,
            retryable: error.retryable,
        }
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
            Self::Failed(failure) => failure.into_bound_py_any(py),
        }
    }
}
