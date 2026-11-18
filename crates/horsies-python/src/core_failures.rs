//! The exception classes of a failed core operation, and the conversion of
//! a core failure into one of them.
//!
//! A failure record of an operation (for example `ReleaseFailed`) carries
//! the cause of its failure in `exception`: an instance of a leaf class
//! below. The class names the cause (`horsies_core::FailureCause`):
//!
//! ```text
//! CoreFailure (Exception)            subject, message
//! ├── CoreDatabaseError              retryable
//! │   ├── CoreServerError            sqlstate: the server answered with an error
//! │   └── CoreConnectionError        no answer from the server
//! ├── CoreContractViolation          a row or a stored value broke the read contract
//! └── CorePreconditionFailed         retryable: a precondition of the operation does not hold
//! ```
//!
//! `CoreFailure` and `CoreDatabaseError` have no constructor. Each leaf class
//! can be built from Python with its fields, so a test can build the value
//! that a record carries. `retryable` is the core's rule for the cause; a
//! contract violation never clears, so its class has no such field.
//!
//! A record keeps the facts of its exception (`FailureException`) and builds
//! the instance when Python reads the attribute, on the reading thread with
//! the GIL: no runtime thread takes the GIL. Each read builds a new
//! instance.

use horsies_core::monitoring::MonitoringQueryError;
use horsies_core::{
    BatchWriteError, BrokerError, BrokerOperationError, ClientStartError, CoveragePassError,
    FailureCause, PrunePassError, WorkflowError,
};
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

/// The number of characters of a SQLSTATE.
const SQLSTATE_LENGTH: usize = 5;

/// Base of the causes of a failed core operation. No constructor.
#[pyclass(module = "horsies._core", extends = PyException, subclass, frozen)]
pub struct CoreFailure {
    /// The core operation that failed, for example `release_claims`.
    #[pyo3(get)]
    subject: String,
    /// What failed and why.
    #[pyo3(get)]
    message: String,
}

#[pymethods]
impl CoreFailure {
    fn __str__(&self) -> String {
        format!("{}: {}", self.subject, self.message)
    }
}

/// Base of the database failures: the server answered with an error, or
/// did not answer. No constructor.
#[pyclass(module = "horsies._core", extends = CoreFailure, subclass, frozen)]
pub struct CoreDatabaseError {
    /// Whether the cause may clear, so a repeat of the operation can
    /// succeed.
    #[pyo3(get)]
    retryable: bool,
}

/// The server answered with an error. `message` is the server's primary
/// message.
#[pyclass(module = "horsies._core", extends = CoreDatabaseError, frozen)]
pub struct CoreServerError {
    /// The five-character SQLSTATE of the server's error.
    #[pyo3(get)]
    sqlstate: String,
}

#[pymethods]
impl CoreServerError {
    /// Raises `ValueError` when `sqlstate` is not five ASCII letters or
    /// digits.
    #[new]
    #[pyo3(signature = (subject, sqlstate, message, retryable))]
    fn new(
        subject: String,
        sqlstate: String,
        message: String,
        retryable: bool,
    ) -> PyResult<PyClassInitializer<Self>> {
        checked_sqlstate(&sqlstate).map_err(PyValueError::new_err)?;
        Ok(PyClassInitializer::from(CoreFailure { subject, message })
            .add_subclass(CoreDatabaseError { retryable })
            .add_subclass(Self { sqlstate }))
    }
}

/// The operation got no answer from the server: a connect or a checkout
/// that failed or timed out, a lost connection, or a reply that did not
/// come within its bound.
#[pyclass(module = "horsies._core", extends = CoreDatabaseError, frozen)]
pub struct CoreConnectionError {}

#[pymethods]
impl CoreConnectionError {
    #[new]
    #[pyo3(signature = (subject, message, retryable))]
    fn new(subject: String, message: String, retryable: bool) -> PyClassInitializer<Self> {
        PyClassInitializer::from(CoreFailure { subject, message })
            .add_subclass(CoreDatabaseError { retryable })
            .add_subclass(Self {})
    }
}

/// A row, a stored value or a result broke the contract that the core reads
/// it with. Not retryable: the same call meets it again.
#[pyclass(module = "horsies._core", extends = CoreFailure, frozen)]
pub struct CoreContractViolation {}

#[pymethods]
impl CoreContractViolation {
    #[new]
    #[pyo3(signature = (subject, message))]
    fn new(subject: String, message: String) -> PyClassInitializer<Self> {
        PyClassInitializer::from(CoreFailure { subject, message }).add_subclass(Self {})
    }
}

/// A precondition of the operation does not hold: the settings, the
/// database's client family or schema, an argument, a row that the call
/// names, a key or a lock that another command holds, a usable transaction,
/// or a client that can build a child spec now.
#[pyclass(module = "horsies._core", extends = CoreFailure, frozen)]
pub struct CorePreconditionFailed {
    /// Whether the precondition may come to hold without a change by the
    /// caller, for example a lock that its holder releases.
    #[pyo3(get)]
    retryable: bool,
}

#[pymethods]
impl CorePreconditionFailed {
    #[new]
    #[pyo3(signature = (subject, message, retryable))]
    fn new(subject: String, message: String, retryable: bool) -> PyClassInitializer<Self> {
        PyClassInitializer::from(CoreFailure { subject, message }).add_subclass(Self { retryable })
    }
}

/// `sqlstate` is five ASCII letters or digits. Pure.
fn checked_sqlstate(sqlstate: &str) -> Result<(), String> {
    let well_formed = sqlstate.len() == SQLSTATE_LENGTH
        && sqlstate.bytes().all(|byte| byte.is_ascii_alphanumeric());
    match well_formed {
        true => Ok(()),
        false => Err(format!(
            "sqlstate must be {SQLSTATE_LENGTH} ASCII letters or digits, not {sqlstate:?}"
        )),
    }
}

/// A core error that a failure record carries as its exception: its text,
/// its cause and its retry rule.
pub trait CoreFailureSource: std::fmt::Display {
    fn exception_cause(&self) -> FailureCause;
    fn exception_retryable(&self) -> bool;
}

impl CoreFailureSource for BrokerError {
    fn exception_cause(&self) -> FailureCause {
        self.failure_cause()
    }
    fn exception_retryable(&self) -> bool {
        self.is_retryable()
    }
}

impl CoreFailureSource for WorkflowError {
    fn exception_cause(&self) -> FailureCause {
        self.failure_cause()
    }
    fn exception_retryable(&self) -> bool {
        self.is_retryable()
    }
}

impl CoreFailureSource for ClientStartError {
    fn exception_cause(&self) -> FailureCause {
        self.failure_cause()
    }
    fn exception_retryable(&self) -> bool {
        self.is_retryable()
    }
}

impl CoreFailureSource for CoveragePassError {
    fn exception_cause(&self) -> FailureCause {
        self.failure_cause()
    }
    fn exception_retryable(&self) -> bool {
        self.is_retryable()
    }
}

impl CoreFailureSource for PrunePassError {
    fn exception_cause(&self) -> FailureCause {
        self.failure_cause()
    }
    fn exception_retryable(&self) -> bool {
        self.is_retryable()
    }
}

impl CoreFailureSource for MonitoringQueryError {
    fn exception_cause(&self) -> FailureCause {
        self.failure_cause()
    }
    fn exception_retryable(&self) -> bool {
        self.retryable()
    }
}

impl CoreFailureSource for BatchWriteError {
    fn exception_cause(&self) -> FailureCause {
        self.failure_cause()
    }
    fn exception_retryable(&self) -> bool {
        matches!(self.transience(), horsies_core::db::Transience::Transient)
    }
}

/// The facts of the exception of a failure record. Python reads it as an
/// instance of the leaf class of `cause`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureException {
    subject: &'static str,
    message: String,
    retryable: bool,
    cause: FailureCause,
}

impl FailureException {
    /// The exception of a failure of the operation `subject`. Pure.
    pub fn new(
        subject: &'static str,
        message: String,
        retryable: bool,
        cause: FailureCause,
    ) -> Self {
        Self {
            subject,
            message,
            retryable,
            cause,
        }
    }

    /// The exception of a core error of the operation `subject`. Pure.
    pub fn of(subject: &'static str, error: &impl CoreFailureSource) -> Self {
        Self::new(
            subject,
            error.to_string(),
            error.exception_retryable(),
            error.exception_cause(),
        )
    }

    /// The exception of a broker operation error of `subject`: its message,
    /// its retry rule and its cause. Pure.
    pub fn of_operation_error(subject: &'static str, error: &BrokerOperationError) -> Self {
        Self::new(
            subject,
            error.message().to_owned(),
            error.retryable(),
            error.failure_cause(),
        )
    }
}

impl<'py> IntoPyObject<'py> for FailureException {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    /// Build the instance through its Python constructor, so `args` holds
    /// the fields and the instance pickles. A server error carries the
    /// server's primary message.
    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self.cause {
            FailureCause::Server(facts) => py.get_type::<CoreServerError>().call1((
                self.subject,
                facts.sqlstate().code(),
                facts.primary_message(),
                self.retryable,
            )),
            FailureCause::Connection => py.get_type::<CoreConnectionError>().call1((
                self.subject,
                self.message,
                self.retryable,
            )),
            FailureCause::ContractViolation => py
                .get_type::<CoreContractViolation>()
                .call1((self.subject, self.message)),
            FailureCause::PreconditionFailed => py.get_type::<CorePreconditionFailed>().call1((
                self.subject,
                self.message,
                self.retryable,
            )),
        }
    }
}

/// Add the exception classes to the module.
pub fn add_failure_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CoreFailure>()?;
    module.add_class::<CoreDatabaseError>()?;
    module.add_class::<CoreServerError>()?;
    module.add_class::<CoreConnectionError>()?;
    module.add_class::<CoreContractViolation>()?;
    module.add_class::<CorePreconditionFailed>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use horsies_core::ServerErrorFacts;

    use super::*;

    #[test]
    fn a_sqlstate_is_five_ascii_letters_or_digits() {
        assert_eq!(checked_sqlstate("55P03"), Ok(()));
        assert_eq!(checked_sqlstate("23505"), Ok(()));
        for refused in ["", "5503", "55P031", "55 03", "55é3"] {
            assert!(checked_sqlstate(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn a_broker_error_keeps_its_text_its_retry_rule_and_its_cause() {
        let error = BrokerError::ClaimContract("ordinality 3 has no answer".to_owned());
        let exception = FailureException::of("release_claims", &error);
        assert_eq!(
            exception,
            FailureException::new(
                "release_claims",
                error.to_string(),
                false,
                FailureCause::ContractViolation,
            )
        );
    }

    #[test]
    fn an_operation_error_keeps_its_message_not_its_display() {
        let error = BrokerOperationError::OperationFailed {
            code: horsies_core::BrokerOperationFailedCode::TaskInfoQueryFailed,
            message: "read failed".to_owned(),
            retryable: true,
            cause: FailureCause::Server(ServerErrorFacts::of_code(
                "57014",
                "canceling statement due to statement timeout".to_owned(),
            )),
        };
        let exception = FailureException::of_operation_error("task_info", &error);
        assert_eq!(exception.message, "read failed");
        assert!(exception.retryable);
        assert_eq!(exception.cause, error.failure_cause());
    }
}
