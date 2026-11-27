//! Schema install and schema status.
//!
//! - Install applies the embedded migrations once per handle, checks the
//!   version and runs the startup coverage pass of the handle's request
//!   (`PostgresBroker::ensure_schema_initialized`); a handle built with
//!   `OBSERVE_ONLY` runs nothing.
//! - Status is the web schema gate's read: the version that the last
//!   successful migration reached, against the version this core expects. A
//!   database that cannot be read answers `UNKNOWN`, as the gate does.

use horsies_core::manifest::SchemaMode;
use horsies_core::web::{SchemaProbe, SchemaState, SchemaStatus};
use horsies_core::{expected_schema_version, DatabaseOperationError};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// The subject of the exception of a failed install.
const INSTALL_SUBJECT: &str = "install_schema";

/// The migrations ran, and the database is at `version`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SchemaInstalled {
    pub version: i64,
}

/// The handle was built with the `OBSERVE_ONLY` or the `ASSUME_INITIALIZED`
/// schema mode, so the install ran nothing.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct SchemaInstallNotRun {}

/// The schema state of the database.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SchemaStatusRecord {
    /// `MATCH`, `MISMATCH`, `ABSENT`, `ZERO_X` or `UNKNOWN`.
    pub state: String,
    /// The stored version, when the database has one.
    pub version: Option<i64>,
    /// The version this core expects.
    pub expected_version: i64,
}

impl From<SchemaStatus> for SchemaStatusRecord {
    fn from(status: SchemaStatus) -> Self {
        let state = match status.state {
            SchemaState::Match => "MATCH",
            SchemaState::Mismatch => "MISMATCH",
            SchemaState::Absent => "ABSENT",
            SchemaState::ZeroX => "ZERO_X",
            SchemaState::Unknown => "UNKNOWN",
        };
        Self {
            state: state.to_owned(),
            version: status.version,
            expected_version: status.expected_version,
        }
    }
}

/// The outcome of one install.
pub enum InstallOutcome {
    Installed(SchemaInstalled),
    NotRun(SchemaInstallNotRun),
    Failed(BrokerFailure),
}

impl<'py> IntoPyObject<'py> for InstallOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Installed(installed) => installed.into_bound_py_any(py),
            Self::NotRun(not_run) => not_run.into_bound_py_any(py),
            Self::Failed(failure) => failure.into_bound_py_any(py),
        }
    }
}

/// Install the schema through the handle's broker: the migrations, then the
/// startup coverage pass. Writes. The broker keeps a success, so a second
/// install on the same handle runs nothing.
pub async fn install_operation(connection: BrokerConnection) -> InstallOutcome {
    match connection.schema_mode() {
        SchemaMode::ObserveOnly | SchemaMode::AssumeInitialized => {
            return InstallOutcome::NotRun(SchemaInstallNotRun {});
        }
        SchemaMode::MigrateAndValidate => {}
    }
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return InstallOutcome::Failed(BrokerFailure::of_operation_error(
                INSTALL_SUBJECT,
                DatabaseOperationError::schema_init_failed(error),
            ))
        }
    };
    match broker.ensure_schema_initialized().await {
        Ok(()) => InstallOutcome::Installed(SchemaInstalled {
            version: expected_schema_version(),
        }),
        Err(error) => InstallOutcome::Failed(BrokerFailure::of_operation_error(
            INSTALL_SUBJECT,
            DatabaseOperationError::schema_init_failed(error),
        )),
    }
}

/// Read the schema state. Reads only; never fails: a database that cannot
/// be reached or read is `UNKNOWN`, and the cause is logged.
pub async fn schema_status_operation(connection: BrokerConnection) -> SchemaStatusRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            tracing::warn!(error = %error, "schema status: the broker could not connect");
            return SchemaStatusRecord::from(SchemaStatus {
                state: SchemaState::Unknown,
                version: None,
                expected_version: expected_schema_version(),
            });
        }
    };
    SchemaStatusRecord::from(SchemaProbe::new(broker).status().await)
}
