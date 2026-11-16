//! `CoreHandle`: the handle that the Python client builds from a producer
//! manifest.
//!
//! Building a handle parses the manifest and starts nothing: no runtime, no
//! connection. The first operation starts the core of the process and
//! connects the handle's broker; a failed connect is not kept, so the next
//! operation connects again.

use std::future::Future;
use std::sync::Arc;

use horsies_core::manifest::{parse_producer_manifest, ProducerManifest, SchemaMode};
use horsies_core::{BrokerError, BrokerErrorCode, PostgresBroker, SchemaInitializationMode};
use pyo3::prelude::*;
use tokio::sync::OnceCell;

use crate::bridge::blocking::call_blocking;
use crate::bridge::completion_queue::call_async;
use crate::bridge::CallKind;
use crate::errors::{refusal_error, ManifestRefused};
use crate::process_core::{core_for_call, Core};
use crate::records::{BrokerFailure, DatabasePingRecord, Outcome};

/// The manifest of a handle and its broker, connected at first use.
#[derive(Clone)]
pub struct BrokerConnection {
    manifest: Arc<ProducerManifest>,
    broker: Arc<OnceCell<Arc<PostgresBroker>>>,
}

impl BrokerConnection {
    /// The connected broker. Connects at the first call; a failure is
    /// returned and not kept.
    pub async fn broker(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        self.broker
            .get_or_try_init(|| connect(&self.manifest))
            .await
            .map(Arc::clone)
    }
}

async fn connect(manifest: &ProducerManifest) -> Result<Arc<PostgresBroker>, BrokerError> {
    let config = manifest
        .database
        .to_postgres_config(manifest.send.retain_rerun_input_default);
    let mode = match manifest.schema {
        SchemaMode::MigrateAndValidate => SchemaInitializationMode::MigrateAndValidate,
        SchemaMode::ObserveOnly => SchemaInitializationMode::ObserveOnly,
    };
    let window = manifest.send.idempotency_reservation_window.as_duration();
    PostgresBroker::connect_with_policies(&config, mode, window)
        .await
        .map(Arc::new)
}

/// The core handle of one producer manifest.
#[pyclass(module = "horsies._core", name = "CoreHandle", frozen)]
pub struct CoreHandle {
    connection: BrokerConnection,
}

impl CoreHandle {
    /// A clone of the connection, for an operation future.
    pub fn connection(&self) -> BrokerConnection {
        self.connection.clone()
    }
}

#[pymethods]
impl CoreHandle {
    /// Build a handle from the JSON text of a producer manifest. Starts
    /// nothing. A refused manifest raises `ManifestRefused`.
    #[new]
    fn new(manifest_json: &str) -> PyResult<Self> {
        let manifest = parse_producer_manifest(manifest_json)
            .map_err(|refused| ManifestRefused::new_err((refused.field_path, refused.cause)))?;
        Ok(Self {
            connection: BrokerConnection {
                manifest: Arc::new(manifest),
                broker: Arc::new(OnceCell::new()),
            },
        })
    }

    /// The version of the manifest this handle was built from.
    #[getter]
    fn manifest_version(&self) -> u32 {
        self.connection.manifest.manifest_version.get()
    }

    /// Ping the database: `DatabasePingRecord` or `BrokerFailure` with
    /// `DB_PING_FAILED`.
    fn ping_database(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.ping_database",
            CallKind::Read,
            ping_database_operation(self.connection()),
        )
    }

    /// The awaitable form of `ping_database`.
    fn ping_database_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.ping_database_async",
            CallKind::Read,
            ping_database_operation(self.connection()),
        )
    }
}

/// The core of this process for one call, or the refusal as a Python error.
pub fn checked_core(py: Python<'_>) -> PyResult<&'static Core> {
    core_for_call(py).map_err(refusal_error)
}

fn ping_database_operation(
    connection: BrokerConnection,
) -> impl Future<Output = Outcome<DatabasePingRecord>> + Send + 'static {
    async move {
        let broker = match connection.broker().await {
            Ok(broker) => broker,
            Err(error) => {
                return Outcome::Failed(BrokerFailure::from_connect_error(
                    BrokerErrorCode::DbPingFailed,
                    &error,
                ))
            }
        };
        match broker.ping_database().await {
            Ok(ping) => Outcome::Done(DatabasePingRecord {
                latency_ms: ping.latency_ms,
            }),
            Err(error) => Outcome::Failed(BrokerFailure::from(error)),
        }
    }
}
