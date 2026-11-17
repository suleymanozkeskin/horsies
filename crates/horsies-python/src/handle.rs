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
use horsies_core::{
    BrokerError, BrokerErrorCode, ClientFamily, PostgresBroker, SchemaInitializationMode,
};
use pyo3::prelude::*;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::bridge::blocking::call_blocking;
use crate::bridge::completion_queue::call_async;
use crate::bridge::CallKind;
use crate::errors::{refusal_error, ManifestRefused};
use crate::operations::enqueue::{enqueue_operation, EnqueueRequest};
use crate::operations::release::{release_claims_operation, ReleaseClaimsRequest};
use crate::operations::schema::{install_operation, schema_status_operation};
use crate::operations::stale_claims::requeue_stale_claimed_operation;
use crate::operations::task_info::{task_info_operation, TaskInfoParts};
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

    /// The schema mode of the handle's manifest.
    pub fn schema_mode(&self) -> SchemaMode {
        self.manifest.schema
    }
}

async fn connect(manifest: &ProducerManifest) -> Result<Arc<PostgresBroker>, BrokerError> {
    let config = manifest
        .database
        .to_postgres_config(manifest.send.retain_rerun_input_default);
    // `AssumeInitialized` runs no schema step, as `ObserveOnly`. Its other
    // rule, no listener, is for the listener operations, which the bridge
    // does not expose yet.
    let mode = match manifest.schema {
        SchemaMode::MigrateAndValidate => SchemaInitializationMode::MigrateAndValidate,
        SchemaMode::ObserveOnly | SchemaMode::AssumeInitialized => {
            SchemaInitializationMode::ObserveOnly
        }
    };
    let window = manifest.send.idempotency_reservation_window.as_duration();
    PostgresBroker::connect_with_policies(&config, mode, window)
        .await
        .map(|broker| Arc::new(broker.with_client_family(ClientFamily::Python)))
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

    /// Apply the embedded migrations and check the schema version, once per
    /// handle: `SchemaInstalled`, `SchemaInstallNotRun` (an `OBSERVE_ONLY` or
    /// `ASSUME_INITIALIZED` handle), or `BrokerFailure` with `SCHEMA_INIT_FAILED`. Writes.
    fn install_schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.install_schema",
            CallKind::Write,
            install_operation(self.connection()),
        )
    }

    /// The awaitable form of `install_schema`.
    fn install_schema_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.install_schema_async",
            CallKind::Write,
            install_operation(self.connection()),
        )
    }

    /// The schema state of the database: `SchemaStatusRecord`. Reads only.
    fn schema_status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.schema_status",
            CallKind::Read,
            schema_status_operation(self.connection()),
        )
    }

    /// The awaitable form of `schema_status`.
    fn schema_status_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.schema_status_async",
            CallKind::Read,
            schema_status_operation(self.connection()),
        )
    }

    /// Enqueue one task: `Enqueued` or one of the send failure classes.
    /// Writes.
    fn enqueue(&self, py: Python<'_>, request: &Bound<'_, EnqueueRequest>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.enqueue",
            CallKind::Write,
            enqueue_operation(self.connection(), request.get().command()),
        )
    }

    /// The awaitable form of `enqueue`.
    fn enqueue_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, EnqueueRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.enqueue_async",
            CallKind::Write,
            enqueue_operation(self.connection(), request.get().command()),
        )
    }

    /// One stale CLAIMED requeue pass with the threshold `stale_after_ms`:
    /// `StaleClaimsDrained`, `StaleClaimsBoundReached`, or
    /// `StaleClaimsRequeueFailed`. Writes; each batch commits alone.
    /// Awaitable only: its caller, `PostgresBroker.requeue_stale_claimed`,
    /// runs on an event loop. A threshold that does not fit in 64 bits
    /// raises `OverflowError`.
    fn requeue_stale_claimed_async<'py>(
        &self,
        py: Python<'py>,
        stale_after_ms: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.requeue_stale_claimed_async",
            CallKind::Write,
            requeue_stale_claimed_operation(self.connection(), stale_after_ms),
        )
    }

    /// Release the claims of one request: `ClaimsReleased` or
    /// `ReleaseFailed`. Writes, in one statement. Awaitable only: its
    /// callers run on the worker's event loop.
    fn release_claims_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, ReleaseClaimsRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.release_claims_async",
            CallKind::Write,
            release_claims_operation(self.connection(), request.get().command()),
        )
    }

    /// The info of one task: `TaskInfoRecord`, `TaskInfoNotFound`, or
    /// `BrokerFailure` with `TASK_INFO_QUERY_FAILED`. Reads only.
    #[pyo3(signature = (task_id, *, include_result, include_failed_reason, include_attempts))]
    fn task_info(
        &self,
        py: Python<'_>,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> PyResult<Py<PyAny>> {
        let parts = TaskInfoParts {
            result: include_result,
            failed_reason: include_failed_reason,
            attempts: include_attempts,
        };
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.task_info",
            CallKind::Read,
            task_info_operation(self.connection(), task_id, parts),
        )
    }

    /// The awaitable form of `task_info`.
    #[pyo3(signature = (task_id, *, include_result, include_failed_reason, include_attempts))]
    fn task_info_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let parts = TaskInfoParts {
            result: include_result,
            failed_reason: include_failed_reason,
            attempts: include_attempts,
        };
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.task_info_async",
            CallKind::Read,
            task_info_operation(self.connection(), task_id, parts),
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
                return Outcome::Failed(BrokerFailure::from_broker_error(
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
