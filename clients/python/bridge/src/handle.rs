//! `CoreHandle`: the handle that the Python client builds from a producer
//! manifest, with the pool profile of its process: a producer, or a worker
//! process.
//!
//! Building a handle parses the manifest and starts nothing: no runtime, no
//! connection. The first operation starts the core of the process and
//! connects the handle's broker; a failed connect is not kept, so the next
//! operation connects again. The lazy handle is the core's
//! ([`horsies_core::handle::CoreHandle`]), with the client family
//! `PYTHON`.

use std::sync::{Arc, OnceLock};

use horsies_core::handle::{CoreHandle as LazyHandle, HandleConnection};
use horsies_core::manifest::{parse_producer_manifest, ProducerManifest, SchemaMode};
use horsies_core::worker_manifest::parse_worker_profiles;
use horsies_core::{
    BrokerError, BrokerOperationError, ClientFamily, CoverageRequest, PoolProfile, PostgresBroker,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::bridge::blocking::{call_blocking, call_blocking_within};
use crate::bridge::completion_queue::{call_async, done_future};
use crate::bridge::CallKind;
use crate::child_spec::{PythonChildSpecSource, RaisedExceptions, RegisteredSource};
use crate::errors::{refusal_error, ManifestRefused};
use crate::operations::cancel::cancel_task_operation;
use crate::operations::claim::{claim_pass_operation, ClaimPassRequest};
use crate::operations::claim_settle::{settle_claimed_workflow_tasks_operation, SettleRequest};
use crate::operations::connectivity::check_connectivity_operation;
use crate::operations::coverage::{
    ensure_partition_coverage_operation, CoverageRequestArgument, InstallCoverageArgument,
};
use crate::operations::enqueue::{enqueue_operation, EnqueueRequest};
use crate::operations::expiry_and_orphans::{
    audit_orphaned_workflow_tasks_operation, expire_pending_tasks_operation,
};
use crate::operations::failed_attempt::{
    finish_ended_attempt_operation, finish_failed_attempt_operation, FinishFailedAttemptRequest,
};
use crate::operations::monitoring_reads::{
    list_tasks_operation, schedules_operation, task_breakdown_operation, task_detail_operation,
    task_facets_operation, task_stats_operation, workflow_names_operation, workflow_node_operation,
    workflow_run_operation, workflow_runs_operation, TaskBreakdownQuery, TaskFacetsQuery,
    TaskListQuery, TaskStatsQuery, WorkflowRunsQuery,
};
use crate::operations::paused_expiry::expire_paused_workflows_operation;
use crate::operations::pruning::prune_expired_partitions_operation;
use crate::operations::queue_starts::{count_queue_starts_operation, QueueStartsRequest};
use crate::operations::release::{release_claims_operation, ReleaseClaimsRequest};
use crate::operations::result::{raw_result_record_operation, result_wait_bound};
use crate::operations::schema::{install_operation, schema_status_operation};
use crate::operations::stale_claims::requeue_stale_claimed_operation;
use crate::operations::stale_running::mark_stale_running_operation;
use crate::operations::success::{
    complete_successes_operation, finish_workflow_success_operation, workflow_success_command,
    SuccessBatchRequest,
};
use crate::operations::task_info::{task_info_operation, TaskInfoParts};
use crate::operations::task_lists::{
    expired_tasks_operation, stale_tasks_operation, stale_threshold,
};
use crate::operations::task_workflow_node::task_workflow_node_operation;
use crate::operations::worker_connections::check_worker_connections_operation;
use crate::operations::worker_ping::{ping_request, ping_wait_bound, ping_workers_operation};
use crate::operations::worker_reads::{
    class_coverage_operation, class_report_operation, history_limit,
    worker_state_history_operation, worker_state_operation, worker_states_operation,
};
use crate::operations::workflow_lifecycle::{cancel_workflow_operation, pause_workflow_operation};
use crate::operations::workflow_phase2::{
    advance_workflow_parents_operation, finish_workflow_node_operation, NodeFinishRequest,
};
use crate::operations::workflow_phase2_recovery::recover_workflow_phase2_operation;
use crate::operations::workflow_reads::{
    wait_workflow_result_operation, workflow_node_result_operation,
    workflow_node_results_operation, workflow_nodes_operation, workflow_status_operation,
};
use crate::operations::workflow_recovery::recover_workflows_operation;
use crate::operations::workflow_resume::resume_workflow_operation;
use crate::operations::workflow_retention::delete_expired_workflows_operation;
use crate::operations::workflow_start::{start_workflow_operation, WorkflowStartRequest};
use crate::process_core::{core_for_call, Core};
use crate::records::{BrokerFailure, DatabasePingRecord, HandleClosed, Outcome};

/// The core's lazy handle of a Python process: a manifest, the pool
/// profile of the process, the client family `PYTHON`, and the broker,
/// connected at first use. Clones share one handle.
#[derive(Clone)]
pub struct BrokerConnection {
    handle: Arc<LazyHandle>,
}

impl BrokerConnection {
    /// The connection of `manifest` with the pools of `profile`. Starts
    /// nothing: the first operation connects.
    pub fn new(manifest: ProducerManifest, profile: PoolProfile) -> Self {
        Self {
            handle: Arc::new(LazyHandle::new(manifest, profile, ClientFamily::Python)),
        }
    }

    /// A new connection of the same manifest and pool profile whose schema
    /// install ends with the coverage pass of `request`
    /// ([`LazyHandle::with_install_coverage`]). It shares no broker with this
    /// one. Starts nothing.
    pub fn with_install_coverage(&self, request: CoverageRequest) -> Self {
        Self {
            handle: Arc::new(
                LazyHandle::new(
                    self.handle.manifest().clone(),
                    self.handle.profile(),
                    ClientFamily::Python,
                )
                .with_install_coverage(request),
            ),
        }
    }

    /// The connected broker. Connects at the first call; a failure is
    /// returned and not kept ([`LazyHandle::broker`]).
    pub async fn broker(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        self.handle.broker().await
    }

    /// The connected broker, with the schema installed once per handle
    /// first ([`LazyHandle::initialized_broker`]).
    pub async fn initialized_broker(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        self.handle.initialized_broker().await
    }

    /// The schema mode of the handle's manifest.
    pub fn schema_mode(&self) -> SchemaMode {
        self.handle.schema_mode()
    }

    /// The rerun-input posture of a send that does not choose one: the
    /// manifest's `retain_rerun_input_default`, which the handle's broker is
    /// built with. Starts nothing.
    pub fn retain_rerun_input_default(&self) -> bool {
        self.handle.manifest().send.retain_rerun_input_default
    }

    /// The manifest of the handle.
    fn manifest(&self) -> &ProducerManifest {
        self.handle.manifest()
    }

    /// The core's lazy handle, for a core function that connects only when
    /// its rule needs a read.
    pub fn lazy_handle(&self) -> &LazyHandle {
        &self.handle
    }

    /// The core's lazy handle, shared, for a service run that outlives the
    /// call (the fleet metrics server).
    pub fn shared_handle(&self) -> Arc<LazyHandle> {
        Arc::clone(&self.handle)
    }

    /// Whether a connect succeeded on this handle. Starts nothing and waits
    /// for nothing: a connect still in flight is `NeverConnected`, and the
    /// core of the process may not have started then.
    fn connect_state(&self) -> HandleConnection {
        self.handle.connection()
    }
}

/// The pool profile of a `CoreHandle`'s process: `PRODUCER` or `WORKER`.
/// Pure.
fn handle_profile(text: &str) -> Result<PoolProfile, String> {
    match text {
        "PRODUCER" => Ok(PoolProfile::Producer),
        "WORKER" => Ok(PoolProfile::Worker),
        other => Err(format!(
            "pool_profile must be 'PRODUCER' or 'WORKER', not {other:?}"
        )),
    }
}

/// The core handle of one producer manifest and the pool profile of its
/// process.
#[pyclass(module = "horsies._core", name = "CoreHandle", frozen)]
pub struct CoreHandle {
    connection: BrokerConnection,
    /// The child spec source of the handle's workflow operations, set once
    /// by `register_child_spec_source`.
    child_spec_source: OnceLock<RegisteredSource>,
}

impl CoreHandle {
    /// A clone of the connection, for an operation future.
    pub fn connection(&self) -> BrokerConnection {
        self.connection.clone()
    }

    /// The registered child spec source, for a monitoring handler that
    /// builds its own source at its first call; None when none is
    /// registered.
    pub fn registered_child_spec_source(&self) -> Option<RegisteredSource> {
        self.child_spec_source.get().map(Arc::clone)
    }

    /// The child spec source of one operation on `core`; an `Exception` of a
    /// build goes where `raised_exceptions` says.
    ///
    /// Failure exits: `RuntimeError` when no source is registered (a defect
    /// of the client: the handle's workflow operations build children).
    fn child_spec_source(
        &self,
        core: &'static Core,
        raised_exceptions: RaisedExceptions,
    ) -> PyResult<PythonChildSpecSource> {
        match self.child_spec_source.get() {
            Some(callable) => Ok(PythonChildSpecSource::new(
                core,
                Arc::clone(callable),
                raised_exceptions,
            )),
            None => Err(PyRuntimeError::new_err(
                "CoreHandle has no child spec source; call register_child_spec_source first",
            )),
        }
    }
}

#[pymethods]
impl CoreHandle {
    /// Build a handle from the JSON text of a producer manifest and the pool
    /// profile of its process: `PRODUCER` (a producer or a monitoring
    /// process: no reserve) or `WORKER` (a worker process: the main core
    /// pool keeps 2 connections free from batch checkouts, such as the claim
    /// pass, for the per-task writes). Starts nothing. A refused manifest
    /// raises `ManifestRefused`; another profile raises `ValueError`.
    #[new]
    #[pyo3(signature = (manifest_json, *, pool_profile))]
    fn new(manifest_json: &str, pool_profile: &str) -> PyResult<Self> {
        let profile = handle_profile(pool_profile).map_err(PyValueError::new_err)?;
        let manifest = parse_producer_manifest(manifest_json)
            .map_err(|refused| ManifestRefused::new_err((refused.field_path, refused.cause)))?;
        Ok(Self {
            connection: BrokerConnection::new(manifest, profile),
            child_spec_source: OnceLock::new(),
        })
    }

    /// Register the child spec source of the handle's workflow operations:
    /// a callable that takes a `ChildSpecRequest` and returns
    /// `ChildSpecBuilt` or `ChildSpecRefused`. The core calls it on the
    /// callback thread for each sub-workflow child that an operation builds.
    /// A second registration raises `RuntimeError`. Starts nothing.
    fn register_child_spec_source(&self, source: Py<PyAny>) -> PyResult<()> {
        self.child_spec_source
            .set(Arc::new(source))
            .map_err(|_already| {
                PyRuntimeError::new_err("CoreHandle has a child spec source already")
            })
    }

    /// A new handle of the same manifest and pool profile whose schema
    /// install ends with the coverage pass of `install_coverage`: the request
    /// of the client's retention config, so the install covers every class
    /// that its sends can stamp. The new handle shares no pool and no child
    /// spec source with this one. Starts nothing.
    fn with_install_coverage(&self, install_coverage: &Bound<'_, InstallCoverageArgument>) -> Self {
        Self {
            connection: self
                .connection
                .with_install_coverage(install_coverage.get().request()),
            child_spec_source: OnceLock::new(),
        }
    }

    /// The version of the manifest this handle was built from.
    #[getter]
    fn manifest_version(&self) -> u32 {
        self.connection.manifest().manifest_version.get()
    }

    /// Close the handle's core pools: `HandleClosed`. No new operation takes
    /// the closed pools; writes in flight run to their end within the close
    /// bound, and the ones that do not are logged with their count. A handle
    /// that never connected returns at once and starts nothing (a preload
    /// process must not start the core). The next operation on the handle
    /// opens new core pools. Runs on to its end when the caller leaves.
    fn close(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let broker = match self.connection.connect_state() {
            HandleConnection::NeverConnected => return HandleClosed {}.into_py_any(py),
            HandleConnection::Connected(broker) => broker,
        };
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.close",
            CallKind::Write,
            close_operation(broker),
        )
    }

    /// The awaitable form of `close`. A handle that never connected returns
    /// a future that is already done, and starts nothing.
    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let broker = match self.connection.connect_state() {
            HandleConnection::NeverConnected => return done_future(py, HandleClosed {}),
            HandleConnection::Connected(broker) => broker,
        };
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.close_async",
            CallKind::Write,
            close_operation(broker),
        )
    }

    /// Ping the database: `DatabasePingRecord` or the generated broker error class with
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

    /// The connectivity step of the live check
    /// (`PostgresBroker::check_connectivity`, the rule of both clients):
    /// `ConnectivityPassed`, or `ConnectivityRefused` with the first step
    /// that refused. Reads only, plus one NOTIFY of the LISTEN probe in
    /// PgBouncer transaction mode; no schema step. Blocking only: its
    /// caller, `Horsies.check(live=True)`, is sync.
    fn check_connectivity(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.check_connectivity",
            CallKind::Read,
            check_connectivity_operation(self.connection()),
        )
    }

    /// The worker connection step of the live check
    /// (`check_handle_worker_connections`, the rule of both clients): one
    /// `WorkerConnectionError` per profile of `profiles_json` whose worker
    /// needs more connections than the server has usable slots, or one when
    /// the limit was not read; an empty list when every profile fits. No
    /// profile, or PgBouncer transaction mode in the handle's manifest,
    /// connects nothing. Reads only; no schema step. Blocking only: its
    /// caller, `Horsies.check(live=True)`, is sync.
    ///
    /// `profiles_json` is the `profiles` member of a worker manifest. A
    /// refused text raises `ManifestRefused`, a defect of the client.
    fn check_worker_connections(&self, py: Python<'_>, profiles_json: &str) -> PyResult<Py<PyAny>> {
        let profiles = parse_worker_profiles(profiles_json)
            .map_err(|refused| ManifestRefused::new_err((refused.field_path, refused.cause)))?;
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.check_worker_connections",
            CallKind::Read,
            check_worker_connections_operation(self.connection(), profiles),
        )
    }

    /// Apply the embedded migrations, check the schema version, then run the
    /// startup coverage pass, once per handle: `SchemaInstalled`,
    /// `SchemaInstallNotRun` (an `OBSERVE_ONLY` or `ASSUME_INITIALIZED`
    /// handle), or the generated broker error class with `SCHEMA_INIT_FAILED`. Writes.
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

    /// Enqueue one task: `Enqueued`, or the generated broker error class of
    /// the failure (`BrokerCoreEnqueueError`). Writes.
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

    /// One stale RUNNING recovery pass with the thresholds `stale_after_ms`
    /// and `finalizing_stale_after_ms`: `StaleRunningRecovered`,
    /// `StaleRunningScanFull`, or `StaleRunningScanFailed`. Writes; each
    /// candidate's statement commits alone, and a candidate whose statement
    /// fails is logged and skipped. Awaitable only: its caller,
    /// `PostgresBroker.mark_stale_tasks_as_failed`, runs on an event loop. A
    /// threshold that does not fit in 64 bits raises `OverflowError`.
    fn mark_stale_running_async<'py>(
        &self,
        py: Python<'py>,
        stale_after_ms: i64,
        finalizing_stale_after_ms: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.mark_stale_running_async",
            CallKind::Write,
            mark_stale_running_operation(
                self.connection(),
                stale_after_ms,
                finalizing_stale_after_ms,
            ),
        )
    }

    /// Write the failed attempt of a child that reported a result
    /// (`horsies_finish_failed_attempt`, claimed-row rule `REFUSE`):
    /// `AttemptFailureApplied`, `AttemptRetryScheduled`, `AttemptAlreadyApplied`,
    /// `AttemptLostClaim`, `AttemptSourceStateConflict`, `AttemptTaskAbsent`,
    /// or `AttemptWriteFailed`. Writes, in one statement. Awaitable only: its
    /// caller, the worker's finalize, runs on the worker's event loop.
    fn finish_failed_attempt_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, FinishFailedAttemptRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.finish_failed_attempt_async",
            CallKind::Write,
            finish_failed_attempt_operation(self.connection(), request.get().command()),
        )
    }

    /// Write the end of an attempt whose child ended without a result
    /// (claimed-row rule `RELEASE`): `AttemptClaimReleased` for a row that
    /// this claim still held CLAIMED, else the outcomes of
    /// `finish_failed_attempt_async`. Writes, in one statement. Awaitable
    /// only: its caller, the worker's dispatch, runs on the worker's event
    /// loop.
    fn finish_ended_attempt_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, FinishFailedAttemptRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.finish_ended_attempt_async",
            CallKind::Write,
            finish_ended_attempt_operation(self.connection(), request.get().command()),
        )
    }

    /// Write the plain successes of one request in one statement
    /// (`horsies_complete_tasks_fused`): `SuccessesWritten` with one row
    /// record per success in the request order, `BatchNotSent`,
    /// `BatchRefused`, or `BatchCommitUnknown`. Writes. Awaitable only: its caller, the
    /// worker's finalize batcher, runs on the worker's event loop. Rows
    /// outside the function's contract raise `PanicException`.
    fn complete_successes_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, SuccessBatchRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (worker_id, batch) = request.get().parts();
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.complete_successes_async",
            CallKind::Write,
            complete_successes_operation(self.connection(), worker_id, batch),
        )
    }

    /// Write phase 1 of a workflow task's success
    /// (`horsies_finish_workflow_success`): `WorkflowSuccessApplied` with the
    /// phase 2 owed, a refusal (`AttemptAlreadyApplied`, `AttemptLostClaim`,
    /// `AttemptSourceStateConflict`, `AttemptTaskAbsent`), or
    /// `AttemptWriteFailed`. Writes, in one statement. Awaitable only: its
    /// caller, the worker's finalize, runs on the worker's event loop. A
    /// naive `claimed_at` raises `TypeError`.
    #[pyo3(signature = (task_id, *, worker_id, claimed_at, result_json))]
    fn finish_workflow_success_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
        worker_id: String,
        claimed_at: &Bound<'_, PyAny>,
        result_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let command = workflow_success_command(task_id, worker_id, claimed_at, result_json)?;
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.finish_workflow_success_async",
            CallKind::Write,
            finish_workflow_success_operation(self.connection(), command),
        )
    }

    /// One claim pass (`horsies_claim`): `ClaimPassRecord` with the claimed
    /// rows in dispatch order, or `ClaimPassFailed`. Writes, in one
    /// statement; the dispatch stays with the caller. Awaitable only: its
    /// caller, the worker's claim loop, runs on the worker's event loop.
    fn claim_pass_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, ClaimPassRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.claim_pass_async",
            CallKind::Write,
            claim_pass_operation(self.connection(), request.get().request()),
        )
    }

    /// One partition coverage pass on the session-work pool
    /// (`PostgresBroker::ensure_partition_coverage`):
    /// `CoverageEnsuredRecord`, `CoverageEnsureFailedRecord` (a contained
    /// refusal), or `CoverageFailed`. Writes (DDL and catalog rows); each
    /// leaf commits alone, and a cancelled await leaves the pass running to
    /// its end. Awaitable only: its callers, the worker boot and the
    /// reaper, run on the worker's event loop.
    fn ensure_partition_coverage_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, CoverageRequestArgument>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.ensure_partition_coverage_async",
            CallKind::Write,
            ensure_partition_coverage_operation(self.connection(), request.get().pass()),
        )
    }

    /// One pruning pass on the session-work pool
    /// (`PostgresBroker::prune_expired_partitions`): `PrunePassRecord` or
    /// `PruneFailed`. Writes (DDL and catalog rows); each leaf's detach and
    /// drop commit alone, at most 64 acted leaves, and a cancelled await
    /// leaves the pass running to its end. Awaitable only: its caller, the
    /// reaper, runs on the worker's event loop.
    fn prune_expired_partitions_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.prune_expired_partitions_async",
            CallKind::Write,
            prune_expired_partitions_operation(self.connection()),
        )
    }

    /// Count the capped queues of one soft-cap start check: `QueueStartsRecord`
    /// or `QueueStartsFailed`. Reads only, in one statement; a check with no
    /// queue sends nothing. Awaitable only: its caller, the worker's claim
    /// loop, runs on the worker's event loop.
    fn count_queue_starts_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, QueueStartsRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.count_queue_starts_async",
            CallKind::Read,
            count_queue_starts_operation(self.connection(), request.get().check()),
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

    /// Cancel one plain task (`horsies_cancel_task`): `TaskCancelledRecord`,
    /// or a generated `TaskActionCoreError` class (`TaskActionNotFound`,
    /// `TaskActionNotCancellable`, `TaskActionWorkflowTask`,
    /// `TaskActionDbOperationFailed` or `TaskActionStoredDataInvalid`). A
    /// RUNNING task is cancelled only with `include_running`. Writes, in one
    /// statement.
    /// Awaitable only: its caller, `horsies.monitoring.task_actions.cancel_task`,
    /// is a coroutine. An outcome that contradicts the function raises
    /// `PanicException`.
    #[pyo3(signature = (task_id, *, include_running))]
    fn cancel_task_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
        include_running: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.cancel_task_async",
            CallKind::Write,
            cancel_task_operation(self.connection(), task_id, include_running),
        )
    }

    /// One pending expiry pass (`horsies_expire_pending_tasks` in bounded
    /// batches): `PendingExpiryDrained`, `PendingExpiryBoundReached`, or
    /// `PendingExpiryFailed`. Writes; each batch commits alone. Awaitable
    /// only: its caller, `PostgresBroker.expire_pending_tasks`, runs on an
    /// event loop.
    fn expire_pending_tasks_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.expire_pending_tasks_async",
            CallKind::Write,
            expire_pending_tasks_operation(self.connection()),
        )
    }

    /// One page of the orphan sweep (`horsies_cancel_orphaned_tasks` and the
    /// page's cursor counts, in one transaction): `OrphanPageAudited`,
    /// `OrphanAuditLockBusy`, or `OrphanAuditFailed`. Writes. Awaitable only:
    /// its caller, `PostgresBroker.audit_orphaned_workflow_tasks`, runs on an
    /// event loop.
    fn audit_orphaned_workflow_tasks_async<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.audit_orphaned_workflow_tasks_async",
            CallKind::Write,
            audit_orphaned_workflow_tasks_operation(self.connection()),
        )
    }

    /// Pause one workflow tree (`horsies_workflow_pause`): `WorkflowPaused`,
    /// `WorkflowNotRunning`, `WorkflowAbsent`, or a generated handle error
    /// class (`HandleDbOperationFailed`, `HandleStoredDataInvalid`). Writes, in
    /// one statement; a caller that leaves does not stop it. A row outside
    /// the function's contract raises `PanicException`.
    fn pause_workflow(&self, py: Python<'_>, workflow_id: Uuid) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.pause_workflow",
            CallKind::Write,
            pause_workflow_operation(self.connection(), workflow_id),
        )
    }

    /// The awaitable form of `pause_workflow`.
    fn pause_workflow_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.pause_workflow_async",
            CallKind::Write,
            pause_workflow_operation(self.connection(), workflow_id),
        )
    }

    /// Start one workflow (`horsies_workflow_start`): `WorkflowStarted`,
    /// `WorkflowExists`, or the generated `WorkflowStartIdConflict`,
    /// `WorkflowStartCommandConflict` or `WorkflowStartEnqueueFailed`
    /// (`horsies._catalog.workflow_start_errors`). Writes;
    /// a caller that leaves does not stop it. The children of its
    /// sub-workflow roots come from the child spec source, which runs on the
    /// callback thread while this thread waits with the GIL released. A
    /// build that raised (`KeyboardInterrupt`, `SystemExit`, or an
    /// `Exception` such as `SerializationError`) rolls the start back and
    /// raises it again. With no child spec source it raises `RuntimeError`.
    fn start_workflow(
        &self,
        py: Python<'_>,
        request: &Bound<'_, WorkflowStartRequest>,
    ) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::ToCaller)?;
        call_blocking(
            py,
            core,
            "CoreHandle.start_workflow",
            CallKind::Write,
            start_workflow_operation(self.connection(), request.get().parts(), source),
        )
    }

    /// The awaitable form of `start_workflow`.
    fn start_workflow_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'py, WorkflowStartRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::ToCaller)?;
        call_async(
            py,
            core,
            "CoreHandle.start_workflow_async",
            CallKind::Write,
            start_workflow_operation(self.connection(), request.get().parts(), source),
        )
    }

    /// Resume one workflow tree (`horsies_workflow_resume`): `WorkflowResumed`,
    /// `WorkflowNotPaused`, `WorkflowAbsent`, or a generated handle error
    /// class (`HandleDbOperationFailed`, `HandleStoredDataInvalid`,
    /// `HandleResumeRecoveryFailed`). Writes; a caller that leaves does not
    /// stop it. The
    /// child starts it owes build their children through the child spec
    /// source, which runs on the callback thread while this thread waits
    /// with the GIL released. A build that raised `KeyboardInterrupt` or
    /// `SystemExit` raises it again; one that raised an `Exception` raises
    /// it again when the resume failed with it. With no child spec source it
    /// raises `RuntimeError`.
    fn resume_workflow(&self, py: Python<'_>, workflow_id: Uuid) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::ToCaller)?;
        call_blocking(
            py,
            core,
            "CoreHandle.resume_workflow",
            CallKind::Write,
            resume_workflow_operation(self.connection(), workflow_id, source),
        )
    }

    /// The awaitable form of `resume_workflow`.
    fn resume_workflow_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::ToCaller)?;
        call_async(
            py,
            core,
            "CoreHandle.resume_workflow_async",
            CallKind::Write,
            resume_workflow_operation(self.connection(), workflow_id, source),
        )
    }

    /// Phase 2 of one finished workflow task (`horsies_workflow_node_finish`):
    /// `NodeFinished` or `NodeFinishFailed`. Writes; the child starts it owes
    /// build their children through the child spec source. A build that
    /// raised `KeyboardInterrupt` or `SystemExit` raises it again; an
    /// `Exception` of a build is a `NodeFinishFailed` with its type and
    /// text. Awaitable only: its caller, the worker's finalize, is a
    /// coroutine. With no child spec source it raises `RuntimeError`.
    fn finish_workflow_node_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'py, NodeFinishRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::InRecord)?;
        let request = request.get();
        call_async(
            py,
            core,
            "CoreHandle.finish_workflow_node_async",
            CallKind::Write,
            finish_workflow_node_operation(
                self.connection(),
                request.task_id,
                request.form,
                request.wake,
                source,
            ),
        )
    }

    /// Advance the parent nodes above `child_workflow_ids`
    /// (`horsies_workflow_child_finish`, one transaction per level):
    /// `ParentsAdvanced` or `ParentAdvanceStopped`. Writes; the child starts
    /// a level owes build their children through the child spec source. A
    /// build that raised `KeyboardInterrupt` or `SystemExit` raises it again;
    /// an `Exception` of a build is a `ParentAdvanceStopped` with its type
    /// and text. With no child spec source it raises `RuntimeError`.
    fn advance_workflow_parents_async<'py>(
        &self,
        py: Python<'py>,
        child_workflow_ids: Vec<Uuid>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::InRecord)?;
        call_async(
            py,
            core,
            "CoreHandle.advance_workflow_parents_async",
            CallKind::Write,
            advance_workflow_parents_operation(self.connection(), child_workflow_ids, source),
        )
    }

    /// One phase-2 recovery pass of the reaper: the phase-2 records older than
    /// `grace_ms` and under `quarantine_after_attempts`, oldest first, at most
    /// `max_rows`, each in its own transaction: `Phase2RecoveryRecord` or
    /// `Phase2RecoveryFailed`. Writes; owed child starts build their children
    /// through the child spec source. A build that raised `KeyboardInterrupt`
    /// or `SystemExit` raises it again after the pass; an `Exception` of a
    /// build fails its record. A negative value raises `OverflowError`; with
    /// no child spec source it raises `RuntimeError`.
    #[pyo3(signature = (*, grace_ms, max_rows, quarantine_after_attempts))]
    fn recover_workflow_phase2_async<'py>(
        &self,
        py: Python<'py>,
        grace_ms: u64,
        max_rows: u32,
        quarantine_after_attempts: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::InRecord)?;
        call_async(
            py,
            core,
            "CoreHandle.recover_workflow_phase2_async",
            CallKind::Write,
            recover_workflow_phase2_operation(
                self.connection(),
                grace_ms,
                max_rows,
                quarantine_after_attempts,
                source,
            ),
        )
    }

    /// One global workflow recovery pass of the reaper, with `failure_limit`
    /// passes per workflow before it fails closed: `RecoveryPassRecord` or
    /// `RecoveryPassFailed`. Writes; each candidate commits alone, and owed
    /// child starts build their children through the child spec source. A
    /// build that raised `KeyboardInterrupt` or `SystemExit` raises it again
    /// after the pass; an `Exception` of a build fails its candidate, which
    /// counts toward the limit. A negative value raises `OverflowError`; with
    /// no child spec source it raises `RuntimeError`.
    #[pyo3(signature = (*, failure_limit))]
    fn recover_workflows_async<'py>(
        &self,
        py: Python<'py>,
        failure_limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        let source = self.child_spec_source(core, RaisedExceptions::InRecord)?;
        call_async(
            py,
            core,
            "CoreHandle.recover_workflows_async",
            CallKind::Write,
            recover_workflows_operation(self.connection(), failure_limit, source),
        )
    }

    /// Settle the claimed workflow tasks of one claim pass
    /// (`horsies_workflow_claimed_tasks_settle`): `ClaimsSettled` with the
    /// tasks of PAUSED and CANCELLED workflows, never to dispatch, or
    /// `SettleFailed`. Writes, one statement per 1000 claims. A claim with no
    /// task sends nothing.
    fn settle_claimed_workflow_tasks_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'py, SettleRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.settle_claimed_workflow_tasks_async",
            CallKind::Write,
            settle_claimed_workflow_tasks_operation(self.connection(), request.get().parts()),
        )
    }

    /// Cancel one workflow tree (`horsies_workflow_cancel`):
    /// `WorkflowCancelled`, `WorkflowNotCancellable`, `WorkflowAbsent`, or a
    /// generated handle error class (`HandleDbOperationFailed`,
    /// `HandleStoredDataInvalid`). Writes, in one statement; a caller that leaves does
    /// not stop it. A row outside the function's contract raises
    /// `PanicException`.
    fn cancel_workflow(&self, py: Python<'_>, workflow_id: Uuid) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.cancel_workflow",
            CallKind::Write,
            cancel_workflow_operation(self.connection(), workflow_id),
        )
    }

    /// The awaitable form of `cancel_workflow`.
    fn cancel_workflow_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.cancel_workflow_async",
            CallKind::Write,
            cancel_workflow_operation(self.connection(), workflow_id),
        )
    }

    /// The status of one workflow: `WorkflowStatusRead`, `WorkflowAbsent` or
    /// a generated handle error class. Reads only; a caller that leaves drops
    /// the read.
    fn workflow_status(&self, py: Python<'_>, workflow_id: Uuid) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.workflow_status",
            CallKind::Read,
            workflow_status_operation(self.connection(), workflow_id),
        )
    }

    /// The awaitable form of `workflow_status`.
    fn workflow_status_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.workflow_status_async",
            CallKind::Read,
            workflow_status_operation(self.connection(), workflow_id),
        )
    }

    /// Wait for the end of one workflow, up to `timeout_ms` (no limit for
    /// None): `WorkflowCompletedRead`, `WorkflowEndedRead`,
    /// `WorkflowPausedRead`, `WaitTimedOut`, `WorkflowAbsent` or a generated
    /// handle error class. Reads only; a caller that leaves drops the wait and
    /// its subscription. The call is bounded by the limit plus the bound of
    /// a blocking call, and not bounded with no limit; signals are checked
    /// every 100 ms, so a raised signal handler (Ctrl+C) ends the wait. A
    /// negative timeout raises `OverflowError`.
    #[pyo3(signature = (workflow_id, *, timeout_ms))]
    fn wait_workflow_result(
        &self,
        py: Python<'_>,
        workflow_id: Uuid,
        timeout_ms: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking_within(
            py,
            core,
            "CoreHandle.wait_workflow_result",
            CallKind::Read,
            result_wait_bound(timeout_ms),
            wait_workflow_result_operation(self.connection(), workflow_id, timeout_ms),
        )
    }

    /// The awaitable form of `wait_workflow_result`. A cancelled await drops
    /// the wait and its subscription.
    #[pyo3(signature = (workflow_id, *, timeout_ms))]
    fn wait_workflow_result_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.wait_workflow_result_async",
            CallKind::Read,
            wait_workflow_result_operation(self.connection(), workflow_id, timeout_ms),
        )
    }

    /// The nodes of one workflow that store a result:
    /// `WorkflowNodeResultsRead`, `WorkflowAbsent` or a generated handle
    /// error class. Reads only; a caller that leaves drops the read.
    fn workflow_node_results(&self, py: Python<'_>, workflow_id: Uuid) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.workflow_node_results",
            CallKind::Read,
            workflow_node_results_operation(self.connection(), workflow_id),
        )
    }

    /// The awaitable form of `workflow_node_results`.
    fn workflow_node_results_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.workflow_node_results_async",
            CallKind::Read,
            workflow_node_results_operation(self.connection(), workflow_id),
        )
    }

    /// The stored result of one node: `StoredNodeResultRecord`,
    /// `NodeResultNotStored` or a generated handle error class. Reads only; a
    /// caller that leaves drops the read.
    fn workflow_node_result(
        &self,
        py: Python<'_>,
        workflow_id: Uuid,
        node_id: String,
    ) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.workflow_node_result",
            CallKind::Read,
            workflow_node_result_operation(self.connection(), workflow_id, node_id),
        )
    }

    /// The awaitable form of `workflow_node_result`.
    fn workflow_node_result_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
        node_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.workflow_node_result_async",
            CallKind::Read,
            workflow_node_result_operation(self.connection(), workflow_id, node_id),
        )
    }

    /// The nodes of one workflow in index order: `WorkflowNodesRead`,
    /// `WorkflowAbsent` or a generated handle error class. Reads only; a
    /// caller that leaves drops the read.
    fn workflow_nodes(&self, py: Python<'_>, workflow_id: Uuid) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.workflow_nodes",
            CallKind::Read,
            workflow_nodes_operation(self.connection(), workflow_id),
        )
    }

    /// The awaitable form of `workflow_nodes`.
    fn workflow_nodes_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.workflow_nodes_async",
            CallKind::Read,
            workflow_nodes_operation(self.connection(), workflow_id),
        )
    }

    /// One workflow retention pass of the reaper: batches of at most
    /// `batch_size` rows (counted by nodes) of terminal workflows older than
    /// `older_than_hours` with no live backing task, with their node rows,
    /// until a batch deletes none or `budget_ms` has passed after a batch:
    /// `WorkflowsDeleted` or `WorkflowDeleteFailed`. Writes; each batch commits
    /// alone. Awaitable only: its caller, the reaper, runs on the worker's
    /// event loop. A negative value raises `OverflowError`.
    fn delete_expired_workflows_async<'py>(
        &self,
        py: Python<'py>,
        older_than_hours: u32,
        batch_size: u32,
        budget_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.delete_expired_workflows_async",
            CallKind::Write,
            delete_expired_workflows_operation(
                self.connection(),
                older_than_hours,
                batch_size,
                budget_ms,
            ),
        )
    }

    /// Expire the PAUSED workflows older than `older_than_us` microseconds,
    /// at most `batch_size`, oldest first (`horsies_workflow_expire_paused`
    /// per candidate): `PausedExpired` or `PausedExpiryFailed`. Writes; each
    /// candidate commits alone. Awaitable only: its caller, the reaper, runs
    /// on the worker's event loop. A batch size that does not fit in 32 bits
    /// raises `OverflowError`.
    fn expire_paused_workflows_async<'py>(
        &self,
        py: Python<'py>,
        older_than_us: i64,
        batch_size: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.expire_paused_workflows_async",
            CallKind::Write,
            expire_paused_workflows_operation(self.connection(), older_than_us, batch_size),
        )
    }

    /// The info of one task: `TaskInfoRecord`, `TaskInfoNotFound`, or
    /// the generated broker error class with `TASK_INFO_QUERY_FAILED` or
    /// `INVALID_JSON_PAYLOAD`. Installs the schema once per handle first;
    /// then reads only.
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

    /// The workflow node that links the task `task_id`:
    /// `TaskWorkflowNodeLinked`, `TaskWorkflowNodeNotLinked`, or the
    /// generated broker error class with `TASK_INFO_QUERY_FAILED`. One
    /// bounded statement of the core (`TASK_WORKFLOW_NODE_READ_END`); no
    /// schema step. The handler child reads it for the span of an attempt
    /// of a workflow task that the worker ended.
    fn task_workflow_node(&self, py: Python<'_>, task_id: Uuid) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.task_workflow_node",
            CallKind::Read,
            task_workflow_node_operation(self.connection(), task_id),
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

    /// The result of one task, waiting up to `timeout_ms` (no limit for
    /// `None`) while it has not finished: `RawResultRecordRecord` (no result
    /// when the limit passed first), `RawResultAbsent`, or the generated broker error class
    /// with `TASK_INFO_QUERY_FAILED` or `INVALID_JSON_PAYLOAD`. Installs the
    /// schema once per handle first; then reads only. The wait takes a wake
    /// from the process listener, or reads again every 200 ms in an
    /// `ASSUME_INITIALIZED` handle. The call is bounded by the limit plus
    /// the bound of a blocking call, and not bounded with no limit;
    /// signals are checked every 100 ms. A negative `timeout_ms` raises
    /// `OverflowError`.
    #[pyo3(signature = (task_id, *, timeout_ms))]
    fn raw_result_record(
        &self,
        py: Python<'_>,
        task_id: Uuid,
        timeout_ms: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking_within(
            py,
            core,
            "CoreHandle.raw_result_record",
            CallKind::Read,
            result_wait_bound(timeout_ms),
            raw_result_record_operation(self.connection(), task_id, timeout_ms),
        )
    }

    /// The awaitable form of `raw_result_record`. A cancelled await drops
    /// the wait.
    #[pyo3(signature = (task_id, *, timeout_ms))]
    fn raw_result_record_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.raw_result_record_async",
            CallKind::Read,
            raw_result_record_operation(self.connection(), task_id, timeout_ms),
        )
    }

    /// One page of the task list (`horsies_monitoring_task_list`):
    /// `MonitoringJson` of the core's `TaskListPage`, or a generated
    /// `MonitoringQueryCoreError` class.
    /// Reads only, in one statement. Awaitable only: its caller,
    /// `horsies.monitoring.list_tasks`, is a coroutine. A cancelled await
    /// drops the read.
    fn monitoring_list_tasks_async<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'_, TaskListQuery>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_list_tasks_async",
            CallKind::Read,
            list_tasks_operation(self.connection(), query.get().query()),
        )
    }

    /// One task with its attempts (`horsies_monitoring_task_detail`):
    /// `MonitoringJson` of the core's `TaskDetail`, `TaskDetailAbsent`, or
    /// a generated `MonitoringQueryCoreError` class. Reads only, in one statement. Awaitable only: its
    /// caller, `horsies.monitoring.get_task_detail`, is a coroutine. A
    /// cancelled await drops the read.
    fn monitoring_task_detail_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_task_detail_async",
            CallKind::Read,
            task_detail_operation(self.connection(), task_id),
        )
    }

    /// The facets of one scope (`horsies_monitoring_task_facets`):
    /// `MonitoringJson` of the core's `Facets`, or a generated
    /// `MonitoringQueryCoreError` class. Reads
    /// only, in one statement. Awaitable only: its caller,
    /// `horsies.monitoring.task_facets`, is a coroutine. A cancelled await
    /// drops the read.
    fn monitoring_task_facets_async<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'_, TaskFacetsQuery>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_task_facets_async",
            CallKind::Read,
            task_facets_operation(self.connection(), query.get().query()),
        )
    }

    /// The count of each task status (`horsies_monitoring_task_stats`):
    /// `MonitoringJson` of the core's `Vec<StatusCount>`, or
    /// a generated `MonitoringQueryCoreError` class. Reads only, in one statement. Awaitable only: its
    /// caller, `horsies.monitoring.task_stats`, is a coroutine. A cancelled
    /// await drops the read.
    fn monitoring_task_stats_async<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'_, TaskStatsQuery>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_task_stats_async",
            CallKind::Read,
            task_stats_operation(self.connection(), query.get().query()),
        )
    }

    /// The task counts per group (`horsies_monitoring_task_breakdown`):
    /// `MonitoringJson` of the core's `Breakdown`, or a generated
    /// `MonitoringQueryCoreError` class.
    /// Reads only, in one statement. Awaitable only. A cancelled await drops
    /// the read.
    fn monitoring_task_breakdown_async<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'_, TaskBreakdownQuery>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_task_breakdown_async",
            CallKind::Read,
            task_breakdown_operation(self.connection(), query.get().query()),
        )
    }

    /// The names of the root runs (`horsies_monitoring_workflow_names`):
    /// `MonitoringJson` of the core's `Vec<String>`, or a generated
    /// `MonitoringQueryCoreError` class.
    /// Reads only, in one statement. Awaitable only. A cancelled await drops
    /// the read.
    fn monitoring_workflow_names_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_workflow_names_async",
            CallKind::Read,
            workflow_names_operation(self.connection()),
        )
    }

    /// The newest root runs (`horsies_monitoring_workflow_runs`):
    /// `MonitoringJson` of the core's `Vec<WorkflowRunSummary>`, or
    /// a generated `MonitoringQueryCoreError` class. Reads only, in one statement. Awaitable only. A
    /// cancelled await drops the read.
    fn monitoring_workflow_runs_async<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'_, WorkflowRunsQuery>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_workflow_runs_async",
            CallKind::Read,
            workflow_runs_operation(self.connection(), query.get().query()),
        )
    }

    /// One run with its nodes and edges (`horsies_monitoring_workflow_run`):
    /// `MonitoringJson` of the core's `Option<WorkflowRunDetail>` (`null`
    /// when no run has the id), or a generated
    /// `MonitoringQueryCoreError` class. Reads only, in one
    /// statement. Awaitable only. A cancelled await drops the read.
    fn monitoring_workflow_run_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_workflow_run_async",
            CallKind::Read,
            workflow_run_operation(self.connection(), workflow_id),
        )
    }

    /// One node with its task (`horsies_monitoring_workflow_node`):
    /// `MonitoringJson` of the core's `Option<WorkflowTaskDetail>` (`null`
    /// when the run has no node at the index), or a generated
    /// `MonitoringQueryCoreError` class. Reads
    /// only, in one statement. Awaitable only. A cancelled await drops the
    /// read. An index outside 32 bits raises `OverflowError`.
    #[pyo3(signature = (workflow_id, *, task_index))]
    fn monitoring_workflow_node_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: Uuid,
        task_index: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_workflow_node_async",
            CallKind::Read,
            workflow_node_operation(self.connection(), workflow_id, task_index),
        )
    }

    /// Every schedule state (`horsies_monitoring_schedules`):
    /// `MonitoringJson` of the core's `Vec<ScheduleStateInfo>`, or
    /// a generated `MonitoringQueryCoreError` class. Reads only, in one statement. Awaitable only. A
    /// cancelled await drops the read.
    fn monitoring_schedules_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.monitoring_schedules_async",
            CallKind::Read,
            schedules_operation(self.connection()),
        )
    }

    /// The RUNNING tasks whose latest runner beat, or their start when they
    /// have no beat, is older than `threshold_minutes`
    /// (`horsies_list_stale_tasks`): `TaskListComplete`,
    /// `TaskListTruncated`, or the generated broker error class with `INVALID_INPUT` or
    /// `MONITORING_QUERY_FAILED`. The threshold follows Python's rule: a
    /// `bool`, a value that is not an `int`, or one outside 0 to 525,600
    /// minutes is `INVALID_INPUT` in a future that is already done, before
    /// any core call. Reads only, in one statement. Awaitable only: its
    /// caller, `PostgresBroker.get_stale_tasks`, is a coroutine. A cancelled
    /// await drops the read.
    fn stale_tasks_async<'py>(
        &self,
        py: Python<'py>,
        threshold_minutes: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let threshold = match stale_threshold(threshold_minutes)? {
            Ok(threshold) => threshold,
            Err(refused) => {
                return done_future(
                    py,
                    BrokerFailure::of_operation_error("get_stale_tasks", refused.error()),
                )
            }
        };
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.stale_tasks_async",
            CallKind::Read,
            stale_tasks_operation(self.connection(), threshold),
        )
    }

    /// The PENDING tasks whose `good_until` passed before a worker took them
    /// (`horsies_list_expired_tasks`): `TaskListComplete`,
    /// `TaskListTruncated`, or the generated broker error class with
    /// `MONITORING_QUERY_FAILED`. Reads only, in one statement. Awaitable
    /// only: its caller, `PostgresBroker.get_expired_tasks`, is a coroutine.
    /// A cancelled await drops the read.
    fn expired_tasks_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.expired_tasks_async",
            CallKind::Read,
            expired_tasks_operation(self.connection()),
        )
    }

    /// Ping the workers and collect the distinct replies within the window
    /// (`PostgresBroker::ping_workers`, on the process listener):
    /// `WorkerPongsRecord`, or the generated broker error class with `INVALID_INPUT`,
    /// `LISTENER_DISABLED` (an `ASSUME_INITIALIZED` handle has no listener)
    /// or `WORKER_PING_FAILED`. The arguments and the handle's listener follow
    /// Python's rules (`ping_request`); a refused argument, or a handle with
    /// no listener, answers at once, before any core call or connect. Sends
    /// one NOTIFY; writes no row. A caller that leaves drops the wait and the
    /// reply subscription. The call is bounded by the window plus the bound
    /// of a blocking call, and not bounded for an infinite window
    /// (`ping_wait_bound`); signals are checked every 100 ms.
    #[pyo3(signature = (*, target_worker_id, timeout_seconds, min_responses))]
    fn ping_workers(
        &self,
        py: Python<'_>,
        target_worker_id: &Bound<'_, PyAny>,
        timeout_seconds: &Bound<'_, PyAny>,
        min_responses: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let listener = self.connection().lazy_handle().listener();
        let request =
            match ping_request(target_worker_id, timeout_seconds, min_responses, listener)? {
                Ok(request) => request,
                Err(refused) => return refused.into_py_any(py),
            };
        let core = checked_core(py)?;
        let bound = ping_wait_bound(request.window);
        call_blocking_within(
            py,
            core,
            "CoreHandle.ping_workers",
            CallKind::Read,
            bound,
            ping_workers_operation(self.connection(), request),
        )
    }

    /// The awaitable form of `ping_workers`: a refused argument, or a handle
    /// with no listener, answers in a future that is already done. A
    /// cancelled await drops the wait and the reply subscription.
    #[pyo3(signature = (*, target_worker_id, timeout_seconds, min_responses))]
    fn ping_workers_async<'py>(
        &self,
        py: Python<'py>,
        target_worker_id: &Bound<'_, PyAny>,
        timeout_seconds: &Bound<'_, PyAny>,
        min_responses: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.connection().lazy_handle().listener();
        let request =
            match ping_request(target_worker_id, timeout_seconds, min_responses, listener)? {
                Ok(request) => request,
                Err(refused) => return done_future(py, refused),
            };
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.ping_workers_async",
            CallKind::Read,
            ping_workers_operation(self.connection(), request),
        )
    }

    /// The latest state snapshot per worker, idle workers included:
    /// `WorkerStatesRecord` or the generated broker error class with
    /// `MONITORING_QUERY_FAILED`. Reads only, in one statement; a caller
    /// that leaves drops the read.
    fn worker_states(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.worker_states",
            CallKind::Read,
            worker_states_operation(self.connection()),
        )
    }

    /// The awaitable form of `worker_states`. A cancelled await drops the
    /// read.
    fn worker_states_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.worker_states_async",
            CallKind::Read,
            worker_states_operation(self.connection()),
        )
    }

    /// The latest state snapshot of one worker: `WorkerStateRecord`,
    /// `WorkerStateAbsent` (no snapshot), or the generated broker error class with
    /// `MONITORING_QUERY_FAILED`. Reads only, in one statement; a caller
    /// that leaves drops the read.
    fn worker_state(&self, py: Python<'_>, worker_id: String) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.worker_state",
            CallKind::Read,
            worker_state_operation(self.connection(), worker_id),
        )
    }

    /// The awaitable form of `worker_state`. A cancelled await drops the
    /// read.
    fn worker_state_async<'py>(
        &self,
        py: Python<'py>,
        worker_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.worker_state_async",
            CallKind::Read,
            worker_state_operation(self.connection(), worker_id),
        )
    }

    /// The state snapshots of one worker, newest first: at most `limit`, or
    /// every retained one for `None`. `WorkerStatesRecord`, or
    /// the generated broker error class with `INVALID_INPUT` (a limit of 0 or less) or
    /// `MONITORING_QUERY_FAILED`. A limit of 0 or less, and a limit that
    /// Python's statement refused, answer at once, before any core call
    /// (`history_limit`); a limit that is not an `int` or `None` raises
    /// `TypeError`. Reads only, in one statement; a caller that leaves drops
    /// the read.
    #[pyo3(signature = (worker_id, *, limit))]
    fn worker_state_history(
        &self,
        py: Python<'_>,
        worker_id: String,
        limit: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let limit = match history_limit(limit)? {
            Ok(limit) => limit,
            Err(refused) => return refused.into_py_any(py),
        };
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.worker_state_history",
            CallKind::Read,
            worker_state_history_operation(self.connection(), worker_id, limit),
        )
    }

    /// The awaitable form of `worker_state_history`: a refused limit answers
    /// in a future that is already done. A cancelled await drops the read.
    #[pyo3(signature = (worker_id, *, limit))]
    fn worker_state_history_async<'py>(
        &self,
        py: Python<'py>,
        worker_id: String,
        limit: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let limit = match history_limit(limit)? {
            Ok(limit) => limit,
            Err(refused) => return done_future(py, refused),
        };
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.worker_state_history_async",
            CallKind::Read,
            worker_state_history_operation(self.connection(), worker_id, limit),
        )
    }

    /// The fleet check: for each (queue, class) with PENDING rows, whether a
    /// live worker serves it. `ClassCoverageRecord` or the generated broker error class with
    /// `MONITORING_QUERY_FAILED`. Reads only, in one statement; a caller
    /// that leaves drops the read.
    fn class_coverage(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.class_coverage",
            CallKind::Read,
            class_coverage_operation(self.connection()),
        )
    }

    /// The awaitable form of `class_coverage`. A cancelled await drops the
    /// read.
    fn class_coverage_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.class_coverage_async",
            CallKind::Read,
            class_coverage_operation(self.connection()),
        )
    }

    /// The pending streams with their oldest wait and coverage, and the
    /// budget load of each live worker. `ClassReportRecord` or
    /// the generated broker error class with `MONITORING_QUERY_FAILED`. Reads only, in one
    /// statement; a caller that leaves drops the read.
    fn class_report(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreHandle.class_report",
            CallKind::Read,
            class_report_operation(self.connection()),
        )
    }

    /// The awaitable form of `class_report`. A cancelled await drops the
    /// read.
    fn class_report_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.class_report_async",
            CallKind::Read,
            class_report_operation(self.connection()),
        )
    }
}

/// The core of this process for one call, or the refusal as a Python error.
pub fn checked_core(py: Python<'_>) -> PyResult<&'static Core> {
    core_for_call(py).map_err(refusal_error)
}

/// Close the pools of a connected handle's broker. Writes in the sense of
/// [`CallKind::Write`]: a caller that leaves does not stop the close. The
/// broker logs the writes that did not end within the close bound.
async fn close_operation(broker: Arc<PostgresBroker>) -> HandleClosed {
    broker.close().await;
    HandleClosed {}
}

async fn ping_database_operation(connection: BrokerConnection) -> Outcome<DatabasePingRecord> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return Outcome::Failed(BrokerFailure::of_operation_error(
                "ping_database",
                BrokerOperationError::db_ping_failed(error),
            ))
        }
    };
    match broker.ping_database().await {
        Ok(ping) => Outcome::Done(DatabasePingRecord {
            latency_ms: ping.latency_ms,
        }),
        Err(error) => Outcome::Failed(BrokerFailure::of_operation_error("ping_database", error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"{
        "manifest_version": 1,
        "database": {
            "database_url": "postgresql://user:secret@127.0.0.1:1/none",
            "session_database": {"kind": "SAME_AS_DATABASE_URL"},
            "pgbouncer_transaction_mode": false,
            "pool_pre_ping": true,
            "pool_size": 5,
            "max_overflow": 0,
            "pool_timeout": 30,
            "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
            "echo": false,
            "tcp_keepalives": {
                "kind": "ON",
                "idle_seconds": 30,
                "interval_seconds": 10,
                "probes": 3
            },
            "tcp_user_timeout": {"kind": "AFTER", "milliseconds": 30000}
        },
        "schema": "MIGRATE_AND_VALIDATE",
        "send": {
            "retain_rerun_input_default": false,
            "idempotency_reservation_window": 86400000000
        }
    }"#;

    #[test]
    fn the_two_pool_profiles_are_read_and_any_other_text_is_refused() {
        assert_eq!(handle_profile("PRODUCER"), Ok(PoolProfile::Producer));
        assert_eq!(handle_profile("WORKER"), Ok(PoolProfile::Worker));
        let refused = handle_profile("CHILD").expect_err("a child has a handle of its own");
        assert!(refused.contains("\"CHILD\""), "{refused}");
    }

    #[test]
    fn a_worker_process_handle_keeps_its_profile_and_starts_nothing() {
        let handle = CoreHandle::new(MANIFEST, "WORKER").expect("the manifest is complete");
        assert_eq!(handle.connection.handle.profile(), PoolProfile::Worker);
        assert_eq!(handle.connection.handle.family(), ClientFamily::Python);
        assert!(matches!(
            handle.connection.connect_state(),
            HandleConnection::NeverConnected
        ));
    }

    /// A handle with an install request keeps the manifest, the profile and
    /// the family, takes the request, and starts nothing.
    #[test]
    fn a_handle_with_an_install_request_keeps_its_inputs_and_starts_nothing() {
        let handle = CoreHandle::new(MANIFEST, "WORKER").expect("the manifest is complete");
        let seven_days = horsies_core::RetentionDays::parse_period(chrono::Duration::days(7))
            .expect("seven days");
        let request = CoverageRequest::new(3, 6, &[seven_days]).expect("a request in bounds");

        let covered = handle.connection.with_install_coverage(request.clone());

        assert_eq!(covered.handle.install_coverage(), &request);
        assert_ne!(
            covered.handle.install_coverage(),
            handle.connection.handle.install_coverage()
        );
        assert_eq!(covered.handle.profile(), PoolProfile::Worker);
        assert_eq!(covered.handle.family(), ClientFamily::Python);
        assert_eq!(
            covered.handle.manifest(),
            handle.connection.handle.manifest()
        );
        assert!(matches!(
            covered.connect_state(),
            HandleConnection::NeverConnected
        ));
    }

    #[test]
    fn a_handle_that_never_connected_closes_without_a_core_call() {
        let handle = CoreHandle::new(MANIFEST, "PRODUCER").expect("the manifest is complete");
        assert!(matches!(
            handle.connection.connect_state(),
            HandleConnection::NeverConnected
        ));
        Python::initialize();
        Python::attach(|py| {
            let closed = handle
                .close(py)
                .expect("the close of a handle that never connected does not fail");
            assert!(closed.bind(py).is_instance_of::<HandleClosed>());
        });
        assert!(matches!(
            handle.connection.connect_state(),
            HandleConnection::NeverConnected
        ));
    }
}
