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

use std::future::Future;
use std::sync::Arc;

use horsies_core::core::lifecycle::OwnedClaim;
use horsies_core::handle::{CoreHandle as LazyHandle, HandleConnection};
use horsies_core::manifest::{parse_producer_manifest, ProducerManifest, SchemaMode};
use horsies_core::{BrokerError, BrokerErrorCode, ClientFamily, PoolProfile, PostgresBroker};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::bridge::blocking::{call_blocking, call_blocking_within};
use crate::bridge::completion_queue::{call_async, done_future};
use crate::bridge::CallKind;
use crate::errors::{refusal_error, ManifestRefused};
use crate::operations::arguments::utc_instant;
use crate::operations::cancel::cancel_task_operation;
use crate::operations::claim::{claim_pass_operation, ClaimPassRequest};
use crate::operations::coverage::{ensure_partition_coverage_operation, CoverageRequestArgument};
use crate::operations::enqueue::{enqueue_operation, EnqueueRequest};
use crate::operations::expiry_and_orphans::{
    audit_orphaned_workflow_tasks_operation, cancel_owned_orphan_operation,
    expire_pending_tasks_operation,
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
use crate::operations::workflow_lifecycle::{cancel_workflow_operation, pause_workflow_operation};
use crate::operations::workflow_retention::delete_expired_workflows_operation;
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

    /// The manifest of the handle.
    fn manifest(&self) -> &ProducerManifest {
        self.handle.manifest()
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
}

impl CoreHandle {
    /// A clone of the connection, for an operation future.
    pub fn connection(&self) -> BrokerConnection {
        self.connection.clone()
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
        })
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
    /// `CancelRefusedWorkflowTask`, `CancelRefusedStatus`, `CancelTaskAbsent`,
    /// `CancelFailed`, or `CancelStoredDataInvalid`. A RUNNING task is
    /// cancelled only with `include_running`. Writes, in one statement.
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

    /// Cancel one claimed workflow task that this claim (`worker_id`,
    /// `claimed_at`) holds and that no runnable node links
    /// (`horsies_cancel_owned_orphan`): `TaskRowEnded`, a refusal
    /// (`AttemptAlreadyApplied`, `AttemptLostClaim`,
    /// `AttemptSourceStateConflict`, `AttemptTaskAbsent`), or
    /// `AttemptWriteFailed`. Writes, in one statement. Awaitable only: its
    /// caller, the worker's finalize, runs on the worker's event loop. A
    /// naive `claimed_at` raises `TypeError`.
    #[pyo3(signature = (task_id, *, worker_id, claimed_at))]
    fn cancel_owned_orphan_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
        worker_id: String,
        claimed_at: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let fence = OwnedClaim {
            worker_id,
            claimed_at: utc_instant(claimed_at)?,
        };
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreHandle.cancel_owned_orphan_async",
            CallKind::Write,
            cancel_owned_orphan_operation(self.connection(), task_id, fence),
        )
    }

    /// Pause one workflow tree (`horsies_workflow_pause`): `WorkflowPaused`,
    /// `WorkflowNotRunning`, `WorkflowAbsent`, or `HandleFailed`. Writes, in
    /// one statement. Awaitable only: its caller,
    /// `horsies.core.workflows.lifecycle.pause_workflow`, is a coroutine. A row
    /// outside the function's contract raises `PanicException`.
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

    /// Cancel one workflow tree (`horsies_workflow_cancel`):
    /// `WorkflowCancelled`, `WorkflowNotCancellable`, `WorkflowAbsent`, or
    /// `HandleFailed`. Writes, in one statement. Awaitable only: its caller,
    /// `WorkflowHandle.cancel_async`, is a coroutine. A row outside the
    /// function's contract raises `PanicException`.
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
    /// `BrokerFailure` with `TASK_INFO_QUERY_FAILED` or
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
    /// when the limit passed first), `RawResultAbsent`, or `BrokerFailure`
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
    /// `MonitoringJson` of the core's `TaskListPage`, or `MonitoringFailed`.
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
    /// `MonitoringFailed`. Reads only, in one statement. Awaitable only: its
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
    /// `MonitoringJson` of the core's `Facets`, or `MonitoringFailed`. Reads
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
    /// `MonitoringFailed`. Reads only, in one statement. Awaitable only: its
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
    /// `MonitoringJson` of the core's `Breakdown`, or `MonitoringFailed`.
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
    /// `MonitoringJson` of the core's `Vec<String>`, or `MonitoringFailed`.
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
    /// `MonitoringFailed`. Reads only, in one statement. Awaitable only. A
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
    /// when no run has the id), or `MonitoringFailed`. Reads only, in one
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
    /// when the run has no node at the index), or `MonitoringFailed`. Reads
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
    /// `MonitoringFailed`. Reads only, in one statement. Awaitable only. A
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
    /// `TaskListTruncated`, or `BrokerFailure` with `INVALID_INPUT` or
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
            Err(refused) => return done_future(py, BrokerFailure::from(refused.error())),
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
    /// `TaskListTruncated`, or `BrokerFailure` with
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
