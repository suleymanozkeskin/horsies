//! `horsies._core`: the extension module through which Python calls
//! horsies-core.
//!
//! Rules of the module:
//! - Importing it starts nothing: no runtime, no thread, no connection.
//! - The first core operation of a process starts one tokio runtime and one
//!   log forwarder thread (`process_core`).
//! - A blocking call runs the operation on the calling thread with the GIL
//!   released and checks signals every 100 ms; an async call returns an
//!   asyncio future that the loop's completion queue sets (`bridge`). No
//!   runtime thread takes the GIL.
//! - A service run (a worker, the scheduler loop) ends at a stop request and
//!   has no total bound (`bridge::service_run`).
//! - A Python callback of the core runs on the process's one callback
//!   thread, started at the first callback (`bridge::callback_thread`).
//! - A panic in an operation raises `PanicException` (a `BaseException`); a
//!   panic in a core background task stops the core in this process, or
//!   aborts a worker process (`set_panic_rule`).
//! - A process forked without `exec` after the core started refuses every
//!   core call with `ForkedAfterCoreStart`.
//! - Core log events reach Python `logging` through a bounded channel and one
//!   forwarder thread (`logging`).
//! - A `CoreHandle` is built once from a typed manifest (`handle`).
//! - A `CoreScheduler` runs the scheduler of a scheduler manifest on the
//!   broker of a `CoreHandle` (`scheduler`).
//! - The Python child program and the preload process speak the core's
//!   parent-child frame codec through pure calls (`frames`).
//! - `prepare_worker` builds the worker of a worker manifest, and
//!   `PreparedWorker.run` runs it on the main thread (`worker`).

mod bridge;
mod child_handle;
mod child_spec;
mod errors;
mod frames;
mod handle;
mod logging;
mod operations;
mod process_core;
mod records;
mod scheduler;
mod worker;

use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use pyo3::exceptions::PyValueError;

use crate::process_core::{CoreStatus, PanicRule};

/// The allocator of musl targets (see `Cargo.toml`).
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The state of the core in this process: `not_started`, `running`,
/// `stopped` or `inherited`. Starts nothing.
#[pyfunction]
fn core_state() -> &'static str {
    match process_core::core_status() {
        CoreStatus::NotStarted => "not_started",
        CoreStatus::Running => "running",
        CoreStatus::Stopped => "stopped",
        CoreStatus::Inherited => "inherited",
    }
}

/// The refusal of a task id mint: the cause from the core's UUIDv7 source.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub(crate) struct IdMintRefused {
    pub(crate) cause: String,
}

/// A minted task id, or the refusal.
enum MintOutcome {
    Minted(Uuid),
    Refused(IdMintRefused),
}

impl<'py> IntoPyObject<'py> for MintOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Minted(task_id) => task_id.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// Mint a new task id (UUIDv7): `uuid.UUID` or `IdMintRefused`. Pure: no
/// I/O, and it starts no runtime; the fork and stop rules still apply.
#[pyfunction]
fn mint_task_id() -> PyResult<MintOutcome> {
    if let Some(refusal) = process_core::pure_call_refusal() {
        return Err(errors::refusal_error(refusal));
    }
    Ok(match horsies_core::mint_task_id() {
        Ok(task_id) => MintOutcome::Minted(task_id),
        Err(error) => MintOutcome::Refused(IdMintRefused {
            cause: error.to_string(),
        }),
    })
}

/// The files of the baseline migration (version 1000) as `(file name, text)`
/// pairs, in the order they run. The texts are compiled into this module from
/// `crates/horsies-core/baseline/`. Pure: no I/O, and it starts no runtime.
#[pyfunction]
fn baseline_sql_files() -> Vec<(&'static str, &'static str)> {
    horsies_core::BASELINE_SQL_FILE_TEXTS.to_vec()
}

/// Set what a panic in a core background task does in this process:
/// `'STOP_CORE'` (the default: the core stops, every later call raises
/// `CoreStopped`) or `'ABORT_PROCESS'` (a worker process: the panic is logged
/// and the process aborts). The entry point calls it before the first core
/// call. The rule in force again changes nothing; another rule raises
/// `PanicRuleFixed` once a rule was set or the core started.
#[pyfunction]
fn set_panic_rule(rule: &str) -> PyResult<()> {
    let rule = match rule {
        "STOP_CORE" => PanicRule::StopCore,
        "ABORT_PROCESS" => PanicRule::AbortProcess,
        other => {
            return Err(PyValueError::new_err(format!(
                "rule must be 'STOP_CORE' or 'ABORT_PROCESS', not {other:?}"
            )))
        }
    };
    process_core::set_panic_rule(rule).map_err(errors::panic_rule_error)
}

/// Read the effective level of every `horsies` logger again, after a change
/// of the logging configuration. Does nothing before the core started; the
/// start reads the levels.
#[pyfunction]
fn refresh_log_levels(py: Python<'_>) -> PyResult<()> {
    match process_core::running_core() {
        Some(core) => core.log().refresh_levels(py),
        None => Ok(()),
    }
}

#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<handle::CoreHandle>()?;
    module.add_class::<child_spec::ChildSpecRequestRecord>()?;
    module.add_class::<child_spec::ChildSpecSourceRow>()?;
    module.add_class::<child_spec::ChildSpecBuilt>()?;
    module.add_class::<child_spec::ChildSpecRefused>()?;
    module.add_class::<operations::workflow_start::WorkflowStartRequest>()?;
    module.add_class::<operations::workflow_start::WorkflowStarted>()?;
    module.add_class::<operations::workflow_start::WorkflowExists>()?;
    module.add_class::<operations::workflow_start::WorkflowIdTaken>()?;
    module.add_class::<operations::workflow_start::WorkflowStartFailed>()?;
    module.add_class::<operations::workflow_resume::WorkflowResumed>()?;
    module.add_class::<operations::workflow_resume::WorkflowNotPaused>()?;
    module.add_class::<operations::workflow_resume::WorkflowResumeRecoveryFailed>()?;
    module.add_class::<operations::workflow_phase2::NodeFinishRequest>()?;
    module.add_class::<operations::workflow_phase2::NodeFinishedRecord>()?;
    module.add_class::<operations::workflow_phase2::NodeFinishFailed>()?;
    module.add_class::<operations::workflow_phase2::ParentsAdvanced>()?;
    module.add_class::<operations::workflow_phase2::ParentAdvanceStoppedRecord>()?;
    module.add_class::<operations::workflow_phase2_recovery::Phase2RecoveryRecord>()?;
    module.add_class::<operations::workflow_phase2_recovery::Phase2RecoveryFailed>()?;
    module.add_class::<child_handle::CoreChildHandle>()?;
    module.add_class::<scheduler::CoreScheduler>()?;
    module.add_class::<records::BrokerFailure>()?;
    module.add_class::<records::DatabasePingRecord>()?;
    module.add_class::<records::HandleClosed>()?;
    module.add_class::<IdMintRefused>()?;
    module.add_class::<operations::arguments::StartNow>()?;
    module.add_class::<operations::arguments::StartAfterDelay>()?;
    module.add_class::<operations::arguments::StartAt>()?;
    module.add_class::<operations::cancel::TaskCancelledRecord>()?;
    module.add_class::<operations::cancel::CancelRefusedWorkflowTask>()?;
    module.add_class::<operations::cancel::CancelRefusedStatus>()?;
    module.add_class::<operations::cancel::CancelTaskAbsent>()?;
    module.add_class::<operations::cancel::CancelFailed>()?;
    module.add_class::<operations::cancel::CancelStoredDataInvalid>()?;
    module.add_class::<operations::attempt_records::AttemptAlreadyApplied>()?;
    module.add_class::<operations::attempt_records::AttemptLostClaim>()?;
    module.add_class::<operations::attempt_records::AttemptSourceStateConflict>()?;
    module.add_class::<operations::attempt_records::AttemptTaskAbsent>()?;
    module.add_class::<operations::attempt_records::AttemptWriteFailed>()?;
    module.add_class::<operations::attempt_records::PhaseTwoOwedRecord>()?;
    module.add_class::<operations::claim::ClaimPassRequest>()?;
    module.add_class::<operations::claim::ClaimedRowRecord>()?;
    module.add_class::<operations::claim::ClaimPassRecord>()?;
    module.add_class::<operations::claim::ClaimPassFailed>()?;
    module.add_class::<operations::coverage::CoverageRequestArgument>()?;
    module.add_class::<operations::coverage::CoveredForeverRecord>()?;
    module.add_class::<operations::coverage::CoveredUntilRecord>()?;
    module.add_class::<operations::coverage::CoverageEnsuredRecord>()?;
    module.add_class::<operations::coverage::CoverageEnsureFailedRecord>()?;
    module.add_class::<operations::coverage::CoverageFailed>()?;
    module.add_class::<operations::pruning::PrunePassRecord>()?;
    module.add_class::<operations::pruning::PruneFailed>()?;
    module.add_class::<operations::rerun::RerunCommandArgument>()?;
    module.add_class::<operations::rerun::RerunPolicyArgument>()?;
    module.add_class::<operations::rerun::PreparedStatementRecord>()?;
    module.add_class::<operations::rerun::RerunWriteRecord>()?;
    module.add_class::<operations::rerun::RerunSourceLiveRecord>()?;
    module.add_class::<operations::rerun::RerunSourceAbsentRecord>()?;
    module.add_class::<operations::rerun::RerunSourceCorruptRecord>()?;
    module.add_class::<operations::rerun::RerunNotEligibleRecord>()?;
    module.add_class::<operations::rerun::RerunInputUnavailableRecord>()?;
    module.add_class::<operations::rerun::RerunInputCorruptRecord>()?;
    module.add_class::<operations::rerun::RerunClassNotRegistered>()?;
    module.add_class::<operations::rerun::RerunClientFamilyMismatch>()?;
    module.add_class::<operations::rerun::RerunCallerKeyRefused>()?;
    module.add_class::<operations::rerun::RerunEnqueuedRecord>()?;
    module.add_class::<operations::rerun::RerunKeyReplayRecord>()?;
    module.add_class::<operations::rerun::RerunKeyConflictRecord>()?;
    module.add_class::<operations::enqueue::EnqueueRequest>()?;
    module.add_class::<operations::enqueue::Enqueued>()?;
    module.add_class::<operations::enqueue::SendSuppressed>()?;
    module.add_class::<operations::enqueue::SendAsyncContext>()?;
    module.add_class::<operations::enqueue::SendValidationFailed>()?;
    module.add_class::<operations::enqueue::SendEnqueueFailed>()?;
    module.add_class::<operations::enqueue::SendPayloadMismatch>()?;
    module.add_class::<operations::enqueue::SendPayloadTooLarge>()?;
    module.add_class::<operations::enqueue::SendKeyConflict>()?;
    module.add_class::<operations::expiry_and_orphans::PendingExpiryDrained>()?;
    module.add_class::<operations::expiry_and_orphans::PendingExpiryBoundReached>()?;
    module.add_class::<operations::expiry_and_orphans::PendingExpiryFailed>()?;
    module.add_class::<operations::expiry_and_orphans::OrphanPageAudited>()?;
    module.add_class::<operations::expiry_and_orphans::OrphanAuditLockBusy>()?;
    module.add_class::<operations::expiry_and_orphans::OrphanAuditFailed>()?;
    module.add_class::<operations::expiry_and_orphans::TaskRowEnded>()?;
    module.add_class::<operations::failed_attempt::NoRetry>()?;
    module.add_class::<operations::failed_attempt::RetryAfter>()?;
    module.add_class::<operations::failed_attempt::FinishFailedAttemptRequest>()?;
    module.add_class::<operations::failed_attempt::RetryNotRequested>()?;
    module.add_class::<operations::failed_attempt::RetryPastDeadline>()?;
    module.add_class::<operations::failed_attempt::CapacityWoken>()?;
    module.add_class::<operations::failed_attempt::AttemptFailureApplied>()?;
    module.add_class::<operations::failed_attempt::AttemptRetryScheduled>()?;
    module.add_class::<operations::failed_attempt::AttemptClaimReleased>()?;
    module.add_class::<operations::queue_starts::QueueStartsRequest>()?;
    module.add_class::<operations::queue_starts::QueueStartsRecord>()?;
    module.add_class::<operations::queue_starts::QueueStartsFailed>()?;
    module.add_class::<operations::release::ReleaseClaimsRequest>()?;
    module.add_class::<operations::release::ClaimsReleased>()?;
    module.add_class::<operations::release::ReleasedRow>()?;
    module.add_class::<operations::release::NotHeldRow>()?;
    module.add_class::<operations::release::ReleaseFailed>()?;
    module.add_class::<operations::result::RawResultRecordRecord>()?;
    module.add_class::<operations::result::RawResultAbsent>()?;
    module.add_class::<operations::start::AttemptStarted>()?;
    module.add_class::<operations::start::AttemptMissed>()?;
    module.add_class::<operations::start::DeadlineBeforeStart>()?;
    module.add_class::<operations::start::StartFailed>()?;
    module.add_class::<operations::finalizing::FinalizingMarked>()?;
    module.add_class::<operations::finalizing::FinalizingNotHeld>()?;
    module.add_class::<operations::finalizing::MarkFinalizingFailed>()?;
    module.add_class::<operations::schema::SchemaInstalled>()?;
    module.add_class::<operations::schema::SchemaInstallNotRun>()?;
    module.add_class::<operations::schema::SchemaStatusRecord>()?;
    module.add_class::<operations::stale_claims::StaleClaimsDrained>()?;
    module.add_class::<operations::stale_claims::StaleClaimsBoundReached>()?;
    module.add_class::<operations::stale_claims::StaleClaimsRequeueFailed>()?;
    module.add_class::<operations::stale_running::StaleRunningRecovered>()?;
    module.add_class::<operations::stale_running::StaleRunningScanFull>()?;
    module.add_class::<operations::stale_running::StaleRunningScanFailed>()?;
    module.add_class::<operations::success::SuccessBatchRequest>()?;
    module.add_class::<operations::success::SuccessApplied>()?;
    module.add_class::<operations::success::SuccessesWritten>()?;
    module.add_class::<operations::success::BatchNotSent>()?;
    module.add_class::<operations::success::BatchRefused>()?;
    module.add_class::<operations::success::BatchCommitUnknown>()?;
    module.add_class::<operations::success::WorkflowSuccessApplied>()?;
    module.add_class::<operations::task_info::TaskInfoRecord>()?;
    module.add_class::<operations::task_info::TaskAttemptRecord>()?;
    module.add_class::<operations::task_info::TaskInfoNotFound>()?;
    module.add_class::<operations::monitoring_reads::TaskListQuery>()?;
    module.add_class::<operations::monitoring_reads::TaskFacetsQuery>()?;
    module.add_class::<operations::monitoring_reads::TaskStatsQuery>()?;
    module.add_class::<operations::monitoring_reads::TaskBreakdownQuery>()?;
    module.add_class::<operations::monitoring_reads::WorkflowRunsQuery>()?;
    module.add_class::<operations::monitoring_reads::MonitoringJson>()?;
    module.add_class::<operations::monitoring_reads::MonitoringFailed>()?;
    module.add_class::<operations::monitoring_reads::TaskDetailAbsent>()?;
    module.add_class::<operations::task_lists::TaskListComplete>()?;
    module.add_class::<operations::task_lists::TaskListTruncated>()?;
    module.add_class::<operations::worker_ping::WorkerPongRecord>()?;
    module.add_class::<operations::worker_ping::WorkerPongsRecord>()?;
    module.add_class::<operations::worker_reads::ResourceShortageRecord>()?;
    module.add_class::<operations::worker_reads::DownSlotRecord>()?;
    module.add_class::<operations::worker_reads::AllSlotsServingRecord>()?;
    module.add_class::<operations::worker_reads::SlotsDownRecord>()?;
    module.add_class::<operations::worker_reads::ClassBudgetRecord>()?;
    module.add_class::<operations::worker_reads::WorkerStateRecord>()?;
    module.add_class::<operations::worker_reads::WorkerStatesRecord>()?;
    module.add_class::<operations::worker_reads::WorkerStateAbsent>()?;
    module.add_class::<operations::worker_reads::StreamServedRecord>()?;
    module.add_class::<operations::worker_reads::StreamWithoutCapacityRecord>()?;
    module.add_class::<operations::worker_reads::StreamUnservedRecord>()?;
    module.add_class::<operations::worker_reads::StreamCoverageRecord>()?;
    module.add_class::<operations::worker_reads::ClassCoverageRecord>()?;
    module.add_class::<operations::worker_reads::OldestWaitingRecord>()?;
    module.add_class::<operations::worker_reads::NoneDueRecord>()?;
    module.add_class::<operations::worker_reads::PendingStreamRecord>()?;
    module.add_class::<operations::worker_reads::BudgetLoadRecord>()?;
    module.add_class::<operations::worker_reads::WorkerClassLoadRecord>()?;
    module.add_class::<operations::worker_reads::ClassReportRecord>()?;
    module.add_class::<operations::monitoring_window::MonitoringWindowRecord>()?;
    module.add_class::<operations::monitoring_window::WindowRefusedRecord>()?;
    module.add_class::<operations::workflow_lifecycle::WorkflowPaused>()?;
    module.add_class::<operations::workflow_lifecycle::WorkflowNotRunning>()?;
    module.add_class::<operations::workflow_lifecycle::WorkflowCancelled>()?;
    module.add_class::<operations::workflow_lifecycle::WorkflowNotCancellable>()?;
    module.add_class::<operations::workflow_lifecycle::WorkflowAbsent>()?;
    module.add_class::<operations::workflow_lifecycle::HandleFailed>()?;
    module.add_class::<operations::workflow_reads::WorkflowStatusRead>()?;
    module.add_class::<operations::workflow_reads::WorkflowCompletedRead>()?;
    module.add_class::<operations::workflow_reads::WorkflowEndedRead>()?;
    module.add_class::<operations::workflow_reads::WorkflowPausedRead>()?;
    module.add_class::<operations::workflow_reads::WaitTimedOut>()?;
    module.add_class::<operations::workflow_reads::StoredNodeResultRecord>()?;
    module.add_class::<operations::workflow_reads::WorkflowNodeResultsRead>()?;
    module.add_class::<operations::workflow_reads::NodeResultNotStored>()?;
    module.add_class::<operations::workflow_reads::StoredNodeRecord>()?;
    module.add_class::<operations::workflow_reads::WorkflowNodesRead>()?;
    module.add_class::<operations::workflow_retention::WorkflowsDeleted>()?;
    module.add_class::<operations::workflow_retention::WorkflowDeleteFailed>()?;
    module.add_class::<operations::paused_expiry::PausedExpired>()?;
    module.add_class::<operations::paused_expiry::PausedExpiryFailed>()?;
    module.add_class::<scheduler::SchedulerStarted>()?;
    module.add_class::<scheduler::SchedulerStartRefused>()?;
    module.add_class::<scheduler::SchedulerStartFailed>()?;
    module.add_class::<scheduler::SchedulerTicked>()?;
    module.add_class::<scheduler::SchedulerTickFailed>()?;
    module.add_class::<scheduler::SchedulerStopped>()?;
    module.add_class::<worker::PreparedWorker>()?;
    module.add_class::<worker::WorkerProfileRefused>()?;
    module.add_class::<worker::WorkerStopped>()?;
    module.add_class::<worker::WorkerBootFailed>()?;
    module.add_class::<worker::WorkerFailed>()?;
    module.add_class::<worker::StopRequested>()?;
    module.add_function(wrap_pyfunction!(baseline_sql_files, module)?)?;
    module.add_function(wrap_pyfunction!(core_state, module)?)?;
    module.add_function(wrap_pyfunction!(mint_task_id, module)?)?;
    module.add_function(wrap_pyfunction!(
        operations::rerun::rerun_read_function,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        operations::rerun::prepare_rerun_write_function,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        operations::rerun::decode_rerun_write_function,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(worker::prepare_worker, module)?)?;
    module.add_function(wrap_pyfunction!(refresh_log_levels, module)?)?;
    module.add_function(wrap_pyfunction!(
        operations::monitoring_window::resolve_monitoring_window,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(set_panic_rule, module)?)?;
    frames::add_frames(module)?;
    errors::add_errors(module)?;
    Ok(())
}
