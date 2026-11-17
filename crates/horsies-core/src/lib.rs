#![cfg_attr(test, allow(clippy::unwrap_used))]

// Test modules derive `WorkflowInput` from horsies-macros, a dev-dependency.
// The derive output names `horsies::InputField` and `horsies::__private`.
#[cfg(test)]
extern crate self as horsies;

// `#[derive(horsies_macros::CodeCatalog)]` names `::horsies_core::catalog`,
// and `#[derive(horsies_macros::ManifestSchema)]` names
// `::horsies_core::manifest_schema`; this crate reaches both through its own
// name.
extern crate self as horsies_core;

// Internal modules (merged from formerly separate crates).
// `core` is `pub` (rather than `pub(crate)`) because `#[macro_export]` macros
// in `core::task::macros` use `$crate::core::…` paths that must be
// resolvable when the macro is expanded in downstream crates.
pub(crate) mod broker;
pub mod catalog;
#[doc(hidden)]
pub mod core;
pub mod db;
pub mod log_targets;
pub mod manifest;
pub mod manifest_schema;
pub mod monitoring;
pub mod scheduler_manifest;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(feature = "web")]
pub mod web;
pub(crate) mod worker;
pub mod worker_manifest;
pub(crate) mod workflow_engine;

// ---------------------------------------------------------------------------
// Public re-exports
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) use horsies_macros::WorkflowInput;

// core re-exports
pub use crate::core::BuilderHookName;
pub use crate::core::RegisteredTask;
pub use crate::core::SpecBuildError;
pub use crate::core::{
    mask_database_url, resolve_node_task_options, AnyNode, AppConfigError, BackoffStrategy,
    BuiltInTaskCode, ContractCode, CronEnumTerm, CronNumericTerm, CronOrdinal, CronSchedule,
    CustomQueueConfig, CustomQueueConfigError, DailySchedule, DaySelector, EnqueueRetention,
    ErrorCategory, ErrorCode, Execution, ExecutionClass, HandleErrorCode, HandleOperationError,
    HandleResult, HourlySchedule, InputField, IntervalSchedule, JoinType, Month, MonthlySchedule,
    NodeKey, NodeRef, OnError, OperationalErrorCode, OutcomeCode, PausedAgeRefused,
    PausedWorkflowAge, PayloadPolicy, PostgresConfig, PostgresConfigError, QueueMode,
    RecoveryConfig, RecoveryConfigError, RegisteredWorkflowSpec as CoreRegisteredWorkflowSpec,
    ResilienceConfigError, ResolvedEnqueue, RetentionChoice, RetentionClassKeyRefused,
    RetentionConfig, RetentionConfigError, RetrievalCode, RetryPolicy, RetryPolicyError,
    ScheduleConfig, SchedulePattern, SpecBuilderFn, SubWorkflowFailure, SubWorkflowNode,
    SubWorkflowParams, SubWorkflowSummary, SuccessCase, SuccessPolicy, TaskAttemptInfo,
    TaskAttemptOutcome, TaskError, TaskErrorCode, TaskErrorKind, TaskInfo, TaskNode, TaskOptions,
    TaskRegistry, TaskResult, TaskSchedule, TaskSendError, TaskSendErrorCode, TaskSendPayload,
    TaskSendResult, TaskStatus, TypedNodeRef, UserCode, UserCodeRefused, ValidationReport, Weekday,
    WeeklySchedule, WorkerResilienceConfig, WorkflowContext, WorkflowDefConfig, WorkflowDefinition,
    WorkflowMeta, WorkflowSpec, WorkflowSpecBuilder, WorkflowSpecRegistry, WorkflowStartError,
    WorkflowStartErrorCode, WorkflowStartResult, WorkflowStatus, WorkflowTaskStatus,
    TASK_TERMINAL_STATES, WF_TASK_TERMINAL_VALUES, WORKFLOW_TASK_TERMINAL_STATES,
    WORKFLOW_TERMINAL_STATES,
};
pub use crate::core::{AppConfig, HorsiesError};

// broker re-exports
pub use crate::broker::{
    expected_schema_version, BrokerError, BrokerErrorCode, BrokerOperationError, BrokerResult,
    CheckInterruption, ClaimedTaskRow, DatabasePing, ExpiredTaskRow, HeartbeatRow, ListenError,
    ListenerPlanError, PostgresBroker, RawResultRecord, SchemaInitializationMode,
    SessionCheckFailure, StaleTaskRow, TaskAttemptRow, TaskHandle, TaskInfoRow, TaskResultRow,
    WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateSnapshot, WorkflowRow,
    WorkflowTaskRow, MIGRATIONS_TABLE, WORKER_PING_CHANNEL,
};
pub use crate::broker::{PoolProfile, PoolSettingsError, RowDecodeError};

// partition maintenance of a client handle (`PostgresBroker::ensure_partition_coverage`)
pub use crate::broker::CoveragePassError;
pub use crate::core::history::errors::HistoryError;
pub use crate::core::history::maintenance::coverage::{
    CoverageEnsureFailed, CoverageEnsured, CoverageOutcome, CoverageRequest, CoverageSerialization,
    HistoryCoverage,
};
pub use crate::core::history::periods::RetentionDays;

// The baseline SQL files, which horsies._core hands to the Python install.
pub use crate::broker::migrations::BASELINE_SQL_FILE_TEXTS;

// lifecycle operations of a client handle (`PostgresBroker::release_claims`,
// `PostgresBroker::requeue_stale_claimed`, `PostgresBroker::cancel_task`)
pub use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, FinishWorkflowSuccess, RetryPlacement,
    RetryRequest,
};
pub use crate::broker::claims::{
    ClaimClasses, ClaimPass, ClaimPassRequest, ClaimTake, ClaimedRow, QueueStartCheck, QueueStarts,
    ReleaseBatch, ReleaseBatchError, ReleaseClaims, ReleaseOutcome, ReleasePlacement, StartAttempt,
    StartMiss, StartOutcome, StartWindow, UnheldBudget, WindowedStart, MAX_RELEASED_CLAIMS,
};
pub use crate::broker::expiry_and_orphans::{
    OrphanAuditPage, PendingExpiryFailed, PendingExpiryPass, TaskRowEnd, ORPHAN_AUDIT_PAGE_ROWS,
    PENDING_EXPIRY_BATCH_ROWS, PENDING_EXPIRY_MAX_BATCHES_PER_PASS,
};
pub use crate::broker::stale_recovery::{
    StaleClaimsRequeueFailed, StaleClaimsRequeuePass, StaleRunningPass, StaleRunningThresholds,
    ThresholdMs, ThresholdOutOfRange, STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS,
};
pub use crate::broker::{
    CancelAnswer, CancelTaskError, EnqueueRefusal, EnqueueRefusalCause, SendKind,
    WorkflowRetentionFailed, WorkflowRetentionPass,
};
pub use crate::core::history::enqueue::SendPart;
pub use crate::worker::execution::FinalizingMark;
pub use crate::worker::finalize_batch::{
    BatchOutcomes, BatchWriteError, PlainSuccess, SuccessBatch, SuccessBatchError,
    FUSED_BATCH_MAX_ROWS,
};

// Child mode of a Rust worker binary: the entry checks `child_mode` first
// and, in child mode, exits with the code of `run_child_host`. Task bodies
// start work that belongs to their run with `spawn_in_run`, and read the
// cancel signal of their run with `run_cancel_signal`.
pub use crate::worker::executor::child_host::{
    child_mode, run_cancel_signal, run_child_host, spawn_in_run, ChildApp, ChildArgs,
    ChildArgsError, ChildHostExit, ChildMode, RunCancelSignal, RunSignalRefused, RunSpawnRefused,
    CHILD_MODE_ARGUMENT, MAX_RUN_WORK,
};

// The parent-child frame codec, for the Python child program and the
// preload process: they speak it through `horsies._core`.
pub use crate::worker::executor::protocol as frame_protocol;

// worker re-exports
pub use crate::worker::{
    cli::LogLevel,
    config::WorkerConfig,
    scheduler::service::{
        spawn_scheduler, ActiveScheduler, SchedulerService, SchedulerStartError, SchedulerStarted,
        SchedulerStop, SchedulerTickError,
    },
    worker::Worker,
    WorkerError,
};

// worker state snapshot values
pub use crate::broker::{
    ClassBudget, ClassBudgets, ClearableBootCause, DownSlot, DownSlots, ShortageErrno, SlotHealth,
    MAX_BOOT_FAILURE_DETAIL_CHARS,
};

// fleet check and class report
pub use crate::broker::{
    BudgetLoad, ClassCoverage, ClassReport, PendingStream, StreamCoverage, StreamCoverageState,
    StreamWait, WorkerClassLoad, LIVE_SNAPSHOT_ROW_LIMIT, PENDING_HEAD_LIMIT, PENDING_STREAM_LIMIT,
};

// the stale and expired task lists of a client handle
pub use crate::broker::task_lists::{
    BoundedTaskList, StaleMinutes, StaleThresholdRefusal, MAX_STALE_THRESHOLD_MINUTES,
    TASK_LIST_BOUND,
};

pub use crate::broker::client_family::{ClientFamily, FamilyCheckFailure};
pub use crate::broker::migrations::SchemaRefusal;
pub use crate::core::history::identity::uuid7::{mint_task_id, Uuid7Error};
pub use crate::core::history::reads::detail::TaskAbsenceReason;
pub use crate::core::history::rerun::{
    NotEligibleReason, RerunEnqueuePolicy, RerunError, RerunOutcome, RerunTask,
};
pub use crate::workflow_engine::bound_handle::WorkflowHandle;
pub use crate::workflow_engine::engine::{
    on_workflow_task_complete, WorkflowTaskCompletion, WorkflowTaskCompletionError,
};
pub use crate::workflow_engine::error::WorkflowError;
pub use crate::workflow_engine::info::WorkflowTaskInfo;
pub use crate::workflow_engine::lifecycle::{cancel_workflow, pause_workflow, resume_workflow};
pub use crate::workflow_engine::query::get_workflow_result;
pub use crate::workflow_engine::recovery::recover_stuck_workflows;
pub use crate::workflow_engine::start::{retry_start, start_workflow, start_workflow_with_retry};

#[cfg(test)]
#[doc(hidden)]
pub(crate) mod __private {
    pub const fn input_field<I, F>(name: &'static str) -> crate::InputField<I, F> {
        crate::InputField::raw(name)
    }
}
