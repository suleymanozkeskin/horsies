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
pub mod handle;
pub mod log_targets;
pub mod manifest;
pub mod manifest_schema;
pub mod monitoring;
pub mod process_runtime;
pub mod scheduler_manifest;
#[cfg(test)]
mod stored_form_vectors;
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
    BuiltInTaskCode, CancelGraceMs, CancelGraceOutOfRange, ContractCode, CronEnumTerm,
    CronNumericTerm, CronOrdinal, CronSchedule, CustomQueueConfig, CustomQueueConfigError,
    DailySchedule, DaySelector, EnqueueRetention, ErrorCategory, ErrorCode, Execution,
    ExecutionClass, HandleErrorCode, HandleOperationError, HandleResult, HourlySchedule,
    InputField, IntervalSchedule, JoinType, Month, MonthlySchedule, NodeKey, NodeRef, OnError,
    OperationalErrorCode, OutcomeCode, PausedAgeRefused, PausedWorkflowAge, PayloadPolicy,
    PostgresConfig, PostgresConfigError, QueueMode, RecoveryConfig, RecoveryConfigError,
    RegisteredWorkflowSpec as CoreRegisteredWorkflowSpec, ResilienceConfigError, ResolvedEnqueue,
    RetentionChoice, RetentionClassKeyRefused, RetentionConfig, RetentionConfigError,
    RetrievalCode, RetryPolicy, RetryPolicyError, ScheduleConfig, SchedulePattern, SpecBuilderFn,
    SubWorkflowFailure, SubWorkflowNode, SubWorkflowParams, SubWorkflowSummary, SuccessCase,
    SuccessPolicy, TaskAttemptInfo, TaskAttemptOutcome, TaskError, TaskErrorCode, TaskErrorKind,
    TaskInfo, TaskNode, TaskOptions, TaskRegistry, TaskResult, TaskSchedule, TaskSendError,
    TaskSendErrorCode, TaskSendPayload, TaskSendResult, TaskStatus, TypedNodeRef, UserCode,
    UserCodeRefused, ValidationReport, Weekday, WeeklySchedule, WorkerResilienceConfig,
    WorkflowContext, WorkflowDefConfig, WorkflowDefinition, WorkflowMeta, WorkflowSpec,
    WorkflowSpecBuilder, WorkflowSpecRegistry, WorkflowStartError, WorkflowStartErrorCode,
    WorkflowStartResult, WorkflowStatus, WorkflowTaskStatus, TASK_TERMINAL_STATES,
    WF_TASK_TERMINAL_VALUES, WORKFLOW_TASK_TERMINAL_STATES, WORKFLOW_TERMINAL_STATES,
};
pub use crate::core::{AppConfig, HorsiesError};

// broker re-exports
pub use crate::broker::{
    expected_schema_version, BrokerError, BrokerErrorCode, BrokerOperationError, BrokerResult,
    CheckInterruption, CheckedSend, ConnectivityRefusal, DatabasePing, ExpiredTaskRow,
    HeartbeatRow, ListenError, ListenProbeFailure, ListenerPlanError, PostgresBroker,
    RawResultRead, RawResultRecord, SchemaInitializationMode, SessionCheckFailure, StaleTaskRow,
    StoredEnvelopeRefused, StoredEnvelopeText, TaskAttemptRow, TaskHandle, TaskInfoRead,
    TaskInfoRow, TaskResultRow, WorkerPingRequest, WorkerPong, WorkerPongPayload,
    WorkerStateSnapshot, WorkflowRow, WorkflowTaskRow, MIGRATIONS_TABLE, WORKER_PING_CHANNEL,
};
pub use crate::broker::{PoolProfile, PoolSettingsError, RowDecodeError};
// The bounded limit of a result wait; a client checks it before the read.
pub use crate::broker::wait_limit::{BoundedWait, WaitLimit, WaitLimitRefused, MAX_WAIT_LIMIT_MS};

// partition maintenance of a client handle (`PostgresBroker::ensure_partition_coverage`,
// `PostgresBroker::prune_expired_partitions`)
pub use crate::broker::{CoveragePassError, PrunePassError};
pub use crate::core::history::errors::HistoryError;
pub use crate::core::history::maintenance::coverage::{
    CoverageEnsureFailed, CoverageEnsured, CoverageOutcome, CoverageRequest, CoverageSerialization,
    HistoryCoverage,
};
pub use crate::core::history::maintenance::pruning::{PrunePass, PruneReach};
pub use crate::core::history::periods::RetentionDays;

// The baseline SQL files, which horsies._core hands to the Python install.
pub use crate::broker::migrations::BASELINE_SQL_FILE_TEXTS;

// lifecycle operations of a client handle (`PostgresBroker::release_claims`,
// `PostgresBroker::requeue_stale_claimed`, `PostgresBroker::cancel_task`)
pub use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, FinishWorkflowSuccess, RetryPlacement,
    RetryRequest,
};
pub use crate::broker::child_writes::{
    WriteDeadline, CANCELLED_WRITE_END, CHILD_REFUSAL_WRITES_BOUND, CHILD_STATEMENT_BOUND,
    CHILD_WRITE_CHECKOUT_WAIT, FINALIZING_MARKER_BOUND, FINALIZING_MARKER_END,
    PRE_START_CONFIRM_ATTEMPTS, START_ATTEMPT_BOUND, START_ATTEMPT_END,
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
    CancelAnswer, CancelTaskError, EnqueueRefusal, EnqueueRefusalCause, OrphanSelfHeal, SendKind,
    StopBeforeStart, StopMove, WorkflowRetentionFailed, WorkflowRetentionPass,
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
// cancel signal of their run with `run_cancel_signal`. A result wait reads
// `process_role` first: a process that runs task bodies refuses it.
pub use crate::worker::executor::child_host::{
    child_command, child_mode, child_producer, process_role, run_cancel_signal, run_child_host,
    spawn_in_run, thread_role, wait_inside_task_message, ChildApp, ChildArgs, ChildArgsError,
    ChildHookFailure, ChildHostExit, ChildLogSetup, ChildMode, ChildProducer, ChildStartFuture,
    ChildStartHook, NamedChildStartHook, ProcessRole, RunCancelSignal, RunSignalRefused,
    RunSpawnRefused, ThreadRole, CHILD_MODE_ARGUMENT, CHILD_START_HOOK_BOUND, MAX_RUN_WORK,
};

// The parent-child frame codec, for the Python child program and the
// preload process: they speak it through `horsies._core`.
pub use crate::worker::executor::protocol as frame_protocol;

// worker re-exports
// The worker connection check of a client's live check (`check --live`):
// on a connected broker (the Rust client), and on a lazy handle (the Python
// bridge).
pub use crate::worker::connection_fit::{
    check_handle_worker_connections, check_worker_connections,
};
pub use crate::worker::from_manifest::{WorkerPlan, WorkerPlanRefused};
pub use crate::worker::run_end::{
    run_end_of, RunPhase, WorkerBootCause, WorkerFailCause, WorkerRunEnd,
};
pub use crate::worker::{
    cli::LogLevel,
    config::WorkerConfig,
    scheduler::service::{
        spawn_scheduler, ActiveScheduler, SchedulerService, SchedulerStartError, SchedulerStarted,
        SchedulerStop, SchedulerTickError,
    },
    worker::{OnStarted, Worker, WorkerStop},
    WorkerError,
};

// worker state snapshot values
pub use crate::broker::{
    checked_history_limit, ping_listener_disabled, ping_min_responses_refused,
    ping_timeout_refused, worker_history_limit_refused, ClassBudget, ClassBudgets,
    ClearableBootCause, DownSlot, DownSlots, ShortageErrno, SlotHealth,
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

pub use crate::broker::client_family::{ClientFamily, FamilyCheckFailure, RecordedFamily};
pub use crate::broker::migrations::SchemaRefusal;
pub use crate::core::history::identity::uuid7::{mint_task_id, Uuid7Error};
pub use crate::core::history::reads::detail::TaskAbsenceReason;
pub use crate::core::history::rerun::{
    NotEligibleReason, RerunEnqueuePolicy, RerunError, RerunOutcome, RerunTask,
};
// One core statement in a transaction that the caller owns: the Rust
// client's adapters implement `CallerTransaction`; the core's own
// `db::Transaction` implements it too.
pub use crate::core::history::rerun::{
    named_bind, named_bind_sql, positional_sql, CallerColumn, CallerRow, CallerRowRefused,
    CallerStatement, CallerStatementRefused, CallerTransaction, CallerType, OwnTransactionError,
    StatementValue,
};
// The rerun as three pure steps for a transaction that the caller owns
// (the Python bridge's `rerun_read`, `prepare_rerun_write`,
// `decode_rerun_write`).
pub use crate::core::history::identity::keys::IdempotencyKeyError;
pub use crate::core::history::rerun::{
    decode_rerun_write, prepare_rerun_write, rerun_read, RerunPreparation, RerunRefusal,
    RerunRowError, RerunWrite, RerunWriteOutcome,
};
pub use crate::workflow_engine::bound_handle::WorkflowHandle;
// The child spec source of a client that keeps its workflow DSL in its own
// language, and the spec data it sends.
pub use crate::broker::workflow_phase2::{NodeFinishForm, NodeFinished, ParentAdvanceStopped};
pub use crate::broker::workflow_start::{ClientStartError, ClientStartOutcome};
pub use crate::workflow_engine::child_spec::{
    ChildSpecFuture, ChildSpecRequest, ChildSpecSource, ChildSpecUnavailable, ChildSpecs,
    ChildStartSource, ClientChildSpec, ClientSpec,
};

// The steps that a client's child spec source runs around its own builder:
// the builder input of a request, and the node failures with Python's error
// envelopes.
pub use crate::workflow_engine::args_merge::ChildBuildInput;
pub use crate::workflow_engine::start::ChildSpecError;
pub use crate::workflow_engine::start_statement::{
    build_failed_error, merged_child_input, node_failure_result, params_refused_error,
    subworkflow_load_failed_error, NodeInput, RefusedMember,
};

pub use crate::workflow_engine::engine::{
    on_workflow_task_complete, WorkflowTaskCompletion, WorkflowTaskCompletionError,
};
pub use crate::workflow_engine::error::WorkflowError;
pub use crate::workflow_engine::info::WorkflowTaskInfo;
pub use crate::workflow_engine::lifecycle::{cancel_workflow, pause_workflow, resume_workflow};
pub use crate::workflow_engine::phase2_recovery::Phase2RecoverySummary;
pub use crate::workflow_engine::query::{
    get_workflow_result, NodeResultsRead, NodesRead, StoredNode, StoredNodeResult,
    TerminalErrorStatus, WorkflowEnd, WorkflowWait,
};
pub use crate::workflow_engine::recovery::recover_stuck_workflows;
pub use crate::workflow_engine::recovery::{
    RecoveryCaseMetrics, RecoveryMetrics, RecoveryPassFailure, RecoveryReport,
};
pub use crate::workflow_engine::spec_data::{
    parse_workflow_spec_data, SpecDataRefused, WorkflowSpecData, WORKFLOW_SPEC_DATA_VERSION,
};
pub use crate::workflow_engine::start::{retry_start, start_workflow, start_workflow_with_retry};

#[cfg(test)]
#[doc(hidden)]
pub(crate) mod __private {
    pub const fn input_field<I, F>(name: &'static str) -> crate::InputField<I, F> {
        crate::InputField::raw(name)
    }
}
