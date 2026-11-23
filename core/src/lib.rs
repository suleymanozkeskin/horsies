#![cfg_attr(test, allow(clippy::unwrap_used))]

// `#[derive(horsies_core_derive::CodeCatalog)]` names `::horsies_core::catalog`,
// `#[derive(horsies_core_derive::ManifestSchema)]` names
// `::horsies_core::manifest_schema`, and
// `#[derive(horsies_core_derive::EventSchema)]` names `::horsies_core::events`;
// this crate reaches them through its own name.
extern crate self as horsies_core;

// Internal modules (merged from formerly separate crates).
// `core` is `pub` (rather than `pub(crate)`) because the clients name some of
// its items by module path (`horsies_core::core::…`).
pub(crate) mod broker;
pub mod catalog;
#[doc(hidden)]
pub mod core;
pub mod db;
pub mod error_catalog;
// The public lifecycle events of the core worker and their wire codec.
pub mod events;
pub mod failure_cause;
pub mod handle;
pub mod log_targets;
pub mod manifest;
pub mod manifest_schema;
pub mod monitoring;
pub mod process_runtime;
pub mod scheduler_manifest;
#[cfg(test)]
mod sql_tests;
#[cfg(test)]
mod stored_form_vectors;
pub mod telemetry;
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

// core re-exports
pub use crate::core::RegisteredTask;
pub use crate::core::{
    error_code_label, mask_database_url, AnyNode, AppConfigError, BackoffStrategy, BuiltInTaskCode,
    CancelGraceMs, CancelGraceOutOfRange, ContractCode, CronEnumTerm, CronNumericTerm, CronOrdinal,
    CronSchedule, CustomQueueConfig, CustomQueueConfigError, DailySchedule, DaySelector,
    DeclaredCodeRefusal, DeclaredCodesRefused, DeclaredErrorCode, DeclaredErrorCodes,
    EnqueueRetention, ErrorCategory, ErrorCode, ErrorCodeLabel, Execution, ExecutionClass,
    HandleErrorCode, HandleOperation, HandleOperationError, HandleResult, HourlySchedule,
    IntervalSchedule, JoinType, Month, MonthlySchedule, NodeKey, OnError, OperationalErrorCode,
    OutcomeCode, PausedAgeRefused, PausedWorkflowAge, PayloadPolicy, PostgresConfig,
    PostgresConfigError, QueueMode, RecoveryConfig, RecoveryConfigError, ResilienceConfigError,
    RetentionChoice, RetentionClassKeyRefused, RetentionConfig, RetentionConfigError,
    RetrievalCode, RetryPolicy, RetryPolicyError, ScheduleConfig, SchedulePattern,
    StartRefusalCause, SubWorkflowFailure, SubWorkflowParams, SubWorkflowSummary, SuccessCase,
    SuccessPolicy, TaskAttemptInfo, TaskAttemptOutcome, TaskError, TaskErrorCode, TaskErrorKind,
    TaskInfo, TaskOptions, TaskRegistry, TaskResult, TaskSchedule, TaskSendError,
    TaskSendErrorCode, TaskSendPayload, TaskSendResult, TaskStatus, UserCode, UserCodeRefused,
    ValidationReport, Weekday, WeeklySchedule, WorkerResilienceConfig, WorkflowContext,
    WorkflowMeta, WorkflowSpec, WorkflowStartError, WorkflowStartErrorCode, WorkflowStartResult,
    WorkflowStatus, WorkflowTaskStatus, TASK_TERMINAL_STATES, UNDECLARED_LABEL,
    WF_TASK_TERMINAL_VALUES, WORKFLOW_TASK_TERMINAL_STATES, WORKFLOW_TERMINAL_STATES,
};
pub use crate::core::{AppConfig, HorsiesError};
// A user value kept as its stored text: a sub-workflow summary output, the `data` of a task error.
pub use crate::core::codec::{JsonText, JsonTextRefused};

// broker re-exports
pub use crate::broker::{
    expected_schema_version, BrokerCheckCause, BrokerError, BrokerErrorCode, BrokerOperationError,
    BrokerOperationFailedCode, BrokerOperationSource, BrokerRefusalCause, BrokerResult,
    CheckInterruption, CheckedSend, ConnectivityRefusal, DatabasePing, ExpiredTaskRow,
    HeartbeatRow, ListenError, ListenProbeFailure, ListenerPlanError, PostgresBroker,
    RawResultRead, RawResultRecord, SchemaInitializationMode, SessionCheckFailure, StaleTaskRow,
    StoredEnvelopeRefused, StoredEnvelopeText, TaskAttemptRow, TaskInfoRead, TaskInfoRow,
    WorkerPingRequest, WorkerPingStep, WorkerPong, WorkerPongPayload, WorkerStateSnapshot,
    WorkflowRow, WorkflowTaskRow, MIGRATIONS_TABLE, WORKER_PING_CHANNEL,
};
pub use crate::broker::{PoolProfile, PoolSettingsError, RowDecodeError};
// The node read of a task by its id: the workflow facts of the span of an
// attempt that the worker ended, read in the handler child.
pub use crate::broker::{
    TaskWorkflowNode, TASK_WORKFLOW_NODE_CHECKOUT_WAIT, TASK_WORKFLOW_NODE_READ_END,
    TASK_WORKFLOW_NODE_STATEMENT_BOUND,
};
// The bounded limit of a result wait; a client checks it before the read.
pub use crate::broker::wait_limit::{BoundedWait, WaitLimit, WaitLimitRefused, MAX_WAIT_LIMIT_MS};

// partition maintenance of a client handle (`PostgresBroker::ensure_partition_coverage`,
// `PostgresBroker::prune_expired_partitions`)
pub use crate::broker::{CoveragePassError, PrunePassError};
pub use crate::core::history::errors::HistoryError;
pub use crate::core::history::maintenance::coverage::{
    CoverageEnsureFailed, CoverageEnsured, CoverageFailureStage, CoverageOutcome, CoverageRequest,
    CoverageSerialization, HistoryCoverage,
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
    ClaimClasses, ClaimPass, ClaimPassRequest, ClaimRead, ClaimTake, ClaimedRow, QueueStartCheck,
    QueueStarts, ReleaseBatch, ReleaseBatchError, ReleaseClaims, ReleaseOutcome, ReleasePlacement,
    RunWorkflow, StartAttempt, StartMiss, StartOutcome, StartWindow, UnheldBudget, WindowedStart,
    WorkflowNodeIndex, MAX_RELEASED_CLAIMS, MAX_WORKFLOW_NODE_INDEX,
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
// start work that belongs to their run with `spawn_in_run`, read the
// cancel signal of their run with `run_cancel_signal`, and read the fencing
// token of their run with `run_fencing_token`. A result wait reads
// `process_role` first: a process that runs task bodies refuses it. The
// event handlers of the app (`EventHandlerSet`) run in the handler child of
// the same binary (`HandlerChildApp`), which also makes the span of each
// attempt that the worker ended through the client's `EndedSpanMaker`.
pub use crate::worker::executor::child_host::{
    child_command, child_handle_setup, child_mode, child_producer, process_role, run_cancel_signal,
    run_child_host, run_fencing_token, spawn_in_run, thread_role, wait_inside_task_message,
    BodyCarrier, BodyOutcome, BodyRun, BodyScope, BodySpans, ChildApp, ChildArgs, ChildArgsError,
    ChildHandleSetup, ChildHookFailure, ChildHostExit, ChildLogSetup, ChildMode, ChildProducer,
    ChildStartFuture, ChildStartHook, EndedAttempt, EndedSpanMaker, EndedSpanScope, EndedWorkflow,
    EventHandlerRefused, EventHandlerSet, HandlerChildApp, HandlerChildHasNoHandle, HandlerError,
    NamedChildStartHook, ProcessRole, RunCancelSignal, RunSignalRefused, RunSpawnRefused,
    ThreadRole, CHILD_MODE_ARGUMENT, CHILD_START_HOOK_BOUND, HANDLER_DRAIN,
    HANDLER_ERROR_LOG_EVERY, HANDLER_LAST_STATS_BOUND, HANDLER_QUEUE_EVENTS,
    HANDLER_STATS_INTERVAL, MAX_RUN_WORK, NODE_READER_STOP_BOUND, NODE_READ_QUEUE_SPANS,
};
// The fencing token of a run, which `run_fencing_token` gives.
pub use crate::core::task::{FencingToken, FencingTokenRefused};

// The parent-child frame codec, for the Python child program and the
// preload process: they speak it through `horsies._core`.
pub use crate::worker::executor::protocol as frame_protocol;

// worker re-exports
// The exit codes of the worker process (the codes of a child are in
// `frame_protocol`): `horsies-codegen` renders every exit code into the
// Python `_catalog`.
pub use crate::worker::liveness::LIVENESS_LOCK_LOST_EXIT_CODE;
pub use crate::worker::runner_beats::RUNNER_HEARTBEAT_LOST_EXIT_CODE;
// The worker connection check of a client's live check (`check --live`):
// on a connected broker (the Rust client), and on a lazy handle (the Python
// bridge).
pub use crate::worker::connection_fit::{
    check_handle_worker_connections, check_worker_connections,
};
pub use crate::worker::from_manifest::{MetricsEndpointPlan, WorkerPlan, WorkerPlanRefused};
pub use crate::worker::run_end::{
    run_end_of, RunPhase, WorkerBootCause, WorkerFailCause, WorkerRunEnd,
};
pub use crate::worker::{
    cli::LogLevel,
    config::WorkerConfig,
    scheduler::service::{
        ActiveScheduler, SchedulerService, SchedulerStartError, SchedulerStarted, SchedulerStop,
        SchedulerTickError,
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

pub use crate::broker::client_family::{
    ClientFamily, FamilyCheckFailure, RecordedFamily, UnknownStoredFamily,
};
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
// The child spec source of a client that keeps its workflow DSL in its own
// language, and the spec data it sends.
pub use crate::broker::workflow_phase2::{NodeFinishForm, NodeFinished, ParentAdvanceStopped};
pub use crate::broker::workflow_start::{ClientStartError, ClientStartOutcome};
pub use crate::workflow_engine::child_spec::{
    ChildSpecFuture, ChildSpecRequest, ChildSpecSource, ChildSpecUnavailable, ChildStartSource,
    ClientChildSpec, ClientSpec,
};

// The steps that a client's child spec source runs around its own builder:
// the builder input of a request, and the node failures with Python's error
// envelopes.
pub use crate::workflow_engine::args_merge::ChildBuildInput;
pub use crate::workflow_engine::start::ChildSpecError;
pub use crate::workflow_engine::start_statement::{
    build_failed_error, merged_child_input, node_failure_result, params_decode_failed_error,
    params_refused_error, subworkflow_load_failed_error, NodeInput, RefusedMember,
};

pub use crate::workflow_engine::engine::{
    on_workflow_task_complete, WorkflowTaskCompletion, WorkflowTaskCompletionError,
};
pub use crate::workflow_engine::error::WorkflowError;
// the cause of a failed operation, which a client names to its user
pub use crate::failure_cause::{FailureCause, ServerErrorFacts};
pub use crate::workflow_engine::lifecycle::{cancel_workflow, pause_workflow};
pub use crate::workflow_engine::phase2_recovery::Phase2RecoverySummary;
pub use crate::workflow_engine::query::{
    NodeResultsRead, NodesRead, StoredNode, StoredNodeResult, TerminalErrorStatus, WorkflowEnd,
    WorkflowWait,
};
pub use crate::workflow_engine::recovery::recover_stuck_workflows;
pub use crate::workflow_engine::recovery::{
    RecoveryCaseMetrics, RecoveryMetrics, RecoveryPassFailure, RecoveryReport,
};
pub use crate::workflow_engine::spec_data::{
    parse_workflow_spec_data, SpecDataRefused, WorkflowSpecData, WORKFLOW_SPEC_DATA_VERSION,
};
