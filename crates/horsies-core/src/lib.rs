#![cfg_attr(test, allow(clippy::unwrap_used))]

// Test modules derive `WorkflowInput` from horsies-macros, a dev-dependency.
// The derive output names `horsies::InputField` and `horsies::__private`.
#[cfg(test)]
extern crate self as horsies;

// `#[derive(horsies_macros::CodeCatalog)]` names `::horsies_core::catalog`,
// which this crate reaches through its own name.
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
pub mod manifest;
pub mod monitoring;
#[cfg(feature = "web")]
pub mod web;
pub(crate) mod worker;
pub(crate) mod workflow_engine;

// ---------------------------------------------------------------------------
// Public re-exports
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) use horsies_macros::WorkflowInput;

// core re-exports
pub use crate::core::RegisteredTask;
pub use crate::core::{
    mask_database_url, resolve_node_task_options, AnyNode, AppConfigError, BackoffStrategy,
    BuiltInTaskCode, ContractCode, CronEnumTerm, CronNumericTerm, CronOrdinal, CronSchedule,
    CustomQueueConfig, CustomQueueConfigError, DailySchedule, DaySelector, EnqueueRetention,
    ErrorCategory, ErrorCode, Execution, ExecutionClass, HandleErrorCode, HandleOperationError,
    HandleResult, HourlySchedule, InputField, IntervalSchedule, JoinType, Month, MonthlySchedule,
    NodeKey, NodeRef, OnError, OperationalErrorCode, OutcomeCode, PayloadPolicy, PostgresConfig,
    PostgresConfigError, QueueMode, RecoveryConfig, RecoveryConfigError,
    RegisteredWorkflowSpec as CoreRegisteredWorkflowSpec, ResilienceConfigError, ResolvedEnqueue,
    RetentionChoice, RetentionConfig, RetentionConfigError, RetrievalCode, RetryPolicy,
    RetryPolicyError, ScheduleConfig, SchedulePattern, SpecBuilderFn, SubWorkflowFailure,
    SubWorkflowNode, SubWorkflowParams, SubWorkflowSummary, SuccessCase, SuccessPolicy,
    TaskAttemptInfo, TaskAttemptOutcome, TaskError, TaskErrorCode, TaskErrorKind, TaskInfo,
    TaskNode, TaskOptions, TaskRegistry, TaskResult, TaskSchedule, TaskSendError,
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
    expected_schema_version, run_horsies_migrations, BrokerError, BrokerErrorCode,
    BrokerOperationError, BrokerResult, ClaimedTaskRow, DatabasePing, ExpiredTaskRow, HeartbeatRow,
    NotifyListener, PostgresBroker, RawResultRecord, SchemaInitializationMode,
    SharedNotifyListener, StaleTaskRow, TaskAttemptRow, TaskHandle, TaskInfoRow, TaskResultRow,
    TaskRunningContextRow, WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateRow,
    WorkerStateSnapshot, WorkflowRow, WorkflowTaskRow, MIGRATIONS_TABLE, WORKER_PING_CHANNEL,
};
pub use crate::broker::{PoolProfile, PoolSettingsError, RowDecodeError};

// worker re-exports
pub use crate::worker::{
    cli::LogLevel,
    config::WorkerConfig,
    scheduler::service::{spawn_scheduler, SchedulerStartError},
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

pub use crate::broker::client_family::ClientFamily;
pub use crate::core::history::identity::uuid7::{mint_task_id, Uuid7Error};
pub use crate::core::history::reads::detail::TaskAbsenceReason;
pub use crate::core::history::rerun::{
    NotEligibleReason, RerunEnqueuePolicy, RerunError, RerunOutcome, RerunTask,
};
pub use crate::workflow_engine::bound_handle::WorkflowHandle;
pub use crate::workflow_engine::engine::{on_subworkflow_complete, on_workflow_task_complete};
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
