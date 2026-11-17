#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod app;
pub mod boot_failure;
pub mod codec;
pub mod config;
pub mod defaults;
pub mod error;
pub(crate) mod history;
pub mod lifecycle;
pub mod python_text;
pub mod registry;
pub mod task;
pub mod types;
pub mod workflow;

// Re-exports for convenience.
pub use app::{
    Horsies, PreparedSchedules, ResolvedEnqueue, ScheduleSendFacts, WorkflowBuilderRegistration,
    WorkflowRegistrationBuilder,
};
pub use config::{
    mask_database_url, AppConfig, AppConfigError, CronEnumTerm, CronNumericTerm, CronOrdinal,
    CronSchedule, CustomQueueConfig, CustomQueueConfigError, DailySchedule, DaySelector,
    EnqueueRetention, HourlySchedule, IntervalSchedule, Month, MonthlySchedule, PausedAgeRefused,
    PausedWorkflowAge, PayloadPolicy, PostgresConfig, PostgresConfigError, QueueMode,
    RecoveryConfig, RecoveryConfigError, ResilienceConfigError, RetentionChoice,
    RetentionClassKeyRefused, RetentionConfig, RetentionConfigError, ScheduleConfig,
    SchedulePattern, TaskSchedule, Weekday, WeeklySchedule, WorkerResilienceConfig,
};
pub use error::{ErrorCategory, ErrorCode, HorsiesError, ValidationReport};
pub use registry::{
    BuilderHookName, RegisteredWorkflowSpec, SpecBuildError, SpecBuilderFn, TaskRegistry,
    WorkflowSpecRegistry,
};
pub use task::{
    AsyncTaskFn, BackoffStrategy, BlockingTaskFn, BuiltInTaskCode, CancelGraceMs,
    CancelGraceOutOfRange, ContractCode, ExceptionRecord, Execution, ExecutionClass,
    OperationalErrorCode, OutcomeCode, RegisteredTask, RetrievalCode, RetryPolicy,
    RetryPolicyError, StoredErrorRefused, SubWorkflowFailure, TaskAttemptInfo, TaskError,
    TaskErrorCode, TaskErrorKind, TaskInfo, TaskOptions, TaskResult, TaskSendError,
    TaskSendErrorCode, TaskSendPayload, TaskSendResult, UserCode, UserCodeRefused,
};
pub use types::{TaskAttemptOutcome, TaskStatus, TASK_TERMINAL_STATES, TASK_TERMINAL_VALUES};
pub use workflow::{
    resolve_node_task_options, AnyNode, HandleErrorCode, HandleOperationError, HandleResult,
    InputField, JoinType, NodeKey, NodeRef, OnError, SubWorkflowNode, SubWorkflowParams,
    SubWorkflowSummary, SuccessCase, SuccessPolicy, TaskNode, TypedNodeRef, WorkflowContext,
    WorkflowDefConfig, WorkflowDefinition, WorkflowMeta, WorkflowSpec, WorkflowSpecBuilder,
    WorkflowStartError, WorkflowStartErrorCode, WorkflowStartResult, WorkflowStatus,
    WorkflowTaskStatus, WF_TASK_TERMINAL_VALUES, WORKFLOW_TASK_TERMINAL_STATES,
    WORKFLOW_TERMINAL_STATES,
};
