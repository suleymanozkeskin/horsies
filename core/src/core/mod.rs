#![cfg_attr(test, allow(clippy::unwrap_used))]

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
pub use registry::TaskRegistry;
pub use task::{
    check_cancel_grace_ms, error_code_label, AsyncTaskFn, BackoffStrategy, BlockingTaskFn,
    BuiltInTaskCode, CancelGraceOutOfRange, ContractCode, CustomCode, CustomCodeRefused,
    DeclaredCodeRefusal, DeclaredCodesRefused, DeclaredErrorCode, DeclaredErrorCodes,
    ErrorCodeLabel, ExceptionRecord, Execution, ExecutionClass, OperationalErrorCode, OutcomeCode,
    RegisteredTask, RetrievalCode, RetryPolicy, RetryPolicyError, StoredErrorRefused,
    SubWorkflowFailure, TaskAttemptInfo, TaskError, TaskErrorCode, TaskErrorKind, TaskInfo,
    TaskOptions, TaskResult, TaskSendError, TaskSendErrorCode, TaskSendPayload, TaskSendResult,
    DEFAULT_CANCEL_GRACE_MS, UNDECLARED_LABEL,
};
pub use types::{TaskAttemptOutcome, TaskStatus, TASK_TERMINAL_STATES, TASK_TERMINAL_VALUES};
pub use workflow::{
    AnyNode, HandleErrorCode, HandleOperation, HandleOperationError, HandleResult, JoinType,
    NodeKey, OnError, StartRefusalCause, SubWorkflowParams, SubWorkflowSummary, SuccessCase,
    SuccessPolicy, WorkflowContext, WorkflowMeta, WorkflowSpec, WorkflowStartError,
    WorkflowStartErrorCode, WorkflowStartResult, WorkflowStatus, WorkflowTaskStatus,
    WF_TASK_TERMINAL_VALUES, WORKFLOW_TASK_TERMINAL_STATES, WORKFLOW_TERMINAL_STATES,
};
