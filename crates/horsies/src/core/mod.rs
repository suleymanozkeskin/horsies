#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod app;
pub mod codec;
pub mod config;
pub mod defaults;
pub mod error;
pub mod registry;
pub mod task;
pub mod types;
pub mod workflow;

// Re-exports for convenience.
pub use app::{Horsies, ResolvedEnqueue, WorkflowBuilderRegistration, WorkflowRegistrationBuilder};
pub use config::{
    mask_database_url, AppConfig, AppConfigError, CustomQueueConfig, CustomQueueConfigError,
    DailySchedule, HourlySchedule, IntervalSchedule, MonthlySchedule, PostgresConfig,
    PostgresConfigError, QueueMode, RecoveryConfig, RecoveryConfigError, ResilienceConfigError,
    ScheduleConfig, SchedulePattern, TaskSchedule, Weekday, WeeklySchedule, WorkerResilienceConfig,
};
pub use error::{ErrorCategory, ErrorCode, HorsiesError, ValidationReport};
pub use registry::{RegisteredWorkflowSpec, SpecBuilderFn, TaskRegistry, WorkflowSpecRegistry};
pub use task::{
    AsyncTaskFn, BackoffStrategy, BlockingTaskFn, BuiltInTaskCode, ContractCode,
    OperationalErrorCode, OutcomeCode, RegisteredTask, RetrievalCode, RetryPolicy,
    RetryPolicyError, SubWorkflowError, TaskAttemptInfo, TaskError, TaskErrorCode, TaskInfo,
    TaskOptions, TaskResult, TaskSendError, TaskSendErrorCode, TaskSendPayload, TaskSendResult,
};
pub use types::{TaskAttemptOutcome, TaskStatus, TASK_TERMINAL_STATES, TASK_TERMINAL_VALUES};
pub use workflow::{
    resolve_node_task_options, AnyNode, HandleErrorCode, HandleOperationError, HandleResult,
    InputField, JoinType, NodeKey, NodeRef, OnError, SubWorkflowNode, SubWorkflowSummary,
    SuccessCase, SuccessPolicy, TaskNode, TypedNodeRef, WorkflowContext, WorkflowDefConfig,
    WorkflowDefinition, WorkflowMeta, WorkflowSpec, WorkflowSpecBuilder, WorkflowStartError,
    WorkflowStartErrorCode, WorkflowStartResult, WorkflowStatus, WorkflowTaskStatus,
    WF_TASK_TERMINAL_VALUES, WORKFLOW_TASK_TERMINAL_STATES, WORKFLOW_TERMINAL_STATES,
};
