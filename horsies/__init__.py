"""Task Library - A Python library for distributed task execution"""

# Install Rust-style error handler on import
from .core.errors import install_error_handler as _install_error_handler

_install_error_handler()

from .core.app import Horsies
from .core.models.app import AppConfig
from .core.models.broker import PostgresConfig
from .core.models.tasks import (
    TaskResult,
    TaskError,
    LibraryErrorCode,
    SubWorkflowError,
    RetryPolicy,
    TaskInfo,
)
from .core.models.queues import QueueMode, CustomQueueConfig
from .core.models.workflow import (
    WorkflowSpec,
    TaskNode,
    SubWorkflowNode,
    AnyNode,
    WorkflowHandle,
    WorkflowStatus,
    WorkflowTaskStatus,
    WorkflowContext,
    WorkflowMeta,
    OnError,
    WorkflowValidationError,
    WorkflowDefinition,
    slugify,
    SubWorkflowRetryMode,
    SubWorkflowSummary,
    SuccessCase,
    SuccessPolicy,
    NodeKey,
    WorkflowTaskInfo,
    WorkflowContextMissingIdError,
    WorkflowHandleMissingIdError,
    WORKFLOW_TERMINAL_STATES,
    WORKFLOW_TASK_TERMINAL_STATES,
)
from .core.workflows.engine import start_workflow, start_workflow_async
from .core.workflows.start_types import (
    WorkflowStartError,
    WorkflowStartErrorCode,
    WorkflowStartResult,
    WorkflowStartStage,
)
from .core.models.schedule import (
    Weekday,
    IntervalSchedule,
    HourlySchedule,
    DailySchedule,
    WeeklySchedule,
    MonthlySchedule,
    SchedulePattern,
    TaskSchedule,
    ScheduleConfig,
)
from .core.models.recovery import RecoveryConfig
from .core.models.resilience import WorkerResilienceConfig
from .core.types.status import TaskStatus, TASK_TERMINAL_STATES
from .core.errors import ErrorCode, ValidationReport, MultipleValidationErrors
from .core.exception_mapper import ExceptionMapper
from .core.task_decorator import from_node, TaskHandle
from .core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationError,
    BrokerResult,
)
from .core.types.result import Result, Ok, Err, is_ok, is_err

__all__ = [
    # Core
    'Horsies',
    'AppConfig',
    'PostgresConfig',
    'TaskResult',
    'TaskError',
    'LibraryErrorCode',
    'SubWorkflowError',
    'RetryPolicy',
    'TaskInfo',
    'QueueMode',
    'CustomQueueConfig',
    'TaskStatus',
    'TASK_TERMINAL_STATES',
    'ErrorCode',
    'ValidationReport',
    'MultipleValidationErrors',
    # Workflow
    'WorkflowSpec',
    'TaskNode',
    'SubWorkflowNode',
    'AnyNode',
    'WorkflowHandle',
    'WorkflowStatus',
    'WorkflowTaskStatus',
    'WorkflowContext',
    'WorkflowMeta',
    'OnError',
    'WorkflowValidationError',
    'WorkflowDefinition',
    'slugify',
    'SubWorkflowRetryMode',
    'SubWorkflowSummary',
    'SuccessCase',
    'SuccessPolicy',
    'NodeKey',
    'WorkflowTaskInfo',
    'WorkflowContextMissingIdError',
    'WorkflowHandleMissingIdError',
    'WORKFLOW_TERMINAL_STATES',
    'WORKFLOW_TASK_TERMINAL_STATES',
    'start_workflow',
    'start_workflow_async',
    'WorkflowStartError',
    'WorkflowStartErrorCode',
    'WorkflowStartResult',
    'WorkflowStartStage',
    # Scheduling
    'Weekday',
    'IntervalSchedule',
    'HourlySchedule',
    'DailySchedule',
    'WeeklySchedule',
    'MonthlySchedule',
    'SchedulePattern',
    'TaskSchedule',
    'ScheduleConfig',
    # Recovery
    'RecoveryConfig',
    'WorkerResilienceConfig',
    # Exception mapper
    'ExceptionMapper',
    # Task handle
    'TaskHandle',
    # Node injection
    'from_node',
    # Broker result types
    'BrokerErrorCode',
    'BrokerOperationError',
    'BrokerResult',
    # Vendored Result type
    'Result',
    'Ok',
    'Err',
    'is_ok',
    'is_err',
]
