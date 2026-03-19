"""Task Library - A Python library for distributed task execution"""

from importlib.metadata import version as _pkg_version

__version__: str = _pkg_version("horsies")

from .core.app import Horsies
from .core.models.app import AppConfig
from .core.models.broker import PostgresConfig
from .core.models.tasks import (
    TaskResult,
    TaskError,
    OperationalErrorCode,
    ContractCode,
    RetrievalCode,
    OutcomeCode,
    BuiltInTaskCode,
    SubWorkflowError,
    RetryPolicy,
    TaskInfo,
    TaskAttemptInfo,
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
    SubWorkflowSummary,
    SuccessCase,
    SuccessPolicy,
    NodeKey,
    WorkflowTaskInfo,
    WorkflowContextMissingIdError,
    WORKFLOW_TERMINAL_STATES,
    WORKFLOW_TASK_TERMINAL_STATES,
    HandleErrorCode,
    HandleOperationError,
    HandleResult,
)
from .core.workflows.engine import (
    start_workflow,
    start_workflow_async,
    pause_workflow,
    pause_workflow_sync,
    resume_workflow,
    resume_workflow_sync,
)
from .core.workflows.start_types import (
    WorkflowStartError,
    WorkflowStartErrorCode,
    WorkflowStartResult,
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
from .core.types.status import TaskStatus, TaskAttemptOutcome, TASK_TERMINAL_STATES
from .core.errors import ErrorCode, ValidationReport, MultipleValidationErrors
from .core.exception_mapper import ExceptionMapper
from .core.task_decorator import from_node, TaskHandle
from .core.models.task_send_types import (
    TaskSendErrorCode,
    TaskSendPayload,
    TaskSendError,
    TaskSendResult,
)
from .core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationError,
    BrokerResult,
)
from .core.types.result import Result, Ok, Err, is_ok, is_err

__all__ = [
    # Version
    '__version__',
    # Core
    'Horsies',
    'AppConfig',
    'PostgresConfig',
    'TaskResult',
    'TaskError',
    'OperationalErrorCode',
    'ContractCode',
    'RetrievalCode',
    'OutcomeCode',
    'BuiltInTaskCode',
    'SubWorkflowError',
    'RetryPolicy',
    'TaskInfo',
    'TaskAttemptInfo',
    'QueueMode',
    'CustomQueueConfig',
    'TaskStatus',
    'TaskAttemptOutcome',
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
    'SubWorkflowSummary',
    'SuccessCase',
    'SuccessPolicy',
    'NodeKey',
    'WorkflowTaskInfo',
    'WorkflowContextMissingIdError',
    'WORKFLOW_TERMINAL_STATES',
    'WORKFLOW_TASK_TERMINAL_STATES',
    'HandleErrorCode',
    'HandleOperationError',
    'HandleResult',
    'start_workflow',
    'start_workflow_async',
    'pause_workflow',
    'pause_workflow_sync',
    'resume_workflow',
    'resume_workflow_sync',
    'WorkflowStartError',
    'WorkflowStartErrorCode',
    'WorkflowStartResult',
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
    # Task send types
    'TaskSendErrorCode',
    'TaskSendPayload',
    'TaskSendError',
    'TaskSendResult',
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
