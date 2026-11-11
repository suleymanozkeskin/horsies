"""Typed read queries over a horsies deployment's own tables.

This package is part of horsies core: it needs no optional extra and never
imports the web layer. Every query takes a ``PostgresBroker``, is async, and
returns ``MonitoringResult[T]``. ``Err`` holds a ``MonitoringQueryError``
variant: a database failure, the event loop limit, or history data that
does not decode. A missing row is ``Ok(None)``; ``get_task_detail`` returns
``Ok(TaskDetailNotFound)`` with a ``TaskAbsenceReason``.

The list and aggregate reads take a ``MonitoringWindow``, which only
``resolve_monitoring_window`` builds. It holds at most
``MONITORING_WINDOW_MAX`` of terminal history.

The response models are the wire contract for the monitoring UI and are
unstable pre-1.0.

```python
from horsies import Err, Ok, TaskStatus
from horsies.core.brokers.postgres import PostgresBroker
from horsies.monitoring import (
    MonitoringResult,
    MonitoringWindow,
    TaskListPage,
    TaskPage,
    WindowRefused,
    list_tasks,
    resolve_monitoring_window,
)


async def failed_in_the_last_day(
    broker: PostgresBroker,
) -> MonitoringResult[TaskListPage]:
    match resolve_monitoring_window(since=None, until=None):
        case MonitoringWindow() as window:
            pass
        case WindowRefused(reason=reason):
            raise AssertionError(f'the default window is servable: {reason}')
    match TaskPage.parse(0, 50):
        case Ok(ok_value=page):
            pass
        case Err(err_value=refused):
            raise AssertionError(f'offset 0, limit 50 is in bounds: {refused.reason}')
    return await list_tasks(
        broker,
        window=window,
        statuses=[TaskStatus.FAILED],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        page=page,
    )
```
"""

from horsies.monitoring.errors import (
    MonitoringQueryDbOperationFailed,
    MonitoringQueryError,
    MonitoringQueryErrorCode,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
    MonitoringResult,
)
from horsies.monitoring.models import (
    Breakdown,
    BudgetLoadInfo,
    ClassReportInfo,
    ClassesNotRecordedInfo,
    DownSlotInfo,
    ErrorCategory,
    ErrorFacet,
    FacetValue,
    Facets,
    GroupRow,
    LeafTaskInfo,
    LivenessReport,
    PendingStreamInfo,
    ScheduleStateInfo,
    StatusCount,
    StreamNoCapacityInfo,
    StreamNoneDueInfo,
    StreamServedInfo,
    StreamUnservedInfo,
    StreamWaitingInfo,
    TaskAbsenceReason,
    TaskAttemptInfo,
    TaskDetail,
    TaskDetailNotFound,
    TaskListPage,
    TaskSummary,
    WorkerClassLoadInfo,
    WorkerHistoryPoint,
    WorkerPingInfo,
    WorkerStateInfo,
    WorkflowEdge,
    WorkflowNodeInfo,
    WorkflowRunDetail,
    WorkflowRunSummary,
    WorkflowTaskDetail,
)
from horsies.monitoring.task_actions import (
    TaskActionDbOperationFailed,
    TaskActionError,
    TaskActionErrorCode,
    TaskActionEventLoopLimit,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionStoredDataInvalid,
    TaskActionWorkflowTask,
    TaskCancelled,
    cancel_task,
)
from horsies.monitoring.history_window import (
    MONITORING_WINDOW_DEFAULT,
    MONITORING_WINDOW_MAX,
    MonitoringWindow,
    WindowRefused,
    resolve_monitoring_window,
)
from horsies.monitoring.pages import (
    MAX_BREAKDOWN_LIMIT,
    MAX_TASK_PAGE_LIMIT,
    MAX_TASK_PAGE_REACH,
    MAX_WORKFLOW_RUNS_LIMIT,
    BreakdownLimit,
    PageRefused,
    TaskPage,
    WorkflowRunsLimit,
)
from horsies.monitoring.queries import (
    SortDirection,
    TaskGroupBy,
    TaskSortField,
    as_utc,
    categorize_error_code,
    elapsed_s,
    get_task_detail,
    get_workflow_node,
    get_workflow_run,
    list_schedules,
    list_tasks,
    list_workflow_names,
    list_workflow_runs,
    nz,
    span_s,
    task_breakdown,
    task_facets,
    task_stats,
)

__all__ = [
    # Errors
    'MonitoringQueryDbOperationFailed',
    'MonitoringQueryError',
    'MonitoringQueryErrorCode',
    'MonitoringQueryEventLoopLimit',
    'MonitoringQueryHistoryContractViolation',
    'MonitoringResult',
    'TaskActionDbOperationFailed',
    'TaskActionError',
    'TaskActionErrorCode',
    'TaskActionEventLoopLimit',
    'TaskActionNotCancellable',
    'TaskActionNotFound',
    'TaskActionStoredDataInvalid',
    'TaskActionWorkflowTask',
    # Actions
    'TaskCancelled',
    'cancel_task',
    # Query parameter vocabularies
    'MAX_BREAKDOWN_LIMIT',
    'MAX_TASK_PAGE_LIMIT',
    'MAX_TASK_PAGE_REACH',
    'MAX_WORKFLOW_RUNS_LIMIT',
    'BreakdownLimit',
    'PageRefused',
    'SortDirection',
    'TaskPage',
    'WorkflowRunsLimit',
    # History window
    'MONITORING_WINDOW_DEFAULT',
    'MONITORING_WINDOW_MAX',
    'MonitoringWindow',
    'WindowRefused',
    'resolve_monitoring_window',
    'TaskGroupBy',
    'TaskSortField',
    # Queries
    'get_task_detail',
    'get_workflow_node',
    'get_workflow_run',
    'list_schedules',
    'list_tasks',
    'list_workflow_names',
    'list_workflow_runs',
    'task_breakdown',
    'task_facets',
    'task_stats',
    # Derivation helpers
    'as_utc',
    'categorize_error_code',
    'elapsed_s',
    'nz',
    'span_s',
    # Response models
    'Breakdown',
    'BudgetLoadInfo',
    'ClassReportInfo',
    'ClassesNotRecordedInfo',
    'DownSlotInfo',
    'ErrorCategory',
    'ErrorFacet',
    'FacetValue',
    'Facets',
    'GroupRow',
    'LeafTaskInfo',
    'LivenessReport',
    'PendingStreamInfo',
    'ScheduleStateInfo',
    'StatusCount',
    'StreamNoCapacityInfo',
    'StreamNoneDueInfo',
    'StreamServedInfo',
    'StreamUnservedInfo',
    'StreamWaitingInfo',
    'TaskAbsenceReason',
    'TaskAttemptInfo',
    'TaskDetail',
    'TaskDetailNotFound',
    'TaskListPage',
    'TaskSummary',
    'WorkerClassLoadInfo',
    'WorkerHistoryPoint',
    'WorkerPingInfo',
    'WorkerStateInfo',
    'WorkflowEdge',
    'WorkflowNodeInfo',
    'WorkflowRunDetail',
    'WorkflowRunSummary',
    'WorkflowTaskDetail',
]
