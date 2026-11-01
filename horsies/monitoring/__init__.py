"""Typed read queries over a horsies deployment's own tables.

This package is part of horsies core: it needs no optional extra and never
imports the web layer. Every query takes a ``PostgresBroker``, is async, and
returns ``MonitoringResult[T]``. ``Err`` holds a ``MonitoringQueryError``
variant: a database failure, the event loop limit, or history data that
does not decode. A missing row is ``Ok(None)``.

The response models are the wire contract for the monitoring UI and are
unstable pre-1.0.

    from datetime import datetime, timedelta, timezone

    from horsies import TaskStatus
    from horsies.core.history.reads.pages import HistoryWindow
    from horsies.monitoring import list_tasks

    now = datetime.now(timezone.utc)
    page = await list_tasks(
        broker,
        window=HistoryWindow(lower=now - timedelta(hours=24), upper=now),
        statuses=[TaskStatus.FAILED],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        offset=0,
        limit=50,
    )
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
    ErrorCategory,
    ErrorFacet,
    FacetValue,
    Facets,
    GroupRow,
    LeafTaskInfo,
    LivenessReport,
    ScheduleStateInfo,
    StatusCount,
    TaskAttemptInfo,
    TaskDetail,
    TaskListPage,
    TaskSummary,
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
from horsies.monitoring.queries import (
    MAX_TASK_PAGE_REACH,
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
    'MAX_TASK_PAGE_REACH',
    'SortDirection',
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
    'ErrorCategory',
    'ErrorFacet',
    'FacetValue',
    'Facets',
    'GroupRow',
    'LeafTaskInfo',
    'LivenessReport',
    'ScheduleStateInfo',
    'StatusCount',
    'TaskAttemptInfo',
    'TaskDetail',
    'TaskListPage',
    'TaskSummary',
    'WorkerHistoryPoint',
    'WorkerPingInfo',
    'WorkerStateInfo',
    'WorkflowEdge',
    'WorkflowNodeInfo',
    'WorkflowRunDetail',
    'WorkflowRunSummary',
    'WorkflowTaskDetail',
]
