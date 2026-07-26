"""Typed read queries over a horsies deployment's own tables.

This package is part of horsies core: it needs no optional extra and never
imports the web layer. Every query takes a ``PostgresBroker``, is async, and
returns ``MonitoringResult[T]`` — ``Err`` only for database failures, with a
missing row reported as ``Ok(None)``.

The response models are the wire contract for the monitoring UI and are
unstable pre-1.0.

    from horsies.monitoring import list_tasks

    page = await list_tasks(
        broker,
        statuses=[TaskStatus.FAILED],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        offset=0,
        limit=50,
    )
"""

from horsies.monitoring.errors import (
    MonitoringQueryError,
    MonitoringQueryErrorCode,
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
    TaskActionError,
    TaskActionErrorCode,
    TaskCancelled,
    TaskRetried,
    cancel_task,
    retry_task,
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
    'MonitoringQueryError',
    'MonitoringQueryErrorCode',
    'MonitoringResult',
    'TaskActionError',
    'TaskActionErrorCode',
    # Actions
    'TaskCancelled',
    'TaskRetried',
    'cancel_task',
    'retry_task',
    # Query parameter vocabularies
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
