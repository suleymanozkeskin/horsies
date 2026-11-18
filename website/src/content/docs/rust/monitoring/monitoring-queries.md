---
title: Monitoring Queries
summary: Read task, workflow and schedule state with the functions of horsies::monitoring, cancel a task, and run the workflow actions as the web routes answer them.
related: [health-reads, web-ui-mount, ../../monitoring/action-semantics, ../../monitoring/web-ui-overview]
tags: [rust, monitoring, task_stats, list_tasks, get_task_detail, cancel_task, MonitoringQueryError]
---

# Monitoring Queries

`horsies::monitoring` holds the reads and the actions that the monitoring web UI serves, as functions over the app. Each function connects the app's handle (in a worker child, the child's producer handle) and runs the core's function of the same name. No function installs the schema. The health reads (pings, worker states, stale tasks) are methods of the app: see [Health Reads](/horsies/rust/monitoring/health-reads/).

## Read Tasks

```rust
use horsies::monitoring::{
    list_tasks, resolve_monitoring_window, task_stats, SortDirection, TaskFilters, TaskListQuery,
    TaskSortField, TaskStatsQuery,
};
use horsies::{Horsies, TaskStatus};

async fn failed_tasks_of_last_day(app: &Horsies) -> Result<(), Box<dyn std::error::Error>> {
    let window = resolve_monitoring_window(None, None, None)?;
    for count in task_stats(app, &TaskStatsQuery::new(window)).await? {
        println!("{}: {}", count.status, count.count);
    }
    let filters = TaskFilters { statuses: vec![TaskStatus::Failed], ..TaskFilters::default() };
    let query = TaskListQuery::new(window)
        .with_filters(filters)
        .with_sort(TaskSortField::FailedAt, SortDirection::Descending)
        .with_pagination(0, 50)?;
    let page = list_tasks(app, &query).await?;
    for task in &page.rows {
        println!("{} {} {:?}", task.id, task.task_name, task.error_code);
    }
    Ok(())
}
```

## Reads

| Function | Returns (`MonitoringResult<_>`) |
|----------|---------------------------------|
| `task_stats(&app, &TaskStatsQuery)` | `Vec<StatusCount>` (`status`, `count`): each of the seven task statuses in a fixed order, over the live table and the history window |
| `task_facets(&app, &TaskFacetsQuery)` | `Facets`: `workers`, `task_names`, `queues` and `error_codes` with counts, and `error_category_totals` |
| `task_breakdown(&app, &TaskBreakdownQuery)` | `Breakdown`: the counts per group, highest total first, with the total and the number of groups |
| `list_tasks(&app, &TaskListQuery)` | `TaskListPage`: `rows: Vec<TaskSummary>` and `total` |
| `get_task_detail(&app, task_id)` | `TaskDetailRead`: `Found(TaskDetail)` or `NotFound(TaskDetailNotFound { task_id, reason })` |
| `list_workflow_names(&app)` | `Vec<String>`: the distinct names of the root runs, at most `LIST_ROW_BOUND` (10,000) |
| `list_workflow_runs(&app, &WorkflowRunsQuery)` | `Vec<WorkflowRunSummary>`: the newest root runs |
| `get_workflow_run(&app, workflow_id)` | `Option<WorkflowRunDetail>`: the run with its nodes and edges; `None` when no run has the id |
| `get_workflow_node(&app, workflow_id, task_index)` | `Option<WorkflowTaskDetail>`: the node with its task and the task's attempts; `None` when the run has no such node |
| `list_schedules(&app)` | `Vec<ScheduleStateInfo>`: every schedule state, soonest next run first, at most `LIST_ROW_BOUND` (10,000) |

- `TaskListPage.total` is exact when any filter is set. With no filter it is the live estimate plus the planner's estimate of the history window.
- A detail read looks in the live table and in the history, so a task that ended still answers. `TaskDetailNotFound.reason` is a `TaskAbsenceReason`: see [Retrieving Results](/horsies/rust/tasks/retrieving-results/#readerror).
- `TaskDetail` has the task's `leaf` (status, times, retry counts, worker, deadline, execution class), its name, queue and priority, its error category, its `attempts`, and its workflow and node index when it is a workflow task.

## Queries

The list and aggregate reads take a query built on a `HistoryWindow`: the history rows of tasks that ended in the window, and every live row.

| Query | Builders | Defaults |
|-------|----------|----------|
| `TaskStatsQuery::new(window)` | `.with_filters(filters)`: the `statuses` filter is dropped, because the read counts every status | – |
| `TaskFacetsQuery::new(window)` | `.with_filters(filters)`, `.with_statuses(statuses)`, `.with_error_categories(categories)`, `.retried_only(bool)` | – |
| `TaskBreakdownQuery::new(window, group_by)` | `.with_filters(filters)`, `.with_limit(limit)` | 50 groups |
| `TaskListQuery::new(window)` | `.with_filters(filters)`, `.with_sort(TaskSortField, SortDirection)`, `.with_pagination(offset, limit)` | `EnqueuedAt`, `Descending`, offset 0, limit 50 |
| `WorkflowRunsQuery::new()` | `.with_name(Option<String>)`, `.with_status(Option<String>)`, `.with_limit(limit)` | 30 runs |

The bounded builders return `Result<_, PaginationRefused>`:

| Builder | Accepts |
|---------|---------|
| `TaskBreakdownQuery::with_limit` | 1 to `MAX_BREAKDOWN_LIMIT` (500) |
| `TaskListQuery::with_pagination` | `offset` 0 or more; `limit` 1 to `MAX_TASK_PAGE_LIMIT` (200); `offset + limit` at most `MAX_TASK_PAGE_REACH` (500) |
| `WorkflowRunsQuery::with_limit` | 1 to `MAX_WORKFLOW_RUNS_LIMIT` (200) |

| Type | Values |
|------|--------|
| `TaskFilters` | `statuses: Vec<TaskStatus>`, `task_names`, `queues`, `workers`, `error_codes: Vec<String>`, `error_categories: Vec<ErrorCategory>`, `retried_only: bool`. `TaskFilters::default()` filters nothing |
| `TaskSortField` | `EnqueuedAt`, `StartedAt`, `CompletedAt`, `FailedAt`, `Status`, `TaskName`, `QueueName`, `Priority`, `RetryCount`, `QueueSeconds`, `ExecutionSeconds` |
| `SortDirection` | `Ascending`, `Descending` |
| `TaskGroupBy` | `Worker`, `TaskName`, `Queue` |
| `ErrorCategory` | `Operational`, `Contract`, `Retrieval`, `Outcome`, `Domain` |

### The Window

`resolve_monitoring_window(since, until, now)` returns `Result<HistoryWindow, WindowRefused>`. Each argument is an `Option<DateTime<Utc>>`:

- `now` defaults to the current time, `until` to `now`, and `since` to `until` minus `MONITORING_WINDOW_DEFAULT` (24 hours).
- `since` at or after `until` is refused.
- A window longer than `MONITORING_WINDOW_MAX` (30 days) is refused.

`HistoryWindow` is `Copy`; one window can build several queries.

## MonitoringQueryError

Every read returns `MonitoringResult<T>`: `Result<T, MonitoringQueryError>`. `MonitoringQueryError` is an enum with one variant per state; each variant has `message` and `source`. The methods `code()`, `message()`, `retryable()`, `error_source()` and `failure_cause()` read them.

| Variant | `code()` | When |
|---------|----------|------|
| `DbOperationFailed { retryable, .. }` | `DbOperationFailed` | The connect or the statement failed. No statement ran after a failed connect. `retryable` is the classification of the cause |
| `HistoryContractViolation` | `HistoryContractViolation` | A stored history row does not decode (`task_stats`, `task_facets`, `task_breakdown`, `list_tasks`, `get_task_detail`, `get_workflow_node`). Not retryable: a repeat gives the same result until the row changes |

`MonitoringQueryErrorCode::EventLoopLimit` is a code of the Python client only; no variant has it.

## Cancel a Task

```rust
use horsies::monitoring::{cancel_task, TaskActionError};
use horsies::Horsies;
use uuid::Uuid;

async fn cancel_pending(app: &Horsies, task_id: Uuid) -> Result<bool, String> {
    match cancel_task(app, task_id, false).await {
        Ok(_cancelled) => Ok(true),
        Err(TaskActionError::NotCancellable { .. } | TaskActionError::NotFound { .. }) => Ok(false),
        Err(
            error @ (TaskActionError::WorkflowTask { .. }
            | TaskActionError::DbOperationFailed { .. }
            | TaskActionError::StoredDataInvalid { .. }),
        ) => Err(error.message().to_owned()),
    }
}
```

`cancel_task(&app, task_id, include_running)` moves a task that is not a workflow task to CANCELLED. It returns `TaskActionResult<TaskCancelled>`: `Result<TaskCancelled, TaskActionError>`. `TaskCancelled` has `task_id` and `was_status`.

- PENDING and CLAIMED tasks are cancelled. A RUNNING task is cancelled only with `include_running = true`: the row becomes CANCELLED, and the task code runs on until it returns.
- A workflow task is refused: use `WorkflowHandle::cancel`, `pause` and `resume`. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/#workflowhandle).

The rules are on [Action Semantics](/horsies/monitoring/action-semantics/#task-cancel).

`TaskActionError` is an enum with one variant per state; each variant has `message` and `task_id`. The methods `code()`, `message()`, `retryable()`, `task_id()` and `failure_cause()` read them.

| Variant | `code()` | When |
|---------|----------|------|
| `NotFound` | `TaskNotFound` | No live or history row has the id. Nothing changed |
| `NotCancellable { current_status, .. }` | `TaskNotCancellable` | The task is terminal, or RUNNING without `include_running`; `current_status` is the status that the cancel found. Nothing changed |
| `WorkflowTask { current_status, .. }` | `TaskIsWorkflowTask` | The task belongs to a workflow. Nothing changed |
| `DbOperationFailed { retryable, cause, .. }` | `DbOperationFailed` | The connect or the statement failed; `cause` is the `BrokerError`. Nothing was written, or the commit reply was lost. A cancel again tells which: a committed cancel answers `NotCancellable` with `Cancelled` |
| `StoredDataInvalid { cause, .. }` | `StoredDataInvalid` | Stored data does not parse; `cause` is the `OutcomeDecodeError`. Nothing changed. Not retryable |

`TaskActionErrorCode::EventLoopLimit` is a code of the Python client only; no variant has it.

## Actions as the Web Routes Answer

These functions write, and return the `ActionOutcome` that the web route returns:

| Function | Web route |
|----------|-----------|
| `cancel_task_action(&app, task_id, include_running)` | `POST /api/tasks/{id}/cancel` |
| `pause_workflow_action(&app, workflow_id)` | `POST /api/workflows/{id}/pause` |
| `resume_workflow_action(&app, workflow_id)` | `POST /api/workflows/{id}/resume` |
| `cancel_workflow_action(&app, workflow_id)` | `POST /api/workflows/{id}/cancel` |

- `ActionOutcome::status_code()` is the HTTP status; `body()` and `into_body()` return the `ActionBody`.
- `ActionBody` is `Success(ActionResponse)` (`outcome`, `was_status`, `warning`), `Conflict { code, current_status }`, `Code { code }`, `Detail { detail }` or `InternalServerError`.
- A failed connect is 503 with the cause.
- `resume_workflow_action` builds the sub-workflow children that the resume owes from the workflow definitions registered on `app`. A resume that committed and whose recovery pass failed answers success with the warning `RESUME_RECOVERY_WARNING`.

The rules of each action are on [Action Semantics](/horsies/monitoring/action-semantics/).

## Things to Avoid

**Do not read an unfiltered `TaskListPage.total` as an exact count.** It is an estimate.

**Do not cancel a RUNNING task to stop its side effects.** The cancel changes the row; the task code runs on until it returns.

**Do not build the reads of a monitoring tool on an app that installs the schema.** Build it with `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)`: see [App Config](/horsies/rust/configuration/app-config/#schema-modes).
