---
title: Monitoring Queries
summary: Read task, workflow and schedule state with the functions of horsies::monitoring, cancel a task, and run the workflow actions as the web routes answer them.
related: [health-reads, web-ui-mount, ../../monitoring/action-semantics, ../../monitoring/web-ui-overview]
tags: [rust, monitoring, task_stats, list_tasks, get_task_detail, get_workflow_run, RunTimingInfo, NodeTimingInfo, AttemptPhasesInfo, cancel_task, MonitoringQueryError]
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
| `get_task_detail(&app, task_id)` | `TaskDetailRead`: `Found(Box<TaskDetail>)` or `NotFound(TaskDetailNotFound { task_id, reason })` |
| `list_workflow_names(&app)` | `Vec<String>`: the distinct names of the root runs, at most `LIST_ROW_BOUND` (10,000) |
| `list_workflow_runs(&app, &WorkflowRunsQuery)` | `Vec<WorkflowRunSummary>`: the newest root runs |
| `get_workflow_run(&app, workflow_id)` | `Option<WorkflowRunDetail>`: the run with its nodes, edges and phase timing; `None` when no run has the id |
| `get_workflow_node(&app, workflow_id, task_index)` | `Option<WorkflowTaskDetail>`: the node with its task and the task's attempts; `None` when the run has no such node |
| `list_schedules(&app)` | `Vec<ScheduleStateInfo>`: every schedule state, soonest next run first, at most `LIST_ROW_BOUND` (10,000) |

- `TaskListPage.total` is exact when any filter is set. With no filter it is the live estimate plus the planner's estimate of the history window.
- A detail read looks in the live table and in the history, so a task that ended still answers. `TaskDetailNotFound.reason` is a `TaskAbsenceReason`: see [Retrieving Results](/horsies/rust/tasks/retrieving-results/#readerror).
- `TaskDetail` has the task's `leaf` (status, times, retry counts, worker, deadline, execution class), its name, queue and priority, its error category, its `attempts`, and its workflow and node index when it is a workflow task.
- Each `TaskAttemptInfo` has `phases`, the phase record of the attempt row (`AttemptPhasesInfo`).
- `WorkflowRunDetail` has `timing` (`RunTimingInfo`), and each `WorkflowNodeInfo` has `timing` (`NodeTimingInfo`). See [Phase Timing](#phase-timing).

## Phase Timing

The lifecycle record keeps the phase times of each attempt and the events of each workflow run. `get_workflow_run` composes the timing of the run and of each node from them, in the core, from the rows of one statement. No statement runs per node or per task. Each model holds times, not durations: subtract the two times of a segment.

### Attempt Phases

`TaskAttemptInfo.phases` is an `AttemptPhasesInfo`:

| Variant | Fields |
|---------|--------|
| `NotCaptured` | The attempt row was written before the lifecycle record (migration 1001), or the history row holds a version-1 attempt archive |
| `Captured` | `enqueued_at` (the due time), `claimed_at` (the claim that ran the attempt), `claims` (`Counted { count, first_claimed_at }` or `NotCounted`), `finalizing` (`Marked { finalizing_at }` or `NotMarked`), `recorded_by` (`Worker { placement }` with `Placed { placed_at }` or `NotObserved`; `Recovery { last_seen_at }`; or `Cancel`) |

A count above 1 holds lost leases and released claims. `placed_at` is the claim time plus the slot wait that the worker measured. `Recovery` means that stale RUNNING recovery wrote the attempt row; `last_seen_at` is the last time the attempt was known alive. `Cancel` means that the cancel of a RUNNING task wrote the attempt row, with the outcome `Cancelled`; its `finished_at` is the time of the cancel.

### Node Timing

`WorkflowNodeInfo.timing` is a `NodeTimingInfo`. Its variant is the coverage label of the node:

| Variant | Fields | When |
|---------|--------|------|
| `Exact` | `segments`, `endpoint` | Every boundary is a recorded time, and no segment is `Unknown` |
| `Partial` | `reasons`, `segments`, `endpoint` | At least one segment is `Unknown`. `reasons` holds each `UnknownReason` once, in name order |
| `Unavailable` | `reason` | The record cannot show the timing of the run (`UnavailableReason`). No segment |
| `Inconsistent` | `conflict` | The record of the node contradicts itself (`TimingConflictInfo`). No segment |

Each `TimingSegmentInfo` has `phase`, `started_at`, `end` (`Closed { at }` or `Open`) and `source` (`Node`; `Task { generation, task_id }`; or `Attempt { generation, task_id, attempt }`). `generation` counts the tasks of the node from 1: a pause abandon or a resume gives a node a new task.

| `SegmentPhaseInfo` | Meaning |
|--------------------|---------|
| `Pending` | The node waits for its dependencies |
| `Ready` | The dependencies are met and nothing is dispatched (a root at the start, a pause gate, an abandon, a refused child start) |
| `Enqueued` | The attempt is due and waits for a claim; a sub-workflow node waits for its child start |
| `RetryWait` | The retry delay of the attempt has not passed |
| `Claimed { claim }` | A worker holds a claim and the task code did not start. `claim` is `Earlier { count }` (the claims before the claim that holds the attempt), `Recorded { placement }` (the claim of the attempt row) or `NotRecorded` (the attempt has no row yet, or a cancel, an expiry or a pause abandon ended it) |
| `Running` | The task code runs; for a sub-workflow node, the child run runs |
| `Finalizing` | The task body ended and the attempt is not recorded yet |
| `Advancing` | The task is in history (or the child run ended) and the workflow has not applied the node yet |
| `Unknown { reason }` | A known interval whose phase the record cannot show |

`endpoint` is `Terminal { status, at }` (`status` is a `TerminalNodeStatus`: `Completed`, `Failed` or `Skipped`; `at` is the completion time of the node) or `NotTerminal`. The endpoint is a marker, not a segment. A node that a workflow cancel, a failure or the recovery limit ended while its task went on keeps the segments of that task, so the endpoint can come before the end of the segments.

| `UnknownReason` | The segment |
|-----------------|-------------|
| `AttemptNotCaptured` | Is an attempt row with no phase record |
| `ClaimsNotCounted` | Is the queue wait of an attempt whose claims were not counted; earlier claims can hide in it |
| `HistoryPruned` | Runs to the time the task moved to history; retention removed its history row |
| `WorkerLost` | Runs from the last sign of life of a recovered attempt to its recovery |

`TimingConflictInfo` is `ClockOrder { earlier, earlier_at, later, later_at }` (two `TimingBoundary` values in the wrong order: `earlier` comes first in the record, and its time is after the time of `later`; a step of the database clock can cause it), `MissingRecord { record }` (the state of the node needs a record that is not there) or `UnexpectedEvent { event_id, event }` (an event that the state of the node or the run does not allow). The composer does not reorder times and does not guess a duration.

### Run Timing

`WorkflowRunDetail.timing` is a `RunTimingInfo`:

- `Unavailable { as_of, captured_since, reason }`: the run began before `captured_since` (`RunBeforeCapture`), or it has more than 100,000 events (`TimingBoundExceeded`). Every node is `Unavailable` with the same reason.
- `Composed { as_of, captured_since, coverage, range_started_at, range_ended_at, open_segments, pauses }`:
  - `as_of` is the database time of the read. An open segment ends there for drawing.
  - `captured_since` is the start of the lifecycle record: the install time of migration 1001.
  - `coverage` is `Exact`, `Partial { reasons }` or `Inconsistent { conflicts }`. Each `RunConflictInfo` is `Node { task_index }` or `Pause { conflict }`.
  - `range_started_at` is the creation of the run. `range_ended_at` is the latest segment end (an open segment at `as_of`), endpoint, pause end and run end. A task that runs on after a workflow cancel makes the range longer than the run.
  - `open_segments` is true when a segment or a pause is open.
  - `pauses` holds one `PauseIntervalInfo { started_at, end }` per pause. `end` is `Resumed { at }`, `RunEnded { at }` (a cancel or a paused expiry ended the run) or `Open`.

The events of a run and its nodes are deleted with the run. A history class shorter than the workflow retention removes task rows first: the node then shows `Partial` with `HistoryPruned`.

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
| `HistoryContractViolation` | `HistoryContractViolation` | A stored history row does not decode (`task_stats`, `task_facets`, `task_breakdown`, `list_tasks`, `get_task_detail`, `get_workflow_run`, `get_workflow_node`); `source` is a `HistoryContractSource` that holds the error. Not retryable: a repeat gives the same result until the row changes |

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

- PENDING and CLAIMED tasks are cancelled. A RUNNING task is cancelled only with `include_running = true`: the row becomes CANCELLED with an attempt of outcome `Cancelled`, and the worker gives the cancel to the task code (`TaskRuntime::cancelled()`, `is_cancelled()` and `cancel_state()`; Python `asyncio.CancelledError` and `runtime.cancel_state()`). The code stops when it reads the cancel. An `isolated` task with `cancel_grace_ms` is killed when it has not stopped `cancel_grace_ms` later.
- A workflow task is refused: use `WorkflowHandle::cancel`, `pause` and `resume`. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/#workflowhandle).

The rules are on [Action Semantics](/horsies/monitoring/action-semantics/#task-cancel).

`TaskActionError` is an enum with one variant per state; each variant has `message` and `task_id`. The methods `code()`, `message()`, `retryable()`, `task_id()` and `failure_cause()` read them.

| Variant | `code()` | When |
|---------|----------|------|
| `NotFound` | `TaskNotFound` | No live or history row has the id. Nothing changed |
| `NotCancellable { current_status, .. }` | `TaskNotCancellable` | The task is terminal, or RUNNING without `include_running`; `current_status` is the status that the cancel found. Nothing changed |
| `WorkflowTask { current_status, .. }` | `TaskIsWorkflowTask` | The task belongs to a workflow. Nothing changed |
| `DbOperationFailed { retryable, cause, .. }` | `DbOperationFailed` | The connect or the statement failed; `cause` is the `Box<BrokerError>`. Nothing was written, or the commit reply was lost. A cancel again tells which: a committed cancel answers `NotCancellable` with `Cancelled` |
| `StoredDataInvalid { cause, .. }` | `StoredDataInvalid` | Stored data does not parse; `cause` is the `OutcomeDecodeError`. Nothing changed. Not retryable |

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
- A database failure, and a failed connect, answer with the cause: 503 when the cause may clear (`retryable`), 500 when it stays (for example SQLSTATE 23514, a CHECK constraint, or a connection setting that the core refuses). The rule is on [Action Semantics](/horsies/monitoring/action-semantics/#conflict-handling).
- `resume_workflow_action` builds the sub-workflow children that the resume owes from the workflow definitions registered on `app`. A resume that committed and whose recovery pass failed answers success with the warning `RESUME_RECOVERY_WARNING`.

The rules of each action are on [Action Semantics](/horsies/monitoring/action-semantics/).

## Things to Avoid

**Do not read an unfiltered `TaskListPage.total` as an exact count.** It is an estimate.

**Do not cancel a RUNNING task to undo its side effects.** The cancel stops the task code when the code reads it; the effects that the code made before then stay. Code that does not read the cancel runs on until it returns, or until a kill.

**Do not build the reads of a monitoring tool on an app that installs the schema.** Build it with `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)`: see [App Config](/horsies/rust/configuration/app-config/#schema-modes).
