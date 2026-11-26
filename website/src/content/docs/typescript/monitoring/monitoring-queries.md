---
title: Monitoring Queries
summary: Read task, workflow and schedule state with the reads of app.monitoring, read the phase timing of a workflow run, and cancel a task with app.monitoring.cancelTask.
related: [health-reads, web-ui-mount, ../../monitoring/action-semantics, ../../monitoring/web-ui-overview]
tags: [typescript, monitoring, taskStats, listTasks, taskDetail, workflowRun, cancelTask, MonitoringReadError, QUERY_REFUSED]
---

# Monitoring Queries

`app.monitoring` holds the reads and the cancel that the monitoring web UI serves. Each read connects the app's handle (in a worker child, the child's producer handle) and runs the core's read of the same name. A read sends one statement, writes nothing and does not install the schema. The health reads (worker states, stale tasks, class coverage) are on [Health Reads](/horsies/typescript/monitoring/health-reads/).

## Read Tasks

```ts
import { type Horsies, type MonitoringReadError, TaskId } from "horsies";

function readErrorText(error: MonitoringReadError): string {
  switch (error.code) {
    case "QUERY_REFUSED":
      return `the read refused ${error.field}: ${error.reason}`;
    case "DB_OPERATION_FAILED":
      return error.retryable ? `the read may pass again: ${error.message}` : error.message;
    case "HISTORY_CONTRACT_VIOLATION":
      return error.message;
  }
}

async function failedTasksOfLastDay(app: Horsies): Promise<void> {
  const stats = await app.monitoring.taskStats();

  if (!stats.ok) {
    console.error(readErrorText(stats.error));

    return;
  }

  for (const count of stats.value) {
    console.log(`${count.status}: ${count.count}`);
  }

  const page = await app.monitoring.listTasks({
    statuses: ["FAILED"],
    sort: { field: "failedAt", direction: "descending" },
    offset: 0,
    limit: 50,
  });

  if (!page.ok) {
    console.error(readErrorText(page.error));

    return;
  }

  for (const task of page.value.rows) {
    console.log(task.id, task.taskName, task.errorCode ?? "no error code");
  }
}
```

A query that leaves out a field takes the core's default: the window of the last 24 hours, every status, the default sort and page. The package holds no copy of a default.

## Reads

| Read | Returns (`Promise<Result<T, E>>`) | `E` |
|------|-----------------------------------|-----|
| `taskStats(query?)` | `readonly StatusCount[]` (`status`, `count`): each of the seven task statuses in a fixed order, over the live table and the history window | `MonitoringReadError` |
| `taskFacets(query?)` | `Facets`: `workers`, `taskNames`, `queues` (each a `FacetValue`: `value`, `count`), `errorCodes` (`ErrorFacet`: `value`, `category`, `count`) and `errorCategoryTotals` (`ReadonlyMap<string, number>`) | `MonitoringReadError` |
| `taskBreakdown(query)` | `Breakdown`: `groupBy`, `groups` (one `GroupRow` per group, highest total first), `total` and `groupCount` | `MonitoringReadError` |
| `listTasks(query?)` | `TaskListPage`: `rows: readonly TaskSummary[]` and `total` | `MonitoringReadError` |
| `taskDetail(taskId)` | `TaskDetailRead`: `{ kind: "found", detail }` or `{ kind: "notFound", taskId, reason }` | `MonitoringQueryCoreError` |
| `workflowNames()` | `readonly string[]`: the distinct names of the root runs, at most 10,000 | `MonitoringQueryCoreError` |
| `workflowRuns(query?)` | `readonly WorkflowRunSummary[]`: the newest root runs | `MonitoringReadError` |
| `workflowRun(workflowId)` | `WorkflowRunRead`: `{ kind: "found", run }` (the run with its nodes, edges and phase timing) or `{ kind: "notFound", workflowId }` | `MonitoringQueryCoreError` |
| `workflowNode(workflowId, taskIndex)` | `WorkflowNodeRead`: `{ kind: "found", node }` (the node with its task and the task's attempts) or `{ kind: "notFound", workflowId, taskIndex }` | `MonitoringReadError` |
| `schedules()` | `readonly ScheduleStateInfo[]`: every schedule state, soonest next run first, at most 10,000 | `MonitoringQueryCoreError` |

- `TaskListPage.total` is exact when any filter is set. With no filter it is the live estimate plus the planner's estimate of the history window.
- `GroupRow` has the `group` (`{ kind: "value", value }`, or `{ kind: "unclaimed" }` for the tasks with no worker in a breakdown by worker) and the counts `pending`, `claimed`, `running`, `completed`, `failed`, `cancelled`, `expired`, `retried` and `total`.
- `TaskSummary` has `id`, `taskName`, `queueName`, `status`, `priority`, `retryCount`, `maxRetries`, `isWorkflowTask`, `workerId`, `workerHostname`, `errorCode`, `errorCategory`, the times `enqueuedAt`, `startedAt`, `completedAt` and `failedAt` (`Temporal.Instant | null`), and the durations `queueS` and `execS` in seconds (`number | null`).
- A detail read looks in the live table and in the history, so a task that ended still answers. `reason` of `notFound` is a `TaskAbsenceReason`: see [Retrieving Results](/horsies/typescript/tasks/retrieving-results/).
- `TaskDetail` has the task's `leaf` (`LeafTaskInfo`: status, times, retry counts, worker, deadline, execution class, trace id), its `taskName`, `queueName` and `priority`, its `errorCategory`, its `attempts`, and its `workflowId` and `workflowTaskIndex` when it is a workflow task.
- Each `TaskAttemptInfo` has `attempt`, `outcome`, `willRetry`, `startedAt`, `finishedAt`, `errorCode`, `errorMessage`, `failedReason`, `workerHostname` and `phases`, the phase record of the attempt row.
- `WorkflowRunDetail` has `run` (`WorkflowRunSummary`), `nodes`, `edges` (`fromIndex`, `toIndex`), `failedCount`, `failedIndices`, `traceId` and `timing`. Each `WorkflowNodeInfo` has `timing`. See [Phase Timing](#phase-timing).
- A field that the core stores as nullable is `T | null` in a read model, as the core writes it.

### Read One Task

```ts
async function attemptsOf(app: Horsies, text: string): Promise<void> {
  const taskId = TaskId.parse(text);

  if (!taskId.ok) {
    console.error(taskId.error.detail);

    return;
  }

  const read = await app.monitoring.taskDetail(taskId.value);

  if (!read.ok) {
    console.error(read.error.message);

    return;
  }

  switch (read.value.kind) {
    case "notFound":
      console.log(`no task ${read.value.taskId}: ${read.value.reason}`);
      break;
    case "found":
      for (const attempt of read.value.detail.attempts) {
        console.log(attempt.attempt, attempt.outcome, attempt.phases.kind);
      }

      break;
  }
}
```

`TaskId.parse` and `WorkflowId.parse` take the canonical UUID text and refuse other text with `{ kind: "notCanonicalUuid", detail }`.

## Queries

Each query is a plain object. Every field is optional except `groupBy` of a breakdown.

| Query | Fields |
|-------|--------|
| `TaskFilters` | `window`, `statuses: readonly TaskStatus[]`, `taskNames`, `queues`, `workers`, `errorCodes: readonly string[]`, `errorCategories: readonly ErrorCategory[]`, `retried: "any" \| "retriedOnly"`. A list that is left out or empty keeps every value |
| `TaskStatsQuery` | `TaskFilters` without `statuses`: the read counts every status |
| `TaskFacetsQuery` | `window`, `statuses`, `errorCategories`, `retried`. Only the coarse filters, so one selection never empties the option lists of the others |
| `TaskBreakdownQuery` | `TaskFilters`, `groupBy: TaskGroupBy` (required), `limit` |
| `TaskListQuery` | `TaskFilters`, `sort: { field, direction }`, `offset`, `limit` |
| `WorkflowRunsQuery` | `name`, `status` (a `WorkflowStatus`), `limit` |

| Type | Values |
|------|--------|
| `TaskSortField` | `"enqueuedAt"`, `"startedAt"`, `"completedAt"`, `"failedAt"`, `"status"`, `"taskName"`, `"queueName"`, `"priority"`, `"retryCount"`, `"queueSeconds"`, `"executionSeconds"` |
| `SortDirection` | `"ascending"`, `"descending"` |
| `TaskGroupBy` | `"worker"`, `"taskName"`, `"queue"` |
| `ErrorCategory` | `"OPERATIONAL"`, `"CONTRACT"`, `"RETRIEVAL"`, `"OUTCOME"`, `"DOMAIN"` |

The core checks the bounds and refuses a value outside them with `QUERY_REFUSED`, before any connect:

| Field | Accepts | Default | `field` of the refusal |
|-------|---------|---------|------------------------|
| `TaskBreakdownQuery.limit` | 1 to `MAX_BREAKDOWN_LIMIT` (500) | 50 groups | `limit` |
| `TaskListQuery.offset`, `TaskListQuery.limit` | `offset` 0 or more; `limit` 1 to `MAX_TASK_PAGE_LIMIT` (200); `offset + limit` at most `MAX_TASK_PAGE_REACH` (500) | offset 0, limit 50 | `page` |
| `TaskListQuery.sort` | A `TaskSort` | `enqueuedAt`, `descending` | - |
| `WorkflowRunsQuery.limit` | 1 to `MAX_WORKFLOW_RUNS_LIMIT` (200) | 30 runs | `limit` |
| `workflowNode` `taskIndex` | A whole number from -2,147,483,648 to 2,147,483,647 | - | `taskIndex` |

A count that is not a whole number, and a text that is not well-formed (it holds a lone surrogate) in `taskNames`, `queues`, `workers`, `errorCodes` or `name`, are refused with `QUERY_REFUSED` before the core sees them. `field` names the field.

### The Window

`MonitoringWindow` has `since` and `until`, each an optional `Temporal.Instant`:

- `until` defaults to now, and `since` to `until` minus `MONITORING_WINDOW_DEFAULT` (24 hours).
- `since` at or after `until` is refused.
- A window longer than 30 days is refused.
- A part of an instant below the microsecond is cut toward the past: the core stores microseconds.

A refused window is `QUERY_REFUSED` with the `field` `window`. The window selects the history rows of tasks that ended in it; every live row is read.

```ts
async function failedInLastSixHours(app: Horsies): Promise<number | undefined> {
  const since = Temporal.Now.instant().subtract({ hours: 6 });
  const stats = await app.monitoring.taskStats({ window: { since } });

  return stats.ok ? stats.value.find((count) => count.status === "FAILED")?.count : undefined;
}
```

## Phase Timing

The lifecycle record keeps the phase times of each attempt and the events of each workflow run. `workflowRun` composes the timing of the run and of each node from them, in the core, from the rows of one statement. No statement runs per node or per task. Each model holds times, not durations: subtract the two times of a segment. What each phase measures is on [Web UI Overview](/horsies/monitoring/web-ui-overview/#phase-timing).

Every variant has a `kind`; switch on it.

### Attempt Phases

`TaskAttemptInfo.phases` is an `AttemptPhasesInfo`:

| `kind` | Fields |
|--------|--------|
| `"notCaptured"` | The attempt row was written before the lifecycle record (migration 1001), or the history row holds a version-1 attempt archive |
| `"captured"` | `enqueuedAt` (the due time), `claimedAt` (the claim that ran the attempt), `claims`, `finalizing`, `recordedBy` |

| Field | Variants |
|-------|----------|
| `claims` | `{ kind: "counted", count, firstClaimedAt }` or `{ kind: "notCounted" }`. A count above 1 holds lost leases and released claims |
| `finalizing` | `{ kind: "marked", finalizingAt }` or `{ kind: "notMarked" }` |
| `recordedBy` | `{ kind: "worker", placement }`, `{ kind: "recovery", lastSeenAt }` or `{ kind: "cancel" }` |
| `placement` | `{ kind: "placed", placedAt }` (the claim time plus the slot wait that the worker measured) or `{ kind: "notObserved" }` |

`recovery` means that stale `RUNNING` recovery wrote the attempt row; `lastSeenAt` is the last time the attempt was known alive. `cancel` means that the cancel of a `RUNNING` task wrote the attempt row, with the outcome `CANCELLED`; its `finishedAt` is the time of the cancel.

### Node Timing

`WorkflowNodeInfo.timing` is a `NodeTimingInfo`. Its `kind` is the coverage label of the node:

| `kind` | Fields | When |
|--------|--------|------|
| `"exact"` | `segments`, `endpoint` | Every boundary is a recorded time, and no segment is `unknown` |
| `"partial"` | `reasons`, `segments`, `endpoint` | At least one segment is `unknown`. `reasons` holds each `UnknownReason` once, in name order |
| `"unavailable"` | `reason` (`UnavailableReason`) | The record cannot show the timing of the run. No segment |
| `"inconsistent"` | `conflict` (`TimingConflictInfo`) | The record of the node contradicts itself. No segment |

Each `TimingSegmentInfo` has `phase`, `startedAt`, `end` (`{ kind: "ended", at }` or `{ kind: "open" }`) and `source` (`{ kind: "node" }`, `{ kind: "task", generation, taskId }` or `{ kind: "attempt", generation, taskId, attempt }`). `generation` counts the tasks of the node from 1: a pause abandon or a resume gives a node a new task.

| `phase.kind` | Meaning |
|--------------|---------|
| `"pending"` | The node waits for its dependencies |
| `"ready"` | The dependencies are met and nothing is dispatched |
| `"enqueued"` | The attempt is due and waits for a claim; a sub-workflow node waits for its child start |
| `"retryWait"` | The retry delay of the attempt has not passed |
| `"claimed"` | A worker holds a claim and the task code did not start. `claim` is `{ kind: "earlier", count }` (the claims before the claim that holds the attempt), `{ kind: "recorded", placement }` (the claim of the attempt row) or `{ kind: "notRecorded" }` |
| `"running"` | The task code runs; for a sub-workflow node, the child run runs |
| `"finalizing"` | The task body ended and the attempt is not recorded yet |
| `"advancing"` | The task is in history (or the child run ended) and the workflow has not applied the node yet |
| `"unknown"` | A known interval whose phase the record cannot show; `reason` is an `UnknownReason` |

`endpoint` is `{ kind: "terminal", status, at }` (`status` is `"COMPLETED"`, `"FAILED"` or `"SKIPPED"`; `at` is the completion time of the node) or `{ kind: "notTerminal" }`. The endpoint is a marker, not a segment. A node that a workflow cancel, a failure or the recovery limit ended while its task went on keeps the segments of that task, so the endpoint can come before the end of the segments.

| `UnknownReason` | The segment |
|-----------------|-------------|
| `"ATTEMPT_NOT_CAPTURED"` | Is an attempt row with no phase record |
| `"CLAIMS_NOT_COUNTED"` | Is the queue wait of an attempt whose claims were not counted; earlier claims can hide in it |
| `"HISTORY_PRUNED"` | Runs to the time the task moved to history; retention removed its history row |
| `"WORKER_LOST"` | Runs from the last sign of life of a recovered attempt to its recovery |

`TimingConflictInfo` is one of:

- `{ kind: "clockOrder", earlier, earlierAt, later, laterAt }`: two `TimingBoundary` values in the wrong order. `earlier` comes first in the record, and its time is after the time of `later`. A step of the database clock can cause it.
- `{ kind: "missingRecord", record }`: the state of the node needs a record that is not there (`MissingRecord`).
- `{ kind: "unexpectedEvent", eventId, event }`: an event (`WorkflowEventKind`) that the state of the node or the run does not allow.

The composer does not reorder times and does not guess a duration.

### Run Timing

`WorkflowRunDetail.timing` is a `RunTimingInfo`:

- `{ kind: "unavailable", asOf, capturedSince, reason }`: the run began before `capturedSince` (`"RUN_BEFORE_CAPTURE"`), or it has more than 100,000 events (`"TIMING_BOUND_EXCEEDED"`). Every node is `unavailable` with the same reason.
- `{ kind: "composed", asOf, capturedSince, coverage, rangeStartedAt, rangeEndedAt, openSegments, pauses }`:
  - `asOf` is the database time of the read. An open segment ends there for drawing.
  - `capturedSince` is the start of the lifecycle record: the install time of migration 1001.
  - `coverage` is `{ kind: "exact" }`, `{ kind: "partial", reasons }` or `{ kind: "inconsistent", conflicts }`. Each `RunConflictInfo` is `{ kind: "node", taskIndex }` or `{ kind: "pause", conflict }`.
  - `rangeStartedAt` is the creation of the run. `rangeEndedAt` is the latest segment end (an open segment at `asOf`), endpoint, pause end and run end. A task that runs on after a workflow cancel makes the range longer than the run.
  - `openSegments` is `true` when a segment or a pause is open.
  - `pauses` holds one `PauseIntervalInfo` (`startedAt`, `end`) per pause. `end` is `{ kind: "resumed", at }`, `{ kind: "runEnded", at }` (a cancel or a paused expiry ended the run) or `{ kind: "open" }`.

The events of a run and its nodes are deleted with the run. A history class shorter than the workflow retention removes task rows first: the node then shows `partial` with `HISTORY_PRUNED`.

## Errors

| Error type | Members |
|------------|---------|
| `MonitoringQueryCoreError` | `MonitoringQueryDbOperationFailed`, `MonitoringQueryHistoryContractViolation` |
| `MonitoringReadError` | `MonitoringQueryCoreError`, `QueryRefused` |

| `code` | Type | When |
|--------|------|------|
| `DB_OPERATION_FAILED` | `MonitoringQueryDbOperationFailed` | The connect or the statement failed. No statement ran after a failed connect. `retryable` is the classification of the cause; `exception` holds the cause |
| `HISTORY_CONTRACT_VIOLATION` | `MonitoringQueryHistoryContractViolation` | A stored history row does not decode. Not retryable: a repeat gives the same result until the row changes |
| `QUERY_REFUSED` | `QueryRefused` | An argument that the read refuses before any connect: no statement ran. `field` is a `QueryRefusedField` (`window`, `page`, `limit`, `taskNames`, `queues`, `workers`, `errorCodes`, `workflowName`, `taskIndex`); `reason` names the rule. It has no `message`. Not retryable |

A read whose query has nothing to refuse (`taskDetail`, `workflowNames`, `workflowRun`, `schedules`) returns `MonitoringQueryCoreError`. A process-state refusal of the core throws a `CoreError`, and a defect crashes loud with a `CoreDefect`: see [Task Errors](/horsies/typescript/tasks/errors/).

## Cancel a Task

```ts
async function cancelPending(app: Horsies, taskId: TaskId): Promise<boolean> {
  const cancelled = await app.monitoring.cancelTask(taskId, { policy: "pendingOrClaimedOnly" });

  if (cancelled.ok) {
    console.log(`cancelled ${cancelled.value.taskId}, was ${cancelled.value.wasStatus}`);

    return true;
  }

  switch (cancelled.error.code) {
    case "TASK_NOT_FOUND":
    case "TASK_NOT_CANCELLABLE":
      return false;
    case "TASK_IS_WORKFLOW_TASK":
    case "DB_OPERATION_FAILED":
    case "STORED_DATA_INVALID":
      console.error(cancelled.error.message);

      return false;
  }
}
```

`app.monitoring.cancelTask(taskId, { policy })` moves a task that no workflow holds to `CANCELLED`, in one statement. It returns `Promise<Result<TaskCancelled, CancelTaskError>>`. `TaskCancelled` has `taskId` and `wasStatus` (`"PENDING"`, `"CLAIMED"` or `"RUNNING"`).

| `policy` (`CancelPolicy`) | Cancels |
|---------------------------|---------|
| `"pendingOrClaimedOnly"` | `PENDING` and `CLAIMED` tasks: no task code ran |
| `"includeRunning"` | `PENDING`, `CLAIMED` and `RUNNING` tasks. The statement records the running attempt with the outcome `CANCELLED`, and the body sees `runtime.signal` abort with the cause `OPERATOR` |

The policy has no default: the caller names whether a running body is cancelled. The body stops when it reads the signal. See [Cancellation](/horsies/typescript/tasks/cancellation/). A workflow task is refused: use `cancel`, `pause` and `resume` of the workflow handle, on [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/). The rules are on [Action Semantics](/horsies/monitoring/action-semantics/#task-cancel).

`CancelTaskError` is `TaskActionCoreError`. Each member has `code`, `message`, `retryable` and `taskId`:

| `code` | Type | When |
|--------|------|------|
| `TASK_NOT_FOUND` | `TaskActionNotFound` | No live or history row has the id. Nothing changed |
| `TASK_NOT_CANCELLABLE` | `TaskActionNotCancellable` | The task is terminal, or `RUNNING` with `pendingOrClaimedOnly`; `currentStatus` is the status that the cancel found. Nothing changed |
| `TASK_IS_WORKFLOW_TASK` | `TaskActionWorkflowTask` | The task belongs to a workflow; `currentStatus` is its status. Nothing changed |
| `DB_OPERATION_FAILED` | `TaskActionDbOperationFailed` | The connect or the statement failed; `exception` holds the cause. Nothing was written, or the commit reply was lost. A cancel again tells which: a committed cancel answers `TASK_NOT_CANCELLABLE` with `CANCELLED` |
| `STORED_DATA_INVALID` | `TaskActionStoredDataInvalid` | Stored data does not parse; `exception` holds the cause. Nothing changed. Not retryable |

## Things to Avoid

**Do not read an unfiltered `TaskListPage.total` as an exact count.** It is an estimate.

**Do not cancel a `RUNNING` task to undo its side effects.** The cancel stops the task code when the code reads `runtime.signal`; the effects that the code made before then stay. Code that does not read the signal runs on until it returns, or until a kill: see [Cancelling a RUNNING task](/horsies/monitoring/action-semantics/#cancelling-a-running-task).

**Do not branch on `message` of a monitoring error.** Branch on `code`. `QueryRefused` has no `message`; read its `field` and `reason`.
