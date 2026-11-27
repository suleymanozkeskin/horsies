# horsies (TypeScript) — Monitoring

Reference for the health reads on the app, the monitoring reads of
`app.monitoring`, the task cancel, the web UI (`createMonitoringHandler` and
the `web` command), the fleet metrics (the `metrics` command and the
worker's `--metrics-listen`), and the rerun of a terminal task
(`app.rerunTask`, `app.rerunTaskIn` and the `pg` and `postgres` adapters). The core
serves every route, guard and event of the web UI, so a TypeScript app, a
Python app and a Rust app answer alike.

Every call in this file returns a `Result`: `{ ok: true, value }` or
`{ ok: false, error }`. Each error is a closed union told apart by `kind`
(the variant name, named after its code), with `code` and `retryable`. A process-state refusal of the core throws `CoreError`; a
defect of the package throws `CoreDefect` (see `SKILL.md`).

## Health Reads on the App

<!-- ts-check: alone -->
```ts
import type { Horsies } from "horsies";

export async function checkHealth(app: Horsies): Promise<boolean> {
  const ping = await app.pingDatabase();

  if (!ping.ok) {
    console.error(`database: ${ping.error.message}`);

    return false;
  }

  const pongs = await app.pingWorkers({ waitMs: 2_000, minResponses: { kind: "atLeast", count: 1 } });

  if (!pongs.ok) {
    switch (pongs.error.kind) {
      case "invalidInput":
        // `subject` is the read; `cause` names the argument and the rule.
        console.error(`workers: ${pongs.error.subject}: ${pongs.error.cause}`);
        break;
      case "listenerDisabled":
      case "workerPingFailed":
      case "seatRelayFull":
        console.error(`workers: ${pongs.error.message}`);
        break;
      default: {
        const unknownError: never = pongs.error;

        throw new Error(`no case for ${String(unknownError)}`);
      }
    }

    return false;
  }

  console.log(`database ${ping.value.latencyMs} ms, ${pongs.value.length} workers answered`);

  return true;
}
```

| Method | Returns |
|---|---|
| `app.pingDatabase()` | `DatabasePing` (`latencyMs`): a timed `SELECT 1` round trip. Error: `DATABASE_PING_FAILED` |
| `app.pingWorkers(options?)` | `readonly WorkerPong[]` (`workerId`, `hostname`, `pid`, `roundTripMs`), the distinct replies by worker id |
| `app.monitoring.listWorkerStates()` | `readonly WorkerStateSnapshot[]`: the latest snapshot of each worker that wrote one |
| `app.monitoring.getWorkerState(workerId)` | `WorkerStateRead`: `{ kind: "found", snapshot }` or `{ kind: "notFound", workerId }` |
| `app.monitoring.getWorkerStateHistory(workerId, limit?)` | `readonly WorkerStateSnapshot[]`, newest first. `WorkerHistoryLimit`: `{ kind: "allRetained" }` (default) or `{ kind: "newest", count }` |
| `app.monitoring.getStaleTasks({ thresholdMinutes })` | `BoundedTaskList<StaleTaskRow>`: `RUNNING` tasks with no beat for `thresholdMinutes` (whole minutes, 0 to 525,600) |
| `app.monitoring.getExpiredTasks()` | `BoundedTaskList<ExpiredTaskRow>`: `PENDING` tasks past their `goodUntil`, oldest deadline first |
| `app.monitoring.getExecutionClassCoverage()` | `ExecutionClassCoverage`: whether a live worker can run each pending stream (a queue and an execution class) |
| `app.monitoring.getExecutionClassReport()` | `ExecutionClassReport`: the pending streams, the budget load of each live worker, and `liveWindow` (how recent a snapshot of a live worker is) |

- `PingWorkersOptions`: `target` (`{ kind: "everyWorker" }`, the default, or
  `{ kind: "worker", workerId }`), `waitMs` (a whole number above 0,
  default `DEFAULT_PING_WAIT_MS`, 2,000), `minResponses`
  (`{ kind: "fullWindow" }`, the default, or `{ kind: "atLeast", count }`).
  The ping returns at the first of: the target worker replied, `count`
  workers replied, the window ended. A worker of `listWorkerStates()` that is not
  in the answer did not reply.
- `pingWorkers` errors: `INVALID_INPUT`, `LISTENER_DISABLED` (the core
  handle of this process runs no listener) and `WORKER_PING_FAILED`.
- The other health reads give `MONITORING_QUERY_FAILED`; `getWorkerState`,
  `getWorkerStateHistory` and `getStaleTasks` add `INVALID_INPUT`.
- `INVALID_INPUT` of a health read is one variant, `HealthInputRefused`
  (`kind: "invalidInput"`): `subject` is the read (`pingWorkers`, `worker`,
  `workerHistory`, `staleTasks`) and `cause` names the argument and the
  rule: a worker id that holds a lone surrogate, or a `waitMs`, a `count`
  or a threshold outside its rule. It is answered before any connect, and
  nothing ran.
- Each health read connects the app's handle and installs the schema once
  per handle, as a send does; then it reads only. The database ping reads
  only.
- `BoundedTaskList<T>` is `{ kind: "complete", rows }` or
  `{ kind: "truncated", rows, bound }`, at most `TASK_LIST_ROW_BOUND` (10,000) rows.
- `WorkerStateSnapshot.profile` is `{ kind: "named", name }` (the worker
  profile that the worker runs) or `{ kind: "notRecorded" }` (a worker of a
  build before schema migration 1004). A value that the worker could not
  measure is `{ kind: "notMeasured" }`; a 64-bit count is a `bigint`; an
  instant is a `Temporal.Instant`.
- `WorkerStateSnapshot` also gives `childCount`, `claimLimits`
  (`ClaimLimits`: `perQueuePerPass` and `claimedPerWorker`, each a
  `TaskLimit`; `claimedPerWorker` is the limit that the worker resolved,
  the seat count when its profile sets none), `clusterWideCap`
  (`ClusterCapSetting`) and `executionClassBudgets`
  (`ExecutionClassBudget[]`). `StaleTaskRow` and `ExpiredTaskRow` name the
  task `taskId`; `StaleTaskRow.lastHeartbeatAt` is a `LastBeat`.
- A pending stream names its `queue`, its `execution` and its `coverage`.
  Its `wait` is `{ kind: "due", oldestDueAt, wait }` or
  `{ kind: "noneDue", nextDueAt }`.
- The meaning of each read is on the shared docs page
  `https://suleymanozkeskin.github.io/horsies/monitoring/worker-health/`.

## Monitoring Reads — `app.monitoring`

Each read connects the app's handle and sends one statement. It writes
nothing and does not install the schema.

<!-- ts-check: alone -->
```ts
import type { Horsies } from "horsies";

export async function failedTasksOfLastDay(app: Horsies): Promise<void> {
  const stats = await app.monitoring.taskStatusCounts();

  if (stats.ok) {
    for (const count of stats.value) {
      console.log(`${count.status}: ${count.count}`);
    }
  }

  const page = await app.monitoring.listTasks({
    window: { since: Temporal.Now.instant().subtract({ hours: 24 }) },
    statuses: ["FAILED"],
    sort: { field: "failedAt", direction: "descending" },
    limit: 50,
  });

  if (!page.ok) {
    console.error(`listTasks: ${page.error.code}`);

    return;
  }

  for (const task of page.value.rows) {
    console.log(task.taskId, task.taskName, task.errorCode ?? "no error code");
  }
}
```

| Method | Returns |
|---|---|
| `taskStatusCounts(query?: TaskStatusCountsQuery)` | `readonly StatusCount[]` (`status`, `count`): each of the seven statuses, live table plus the history window |
| `taskFacets(query?: TaskFacetsQuery)` | `Facets`: workers, task names, queues and error codes with counts |
| `taskBreakdown(query: TaskBreakdownQuery)` | `Breakdown`: counts per `groupBy` (`"worker"`, `"taskName"`, `"queue"`) |
| `listTasks(query?: TaskListQuery)` | `TaskListPage` (`rows: readonly TaskSummary[]`, `total`) |
| `getTaskDetail(taskId: TaskId)` | `TaskDetailRead`: `{ kind: "found", taskDetail }` or `{ kind: "notFound", taskId, reason: TaskAbsenceReason }` |
| `listWorkflowNames()` | `readonly string[]` |
| `listWorkflowRuns(query?: WorkflowRunsQuery)` | `readonly WorkflowRunSummary[]`, newest first |
| `getWorkflowRun(workflowId: WorkflowId)` | `WorkflowRunRead`: `{ kind: "found", run }` (nodes, edges and phase timing) or `{ kind: "notFound", workflowId }` |
| `getWorkflowNode(workflowId, nodeIndex)` | `WorkflowNodeRead`: `{ kind: "found", node }` or `{ kind: "notFound", workflowId, nodeIndex }` |
| `listSchedules()` | `readonly ScheduleStateInfo[]`: the state rows of the schedules in the database |

Queries:

| Query | Fields |
|---|---|
| `TaskFilters` | `window`, `statuses` (`TaskStatus[]`), `taskNames`, `queues`, `workers`, `errorCodes`, `errorCategories` (`ErrorCategory[]`), `retriedOnly` (`true`: only the tasks that ran more than one attempt). A list left out, or empty, keeps every value |
| `TaskStatusCountsQuery` | `TaskFilters` without `statuses`: the read counts each status |
| `TaskFacetsQuery` | `window`, `statuses`, `errorCategories`, `retriedOnly` |
| `TaskBreakdownQuery` | `TaskFilters` plus `groupBy` and `limit` (1 to `MAX_BREAKDOWN_LIMIT`, 500) |
| `TaskListQuery` | `TaskFilters` plus `sort` (`{ field: TaskSortField, direction: "ascending" \| "descending" }`), `offset` (0 or more) and `limit` (1 to `MAX_TASK_PAGE_LIMIT`, 200); `offset + limit` at most `MAX_TASK_PAGE_REACH` (500) |
| `WorkflowRunsQuery` | `workflowName`, `status` (a workflow status), `limit` (1 to `MAX_WORKFLOW_RUNS_LIMIT`, 200) |

- `MonitoringWindow` is `{ since?, until? }` of `Temporal.Instant`. A bound
  left out takes the core's default: `until` is now, and `since` is
  `MONITORING_WINDOW_DEFAULT` (24 hours) before `until`. The core refuses a
  window that does not increase or that is longer than 30 days.
- `TaskSortField`: `enqueuedAt`, `startedAt`, `completedAt`, `failedAt`,
  `status`, `taskName`, `queue`, `priority`, `retryCount`,
  `queueSeconds`, `executionSeconds`.
- A field that a query leaves out takes the core's default.
- `TaskListPage.total` is exact when any filter is active. With no filter it
  is an estimate: the live estimate plus the planner's estimate of the
  history window.
- A detail read resolves on the live table and the history, so a task that
  ended still answers. `TaskAbsenceReason` is `PREDATES_RETAINED_HISTORY`,
  `WITHIN_RETAINED_HISTORY` or `NOT_CLASSIFIED`.
- `TaskId.parse(text)` and `WorkflowId.parse(text)` give the id types from a
  stored text: `Result<TaskId, IdRefused>` (see `tasks.md`).

Errors:

| Code | When |
|---|---|
| `INVALID_INPUT` (`invalidInput`) | An argument that the call refuses before any connect: `field` (`window`, `page`, `limit`, `taskNames`, `queues`, `workers`, `errorCodes`, `workflowName`, `nodeIndex`), `reason`, and `message` (`<field>: <reason>`). A count that is not a whole number, a value outside the core's bound, and text with a lone surrogate. Nothing ran. Not retryable |
| `DATABASE_OPERATION_FAILED` | The database or the connect failed. `retryable` is the classification of the cause; `cause` holds the cause |
| `HISTORY_CONTRACT_VIOLATION` | A history row does not decode. Not retryable |

`getTaskDetail`, `listWorkflowNames`, `getWorkflowRun` and `listSchedules` take no query,
so their error is `MonitoringQueryError` (no `invalidInput`).

Phase timing (the lifecycle record of migration 1001):

- `TaskAttemptDetail.phases`: `{ kind: "notCaptured" }` (a row written before
  the record) or `{ kind: "captured", enqueuedAt, claimedAt, claims,
  finalizing, recordedBy }`. `recordedBy` names the writer: the worker, the
  recovery, or the cancel of a `RUNNING` task (outcome `CANCELLED`).
- `WorkflowNodeSummary.timing` (`NodeTimingInfo`): `exact`, `partial`,
  `unavailable` or `inconsistent`. A segment (`TimingSegmentInfo`) has a
  phase (`SegmentPhaseInfo`), a start, an end (closed or open) and a source.
- `WorkflowRunDetail.timing` (`RunTimingInfo`): `{ kind: "unavailable",
  asOf, capturedSince, reason }` or `{ kind: "composed", asOf,
  capturedSince, coverage, rangeStartedAt, rangeEndedAt, openSegments,
  pauses }`.
- The models hold times, not durations. An open segment ends at `asOf` for
  drawing. A conflict makes the node `inconsistent`: the core does not
  reorder or guess times.

## Task Cancel

<!-- ts-check: alone -->
```ts
import { type Horsies, TaskId } from "horsies";

export async function cancelPending(app: Horsies, storedId: string): Promise<boolean> {
  const taskId = TaskId.parse(storedId);

  if (!taskId.ok) {
    console.error(`not a task id: ${taskId.error.detail}`);

    return false;
  }

  const cancelled = await app.monitoring.cancelTask(taskId.value, { policy: "pendingOrClaimedOnly" });

  if (cancelled.ok) {
    console.log(`cancelled ${cancelled.value.taskId}, was ${cancelled.value.wasStatus}`);

    return true;
  }

  switch (cancelled.error.kind) {
    case "notFound":
    case "notCancellable":
      return false;
    case "isWorkflowTask":
    case "databaseOperationFailed":
    case "storedDataInvalid":
    case "seatRelayFull":
      console.error(cancelled.error.message);

      return false;
    default: {
      const unknownError: never = cancelled.error;

      throw new Error(`no case for ${String(unknownError)}`);
    }
  }
}
```

`app.monitoring.cancelTask(taskId, { policy })` returns
`Result<TaskCancelled, CancelTaskError>`. `TaskCancelled` has `taskId` and
`wasStatus` (`"PENDING"`, `"CLAIMED"` or `"RUNNING"`).

- `CancelPolicy` has no default: `"pendingOrClaimedOnly"` cancels `PENDING`
  and `CLAIMED` tasks (no user code ran); `"includeRunning"` also cancels a
  `RUNNING` task.
- One statement. A committed `CANCELLED` is final: claim, finalize, retry
  and recovery cannot overwrite it.
- A `RUNNING` task: the statement records the running attempt with the
  outcome `CANCELLED` and moves the task to history. The body sees
  `runtime.signal` abort, and `runtime.cancelState()` gives
  `{ kind: "requested", cause: "OPERATOR" }` (see `tasks.md`). The body
  stops when it reads the cancel; side effects made before then stay, and
  what it returns is dropped.
- A waiting `handle.get` returns a failed outcome with `TASK_CANCELLED`.
- A task of a workflow is refused: use the workflow handle's `cancel`,
  `pause` and `resume` (see `workflows.md`).

| Code | When |
|---|---|
| `TASK_NOT_FOUND` | No live or history row has the id. Nothing changed |
| `TASK_NOT_CANCELLABLE` | Terminal, or `RUNNING` with `pendingOrClaimedOnly`; `currentStatus` is the status found. Nothing changed |
| `TASK_IS_WORKFLOW_TASK` | The task belongs to a workflow; `currentStatus` is the status found. Nothing changed |
| `DATABASE_OPERATION_FAILED` | The statement or the connect failed; `retryable` follows the cause. The outcome is unknown: nothing was written, or the commit reply was lost. A repeated cancel tells which: a committed cancel answers `TASK_NOT_CANCELLABLE` with `CANCELLED` |
| `STORED_DATA_INVALID` | Stored data does not parse. Nothing changed |

There is no manual retry action. Re-execution is a rerun (Rerun, below).

## Web UI: `createMonitoringHandler`

`createMonitoringHandler(app, options)` returns a Fetch API handler,
`(request: Request) => Promise<Response>`, with `close()`. Mount it in
`Bun.serve`, Hono, a Next.js route handler, or any server that gives a
`Request`.

<!-- ts-check: skip partial -- Bun.serve needs the Bun types, and the docs check loads the Node types only -->
```ts
import { createMonitoringHandler, type Horsies, trustedHeader } from "horsies";

export function serveMonitoring(app: Horsies): boolean {
  const policy = trustedHeader("X-Forwarded-User", { actions: "enabled" });

  if (!policy.ok) {
    console.error(`trusted header refused: ${policy.error.kind}`);

    return false;
  }

  const handler = createMonitoringHandler(app, { authorizationPolicy: policy.value, basePath: "/monitoring" });

  // The event stream sends a heartbeat after 15 s; Bun closes an idle connection after 10 s by default.
  Bun.serve({ hostname: "127.0.0.1", port: 8600, idleTimeout: 30, fetch: handler });

  return true;
}
```

<!-- ts-check: skip partial -- hono is not a dependency of the docs check -->
```ts
import { Hono } from "hono";
import { createMonitoringHandler, viewOnly } from "horsies";

const monitoring = createMonitoringHandler(app, { authorizationPolicy: viewOnly(), basePath: "/monitoring" });
const server = new Hono();

server.all("/monitoring", (context) => monitoring(context.req.raw));
server.all("/monitoring/*", (context) => monitoring(context.req.raw));
```

<!-- ts-check: skip partial -- next is not a dependency of the docs check -->
```ts
// app/monitoring/[[...path]]/route.ts
import { createMonitoringHandler, trustedHeader } from "horsies";

import { app } from "@/horsies/app";

const policy = trustedHeader("X-Forwarded-User", { actions: "disabled" });

if (!policy.ok) {
  throw new Error("the trusted header name is not valid");
}

const handler = createMonitoringHandler(app, { authorizationPolicy: policy.value, basePath: "/monitoring" });

export const GET = handler;
export const POST = handler;
```

The handler serves every path of the mount: the page and its assets,
`/api/...`, the event stream (`GET /api/events`), and `GET /metrics` (the
fleet metrics of the app's database, behind the view verdict).

| `MonitoringHandlerOptions` field | Type |
|---|---|
| `authorizationPolicy` | `MonitoringAuthorizationPolicy`. Required, with no default |
| `actions` | `ActionsSwitch`: `"enabled"` (default) or `"disabled"` |
| `ui` | `MonitoringUiOptions`: `customCssUrl` (a stylesheet that the page loads after its own), `traceLink` (`TraceLinkTemplate`; absent: the task detail shows the trace id as text) |
| `basePath` | `` `/${string}` ``, the mount path as it appears in the URL. Default `/`. A trailing `/` is ignored |

- `createMonitoringHandler` starts nothing: the first request connects the
  app's handle. It throws `AddonOutsideMainThread` off the main thread.
- A path outside `basePath` answers 404, and no policy runs. A prefix
  matches whole path segments only.
- A workflow resume builds the sub-workflow children that it owes from the
  workflow contracts of `app`. Create the handler from the app's own entry,
  after the implementations, when the UI must resume such workflows.
- `close()` ends every event stream of the handler after the degraded
  frame. The other routes still answer.
- The handler answers every expected outcome with a response. It throws
  only `CoreError` and defects.
- `TraceLinkTemplate.parse(text)` returns
  `Result<TraceLinkTemplate, TraceLinkRefusal>`: an `http://` or `https://`
  URL with a host and one `{trace_id}`, 1 to 2,048 bytes of printable ASCII
  with no space and no other brace. A refusal has `kind` (`length`,
  `character`, `scheme`, `noHost`, `placeholderCount`, `strayBrace`),
  `template` and `rule`.

### Access policy

| Policy | `canView` | `canAct` |
|---|---|---|
| `allowAll()` | Yes | Yes. The host owns authentication: mount the handler behind it |
| `viewOnly()` | Yes | No |
| `trustedHeader(headerName, { actions })` | The header is present and no member between its commas is blank | `actions: "enabled"` and the header check |

```ts
import type { MonitoringAuthorizationPolicy } from "horsies";

export const operatorsOnly: MonitoringAuthorizationPolicy = {
  canView: (request) => request.headers.has("x-admin-user"),
  canAct: (request) => request.headers.get("x-admin-role") === "operator",
};
```

- `MonitoringAuthorizationPolicy` has `canView(request)` and `canAct(request)`; each
  returns `boolean` or `Promise<boolean>`. Only `true` allows. A throw or a
  rejection answers the request with 500, and the error is logged with the
  route.
- The `request` of a verdict has the method, the URL, the headers and the
  signal of the incoming request, and no body: the core reads the body.
- The handler asks only the verdicts that the route needs, in order, and
  `canAct` only after `canView` allowed: `canView` for every read route, the
  event stream and `/metrics`; then `canAct` for an action route and for
  `/api/meta`. The page and its assets ask nothing.
- An action route also needs the header `X-Horsies-Intent: action`
  (`INTENT_HEADER`, `INTENT_VALUE`). It is a guard against cross-site requests, not authentication. With
  `actions: "disabled"` every action route answers 403.
- `trustedHeader` returns `Result<MonitoringAuthorizationPolicy,
  TrustedHeaderRefused>`: `{ kind: "blankHeaderName" }` or
  `{ kind: "invalidHeaderName", headerName, rule }` (a name that is not an
  HTTP token).
- A trusted header is safe only when the reverse proxy strips or overwrites
  it on each incoming request.

| Action route | Does |
|---|---|
| `POST /api/tasks/{id}/cancel` | Cancels one task (the rules of Task Cancel) |
| `POST /api/workflows/{id}/pause` | Pauses a workflow |
| `POST /api/workflows/{id}/resume` | Resumes a workflow and builds the sub-workflow children it owes |
| `POST /api/workflows/{id}/cancel` | Cancels a workflow |

A database failure answers with the cause: 503 when the cause may clear,
500 when it stays. The rules of each action are on the shared docs page
`https://suleymanozkeskin.github.io/horsies/monitoring/action-semantics/`.

## Web UI: the `web` Command

`<entry> web` serves the handler of the app on its own server until
`SIGTERM` or `SIGINT`. The entry is the app's own entry file with
`cli.run(app)` (see `configs.md`).

```bash
node app.ts web
node app.ts web --host 0.0.0.0 --authorization trusted-header --enable-actions
```

| Flag | Default | Description |
|---|---|---|
| `--host HOST` | `127.0.0.1` | Bind address |
| `--port PORT` | `8600` | Bind port, 0 to 65,535; `0` asks the operating system for a free port |
| `--authorization MODE` | `none` | `none` (a loopback host only: `localhost`, `127.0.0.0/8`, `::1`) or `trusted-header` |
| `--trusted-header NAME` | `X-Forwarded-User` | The identity header that the proxy sets |
| `--enable-actions` | off | Allow task cancel and workflow pause, resume and cancel |
| `--trace-link TEMPLATE` | none | The trace link template of the task detail |
| `--log-level LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

- The static check of the app runs first (see `configs.md`, The Check); a
  refusal prints the report and exits 1.
- `--authorization none` gives `allowAll()` with `--enable-actions`, else
  `viewOnly()`. `--authorization trusted-header` writes a warning line that names
  the header.
- Exit codes: 0 after a stop signal; 1 for a refused check or an address
  that does not bind; 2 for arguments that do not parse, `--authorization none` on a
  host that is not a loopback host, a header name that is not valid, and a
  trace link template that the core refuses.
- At a stop signal the command closes the handler's event streams, then the
  server.
- The bind and access rules are on the shared docs page
  `https://suleymanozkeskin.github.io/horsies/monitoring/web-ui-deployment/`.

## Fleet Metrics: the `metrics` Command

`<entry> metrics --metrics-listen HOST:PORT [--log-level LEVEL]` serves the fleet
metrics of the app's database at `GET /metrics` (Prometheus text format
0.0.4) until `SIGTERM` or `SIGINT`.

- `--metrics-listen` is required: `HOST:PORT` or `[IPV6]:PORT`. The core checks the
  value; a refusal exits 2.
- The core reads the fleet state at the start and then at most once per
  15 s. A scrape renders the last read, so scrapes add no load on the
  database.
- The `queue` label values are the queue names of the app config; other
  names are `(unknown)`.
- The server reads through its own connection, with no schema step and no
  listener. A failed connect or a failed statement is a failure gauge in
  the text, never an end of the command.
- Exit codes: 0 after a stop signal; 1 when the address does not bind; 2 for
  arguments that do not parse.

## Worker Metrics Endpoint — `--metrics-listen`

`<entry> worker --profile NAME --metrics-listen HOST:PORT` serves, beside
the worker:

- `GET /metrics`: the worker metrics, Prometheus text format 0.0.4;
- `GET /livez`: the liveness of the claim loop;
- `GET /readyz`: 200 when the pools have no failure, a child serves and the
  liveness lock is held.

There is no default and no config field. A value that the core refuses
exits 2 before the start. The endpoint, its bounds and its labels are on
the shared docs pages
`https://suleymanozkeskin.github.io/horsies/observability/metrics/` and
`https://suleymanozkeskin.github.io/horsies/observability/metric-reference/`.

## Rerun

A rerun makes a new task from the stored input of a retained terminal task.
The source task does not change.

<!-- ts-check: alone -->
```ts
import { type Horsies, TaskId } from "horsies";

export async function rerunFailedCharge(app: Horsies, storedId: string): Promise<void> {
  const sourceTaskId = TaskId.parse(storedId);

  if (!sourceTaskId.ok) {
    console.error(`not a task id: ${sourceTaskId.error.detail}`);

    return;
  }

  const rerun = await app.rerunTask(
    { sourceTaskId: sourceTaskId.value, goodUntil: { kind: "none" }, idempotencyKey: `rerun:${storedId}` },
    { retention: { kind: "period", days: 30 }, retainRerunInput: "keep", reservationWindow: Temporal.Duration.from({ hours: 24 }) },
  );

  if (!rerun.ok) {
    console.error(`${rerun.error.code}: ${rerun.error.message}`);

    return;
  }

  switch (rerun.value.kind) {
    case "enqueued":
      console.log(`new task ${rerun.value.newTaskId}`);
      break;
    case "keyReplay":
      console.log(`already rerun as ${rerun.value.existingTaskId}`);
      break;
    case "sourceLive":
    case "sourceAbsent":
    case "sourceCorrupt":
    case "notEligible":
    case "inputUnavailable":
    case "inputCorrupt":
    case "keyConflict":
      console.log(`not rerun: ${rerun.value.kind}`);
      break;
    default: {
      const unknownOutcome: never = rerun.value;

      throw new Error(`no case for ${String(unknownOutcome)}`);
    }
  }
}
```

| Method | Returns |
|---|---|
| `app.rerunTask(command, policy)` | `Result<RerunOutcome, RerunError>`: the rerun in a transaction of the app's handle, committed. The handle connects and installs the schema once, as a send does |
| `app.rerunTaskIn(transaction, command, policy)` | `Result<RerunOutcome, CallerRerunError>`: the rerun in `transaction`, which the caller owns. Nothing commits here |

`RerunCommand`:

| Field | Type |
|---|---|
| `sourceTaskId` | `TaskId`: the task to run again |
| `goodUntil` | `RerunDeadline`: `{ kind: "until", at: Temporal.Instant }` or `{ kind: "none" }`. A part below the microsecond is cut toward the past |
| `idempotencyKey` | `string`, optional: a second rerun with the same key and the same command answers `keyReplay`. Left out: an unkeyed rerun |

`RerunEnqueuePolicy`:

| Field | Type |
|---|---|
| `retention` | `RerunRetention`: `{ kind: "period", days }` (1 to 1,825) or `{ kind: "forever" }` |
| `retainRerunInput` | `"keep"` or `"drop"`: whether the new task keeps its input for a later rerun |
| `reservationWindow` | `Temporal.Duration`: how long the idempotency key holds the rerun. More than 0, at most 30 days, whole microseconds, in days and smaller units only |

`RerunOutcome` (only `enqueued` writes; every other outcome writes
nothing):

| `kind` | Fields | When |
|---|---|---|
| `enqueued` | `newTaskId`, `sourceTaskId`, `rerunRootTaskId` | The new task exists. `rerunRootTaskId` is the first task of the rerun chain |
| `sourceLive` | `taskId` | The source is live: its own lifecycle owns its retry |
| `sourceAbsent` | `taskId`, `reason: TaskAbsenceReason` | No live or history record has the id |
| `sourceCorrupt` | `taskId`, `detail` | The history row of the source does not decode |
| `notEligible` | `taskId`, `reason` (`COMPLETED_SOURCE`, `WORKFLOW_TASK`) | The source completed, or it is a workflow task |
| `inputUnavailable` | `taskId`, `disposition` | The source stores no input, for example `DECLINED_BY_POLICY` |
| `inputCorrupt` | `taskId`, `detail` | The stored input or its options fail their checks |
| `keyConflict` | `taskId`, `ownerTaskId` | The idempotency key holds another command; `ownerTaskId` holds it |
| `keyReplay` | `existingTaskId` | The idempotency key holds this command already |

Errors (each with `kind`, the code in camelCase, `code` and `message`;
`RerunErrorCode` names the codes):

| Code | Union | When |
|---|---|---|
| `INVALID_COMMAND`, `INVALID_POLICY` | both | A field breaks its rule. Nothing ran |
| `CLASS_NOT_REGISTERED` | both | The retention class of the new task has no class row (`classKey`). Nothing is written |
| `IDEMPOTENCY_KEY_REFUSED` | both | The idempotency key is empty or more than 255 UTF-8 bytes |
| `ID_MINT_REFUSED` | both | The task id source refused to mint the new id |
| `CLIENT_FAMILY_MISMATCH`, `UNKNOWN_STORED_FAMILY` | both | Another client family installed the database, or its family row is not a family |
| `CONTRACT_VIOLATION` | both | A row or stored data broke the rerun's contract. Nothing committed |
| `CONNECT_FAILED` | `RerunError` | The app's handle could not connect or install the schema. Nothing ran |
| `STATEMENT_FAILED` | `RerunError` | A statement failed; `stateAfter` is `"notCommitted"` or `"unknown"` (the `COMMIT` reply was lost: a keyed rerun sent again answers `keyReplay` if the first one committed) |
| `FAMILY_CHECK_FAILED` | `RerunError` | The family row could not be read |
| `DRIVER_FAILED` | `CallerRerunError` | A statement failed in the caller's driver: `statement`, `driverError`. The transaction is aborted: roll it back |
| `NO_OPEN_TRANSACTION` | `CallerRerunError` | The connection had no open transaction, so the statement did not run; `state` tells why (`idle`, `failedTransaction`, `notConnected`, `unrecognized`) |
| `RESULT_NOT_TEXT` | `CallerRerunError` | The driver gave a column in a format other than text (`column`, `format`), for example a `pg` client with `binary: true`. Roll back |

### Rerun in the Caller's Transaction

`app.rerunTaskIn` runs the source read and, for an eligible source, the write on
the caller's transaction, so the rerun commits or rolls back with the
caller's own writes. After an error, do not commit. Two adapters give a
`CallerTransaction`:

| Entry | Adapter | Takes |
|---|---|---|
| `horsies/rerun/pg` | `pgTransaction(client)` | A `pg` `Client` or `PoolClient` on which the caller ran `BEGIN`. A `Pool` does not compile: each query of a pool can run on another connection |
| `horsies/rerun/postgres` | `postgresTransaction(sql)` | The transaction `sql` that `sql.begin` of `postgres` gives its callback. The root `sql` does not compile |

`pg` (peer range `>=8.23.1 <8.24.0`) and `postgres` (peer range
`>=3.4.9 <3.5.0`) are optional peer dependencies. Each adapter imports
nothing of its driver: it calls the object that the caller passes.

<!-- ts-check: alone -->
```ts
import type { PoolClient } from "pg";
import type { Horsies, RerunCommand, RerunEnqueuePolicy } from "horsies";
import { pgTransaction } from "horsies/rerun/pg";

export async function rerunWithAudit(
  app: Horsies,
  client: PoolClient,
  command: RerunCommand,
  policy: RerunEnqueuePolicy,
): Promise<boolean> {
  await client.query("BEGIN");
  await client.query("INSERT INTO rerun_audit (source_task_id) VALUES ($1)", [command.sourceTaskId]);

  const rerun = await app.rerunTaskIn(pgTransaction(client), command, policy);

  if (!rerun.ok || rerun.value.kind !== "enqueued") {
    await client.query("ROLLBACK");

    return false;
  }

  await client.query("COMMIT");

  return true;
}
```

<!-- ts-check: alone -->
```ts
import type { Sql } from "postgres";
import type { Horsies, RerunCommand, RerunOutcome, RerunEnqueuePolicy } from "horsies";
import { postgresTransaction } from "horsies/rerun/postgres";

/** Thrown inside `sql.begin` so that postgres.js rolls the transaction back. */
class RerunRolledBack extends Error {
  readonly code: string;

  constructor(code: string) {
    super(`the rerun failed with ${code}; the transaction rolled back`);
    this.code = code;
  }
}

export async function rerunInPostgres(
  app: Horsies,
  sql: Sql,
  command: RerunCommand,
  policy: RerunEnqueuePolicy,
): Promise<RerunOutcome> {
  return sql.begin(async (transaction) => {
    await transaction`INSERT INTO rerun_audit (source_task_id) VALUES (${command.sourceTaskId})`;

    const rerun = await app.rerunTaskIn(postgresTransaction(transaction), command, policy);

    if (!rerun.ok) {
      throw new RerunRolledBack(rerun.error.code);
    }

    return rerun.value;
  });
}
```

- The `pg` adapter checks the transaction status of the client before each
  statement: a client with no open transaction runs nothing
  (`NO_OPEN_TRANSACTION`).
- The `postgres` adapter sends each parameter as text and reads each row
  raw, so the stored result is the same as with `pg`.
- postgres.js rolls back when the `sql.begin` callback throws or rejects.
- Whether a task can be rerun is decided at its send: the broker setting
  `retainRerunInputDefault` (see `configs.md`) and the send's own choice.
  The rules and the lineage of a rerun are on the shared docs page
  `https://suleymanozkeskin.github.io/horsies/monitoring/action-semantics/`
  ("Task rerun").

## Things to Avoid

**Do not treat an unfiltered `TaskListPage.total` as an exact count.** It
is an estimate.

**Do not cancel a `RUNNING` task to undo its side effects.** The cancel
stops the body when the body reads `runtime.signal`; effects made before
then stay. A body that does not read the signal runs on until it returns or
until its deadline.

**Do not use `allowAll()` without the host's own authentication.** It
allows every read and every action.

**Do not pass a `pg.Pool` or the root `sql` of postgres.js to `rerunTaskIn`.**
The types refuse them: a query of a pool can run outside the caller's
transaction.

**Do not commit the caller's transaction after a `rerunTaskIn` error.** Roll it
back.

## All Key Imports

<!-- ts-check: alone -->
```ts
import {
  // Health reads
  DEFAULT_PING_WAIT_MS,
  type DatabasePing, type WorkerPong, type PingTarget, type PingWorkersOptions, type MinResponses,
  type WorkerHistoryLimit, type WorkerStateSnapshot, type WorkerStateRead, type SnapshotProfile,
  type StaleTaskRow, type ExpiredTaskRow, type BoundedTaskList, type ExecutionClassCoverage, type ExecutionClassReport,
  type PingDatabaseError, type PingWorkersError, type HealthReadError, type HealthInputError,
  type WorkerReadError, type WorkerHistoryError, type HealthInputRefused, type HealthInputSubject,
  type StaleTasksQuery,
  // Monitoring reads
  type Monitoring, MONITORING_WINDOW_DEFAULT, MAX_TASK_PAGE_LIMIT, MAX_TASK_PAGE_REACH, MAX_BREAKDOWN_LIMIT,
  MAX_WORKFLOW_RUNS_LIMIT, TASK_LIST_ROW_BOUND, MonitoringQueryRefusalCode, TaskStatus,
  type MonitoringWindow, type TaskFilters, type TaskStatusCountsQuery, type TaskFacetsQuery, type TaskBreakdownQuery,
  type TaskListQuery, type TaskSort, type TaskSortField, type SortDirection, type TaskGroupBy,
  type ErrorCategory, type WorkflowRunsQuery,
  type StatusCount, type Facets, type Breakdown, type TaskListPage, type TaskSummary, type TaskDetail,
  type TaskDetailRead, type WorkflowRunSummary, type WorkflowRunDetail, type WorkflowRunRead,
  type WorkflowNodeDetail, type WorkflowNodeRead, type ScheduleStateInfo, type TaskAbsenceReason,
  type MonitoringReadError, type MonitoringQueryCoreError, type QueryRefused, type QueryRefusedField,
  type AttemptPhasesInfo, type NodeTimingInfo, type RunTimingInfo, type TimingSegmentInfo,
  // Cancel
  type CancelPolicy, type CancelTaskOptions, type TaskCancelled, type CancelTaskError,
  // Web
  createMonitoringHandler, allowAll, viewOnly, trustedHeader, TraceLinkTemplate, INTENT_HEADER, INTENT_VALUE,
  type MonitoringHandler, type MonitoringHandlerOptions, type MonitoringUiOptions, type MountPathText,
  type MonitoringAuthorizationPolicy, type ActionsSwitch, type TrustedHeaderOptions, type TrustedHeaderRefused,
  type TraceLinkRefusal,
  // Rerun
  RerunErrorCode,
  type RerunCommand, type RerunEnqueuePolicy, type RerunDeadline, type RerunRetention, type RerunInputChoice,
  type RerunOutcome, type RerunEnqueued, type RerunError, type CallerRerunError, type CallerTransaction,
} from "horsies";
import { pgTransaction } from "horsies/rerun/pg";
import { postgresTransaction } from "horsies/rerun/postgres";
```
