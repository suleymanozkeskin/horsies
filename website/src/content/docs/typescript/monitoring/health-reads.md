---
title: Health Reads
summary: The health reads of the TypeScript app - the database and worker pings on the app, and the worker state snapshots, the stale and expired tasks and the execution class reads on app.monitoring.
related: [monitoring-queries, web-ui-mount, ../../monitoring/worker-health, ../../workers/heartbeats-recovery]
tags: [typescript, monitoring, health, pingDatabase, pingWorkers, worker state, stale tasks]
---

# Health Reads

The app has the health reads of the Python and Rust clients. `app.pingDatabase` and `app.pingWorkers` are methods of the app; the other health reads are members of `app.monitoring`. What each read means, and what to do with its result, is on [Worker & Database Health](/horsies/monitoring/worker-health/).

```ts
import type { Horsies } from "horsies";

async function checkHealth(app: Horsies): Promise<void> {
  const ping = await app.pingDatabase();

  if (!ping.ok) {
    console.error(ping.error.message);

    return;
  }

  const pongs = await app.pingWorkers({ waitMs: 2_000, minResponses: { kind: "atLeast", count: 1 } });

  if (!pongs.ok) {
    console.error(pongs.error.code);

    return;
  }

  console.log(`database: ${ping.value.latencyMs} ms; ${pongs.value.length} workers answered`);
}
```

- `pingDatabase` connects the app's handle and sends a timed `SELECT 1`. It does not install the schema.
- Each other read connects the app's handle and installs the schema once per handle, as a send does. Then it reads only.
- Each read calls the addon on the main thread of the process. In a worker child, the read uses the child's producer handle. In a `SHARED_THREAD` body, the read goes through the seat relay. A call in another `worker_threads` thread throws `AddonOutsideMainThread`.

## Reads

| Read | Returns (`Promise<Result<T, E>>`) | Error codes |
|------|-----------------------------------|-------------|
| `app.pingDatabase()` | `DatabasePing`: `latencyMs`, the latency of one `SELECT 1` round trip | `DATABASE_PING_FAILED`, `SEAT_RELAY_FULL` |
| `app.pingWorkers(options?)` | `readonly WorkerPong[]`: the distinct replies by worker id | `INVALID_INPUT`, `LISTENER_DISABLED`, `WORKER_PING_FAILED`, `SEAT_RELAY_FULL` |
| `app.monitoring.listWorkerStates()` | `readonly WorkerStateSnapshot[]`: the latest snapshot of every worker that wrote one, idle workers included | `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `app.monitoring.getWorkerState(workerId)` | `WorkerStateRead`: `{ kind: "found", snapshot }` or `{ kind: "notFound", workerId }` (the worker never wrote a snapshot, or retention removed its rows) | `INVALID_INPUT`, `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `app.monitoring.getWorkerStateHistory(workerId, limit?)` | `readonly WorkerStateSnapshot[]`, newest first | `INVALID_INPUT`, `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `app.monitoring.getStaleTasks({ thresholdMinutes })` | `BoundedTaskList<StaleTaskRow>` | `INVALID_INPUT`, `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `app.monitoring.getExpiredTasks()` | `BoundedTaskList<ExpiredTaskRow>` | `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `app.monitoring.getExecutionClassCoverage()` | `ExecutionClassCoverage`: for each pending stream of an execution class, whether a live worker can run it | `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `app.monitoring.getExecutionClassReport()` | `ExecutionClassReport`: the pending streams and the claim budget load of each live worker | `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |

A value that may be missing is a variant that names the missing state, not `null`. A 64-bit count is a `bigint`, a time is a `Temporal.Instant`, and a length of time is a `Temporal.Duration`.

## Ping the Workers

`PingWorkersOptions` has three optional fields:

| Field | Type | Default |
|-------|------|---------|
| `target` | `PingTarget`: `{ kind: "everyWorker" }` or `{ kind: "worker", workerId }` | `{ kind: "everyWorker" }` |
| `waitMs` | `number`: the window of the ping, a whole number of milliseconds above 0 | `DEFAULT_PING_WAIT_MS` (2,000) |
| `minResponses` | `MinResponses`: `{ kind: "fullWindow" }` or `{ kind: "atLeast", count }` (`count` 1 or more) | `{ kind: "fullWindow" }` |

The ping collects replies until the first of these holds:

| Option | Stop condition |
|--------|----------------|
| `target: { kind: "worker", workerId }` | That worker replied |
| `minResponses: { kind: "atLeast", count }` | `count` distinct workers replied |
| `waitMs` | The window passed. With every worker and `fullWindow`, the ping waits the whole window and lists every worker that replied |

`WorkerPong` has `workerId`, `hostname`, `pid` and `roundTripMs`. A worker that `app.monitoring.listWorkerStates()` lists and the ping does not return did not answer.

The checks run in this order, before any connect:

1. A target `workerId` that is not well-formed text (it holds a lone surrogate) is `INVALID_INPUT` with the subject `worker`.
2. A `waitMs` or a `minResponses.count` that is not a whole number above 0 is `INVALID_INPUT` with the subject `pingWorkers`.
3. A handle with no listener is `LISTENER_DISABLED`. The handle of an app runs the schema mode that installs and validates the schema, and that mode has a listener.

## Read the Worker States

```ts
async function downSlots(app: Horsies): Promise<void> {
  const states = await app.monitoring.listWorkerStates();

  if (!states.ok) {
    console.error(states.error.message);

    return;
  }

  for (const snapshot of states.value) {
    switch (snapshot.slotHealth.kind) {
      case "allSlotsServing":
        break;
      case "slotsDown":
        for (const slot of snapshot.slotHealth.slots) {
          console.log(`${snapshot.workerId} slot ${slot.slot}: ${slot.cause.kind} since ${slot.downSince.toString()}`);
        }

        break;
    }
  }
}
```

`WorkerStateSnapshot` holds the fields of [Snapshot Fields](/horsies/monitoring/worker-health/#snapshot-fields), in camelCase:

| Field | Type |
|-------|------|
| `workerId`, `hostname` | `string` |
| `snapshotAt`, `workerStartedAt` | `Temporal.Instant` |
| `pid`, `childCount`, `tasksRunning`, `tasksClaimed` | `number` |
| `claimLimits` | `ClaimLimits`: `perQueuePerPass` and `claimedPerWorker`, each a `TaskLimit`: `{ kind: "taskLimit", tasks }` or `{ kind: "noTaskLimit" }`. `claimedPerWorker` is the limit that the worker resolved |
| `clusterWideCap` | `{ kind: "clusterCap", tasks }` or `{ kind: "noClusterCap" }` |
| `queues` | `readonly string[]` |
| `queuePriorities`, `queueMaxConcurrency` | `ReadonlyMap<string, bigint>` |
| `recoveryConfig` | `RecoveryConfigSnapshot`: the recovery settings and the health of the maintenance passes, as the worker wrote them (a JSON object) |
| `memoryUsageMb`, `memoryPercent`, `cpuPercent`, `childrenMemoryMb` | `Measure`: `{ kind: "measured", value }` or `{ kind: "notMeasured" }`. `memoryUsageMb` is the resident memory of the worker process only; `childrenMemoryMb` is the sum over its children |
| `slotHealth` | `{ kind: "allSlotsServing" }` or `{ kind: "slotsDown", slots }`. Each `DownSlot` has `slot`, `cause` (`{ kind: "resourceShortage", errno }` or `{ kind: "threadStartRefused" }`), `detail`, `downSince` and `failedBoots` (`bigint`) |
| `executionClassBudgets` | `readonly ExecutionClassBudget[]`: each claim budget with its `budget` execution class, its `capacity` (`bigint`) and the execution classes it `serves` |
| `profile` | `SnapshotProfile`: `{ kind: "named", name }`, or `{ kind: "notRecorded" }` for a snapshot that holds no profile name |

`getWorkerStateHistory` takes a `WorkerHistoryLimit`: `{ kind: "allRetained" }` (the default: every retained snapshot) or `{ kind: "newest", count }` (the newest `count`, 1 or more). A `count` that is not a whole number above 0 is `INVALID_INPUT` with the subject `workerHistory`, with no connect.

## Stale and Expired Tasks

`getStaleTasks({ thresholdMinutes })` takes whole minutes from 0 to 525,600 (one year). Another value is `INVALID_INPUT` with the subject `staleTasks`, with no connect. A `RUNNING` task is stale when its latest runner beat since its start, or its start when it has no such beat, is older than the threshold. The read does not recover the task: the reaper does. See [Heartbeats & Recovery](/horsies/workers/heartbeats-recovery/).

| `StaleTaskRow` field | Type |
|----------------------|------|
| `taskId` | `TaskId` |
| `taskName` | `string` |
| `workerHostname`, `workerPid`, `workerProcessName` | `Recorded<T>`: `{ kind: "recorded", value }` or `{ kind: "notRecorded" }` |
| `startedAt` | `Temporal.Instant` |
| `lastHeartbeatAt` | `LastBeat`: `{ kind: "beat", at }` or `{ kind: "noBeatSinceStart" }`. The start writes no beat, and a beat of an earlier attempt of a retried task does not count |

`getExpiredTasks()` lists the `PENDING` tasks past their `goodUntil`, oldest deadline first. No worker claims them.

| `ExpiredTaskRow` field | Type |
|------------------------|------|
| `taskId` | `TaskId` |
| `taskName`, `queueName` | `string` |
| `priority` | `number` |
| `sentAt`, `enqueuedAt`, `goodUntil` | `Temporal.Instant` |
| `expiredFor` | `Temporal.Duration`: how long ago `goodUntil` passed, on the database clock |

Both reads return at most 10,000 rows:

| `BoundedTaskList<T>` | Holds |
|----------------------|-------|
| `{ kind: "complete", rows }` | Every matching row |
| `{ kind: "truncated", rows, bound }` | The first `bound` matching rows; more rows match |

## Execution Class Coverage and Report

```ts
async function unservedStreams(app: Horsies): Promise<void> {
  const coverage = await app.monitoring.getExecutionClassCoverage();

  if (!coverage.ok) {
    console.error(coverage.error.message);

    return;
  }

  for (const stream of coverage.value.streams) {
    switch (stream.coverage.kind) {
      case "served":
      case "withoutCapacity":
        break;
      case "unserved":
        console.log(`no live worker runs ${stream.execution} on the queue ${stream.queue}`);
        break;
    }
  }
}
```

| Type | Fields |
|------|--------|
| `ExecutionClassCoverage` | `checkedAt` (database time), `liveWorkers` (`bigint`), `streams: readonly StreamCoverage[]` |
| `StreamCoverage` | `queue`, `execution` (`"ISOLATED"`, `"SHARED_ASYNC"` or `"SHARED_THREAD"`), `coverage` |
| `StreamCoverageState` | `{ kind: "served", workers, capacity }`, `{ kind: "withoutCapacity", workers }` or `{ kind: "unserved" }` |
| `ExecutionClassReport` | `checkedAt`, `liveWindow` (`Temporal.Duration`: how recent a snapshot of a live worker is), `streams: readonly PendingStream[]`, `workers: readonly WorkerExecutionClassLoad[]` |
| `PendingStream` | `queue`, `execution`, `wait`, `coverage`. `wait` is `{ kind: "due", oldestDueAt, wait }` (`wait` is a `Temporal.Duration`) or `{ kind: "noneDue", nextDueAt }` |
| `WorkerExecutionClassLoad` | `workerId`, `snapshotAt`, `budgets: readonly BudgetLoad[]`. Each `BudgetLoad` has `budget`, `capacity`, `serves` and `inFlight` (`bigint`) |

What the coverage states mean, and what to do for an unserved stream, is on [Worker & Database Health](/horsies/monitoring/worker-health/#does-a-live-worker-serve-every-pending-class).

## Errors

Each error is a frozen record with `kind`, `code`, `message` and `retryable`. Switch on `kind`, or on `code`:

| `kind` | `code` | When |
|--------|--------|------|
| `databaseOperationFailed` variant of the read | `DATABASE_PING_FAILED`, `WORKER_PING_FAILED` or `MONITORING_QUERY_FAILED`: the code of the read | The connect, the schema install or the statement failed. `retryable` is the classification of the cause; `cause` holds the cause (`CoreCause`) |
| `invalidInput` (`HealthInputRefused`) | `INVALID_INPUT` | The core refused an argument. `subject` is the read (`pingWorkers`, `worker`, `workerHistory`, `staleTasks`) and `cause` names the argument and the rule. No connect. Not retryable |
| `listenerDisabled` | `LISTENER_DISABLED` | The handle has no listener. Not retryable |
| `seatRelayFull` | `SEAT_RELAY_FULL` | A `SHARED_THREAD` body only: the seat relay holds its bound of waiting calls. Nothing was read. Retryable |

The error type of each read lists only the variants that the read can give:

| Error type | Members |
|------------|---------|
| `PingDatabaseError` | `DATABASE_PING_FAILED`, `SEAT_RELAY_FULL` |
| `PingWorkersError` | `HealthInputRefused`, `LISTENER_DISABLED`, `WORKER_PING_FAILED`, `SEAT_RELAY_FULL` |
| `HealthReadError` | `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `WorkerReadError` | `HealthReadError`, `HealthInputRefused` |
| `HealthInputError` | `HealthInputRefused`, `MONITORING_QUERY_FAILED`, `SEAT_RELAY_FULL` |
| `WorkerHistoryError` | `HealthInputError` |

A process-state refusal of the core throws `CoreError`. A defect of the package throws `CoreDefect`. The thrown errors are on [Task Errors](/horsies/typescript/tasks/errors/).

## Things to Avoid

**Do not read a missing `WorkerPong` as a dead worker after a short window.** With `minResponses: { kind: "atLeast", count: 1 }` the ping returns at the first reply, and the other workers are not listed. Use `fullWindow` to list every worker that answers.

**Do not recover stale tasks from `getStaleTasks`.** The read lists them only. The reaper of the workers moves them.
