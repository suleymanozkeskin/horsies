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

  const pongs = await app.pingWorkers({ timeoutMs: 2_000, minResponses: { kind: "atLeast", count: 1 } });

  if (!pongs.ok) {
    console.error(pongs.error.code);

    return;
  }

  console.log(`database: ${ping.value.latencyMs} ms; ${pongs.value.length} workers answered`);
}
```

- `pingDatabase` connects the app's handle and sends a timed `SELECT 1`. It does not install the schema.
- Each other read connects the app's handle and installs the schema once per handle, as a send does. Then it reads only.
- Each read calls the addon on the main thread of the process. In a worker child, the read uses the child's producer handle. A call in a `worker_threads` thread throws `AddonOutsideMainThread`.

## Reads

| Read | Returns (`Promise<Result<T, E>>`) | Error codes |
|------|-----------------------------------|-------------|
| `app.pingDatabase()` | `DatabasePing`: `latencyMs`, the latency of one `SELECT 1` round trip | `DB_PING_FAILED` |
| `app.pingWorkers(options?)` | `readonly WorkerPong[]`: the distinct replies by worker id | `QUERY_REFUSED`, `INVALID_INPUT`, `LISTENER_DISABLED`, `WORKER_PING_FAILED` |
| `app.monitoring.workers()` | `readonly WorkerStateSnapshot[]`: the latest snapshot of every worker that wrote one, idle workers included | `MONITORING_QUERY_FAILED` |
| `app.monitoring.worker(workerId)` | `WorkerStateRead`: `{ kind: "found", snapshot }` or `{ kind: "notFound", workerId }` (the worker never wrote a snapshot, or retention removed its rows) | `QUERY_REFUSED`, `MONITORING_QUERY_FAILED` |
| `app.monitoring.workerHistory(workerId, limit?)` | `readonly WorkerStateSnapshot[]`, newest first | `QUERY_REFUSED`, `INVALID_INPUT`, `MONITORING_QUERY_FAILED` |
| `app.monitoring.staleTasks({ thresholdMinutes })` | `BoundedTaskList<StaleTaskRow>` | `INVALID_INPUT`, `MONITORING_QUERY_FAILED` |
| `app.monitoring.expiredTasks()` | `BoundedTaskList<ExpiredTaskRow>` | `MONITORING_QUERY_FAILED` |
| `app.monitoring.classCoverage()` | `ClassCoverage`: for each pending stream of an execution class, whether a live worker can run it | `MONITORING_QUERY_FAILED` |
| `app.monitoring.classReport()` | `ClassReport`: the pending streams and the claim budget load of each live worker | `MONITORING_QUERY_FAILED` |

A value that may be missing is a variant that names the missing state, not `null`. A 64-bit count is a `bigint`, a time is a `Temporal.Instant`, and a length of time is a `Temporal.Duration`.

## Ping the Workers

`PingWorkersOptions` has three optional fields:

| Field | Type | Default |
|-------|------|---------|
| `target` | `PingTarget`: `{ kind: "everyWorker" }` or `{ kind: "worker", workerId }` | `{ kind: "everyWorker" }` |
| `timeoutMs` | `number`: the window of the ping, a whole number of milliseconds above 0 | `DEFAULT_PING_TIMEOUT_MS` (2,000) |
| `minResponses` | `MinResponses`: `{ kind: "fullWindow" }` or `{ kind: "atLeast", count }` (`count` 1 or more) | `{ kind: "fullWindow" }` |

The ping collects replies until the first of these holds:

| Option | Stop condition |
|--------|----------------|
| `target: { kind: "worker", workerId }` | That worker replied |
| `minResponses: { kind: "atLeast", count }` | `count` distinct workers replied |
| `timeoutMs` | The window passed. With every worker and `fullWindow`, the ping waits the whole window and lists every worker that replied |

`WorkerPong` has `workerId`, `hostname`, `pid` and `roundTripMs`. A worker that `app.monitoring.workers()` lists and the ping does not return did not answer.

The checks run in this order, before any connect:

1. A target `workerId` that is not well-formed text (it holds a lone surrogate) is `QUERY_REFUSED`.
2. A `timeoutMs` or a `minResponses.count` that is not a whole number above 0 is `INVALID_INPUT`.
3. A handle with no listener is `LISTENER_DISABLED`. The handle of an app runs the schema mode that installs and validates the schema, and that mode has a listener.

## Read the Worker States

```ts
async function downSlots(app: Horsies): Promise<void> {
  const states = await app.monitoring.workers();

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
| `pid`, `processes`, `maxClaimBatch`, `maxClaimPerWorker`, `tasksRunning`, `tasksClaimed` | `number` |
| `clusterWideCap` | `{ kind: "capped", cap }` or `{ kind: "uncapped" }` |
| `queues` | `readonly string[]` |
| `queuePriorities`, `queueMaxConcurrency` | `ReadonlyMap<string, bigint>` |
| `recoveryConfig` | `RecoveryConfigSnapshot`: the recovery settings and the health of the maintenance passes, as the worker wrote them (a JSON object) |
| `memoryUsageMb`, `memoryPercent`, `cpuPercent`, `childrenMemoryMb` | `Measure`: `{ kind: "measured", value }` or `{ kind: "notMeasured" }`. `memoryUsageMb` is the resident memory of the worker process only; `childrenMemoryMb` is the sum over its children |
| `slotHealth` | `{ kind: "allSlotsServing" }` or `{ kind: "slotsDown", slots }`. Each `DownSlot` has `slot`, `cause` (`{ kind: "resourceShortage", errno }` or `{ kind: "threadStartRefused" }`), `detail`, `downSince` and `failedBoots` (`bigint`) |
| `classCapacity` | `readonly ClassBudget[]`: each claim budget with its `budget` class, its `capacity` (`bigint`) and the classes it `serves` |
| `profile` | `SnapshotProfile`: `{ kind: "named", name }`, or `{ kind: "notRecorded" }` for a snapshot that holds no profile name |

`workerHistory` takes a `WorkerHistoryLimit`: `{ kind: "allRetained" }` (the default: every retained snapshot) or `{ kind: "newest", count }` (the newest `count`, 1 or more). A `count` that is not a whole number above 0 is `INVALID_INPUT`, with no connect.

## Stale and Expired Tasks

`staleTasks({ thresholdMinutes })` takes whole minutes from 0 to 525,600 (one year). Another value is `INVALID_INPUT`, with no connect. A `RUNNING` task is stale when its latest runner beat since its start, or its start when it has no such beat, is older than the threshold. The read does not recover the task: the reaper does. See [Heartbeats & Recovery](/horsies/workers/heartbeats-recovery/).

| `StaleTaskRow` field | Type |
|----------------------|------|
| `id` | `TaskId` |
| `taskName` | `string` |
| `workerHostname`, `workerPid`, `workerProcessName` | `Recorded<T>`: `{ kind: "recorded", value }` or `{ kind: "notRecorded" }` |
| `startedAt` | `Temporal.Instant` |
| `lastHeartbeat` | `LastBeat`: `{ kind: "beat", at }` or `{ kind: "noBeatSinceStart" }`. The start writes no beat, and a beat of an earlier attempt of a retried task does not count |

`expiredTasks()` lists the `PENDING` tasks past their `goodUntil`, oldest deadline first. No worker claims them.

| `ExpiredTaskRow` field | Type |
|------------------------|------|
| `id` | `TaskId` |
| `taskName`, `queueName` | `string` |
| `priority` | `number` |
| `sentAt`, `enqueuedAt`, `goodUntil` | `Temporal.Instant` |
| `expiredFor` | `Temporal.Duration`: how long ago `goodUntil` passed, on the database clock |

Both reads return at most 10,000 rows:

| `BoundedTaskList<T>` | Holds |
|----------------------|-------|
| `{ kind: "complete", rows }` | Every matching row |
| `{ kind: "truncated", rows, bound }` | The first `bound` matching rows; more rows match |

## Class Coverage and the Class Report

```ts
async function unservedStreams(app: Horsies): Promise<void> {
  const coverage = await app.monitoring.classCoverage();

  if (!coverage.ok) {
    console.error(coverage.error.message);

    return;
  }

  for (const stream of coverage.value.streams) {
    switch (stream.state.kind) {
      case "served":
      case "withoutCapacity":
        break;
      case "unserved":
        console.log(`no live worker runs ${stream.execution} on the queue ${stream.queueName}`);
        break;
    }
  }
}
```

| Type | Fields |
|------|--------|
| `ClassCoverage` | `checkedAt` (database time), `liveWorkers` (`bigint`), `streams: readonly StreamCoverage[]` |
| `StreamCoverage` | `queueName`, `execution` (`"ISOLATED"`, `"SHARED_ASYNC"` or `"SHARED_THREAD"`), `state` |
| `StreamCoverageState` | `{ kind: "served", workers, capacity }`, `{ kind: "withoutCapacity", workers }` or `{ kind: "unserved" }` |
| `ClassReport` | `checkedAt`, `streams: readonly PendingStream[]`, `workers: readonly WorkerClassLoad[]` |
| `PendingStream` | `queueName`, `execution`, `wait`, `state`. `wait` is `{ kind: "oldestWaiting", enqueuedAt, wait }` (`wait` is a `Temporal.Duration`) or `{ kind: "noneDue", nextDueAt }` |
| `WorkerClassLoad` | `workerId`, `snapshotAt`, `budgets: readonly BudgetLoad[]`. Each `BudgetLoad` has `budget`, `capacity`, `serves` and `inFlight` (`bigint`) |

What the coverage states mean, and what to do for an unserved stream, is on [Worker & Database Health](/horsies/monitoring/worker-health/#does-a-live-worker-serve-every-pending-class).

## Errors

Each error is a frozen record with `code` and `retryable`. Switch on `code`:

| Type | `code` | When |
|------|--------|------|
| `BrokerOperationFailed` | `DB_PING_FAILED`, `WORKER_PING_FAILED` or `MONITORING_QUERY_FAILED`: the code of the read | The connect, the schema install or the statement failed. `retryable` is the classification of the cause; `message` is the text; `exception` holds the cause (`CoreCause`) |
| `BrokerInvalidInput` | `INVALID_INPUT` | The core refused a count or a threshold; `message` names the value. No connect. Not retryable |
| `BrokerListenerDisabled` | `LISTENER_DISABLED` | The handle has no listener. Not retryable |
| `QueryRefused` | `QUERY_REFUSED` | A worker id that is not well-formed text. It has no `message`: `field` is `workerId`, and `reason` names the rule. No connect. Not retryable |

The error type of each read lists only the codes that the read can give:

| Error type | Members |
|------------|---------|
| `PingDatabaseError` | `BrokerOperationFailed` with `DB_PING_FAILED` |
| `PingWorkersError` | `BrokerInvalidInput`, `BrokerListenerDisabled`, `BrokerOperationFailed` with `WORKER_PING_FAILED`, `QueryRefused` |
| `HealthReadError` | `BrokerOperationFailed` with `MONITORING_QUERY_FAILED` |
| `WorkerReadError` | `HealthReadError`, `QueryRefused` |
| `HealthInputError` | `BrokerInvalidInput`, `BrokerOperationFailed` with `MONITORING_QUERY_FAILED` |
| `WorkerHistoryError` | `HealthInputError`, `QueryRefused` |

The message of a failed connect or install is the text of the Python client: `<read> failed: <cause>`, with the Python name of the read (`ping_workers`, `list_worker_states`, `get_stale_tasks`). A code outside the list of a read is a defect of the package, not an error value: it crashes loud with `ClientDefect`. The thrown errors are on [Task Errors](/horsies/typescript/tasks/errors/).

## Things to Avoid

**Do not read a missing `WorkerPong` as a dead worker after a short window.** With `minResponses: { kind: "atLeast", count: 1 }` the ping returns at the first reply, and the other workers are not listed. Use `fullWindow` to list every worker that answers.

**Do not recover stale tasks from `staleTasks`.** The read lists them only. The reaper of the workers moves them.
