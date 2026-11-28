---
title: App Config
summary: Build the app with Horsies.create from a config and its contract lists, read its refusals, and set the database, the queues, the claim mode, the schedules, recovery, retention, the payload guardrail, resilience, the schema mode and the log format.
related: [../tasks/defining-tasks, ../workers/worker-setup, ../../configuration/broker-config, ../../configuration/recovery-config, ../../configuration/retention-config, ../../concepts/queue-modes]
tags: [typescript, configuration, Horsies.create, HorsiesConfig, AppConfigInput, ValidationReport, HorsiesError]
---

# App Config

`Horsies` is the app of a TypeScript process: its config, its task and workflow contracts, its implementations and state, and the core handle that connects at the first operation. Every process of a deployment builds the same app with `Horsies.create`.

## Create the App

```ts
import { defineTask, h, Horsies } from "horsies";

const sendReceipt = defineTask({
  name: "send_receipt",
  input: { orderId: h.uuid() },
  output: h.null(),
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [sendReceipt],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`error[${refusal.code}]: ${refusal.message}`);

    for (const line of refusal.helpText) {
      console.error(`  = help: ${line}`);
    }
  }

  process.exit(1);
}

export const app = created.value;
```

`Horsies.create(config: HorsiesConfig): Result<Horsies, ValidationReport>` opens no database connection:

- It checks the config, then each contract of `tasks` and `workflows`, and returns every refusal in one `ValidationReport`. It never throws for a config or a contract.
- On the main thread of a process, the core checks the config and each contract: the native addon loads, and the core does not start. On a `worker_threads` thread, only the checks that are TypeScript code run, and no addon loads.
- The app connects at its first operation (a send, a read, a workflow start) and runs the schema step there. See [Broker Config](/horsies/configuration/broker-config/#schema-initialization). The `schemaMode` field sets what the schema step does.
- A send of a contract that `tasks` does not list is refused. A start of a workflow that `workflows` does not list is refused.

### Refusals

`ValidationReport` is `{ refusals: NonEmpty<HorsiesError> }`, in this order: the config fields, then the task contracts in list order, then the workflow contracts.

| Field of `HorsiesError` | Type | Value |
|--------------------|------|-------|
| `code` | `RefusalCode` | The `HRS-nnn` code of the rule (a value of `HorsiesErrorCode`). See [Errors Reference](/horsies/tasks/errors/#config-error-codes-horsieserror) |
| `path` | `string` | The config field (`customQueues[1].name`, `database.databaseUrl`) or the contract (`tasks[2] charge_payment`) |
| `message` | `string` | The path, then the rule that the value breaks |
| `helpText` | `readonly string[]` | How to fix it: zero or more lines |

## HorsiesConfig

`HorsiesConfig` is `AppConfigInput` with the contract lists. Only `database.databaseUrl` and `tasks` are required. A field that the config leaves out has the core's default, which the Python and Rust clients have too. The meaning, the default and the range of each field are on the shared pages.

| Field | Type | Meaning |
|-------|------|---------|
| `tasks` | `readonly AnyTaskContract[]` | Every task contract of the app. See [Defining Tasks](/horsies/typescript/tasks/defining-tasks/) |
| `workflows` | `readonly AnyWorkflowContract[]` | Every workflow contract of the app. See [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/) |
| `queueMode`, `customQueues` | `"default"`, or `"custom"` with a non-empty list of `CustomQueueInput` | [Queues](#queues), [Queue Modes](/horsies/concepts/queue-modes/) |
| `schedule` | `ScheduleSection` | [Schedules](#schedules), [Schedule Config](/horsies/scheduling/schedule-config/) |
| `database` | `DatabaseConfigInput` | [Database](#database), [Broker Config](/horsies/configuration/broker-config/) |
| `clusterWideCap` | `ClusterCapSetting`: `{ kind: "clusterCap", tasks }` or `{ kind: "noClusterCap" }` (the default) | [Concurrency](/horsies/workers/concurrency/#cluster-level-cluster_wide_cap) |
| `claim` | `ClaimMode` | [Claim Mode](#claim-mode) |
| `maxClaimRenewAgeMs` | `number` | [Heartbeats & Recovery](/horsies/workers/heartbeats-recovery/) |
| `payload` | `PayloadPolicyInput` | [Payload Guardrail](#payload-guardrail) |
| `recovery` | `RecoveryConfigInput` | [Recovery](#recovery), [Recovery Config](/horsies/configuration/recovery-config/) |
| `retention` | `RetentionConfigInput` | [Retention](#retention), [Retention Config](/horsies/configuration/retention-config/) |
| `resilience` | `ResilienceConfigInput` | [Resilience](#resilience) |
| `workerProfiles` | `Readonly<Record<string, WorkerProfileInput>>` | [Worker Setup](/horsies/typescript/workers/worker-setup/) |
| `resendOnTransientError` | `boolean` | `true`: a send that fails with a transient database error is sent again, up to 3 times. Left out: `false`, no resend. See [Sending Tasks](/horsies/typescript/tasks/sending-tasks/) |
| `schemaMode` | `SchemaMode` | [Schema Mode](#schema-mode) |
| `logFormat` | `LogFormat` | [Log Format](#log-format) |

Every field of the config is `readonly`. A number field takes a whole number in the range of the core's field; a number outside it is refused with the code of its section.

## Secrets

A database URL is a `Secret`:

| Form | Read |
|------|------|
| `{ kind: "fromEnv", name: "DATABASE_URL" }` | The value of the environment variable, read at `Horsies.create`. A variable that is not set is refused with `HRS-203`, and the message names the variable |
| `{ kind: "value", value: "postgresql://..." }` | The value as given |

Use `fromEnv` in code that goes into version control.

## Database

`DatabaseConfigInput` holds the database and the general pool. Only `databaseUrl` is required.

| Field | Type | Core field |
|-------|------|------------|
| `databaseUrl` | `Secret` | `database_url` |
| `connection` | `DatabaseConnection` | `session_database_url`, `pgbouncer_transaction_mode` |
| `poolPrePing` | `boolean` | `pool_pre_ping` |
| `workerPoolPrePing` | `boolean` | `worker_pool_pre_ping` |
| `poolSize` | `number` | `pool_size` |
| `maxOverflow` | `number` | `max_overflow` |
| `poolTimeoutSeconds` | `number` | `pool_timeout` |
| `poolRecycle` | `{ kind: "never" }` or `{ kind: "after", seconds }` (`0`: at every checkout) | `pool_recycle` |
| `retainRerunInputDefault` | `"keep"` or `"drop"`: whether a send keeps the input that a rerun needs, when the send does not choose | `retain_rerun_input_default` |
| `echo` | `boolean`: log each SQL statement | `echo` |
| `tcpKeepalives` | `{ kind: "off" }` or `{ kind: "on", idleSeconds, intervalSeconds, probes }` | `tcp_keepalives` |
| `tcpUserTimeout` | `{ kind: "systemDefault" }` or `{ kind: "after", milliseconds }` | `tcp_user_timeout` |

The meaning and the default of each core field are on [Broker Config](/horsies/configuration/broker-config/#fields). `poolSize` and `maxOverflow` size the general pool of this process; the pools of a worker are in its profile. See [Worker Setup](/horsies/typescript/workers/worker-setup/#pools).

### PgBouncer

`DatabaseConnection` has two forms:

| Form | `databaseUrl` | Session work (schema installs, partition maintenance, LISTEN/NOTIFY) |
|------|---------------|-----------------------------------------------------------------------|
| `{ kind: "direct" }` | A connection that keeps session state | On `databaseUrl` |
| `{ kind: "direct", sessionDatabase: { kind: "separate", url } }` | A connection that keeps session state | On `url` |
| `{ kind: "pgbouncerTransaction", sessionDatabaseUrl }` | A PgBouncer transaction-pool endpoint with prepared statement tracking on (`max_prepared_statements > 0`) | On `sessionDatabaseUrl`, which keeps session state |

`pgbouncerTransaction` requires `sessionDatabaseUrl`: a config without it does not typecheck.

`Horsies.create` refuses a `sessionDatabaseUrl` that names the same server as `databaseUrl`, with `HRS-203`. The check reads `postgres://` and `postgresql://` as one scheme.

```ts
import type { AppConfigInput } from "horsies";

export const pooledConfig: AppConfigInput = {
  database: {
    databaseUrl: { kind: "fromEnv", name: "PGBOUNCER_URL" },
    connection: {
      kind: "pgbouncerTransaction",
      sessionDatabaseUrl: { kind: "fromEnv", name: "DIRECT_DATABASE_URL" },
    },
  },
};
```

The rules of the mode are on [Broker Config](/horsies/configuration/broker-config/#pgbouncer--transaction-pooling) and [PgBouncer Troubleshooting](/horsies/troubleshooting/pgbouncer/).

## Queues

| `queueMode` | `customQueues` | A task contract |
|-------------|----------------|-----------------|
| Left out, or `"default"` | None | Names no queue. A contract that names a queue is refused with `HRS-200` |
| `"custom"` | A non-empty list of `CustomQueueInput` | Names one of the custom queues. A contract that names no queue is refused with `HRS-200`; a queue that is not in the list is refused with `HRS-103` |

`CustomQueueInput`:

| Field | Type | Meaning |
|-------|------|---------|
| `name` | `string` | The queue name |
| `priority` | `number`, 1 to 100 | 1 runs first. Left out: the core's default |
| `maxConcurrency` | `TaskCap` | The most tasks of the queue that run at once in the cluster. `{ kind: "cap", tasks: 0 }` pauses the queue. Left out: the core's default |

<!-- ts-check: alone -->
```ts
import { defineTask, h, Horsies } from "horsies";

const chargeCard = defineTask({
  name: "charge_card",
  queue: "critical",
  input: { orderId: h.uuid(), cents: h.int() },
  output: h.null(),
});

const buildReport = defineTask({
  name: "build_report",
  queue: "standard",
  input: { day: h.plainDate() },
  output: h.string(),
});

export const queued = Horsies.create({
  queueMode: "custom",
  customQueues: [
    { name: "critical", priority: 1 },
    { name: "standard", priority: 50, maxConcurrency: { kind: "cap", tasks: 10 } },
  ],
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [chargeCard, buildReport],
});
```

What a priority and a cap do is on [Queue Modes](/horsies/concepts/queue-modes/).

## Claim Mode

`claim` sets how a worker claims tasks:

| Form | A worker claims |
|------|-----------------|
| Left out | The core's default: the hard cap mode |
| `{ kind: "hardCap" }`, `{ kind: "hardCap", leaseMs }` | No task past its free seats. `leaseMs`: how long a claim holds before it expires; left out, the core's default |
| `{ kind: "softCap", prefetchBuffer, leaseMs }` | Up to `prefetchBuffer` tasks (1 or more) past its free seats; each claim holds for `leaseMs` milliseconds. `leaseMs` is required: a soft cap without it does not typecheck |

A `prefetchBuffer` of 0 is refused with `HRS-202`: the hard cap mode has its own form. See [Hard Cap vs Soft Cap](/horsies/workers/concurrency/#hard-cap-vs-soft-cap).

## Payload Guardrail

`payload` sets the input sizes at which a send warns and at which a send is refused:

| Field | Type | Effect |
|-------|------|--------|
| `warnBytes` | `{ kind: "bytes", bytes }` or `{ kind: "noBound" }` | A send above the size logs a warning, once per task name |
| `rejectBytes` | `{ kind: "bytes", bytes }` or `{ kind: "noBound" }` | A send above the size is refused with `PAYLOAD_TOO_LARGE` |

A left-out field has the core's default. A refused payload field has the code `HRS-227`.

## Recovery

`RecoveryConfigInput` has the fields of the core's recovery config in camelCase, each optional:

| Field | Core field |
|-------|------------|
| `requeueStaleClaimed` | `auto_requeue_stale_claimed` |
| `claimedStaleThresholdMs` | `claimed_stale_threshold_ms` |
| `failStaleRunning` | `auto_fail_stale_running` |
| `terminateOrphanedWorkflowTasks` | `auto_terminate_orphaned_workflow_tasks` |
| `orphanTaskAuditIntervalMs` | `orphan_task_audit_interval_ms` |
| `runningStaleThresholdMs` | `running_stale_threshold_ms` |
| `finalizingStaleThresholdMs` | `finalizing_stale_threshold_ms` |
| `crashedWorkerRecoveryGraceMs` | `crashed_worker_recovery_grace_ms` |
| `finalizationQuarantineAfterAttempts` | `phase2_quarantine_after_attempts` |
| `workflowRecoveryFailureLimit` | `workflow_recovery_failure_limit` |
| `checkIntervalMs` | `check_interval_ms` |
| `runnerHeartbeatIntervalMs` | `runner_heartbeat_interval_ms` |
| `claimerHeartbeatIntervalMs` | `claimer_heartbeat_interval_ms` |
| `workerStateSnapshotIntervalMs` | `worker_state_snapshot_interval_ms` |

The meaning, the default and the range of each field are on [Recovery Config](/horsies/configuration/recovery-config/). The core checks each range; a refusal has the code `HRS-204`.

## Retention

| Field | Type | Core field |
|-------|------|------------|
| `workerStateRetention` | `{ kind: "hours", hours }` or `{ kind: "noPruning" }` | `worker_state_retention_hours` |
| `terminalRecordRetention` | `{ kind: "hours", hours }` or `{ kind: "noPruning" }` | `terminal_record_retention_hours` |
| `pausedWorkflowCancelAfter` | `{ kind: "never" }` or `{ kind: "after", age: Temporal.Duration }` | `paused_workflow_auto_cancel_after` |
| `historyLeafHorizonDays` | `number` | `history_leaf_horizon_days` |
| `heartbeatLeafHorizonHours` | `number` | `heartbeat_leaf_horizon_hours` |
| `retentionPeriods` | `readonly { days: number }[]`: the periods that a send may choose besides the 30-day default and the periods of `queueRetention` | `retention_periods` |
| `queueRetention` | `Readonly<Record<string, QueueRetention>>`: `{ kind: "days", days }` or `{ kind: "forever" }` per queue | `queue_retention` |
| `partitionMaintenanceIntervalSeconds` | `number` | `partition_maintenance_interval_s` |
| `retentionSweepIntervalSeconds` | `number` | `retention_sweep_interval_s` |
| `retentionDeleteBatchSize` | `number` | `retention_delete_batch_size` |

- The age of `pausedWorkflowCancelAfter` is in whole microseconds and has no years or months: their length is not fixed. Another age is refused with `HRS-216`.
- A retention period is whole days. A send chooses one with its `retention` option. See [Sending Tasks](/horsies/typescript/tasks/sending-tasks/).
- `Horsies.create` checks these bounds. A value outside a bound is refused with `HRS-216`, and the message names the field path:

| Field | Bound |
|-------|-------|
| `retentionPeriods[].days`, `queueRetention.<queue>.days` | A whole number from 1 to 1,825 |
| `historyLeafHorizonDays` | 2 to 14 |
| `heartbeatLeafHorizonHours` | 2 to 48 |
| Each other count | A whole number from 0 to 4,294,967,295 |

The meaning and the default of each field are on [Retention Config](/horsies/configuration/retention-config/).

```ts
import type { RetentionConfigInput } from "horsies";

export const retention: RetentionConfigInput = {
  retentionPeriods: [{ days: 7 }, { days: 365 }],
  queueRetention: { critical: { kind: "days", days: 90 } },
  pausedWorkflowCancelAfter: { kind: "after", age: Temporal.Duration.from({ days: 14 }) },
};
```

## Resilience

`ResilienceConfigInput` sets the retry of transient database errors in a worker and the poll interval of the notification listener:

| Field | Type | Core field |
|-------|------|------------|
| `databaseRetryInitialMs` | `number` | `db_retry_initial_ms` |
| `databaseRetryMaxMs` | `number` | `db_retry_max_ms` |
| `databaseRetryMaxAttempts` | `{ kind: "attempts", attempts }` (1 or more) or `{ kind: "noLimit" }` | `db_retry_max_attempts` |
| `notifyPollIntervalMs` | `number` | `notify_poll_interval_ms` |

An attempt count of 0 is refused with `HRS-208`: no limit has its own form. See [Worker Architecture](/horsies/workers/worker-architecture/#resilience).

## Schedules

The `schedule` section holds the schedules of the app. Each entry comes from `taskSchedule`, which checks the input against the input type of the task contract of the entry. A plain object is not an entry: it does not compile.

| Field of `ScheduleSection` | Type | Meaning |
|-------|------|---------|
| `schedules` | `readonly TaskSchedule[]` | The entries, each from `taskSchedule` |
| `enabled` | `boolean` | `false` on the section: the `scheduler` command stops at its start. On an entry: the entry keeps its name and sends nothing |
| `checkIntervalSeconds` | `number`, 1 to 60 | How often the scheduler checks for due entries. Default 1 |

<!-- ts-check: alone -->
```ts
import { defineTask, h, Horsies, taskSchedule } from "horsies";

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });

export const scheduled = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [buildReport],
  schedule: {
    schedules: [
      taskSchedule({
        name: "daily-report",
        task: buildReport,
        input: { format: "pdf" },
        pattern: { kind: "daily", time: Temporal.PlainTime.from("09:00") },
        timezone: "Europe/Berlin",
      }),
    ],
    checkIntervalSeconds: 5,
  },
});
```

- `Horsies.create` records the section once. These cases go to the report of `check` and of the scheduler start: a contract that the app does not list, a queue that the app does not admit, an input that does not fit or is over the payload bound, an unknown time zone, a pattern that breaks a rule, a repeated name, and a bad check interval. `Horsies.create` is not refused for them. The worker start skips them.
- Run the schedules with the `scheduler` command. See [CLI Reference](/horsies/typescript/cli/#scheduler).
- The meaning of each pattern is on [Schedule Patterns](/horsies/scheduling/schedule-patterns/).

## Schema Mode

`schemaMode` is the schema mode of the core handle of the app. The producer, the scheduler, `web` and the task sends of each worker child use it.

| Value | Does |
|-------|------|
| `"MIGRATE_AND_VALIDATE"` | The default. Installs and checks the schema |
| `"OBSERVE_ONLY"` | Runs no schema statement |
| `"ASSUME_INITIALIZED"` | Runs no schema statement and uses no listener |

The connection of the worker itself always migrates and validates. `check --live` reads through a connection of its own with `"OBSERVE_ONLY"`.

## Log Format

`logFormat` sets the format of the stderr lines of the worker, the scheduler, `check` and each worker child:

| Value | Format |
|-------|--------|
| `{ kind: "textLogs" }` | The default. One text line per record |
| `{ kind: "jsonLogs" }` | One JSON object per line. Each record has the keys `ts`, `level`, `logger`, `message`, `process`, `task_id`, `task_name`, `attempt`, `trace_id`, `span_id` and `exc`. A key with no value is `null` |

A log sink function gets the records, so the format does not apply to it.

## Refusal Codes

| Code | Refused |
|------|---------|
| `HRS-102` | A task name that cannot be stored |
| `HRS-103` | A task queue that is not a custom queue |
| `HRS-105` | A parameter name or a schema that has no stored form |
| `HRS-200` | A queue field, or a task queue that the queue mode does not take |
| `HRS-201` | `clusterWideCap` |
| `HRS-202` | A `claim` field, or `maxClaimRenewAgeMs` |
| `HRS-203` | A database URL, or an environment variable of a `fromEnv` secret that is not set |
| `HRS-204` | A `recovery` field |
| `HRS-208` | A `resilience` field |
| `HRS-213` | A database pool field |
| `HRS-215` | A `tcpKeepalives` or `tcpUserTimeout` field |
| `HRS-216` | A `retention` field |
| `HRS-218` | A worker profile field |
| `HRS-227` | A `payload` field |
| `HRS-301` | A task contract listed twice, or two contracts with one task name |

The full list of codes is on [Errors Reference](/horsies/tasks/errors/).

## Things to Avoid

**Do not write a database URL into code.** Use `{ kind: "fromEnv", name }`.

**Do not ignore the refusals.** `Horsies.create` returns them; it does not throw them.

<!-- ts-check: skip elided -- a Wrong / Correct pair of statements -->
```ts
// Wrong: created can be a refusal, so created.value does not typecheck
const app = Horsies.create(config).value;

// Correct
const created = Horsies.create(config);
if (!created.ok) {
  process.exit(1);
}
const app = created.value;
```
