# horsies (TypeScript) — Configuration

Reference for the app, its configuration, worker profiles, app state and
child start hooks, logging, the check and the commands of `cli.run(app)` in
the `horsies` npm package. The meaning, the default and the range of each
config field are on the shared docs pages; this file states the TypeScript
API.

## The App

`Horsies` is the app of a process: its configuration, its contract lists,
its implementations, its state, and the core handle that connects at the
first operation. Every process of a deployment runs the same entry file and
builds the same app.

```ts
import { defineTask, h, Horsies } from "horsies";

const chargePayment = defineTask({
  name: "charge_payment",
  queue: "critical",
  input: { orderId: h.uuid(), amountCents: h.int() },
  output: h.null(),
});

const sendReceipt = defineTask({
  name: "send_receipt",
  queue: "standard",
  input: { orderId: h.uuid() },
  output: h.null(),
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  queueMode: "custom",
  customQueues: [
    { name: "critical", priority: 1 },
    { name: "standard", priority: 50, maxConcurrency: { kind: "cap", tasks: 10 } },
  ],
  tasks: [chargePayment, sendReceipt],
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

const app = created.value;
```

`Horsies.create(config)` returns `Result<Horsies, ValidationReport>`:

- `ValidationReport.refusals` is a non-empty list of `HorsiesError`. The order: the
  config (the TypeScript parse, then the core's check), then each listed
  contract in list order, then the workflows.
- A `HorsiesError` has `code` (an `HRS-nnn` value of `HorsiesErrorCode`), `path`
  (the config field, for example `customQueues[1].name`, or the contract, for
  example `tasks[2] charge_payment`), `message` and `helpText` (zero or more
  lines). The codes are on the shared docs page
  `https://suleymanozkeskin.github.io/horsies/tasks/errors/` ("Config error
  codes").
- `create` does no I/O. On the main thread the addon loads and the core
  checks the config and each contract; the core does not start. The core
  handle connects at the first send, read or command.
- On a `worker_threads` thread only the checks that are TypeScript code run,
  and the addon does not load.
- `tasks` lists every task contract of the app; a send of a contract that is
  not listed is refused. `workflows` lists every workflow contract (see
  `workflows.md`). A worker refuses to start while a listed contract has no
  implementation in the entry (`HRS-300`).

## `HorsiesConfig`

`HorsiesConfig` is `AppConfigInput` plus the contract lists and the
`schedule` section. Only
`database.databaseUrl` is required. A field that the config leaves out takes
the core's default: the package holds no copy of a default.

| Field | Type | Shared docs page |
|---|---|---|
| `tasks` | `readonly AnyTaskContract[]` | `tasks.md` |
| `workflows` | `readonly AnyWorkflowContract[]` | `workflows.md` |
| `schedule` | `ScheduleSection`: `enabled?`, `schedules: readonly TaskSchedule[]`, `checkIntervalSeconds?` (1 to 60, default 1) | `scheduling/schedule-config` |
| `queueMode`, `customQueues` | `"default"`, or `"custom"` with a non-empty `CustomQueueInput[]` | `concepts/queue-modes` |
| `database` | `DatabaseConfigInput` | `configuration/broker-config` |
| `clusterWideCap` | `ClusterCapSetting`: `{ kind: "clusterCap", tasks }` or `{ kind: "noClusterCap" }` (the default) | `workers/concurrency` |
| `claim` | `ClaimMode`: `{ kind: "hardCap", leaseMs? }` or `{ kind: "softCap", prefetchBuffer, leaseMs }` | `workers/concurrency` |
| `maxClaimRenewAgeMs` | `number` | `workers/heartbeats-recovery` |
| `payload` | `PayloadPolicyInput` (`warnBytes`, `rejectBytes`: `PayloadBound`) | `tasks.md` (sends) |
| `recovery` | `RecoveryConfigInput` | `configuration/recovery-config` |
| `retention` | `RetentionConfigInput` | `configuration/retention-config` |
| `resilience` | `ResilienceConfigInput` | `workers/worker-architecture` |
| `workerProfiles` | `Readonly<Record<string, WorkerProfileInput>>` | `workers/concurrency` |
| `resendOnTransientError` | `boolean`, default `false` | `operations/atomic-enqueue-and-topology` |
| `schemaMode` | `SchemaMode`: `"MIGRATE_AND_VALIDATE"` (the default), `"OBSERVE_ONLY"` or `"ASSUME_INITIALIZED"` | `configuration/broker-config` |
| `logFormat` | `LogFormat`: `{ kind: "textLogs" }` (the default) or `{ kind: "jsonLogs" }` | `configuration/app-config` |

The shared pages are under `https://suleymanozkeskin.github.io/horsies/`,
for example `https://suleymanozkeskin.github.io/horsies/configuration/broker-config/`.

- `claim`: `hardCap` claims no task past the free seats of a worker.
  `softCap` claims up to `prefetchBuffer` (at least 1) tasks past them, and
  each claim holds for `leaseMs` milliseconds. A `softCap` with no `leaseMs`
  does not compile.
- `resendOnTransientError: true` sends again, up to 3 times, after a send
  fails with a transient database error.
- `schemaMode` is the mode of the app's core handle: the producer, the
  scheduler, `web` and the task sends of each worker child use it.
  `"OBSERVE_ONLY"` runs no schema statement; `"ASSUME_INITIALIZED"` runs no
  schema statement and uses no listener. The worker's own connection always
  migrates and validates, and `check --live` reads through a connection of
  its own with `"OBSERVE_ONLY"`.

### `schedule`

Each entry of `schedules` comes from `taskSchedule`, which checks the input
against the input type of the entry's task contract. A plain object is no
entry: it does not compile.

<!-- ts-check: alone -->
```ts
import { defineTask, h, Horsies, taskSchedule } from "horsies";

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });

const created = Horsies.create({
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

- `Horsies.create` records the section once. A refusal of an entry (a
  contract that the app does not list, a queue that it does not admit, an
  input that does not fit or is over the payload bound, an unknown zone, a
  pattern that breaks a rule, a repeated name) or of the check interval goes
  to the report of `check` and of the scheduler start; the worker start
  skips them, and the create is not refused.
- `enabled: false` on the section stops the `scheduler` command at its start;
  on an entry it keeps the entry's name known and sends nothing.

### `database`

| Field | Type |
|---|---|
| `databaseUrl` | `Secret`: `{ kind: "fromEnv", name }` (read at `Horsies.create`) or `{ kind: "value", value }` |
| `connection` | `DatabaseConnection`: `{ kind: "direct", sessionDatabase? }` or `{ kind: "pgbouncerTransaction", sessionDatabaseUrl: Secret }` |
| `poolSize`, `maxOverflow` | `number` (defaults 5 and 10) |
| `poolTimeoutSeconds` | `number` |
| `poolRecycle` | `PoolRecycle`: `{ kind: "never" }` or `{ kind: "after", seconds }` (0: at every checkout) |
| `poolPrePing`, `workerPoolPrePing`, `echo` | `boolean` |
| `tcpKeepalives` | `TcpKeepalives`: `{ kind: "off" }` or `{ kind: "on", idleSeconds, intervalSeconds, probes }` |
| `tcpUserTimeout` | `TcpUserTimeout`: `{ kind: "systemDefault" }` or `{ kind: "after", milliseconds }` |
| `retainRerunInputDefault` | `RerunInputDefault`: `"keep"` or `"drop"` |

- `sessionDatabase` of `direct` is `{ kind: "sameAsDatabaseUrl" }` or
  `{ kind: "separate", url: Secret }`. Schema installs, partition
  maintenance and LISTEN/NOTIFY connect there.
- `pgbouncerTransaction`: `databaseUrl` is a PgBouncer transaction-pool
  endpoint with `max_prepared_statements > 0`. The mode needs
  `sessionDatabaseUrl`, a URL that keeps session state; without it the
  config does not compile.
- A `fromEnv` variable that is not set is refused with `HRS-203`. The
  message names the variable and never the value.
- `poolSize` persistent connections and up to `maxOverflow` more under load
  size the general pool. It serves producers, web processes and the
  app-level reads. It does not size the pools of a worker: the worker
  process opens the `pool` of its worker profile, and task code in a worker
  child opens the `taskSendPool` of its child kind.

| Deployment | `poolSize` | `maxOverflow` |
|---|---|---|
| Development | 2-5 | 5 |
| Small production | 5-10 | 10-20 |
| High traffic | 10-20 | 20-40 |

<!-- ts-check: alone -->
```ts
import type { AppConfigInput } from "horsies";

export const highTrafficConfig: AppConfigInput = {
  database: {
    databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" },
    poolSize: 20,
    maxOverflow: 40,
    tcpKeepalives: { kind: "on", idleSeconds: 30, intervalSeconds: 10, probes: 3 },
  },
};
```

### Rules of the TypeScript Config

| Config | Refusal of `Horsies.create` |
|---|---|
| A number field that is not a whole number in the range of its core field | The code of its section, with "must be a whole number from MIN to MAX" |
| A text with a lone UTF-16 surrogate | The code of its section, with "holds a lone surrogate". The message holds no part of the text |
| A `fromEnv` secret whose variable is not set | `HRS-203` |
| `retention.pausedWorkflowCancelAfter` with years or months, or not whole microseconds | `HRS-216` |

The code of a section: queues `HRS-200`, `clusterWideCap` `HRS-201`,
`claim` and `maxClaimRenewAgeMs` `HRS-202`, `database` URLs `HRS-203`,
`recovery` `HRS-204`, `resilience` `HRS-208`, `database` pool fields
`HRS-213`, `database` socket fields `HRS-215`, `retention` `HRS-216`,
`workerProfiles` `HRS-218`, `payload` `HRS-227`.

### Queues

- `queueMode` left out, or `"default"`: one queue, `default`. A contract
  that names a `queue` is refused with `HRS-200`.
- `queueMode: "custom"`: each contract names one of `customQueues`. A
  contract with no `queue` is refused with `HRS-200`; an unknown queue with
  `HRS-103`.
- `CustomQueueInput`: `name`, `priority` (1 runs first, 100 runs last; left
  out, the core's default) and `maxConcurrency` (`TaskCap`; a cap of 0
  pauses the queue).

### `payload`, `recovery`, `retention`, `resilience`

| Section | Fields |
|---|---|
| `payload` | `warnBytes` (a send above it logs a warning, once per task name), `rejectBytes` (a send above it is refused with `PAYLOAD_TOO_LARGE`). Each is `{ kind: "bytes", bytes }` or `{ kind: "noBound" }` |
| `recovery` | `requeueStaleClaimed`, `claimedStaleThresholdMs`, `failStaleRunning`, `terminateOrphanedWorkflowTasks`, `orphanTaskAuditIntervalMs`, `runningStaleThresholdMs`, `finalizingStaleThresholdMs`, `crashedWorkerRecoveryGraceMs`, `finalizationQuarantineAfterAttempts`, `workflowRecoveryFailureLimit`, `checkIntervalMs`, `runnerHeartbeatIntervalMs`, `claimerHeartbeatIntervalMs`, `workerStateSnapshotIntervalMs` |
| `retention` | `workerStateRetention`, `terminalRecordRetention` (`HoursRetention`: `{ kind: "hours", hours }` or `{ kind: "noPruning" }`), `pausedWorkflowCancelAfter` (`{ kind: "never" }` or `{ kind: "after", age: Temporal.Duration }`), `historyLeafHorizonDays`, `heartbeatLeafHorizonHours`, `retentionPeriods` (`{ days }[]`), `queueRetention` (by queue name: `{ kind: "days", days }` or `{ kind: "forever" }`), `partitionMaintenanceIntervalSeconds`, `retentionSweepIntervalSeconds`, `retentionDeleteBatchSize` |
| `resilience` | `databaseRetryInitialMs`, `databaseRetryMaxMs`, `databaseRetryMaxAttempts` (`{ kind: "attempts", attempts }` with at least 1, or `{ kind: "noLimit" }`), `notifyPollIntervalMs` |

The core checks the range of each field. The ranges are on the shared
pages `configuration/recovery-config` and `configuration/retention-config`.

## Worker Profiles

A worker runs one named profile of `workerProfiles`. The profile holds
every worker setting: the children of each execution class, their recycle
limits and pools, the pool of the worker process, the claim limits and the
drain timeout.

<!-- ts-check: alone -->
```ts
import { defineTask, h, Horsies } from "horsies";

const resizeImage = defineTask({
  name: "resize_image",
  input: { url: h.string(), width: h.int() },
  output: h.string(),
});

export const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: {
    default: {
      isolated: {
        kind: "isolatedChildren",
        count: 2,
        recycle: { after: { kind: "taskLimit", tasks: 500 }, above: { kind: "memoryLimit", mb: 1024 } },
      },
      sharedAsync: { kind: "sharedAsyncChildren", count: 4, seats: 25 },
      drainTimeoutMs: 120_000,
    },
  },
  tasks: [resizeImage],
});
```

| `WorkerProfileInput` field | Type |
|---|---|
| `isolated` | `IsolatedSetting`: `IsolatedChildren` (`kind: "isolatedChildren"`, `count` (1 to 1,024), `recycle` (`after: TaskLimit`, `above: MemoryLimit`), `pool` (`minSize`, `maxSize: PoolSize`, `prePing`), `taskSendPool: PoolSize`) or `NoIsolatedChildren` (`{ kind: "noIsolatedChildren" }`) |
| `sharedAsync` | `SharedAsyncSetting`: `SharedAsyncChildren` (`kind: "sharedAsyncChildren"`, `count` (1 to 1,024), `seats` (1 to 1,000), `recycle` (`afterPerSeat: TaskLimit`, `above: MemoryLimit`), `pool` (`minSize`, `maxSize: SharedPoolSize`, `prePing`), `executor: ExecutorThreads`, `taskSendPool`) or `NoSharedAsyncChildren` (`{ kind: "noSharedAsyncChildren" }`) |
| `pool` | `ParentPoolInput`: `maxSize: SharedPoolSize` |
| `claim` | `ClaimLimitsInput`: `perQueuePerPass`, `claimedPerWorker` (`TaskLimit`) |
| `drainTimeoutMs` | `number`: 1 to 86,400,000 ms; default 300,000 ms (300 s) |
| `parentCpu` | `"shared"` (the CPUs that the worker inherits) or `"ownCore"` (one physical core, Linux only) |

- `TaskLimit` is `{ kind: "taskLimit", tasks }` (1 to 2,147,483,647) or
  `{ kind: "noTaskLimit" }`. `MemoryLimit` is `{ kind: "memoryLimit", mb }` (1 to
  16,777,216) or `{ kind: "noMemoryLimit" }`. `PoolSize` is
  `{ kind: "connections", connections }` (1 to 1,000); `SharedPoolSize`
  adds `{ kind: "fromSeats" }`.
- `ExecutorThreads` is `{ kind: "threads", threads }` (1 to 256) or
  `{ kind: "fromSeats" }` (the default): the threads of the core's runtime in
  each shared async child, which run its database writes, its connections and
  its frames to the worker. Task code runs on the child's event loop.
- A class that the profile leaves out has no children, as its
  `no...Children` setting gives. The default recycle is after 100 tasks (100
  tasks per seat for a shared child) and no memory limit.
- A child is recycled when either limit is reached.
- A `SHARED_ASYNC` task runs on a shared async child. The routing and the
  sizing are on the shared docs pages `workers/execution-modes` and
  `workers/concurrency`.
- A profile field outside its bound is refused with `HRS-218` at
  `Horsies.create`, with the path of the field
  (`workerProfiles["default"].isolated.count`).

## Implementations, State and Child Start Hooks

| Method | Does |
|---|---|
| `app.implement(contract, body, options?)` | Gives the body of a listed contract (see `tasks.md`). Calls no addon function and never throws: a contract that the app does not list, or a second implementation, is a refusal that `check` and the worker start report |
| `app.provide(factory)` | Adds a provider of app state and returns its `StateKey<T>`. A body reads the value with `runtime.state(key)` |
| `app.onChildStart(hook)` | Runs `hook` once in each task child |

```ts
const Prices = app.provide(async () => new Map<string, number>([["sku-1", 1250]]));

app.onChildStart(() => {
  console.log(`child ${process.pid} is ready to serve`);
});
```

- A factory and a hook run in each task child, a replacement child
  included, before the child reports ready. The factories run first, in
  registration order, then the hooks.
- Each factory and each hook runs for at most 10 s.
- A factory or a hook that throws, rejects or runs past its bound stops the
  worker with `CHILD_HOOK_FAILED`; the child exits with code 173.
- A call of `provide` or `onChildStart` after `cli.run` serves a child is a
  defect: `ClientDefect`.
- Only task children run the factories and the hooks. The worker process
  and a producer never call them.

## Send Suppression

Sends are suppressed in these processes. A suppressed send returns
`SEND_SUPPRESSED` and writes nothing.

| Process | Suppressed |
|---|---|
| A task child, before it serves | Yes. `cli.run` lifts it when the child starts to serve |
| The `check` command | Yes, for the whole command |
| Any process with `HORSIES_SUPPRESS_SENDS=1` | Yes. The variable is read at each send, trimmed |

Every line of the entry before `cli.run` runs again in each child, so a
send at module level does not run once per child start.

## Logging — `configureLogging`

The core writes its records to 19 named loggers (`LOGGER_NAMES`, from
`horsies.app` to `horsies.workflow.recovery`). The default threshold is
`WARNING`, and the default sink writes one line per record to stderr.

<!-- ts-check: alone -->
```ts
import { configureLogging, type LogRecord } from "horsies";

configureLogging({
  levels: {
    default: "WARNING",
    loggers: { "horsies.worker": "INFO", "horsies.workflow": "DEBUG" },
  },
  sink: {
    kind: "function",
    sink: (records: readonly LogRecord[]) => {
      for (const record of records) {
        const line = { logger: record.logger, level: record.level, message: record.message, time: record.time.toString() };

        console.log(JSON.stringify(line));
      }

      return undefined;
    },
  },
});
```

| Item | Type |
|---|---|
| `configureLogging(options)` | `options: LoggingOptions`; returns nothing. An absent member keeps its setting |
| `LoggingOptions` | `sink?: LogSinkSetting`, `levels?: LogLevels` |
| `LogSinkSetting` | `{ kind: "stderr" }` (one line per record on stderr, the default) or `{ kind: "function", sink: LogSink }` |
| `LogLevels` | `default: LogThreshold`, `loggers?: Partial<Record<LoggerKey, LogThreshold>>` |
| `LogThreshold` | `LogLevel` or `"CRITICAL"` (forwards no core record) |
| `LogLevel` | `"TRACE"`, `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"` |
| `LoggerKey` | A `LoggerName`, or a dotted parent of `LOGGER_PARENTS` (`horsies`, `horsies.workflow`) |
| `LogRecord` | `logger`, `level`, `message`, `fields: readonly LogField[]`, `time: Temporal.Instant`, `thread`, `source: LogSource` |
| `LogField` | `name`, `value`: `{ kind: "flag", value }`, `{ kind: "integer", value: bigint }`, `{ kind: "float", value }` or `{ kind: "text", text }` |

- A key applies to the logger of its name and to the loggers below it in
  the dotted names. The longest key that applies wins. A logger that no key
  applies to has the default.
- `configureLogging` can run before the first core call and at any later
  time; it starts no core. A change applies to later records.
- The sink runs on the JavaScript thread with one batch at a time. It is
  sync: its type refuses an async function. A batch that the sink throws on
  goes to stderr.
- Each command of `cli.run` sets every logger to the threshold of its
  `--log-level`, so the thresholds that the entry set before `cli.run` do not
  stay. The sink stays.
- The worker, the scheduler, `check` and each worker child write the stderr
  lines in the app's `logFormat`. `{ kind: "jsonLogs" }` writes one JSON
  object per line with the same keys in every record: `ts` (ISO 8601 in UTC,
  in microseconds), `level`, `logger`, `message`, `process`, `task_id`,
  `task_name`, `attempt`, `trace_id`, `span_id` (`null` when the record has no
  field of that name) and `exc` (`null`). The output is ASCII. A sink
  function gets the records, so the format does not apply to it.

## The Check

The `check` command and the worker start run the static check. It does no
I/O and stops at the first phase that refuses:

1. Registrations: each refusal of `app.implement` (an unlisted contract, a
   second implementation).
2. Workflows: each listed workflow with no params and each check case is
   built and checked (`HRS-028`, `HRS-029`, the spec checks), and the walk
   into its sub-workflows refuses a nesting that fails for certain
   (`HRS-036`, `HRS-037`). The builds run in the check's process.
3. Implementations: each listed contract has an implementation in this
   entry (`HRS-300`), so a passing check means that a worker start from the
   same entry passes the same rule.
4. Exception codes: the codes of each implementation's `exceptionMapper` list
   pass the core's code rules.

The config, the contracts and each workflow definition are checked earlier,
at `Horsies.create`.

`check --live` adds, after a passing static check:

5. Connectivity: a connect and `SELECT 1` on the database, and in
   PgBouncer transaction mode a LISTEN probe on the session URL. A refusal
   is `HRS-203`.
6. Worker connections: each worker profile against the connection limit of
   the server (`HRS-219`).

`check --live` installs no schema and writes nothing but the LISTEN probe's
one NOTIFY.

A report prints each refusal as `error[HRS-nnn]: <message>`, one
`  = help: <line>` per help line, then
`error: aborting due to <n> previous errors`. A passing check prints
`ok: all validations passed`, the count of listed tasks, and the declared
error codes of each task (`declared error codes: none` when no task
declares one).

## CLI — `cli.run(app)`

The entry file of the app is the one program of every process: the
producer, the worker, each child of the worker, the web server and the
metrics server.

<!-- ts-check: alone -->
```ts
import { cli, defineTask, h, Horsies } from "horsies";

const greet = defineTask({ name: "greet", input: { name: h.string() }, output: h.string() });

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { kind: "isolatedChildren", count: 2 } } },
  tasks: [greet],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`error[${refusal.code}]: ${refusal.message}`);
  }

  process.exit(1);
}

const app = created.value;

app.implement(greet, ({ name }, { ok }) => ok(`hello, ${name}`));

process.exitCode = await cli.run(app);
```

```bash
node app.ts worker --profile default
bun app.ts check --live
```

1. `cli.run` runs on the main thread of a process. On a `worker_threads`
   thread it throws `ClientDefect`.
2. Child mode first. The worker starts this entry again for each child,
   with `--horsies-child` after the entry script. `cli.run` serves the
   child; the child ends its own process.
3. Every line of the entry before `cli.run` runs in each child too. Build
   the app there (config, contracts, implementations, providers, hooks), and
   start nothing else.
4. Any other process runs the command named by the first argument after the
   entry script.
5. `cli.run` ends the process itself in every mode, so an open handle of
   user code (a pool, a timer) does not keep a stopped command alive. Before
   the end it waits at most 2,000 ms for the core log records that still
   wait to reach the sink.

| Command | Does |
|---|---|
| `<entry> worker --profile NAME [--log-level LEVEL] [--metrics-listen HOST:PORT]` | Starts a worker of one profile. The static check runs first. `--log-level` default `INFO`. A missing or unknown `--profile` exits 1 and the core names the profiles of the app |
| `<entry> check [--live] [--log-level LEVEL]` | Checks the app. `--log-level` default `WARNING`. Sends stay suppressed |
| `<entry> web [--host HOST] [--port PORT] [--authorization none\|trusted-header] [--trusted-header NAME] [--enable-actions] [--trace-link TEMPLATE] [--log-level LEVEL]` | Serves the monitoring web UI of the app (see `monitoring.md`) |
| `<entry> metrics --metrics-listen HOST:PORT [--log-level LEVEL]` | Serves the fleet metrics of the app's database at `GET /metrics` (see `monitoring.md`) |

- `--log-level` takes `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`, in
  any case.
- `SIGTERM` or `SIGINT` stops the worker, the web server and the metrics
  server. The first signal asks the stop once; each later signal writes one
  line to stderr and does nothing more. The worker drains within the
  profile's `drainTimeoutMs`.
- A flag that the command does not know, a text flag with no value, a value
  given to a switch and a positional argument are usage errors: the usage
  line and the error go to stderr, and the exit code is 2.

| Exit code | Meaning |
|---|---|
| 0 | Stopped at `SIGTERM` or `SIGINT` after the drain, or the check passed |
| 1 | A refused check or profile, a failed start, a failed run, an address that does not bind, a command that threw |
| 2 | Arguments that do not parse, no command, an unknown command |
| 77 | `worker`: the worker could not write runner heartbeats; the core ended the process (it fails closed) |
| 79 | `worker`: the worker lost its liveness lock; the core ended the process (it fails closed) |

### The child command

The worker starts each child with the runtime of the worker process
(`process.execPath`: Node or Bun), its runtime flags, the entry script, and
`--horsies-child`. The environment and the working directory reach the
children unchanged.

- These runtime flags do not reach a child: `--watch`, `--watch-path`,
  `--watch-preserve-output`, `--hot`, `--inspect`, `--inspect-brk`,
  `--inspect-wait`, `--inspect-port`, `--inspect-publish-uid`.
- `NODE_OPTIONS` is part of the environment, so its flags reach each child.
  Put a flag that a child must not get on the command line of the worker.
- A worker start with no entry script (`-e`, `--eval`, `-p`, `--print`, a
  script from stdin), and a compiled executable (a Node single executable
  application, a `bun build --compile` binary), is refused with exit code
  1: nothing starts.

## Things to Avoid

**Do not start servers, timers or loops in the entry before `cli.run`.**
Each worker child runs the entry again.

**Do not send at module level.** A child runs the module again; its sends
are suppressed before it serves, and a producer process sends once per
start.

**Do not put `--inspect` in `NODE_OPTIONS` for a worker.** Each child gets
it and tries to bind the same debugger port.

**Do not set thresholds with `configureLogging` and expect them to stay
after `cli.run`.** Each command sets every logger to its `--log-level`.

## All Key Imports

<!-- ts-check: alone -->
```ts
import {
  // App
  Horsies, HorsiesErrorCode, cli,
  type HorsiesConfig, type ValidationReport, type HorsiesError, type RefusalCode,
  // Config
  type AppConfigInput, type DatabaseConfigInput, type DatabaseConnection, type SessionDatabase, type Secret,
  type PoolRecycle, type TcpKeepalives, type TcpUserTimeout, type RerunInputDefault, type ClaimMode, type ClusterCapSetting,
  type TaskCap, type CustomQueueInput, type QueueConfigInput, type PayloadPolicyInput, type PayloadBound,
  type RecoveryConfigInput, type RetentionConfigInput, type HoursRetention, type PausedWorkflowExpiry,
  type RetentionPeriod, type QueueRetention, type ResilienceConfigInput, type RetryAttempts,
  type SchemaMode, type LogFormat,
  // Schedules
  taskSchedule, type ScheduleSection, type TaskSchedule, type TaskScheduleOptions, type SchedulePattern,
  // Worker profiles
  type WorkerProfileInput, type IsolatedChildren, type IsolatedRecycleInput, type IsolatedPoolInput,
  type SharedAsyncChildren, type SharedRecycleInput, type SharedPoolInput, type SharedPoolSize,
  type ParentPoolInput, type ClaimLimitsInput, type TaskLimit, type MemoryLimit, type PoolSize, type ParentCpu,
  // State and hooks
  type StateKey, type ProviderFactory, type ChildStartHook,
  // Logging
  configureLogging, LOGGER_NAMES, LOGGER_PARENTS,
  type LoggingOptions, type LogLevels, type LogLevel, type LogThreshold, type LoggerName, type LoggerKey,
  type LogSink, type LogRecord, type LogField, type LogFieldValue, type LogSource,
} from "horsies";
```
