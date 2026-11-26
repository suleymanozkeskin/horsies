---
name: horsies
description: Guidance for the horsies npm package, the TypeScript client of the horsies task queue and workflow engine (PostgreSQL broker), for Node and Bun. Use when code imports from `horsies`, `horsies/zod`, `horsies/rerun/pg` or `horsies/rerun/postgres`, or uses `defineTask`, `defineWorkflow`, `Horsies.create`, `app.implement`, the `h.*` wire types, `TaskOutcome` or `cli.run`, or when the user works on horsies tasks, workflows, configuration, workers or monitoring in TypeScript.
---

# horsies (TypeScript) — Quick Reference

The npm package `horsies` is the TypeScript client of the horsies core
(`horsies-core`). The Python and Rust clients run on the same core, so the
behaviour and the guarantees are the same for the three clients. The package
loads the core as a native addon. It runs on Node 26 and later and on Bun 1.4
and later, and it is an ES module only.

This file is an introductory reference. Open a reference file of this
folder only when the task needs it:

| File | When to open |
|---|---|
| `tasks.md` | `defineTask`, the `h.*` wire types, the zod adapter, the stored input, `app.implement`, `ok` and `err`, the exception map, `TaskRuntime`, the fencing token, the cancel signal, `runtime.spawn`, app state, retry policies, `app.send`, `TaskSendError`, `retrySend`, `TaskHandle`, `TaskOutcome`, `ReadError`, tests of a body |
| `workflows.md` | `defineWorkflow`, the builder `w`, a value or an upstream node per parameter, `h.resultOf`, joins, the success policy, check cases, sub-workflows, `app.start`, `WorkflowStartError`, `retryStart`, `WorkflowHandle`, `app.workflowHandle` |
| `configs.md` | `Horsies.create` and its refusals, the broker, queues, worker profiles, recovery, retention, `cli.run` and its commands |
| `monitoring.md` | `app.pingDatabase`, `app.pingWorkers`, `app.monitoring`, the operator cancel, `createMonitoringHandler`, `app.rerun` and `app.rerunIn` |

The package has four entry points. The `exports` map allows no deep import:

| Import | Holds |
|---|---|
| `horsies` | The app, the contracts, the wire types, sends, reads, workflows, monitoring, `cli` |
| `horsies/zod` | `hz`: zod codecs for the Temporal types and `Decimal`, and `hz.wire` |
| `horsies/rerun/pg` | The rerun adapter for a `pg` transaction |
| `horsies/rerun/postgres` | The rerun adapter for a postgres.js transaction |

The docs site is `https://suleymanozkeskin.github.io/horsies/`. The shared
pages there state the behaviour of the core for every client.

## Deployment facts

- Use a dedicated PostgreSQL instance for the broker. Ownership,
  finalization and workflow transitions are transactional state changes in
  that database.
- A failover under asynchronous replication can lose committed sends,
  claims, task ends and workflow progress, so a task body can run again.
  Use synchronous replication for the horsies instance, or make each effect
  outside horsies idempotent. The reasons and the failover limits are on the
  shared docs page `operations/deployment-topology`.
- A database belongs to one client family. The core writes only into a
  database that the TypeScript client installed. It refuses a write into a
  database of the Python or the Rust client. Reads are not checked.
- Task bodies never run in the worker process. The worker starts the app's
  entry script again for each child, with the same runtime and its flags
  (except the `--watch` and `--inspect` flags). Every line of the entry
  before `cli.run(app)` runs in each child too. A start with no entry script
  (`-e`, a REPL) and a compiled executable are refused.
- Pre-1.0: breaking API changes may land in minor releases.

## Define a Task

A task has two parts. The contract (`defineTask`) holds everything that a
sender needs. It does no I/O, so an API server imports it without the
worker's code. The implementation (`app.implement`) holds the body. Only the
worker entry calls it.

```ts
import { defineTask, h } from "horsies";

export const addNumbers = defineTask({
  name: "add_numbers",
  input: { a: h.int(), b: h.int() },
  output: h.int(),
});
```

- `input` has one wire type per parameter. Each parameter is stored under
  its name, as Python stores keyword arguments: `{"a": 2, "b": 3}`. A task
  with no parameters has `input: {}` and stores `{}`.
- `output` is the wire type of the value. A zod 4 schema is accepted in
  `input` and `output` too. See `tasks.md`, Wire Types and The zod Adapter.
- `errors: ["CODE", ...]` declares the task's own codes. With it, a body can
  return only these codes, and a read switches over them.

## Run the App

One entry script is the worker, each child of the worker, and the `check`
command. It builds the app, implements every listed contract, and hands the
process to `cli.run`:

```ts
import { cli, Horsies } from "horsies";

const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { count: 4 } } },
  tasks: [addNumbers],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`${refusal.code} ${refusal.message}`);
  }

  process.exit(1);
}

const app = created.value;

app.implement(addNumbers, ({ a, b }) => addNumbers.ok(a + b));

process.exitCode = await cli.run(app);
```

```bash
node worker.ts worker --profile default
node worker.ts check --live
bun worker.ts worker --profile default
```

- `Horsies.create` does no I/O. It checks the config and every listed
  contract, and gives every refusal in one list (`HRS-nnn` codes). The app
  connects at its first operation.
- A worker refuses to start when a listed contract has no implementation.
- `cli.run` ends the process itself, with the exit code of the command.
- Start nothing in the entry before `cli.run`. A server, a timer or a loop
  started there starts again in each child.

See `configs.md` for the config fields, worker profiles and the commands.

## Send a Task and Read the Result

A producer (an API server, a script) builds an app with the same contracts.
It does not implement them:

```ts
export async function sendSum(producer: Horsies): Promise<void> {
  const sent = await producer.send(addNumbers, { a: 2, b: 3 });

  if (!sent.ok) {
    console.error(`${sent.error.code}: ${sent.error.message}`);

    return;
  }

  const read = await sent.value.get({ timeoutMs: 30_000 });

  if (!read.ok) {
    console.error(`the read failed: ${read.error.kind}`);

    return;
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(`sum = ${outcome.value}`);
      break;
    case "failed":
      console.log(`failed with ${outcome.error.code.code}`);
      break;
  }
}
```

- `send` returns `Result<TaskHandle<O, E>, TaskSendError<O, E>>`. Every
  refusal before the enqueue writes nothing.
- `get` returns `Result<TaskOutcome<O, E>, ReadError>`. The outer error is
  the read that failed (the wait ended, the task is held nowhere, the stored
  data does not read, the database failed). The inner value is what the task
  ended with.
- A result wait in a worker child, the process that runs task bodies, is
  refused with `waitInsideTask` before any statement. Compose tasks with a
  workflow.

## Define a Workflow

<!-- ts-check: alone -->
```ts
import { defineTask, defineWorkflow, h } from "horsies";

const fetchReport = defineTask({
  name: "fetch_report",
  input: { region: h.string() },
  output: h.string(),
});

const publishReport = defineTask({
  name: "publish_report",
  input: { report: h.resultOf(h.string()) },
  output: h.int(),
});

export const dailyReport = defineWorkflow(
  {
    name: "daily_report",
    definitionKey: "reports.daily_report.v1",
    params: { region: h.string() },
    output: h.int(),
    checkCases: [{ region: "eu" }],
  },
  (w, { region }) => {
    const fetched = w.task(fetchReport, { region }, { id: "fetch" });
    const published = w.task(publishReport, { report: fetched }, { id: "publish" });

    return { output: published };
  },
);
```

- A parameter declared `h.resultOf(T)` takes an upstream node. The node
  waits for it, and the body gets the upstream outcome.
- Every node has an `id`. A workflow with `params` declares `checkCases`.
- List the workflow in `Horsies.create({ tasks, workflows })` and start it
  with `app.start(dailyReport, { region: "eu" })`. See `workflows.md`.

## Result Types

| Operation | Returns |
|---|---|
| Task body | `contract.ok(value)` or `contract.err({ code, message, data })`, or a Promise of one |
| `Horsies.create` | `Result<Horsies, CreateRefused>` |
| `app.send`, `app.retrySend` | `Promise<Result<TaskHandle<O, E>, TaskSendError<O, E>>>` |
| `handle.get` | `Promise<Result<TaskOutcome<O, E>, ReadError>>` |
| `handle.info` | `Promise<Result<TaskInfo, ReadError>>` |
| `app.start`, `app.retryStart` | `Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>>` |
| Workflow `get`, `resultFor` | `Promise<Result<TaskOutcome<O, E>, WorkflowReadError>>` |
| Workflow `status`, `results`, `tasks`, `pause`, `resume`, `cancel` | `Promise<Result<_, HandleCoreError>>` |
| Health and monitoring reads | See `monitoring.md` |

`Result<T, E>` is `{ ok: true, value: T }` or `{ ok: false, error: E }`.
Every error type is a closed union. A send, start or handle error has a
`code`; a read error has a `kind`. Switch over it with a case for each
member. `retryable: true` means that the cause may clear; it does not mean
that a repeat is safe. The codes and their meaning are on the shared docs
page `tasks/errors`.

The lint package `@horsies/eslint-plugin` has the rule
`horsies/no-dropped-result`: an expression statement that drops a `Result`,
or a Promise of one, is an error. Write `void (await call())` for an
explicit discard. Its config also turns on
`@typescript-eslint/switch-exhaustiveness-check`.

### Thrown, never returned

These classes are thrown, and no result type has a variant for them. Import
them from `horsies`.

| Class | Cause |
|---|---|
| `CoreStopped` | A background task of the core panicked, so the core is stopped in this process. `stateAfter` is `noChange` for a read and `outcomeUnknown` for a write. Restart the process |
| `CoreStartFailed` | The core could not start in this process: its runtime, its log forwarder or its native addon |
| `TooManyPendingCalls` | The process has `bound` core calls in flight, the most it may have |
| `AddonOutsideMainThread` | A `worker_threads` isolate called the native addon, which runs on the main thread only |
| `ClientDefect` | The package met a value or a call that it must refuse first: a defect of the package or a misuse (a state key of another app) |

`CoreError` is the base of the process-state errors and `CoreDefect` the
base of the defects.

## CLI

```bash
node app.ts worker --profile NAME [--loglevel LEVEL] [--metrics-listen HOST:PORT]
node app.ts check [--live] [--loglevel LEVEL]
node app.ts web [--host HOST] [--port PORT] [--auth none|trusted-header] [--trusted-header NAME] [--enable-actions] [--trace-link TEMPLATE] [--loglevel LEVEL]
node app.ts metrics --listen HOST:PORT [--loglevel LEVEL]
npx horsies get-docs [--output DIR] [--ref main]
npx horsies skills install --target DIR [--force]
```

- `app.ts` is the app's own entry script. `bun app.ts ...` runs the same
  commands on Bun.
- Exit codes of the entry: 0 after an operator stop or a passing check, 1
  for a failure, 2 for arguments that do not parse.
- `--profile` has no default. Worker settings are in named profiles in
  `workerProfiles`, not in flags. See `configs.md`.
- The `horsies` bin of the package copies this skill folder to
  `DIR/horsies/` and downloads the docs of the release to `.horsies-docs/`
  (the TypeScript pages, the shared pages and `llms.txt`). It runs no app
  command.
