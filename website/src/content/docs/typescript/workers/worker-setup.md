---
title: Worker Setup
summary: Define the worker profiles of a TypeScript app, give task bodies app state with app.provide, add child start hooks, and run a worker of a profile with cli.run on Node or Bun.
related: [../cli, ../configuration/app-config, ../tasks/defining-tasks, ../../workers/execution-modes, ../../workers/concurrency, ../../workers/worker-architecture]
tags: [typescript, worker, WorkerProfileInput, provide, onChildStart, child mode, Bun]
---

# Worker Setup

A worker runs one named profile of `workerProfiles`. The profile holds every setting of the worker: the children of each execution class, their recycling, the pools, the claim limits and the drain timeout. The worker starts the app's entry file again in child mode for each child, and the task bodies run there. The process model is on [Worker Architecture](/horsies/workers/worker-architecture/).

## Define a Profile

```ts
import type { WorkerProfileInput } from "horsies";

export const workerProfiles: Readonly<Record<string, WorkerProfileInput>> = {
  default: {
    isolated: {
      kind: "isolatedChildren",
      count: 2,
      recycle: { after: { kind: "taskLimit", tasks: 500 }, above: { kind: "memoryLimit", mb: 1024 } },
    },
    sharedAsync: { kind: "sharedAsyncChildren", count: 4, seats: 25 },
    drainTimeoutMs: 120_000,
  },
};
```

Pass the profiles to `Horsies.create({ ..., workerProfiles })`, then start a worker of one profile:

```bash
node main.ts worker --profile default
```

- A class that the profile leaves out has no children. Its `kind` can also say so: `{ kind: "noIsolatedChildren" }` or `{ kind: "noSharedAsyncChildren" }`. The core refuses a profile that starts no children, at `Horsies.create`.
- A task runs on the children of its execution class. The routing of each class, and the class to choose by the work of a task, are on [Choosing an Execution Mode](/horsies/workers/execution-modes/).
- A field outside its bound is refused at `Horsies.create` with `HRS-218` and the path of the field.

## WorkerProfileInput

| Field | Type | Default |
|-------|------|---------|
| `isolated` | `IsolatedSetting`: `IsolatedChildren` or `NoIsolatedChildren` | No isolated children |
| `sharedAsync` | `SharedAsyncSetting`: `SharedAsyncChildren` or `NoSharedAsyncChildren` | No shared async children |
| `pool` | `ParentPoolInput`: the connection pool of the worker process | Its size follows the seats |
| `claim` | `ClaimLimitsInput` | No limits |
| `drainTimeoutMs` | `number`: how long a stopping worker waits for running tasks, in milliseconds | 300,000 (300 s) |
| `parentCpu` | `"shared"` or `"ownCore"` | `"shared"` |

### Children

| Type | Fields |
|------|--------|
| `IsolatedChildren` | `kind: "isolatedChildren"`, `count`, `recycle?: IsolatedRecycleInput`, `pool?: IsolatedPoolInput`, `taskSendPool?: PoolSize` |
| `NoIsolatedChildren` | `kind: "noIsolatedChildren"` |
| `SharedAsyncChildren` | `kind: "sharedAsyncChildren"`, `count`, `seats`, `recycle?: SharedRecycleInput`, `pool?: SharedPoolInput`, `executor?: ExecutorThreads`, `taskSendPool?: PoolSize` |
| `NoSharedAsyncChildren` | `kind: "noSharedAsyncChildren"` |

An isolated child runs one task at a time in its own process. A shared async child runs up to `seats` tasks at once on its one event loop. The task code of a shared async child runs on its event loop. `executor` sets the threads of the core's runtime in the child: they run its database writes, its connections and its frames to the worker. `ExecutorThreads` is `{ kind: "threads", threads }` (1 to 256) or `{ kind: "fromSeats" }` (the default). See [Concurrency](/horsies/workers/concurrency/#worker-level-children).

### Recycling

| Type | Fields | Default |
|------|--------|---------|
| `IsolatedRecycleInput` | `after?: TaskLimit`: replace a child after it completes this many tasks; `above?: MemoryLimit`: replace a child when its resident memory reaches this limit | `after` 100 tasks, `above` no limit |
| `SharedRecycleInput` | `afterPerSeat?: TaskLimit`: replace a child after `tasks × seats` tasks; `above?: MemoryLimit` | `afterPerSeat` 100 tasks, `above` no limit |

- `TaskLimit` is `{ kind: "taskLimit", tasks }` or `{ kind: "noTaskLimit" }`.
- `MemoryLimit` is `{ kind: "memoryLimit", mb }` or `{ kind: "noMemoryLimit" }`.
- A child is recycled when either limit is reached. The worker start fails when a memory limit is at or below the measured child baseline.

The rules are on [Concurrency](/horsies/workers/concurrency/#process-recycling).

### Pools

| Type | Fields | Default |
|------|--------|---------|
| `IsolatedPoolInput` | `minSize?: number`, `maxSize?: PoolSize`, `prePing?: boolean` | 0 open, at most 1 connection, no check |
| `SharedPoolInput` | `minSize?: number`, `maxSize?: SharedPoolSize`, `prePing?: boolean` | 0 open, a size from the seats, no check |
| `ParentPoolInput` | `maxSize?: SharedPoolSize` | A size from the seats |
| `PoolSize` | `{ kind: "connections", connections }` | `taskSendPool`: 2 connections |

- `SharedPoolSize` is a `PoolSize` or `{ kind: "fromSeats" }`.
- `prePing: true` checks each connection when a write takes it from the pool.
- `taskSendPool` is the pool that a send from task code opens in its child.

### Claim Limits and Drain

| Item | Does |
|------|------|
| `claim.perQueuePerPass` | `TaskLimit`: the most tasks that one claim pass takes from one queue |
| `claim.claimedPerWorker` | `TaskLimit`: the most CLAIMED tasks that the worker holds |
| `drainTimeoutMs` | 1 to 86,400,000 (24 hours). At the deadline the worker kills the children that still run a task |
| `parentCpu: "ownCore"` | The worker process runs on one physical core and the children on the other CPUs. Linux only: the start fails on another platform or with fewer than 2 physical cores |

Set the drain timeout below the stop window of the supervisor. See [Graceful Shutdown](/horsies/workers/worker-architecture/#graceful-shutdown).

### Bounds

| Field | Bounds |
|-------|--------|
| `isolated.count`, `sharedAsync.count` | 1 to 1,024 |
| `executor.threads` | 1 to 256 |
| `sharedAsync.seats` | 1 to 1,000 |
| `tasks` of a `TaskLimit` | 1 to 2,147,483,647 |
| `mb` of a `MemoryLimit` | 1 to 16,777,216 |
| `connections` of a `PoolSize` | 1 to 1,000 |
| `minSize` | 0 to 1,000 |

## Execution Classes on Node and Bun

A worker starts each child with the runtime that runs the worker: a worker started with `bun` has Bun children, and a worker started with `node` has Node children.

<!-- members: Execution -->

| Class | Node 26 | Bun 1.4 |
|-------|---------|---------|
| `ISOLATED` | Yes | Yes |
| `SHARED_ASYNC` | Yes | Yes |
| `SHARED_THREAD` | Yes | No. The worker refuses a profile with `SHARED_THREAD` children with `sharedThreadUnavailable`: see [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/) |

A shared thread child runs each seat in a `worker_threads` thread of its own:

- The JavaScript heap of each seat has the heap limit of the child's main thread: the limit that `--max-old-space-size` on the worker command sets, or the default of V8. The profile has no member for it. A seat at that limit ends alone: its run ends `WORKER_CRASHED`, and a new thread takes the seat.
- The limit does not cover `ArrayBuffer` and `Buffer` memory. That memory of every seat counts toward the whole child. `recycle.above` bounds the resident memory of the whole child: the worker replaces the child after a task once the child reaches it.
- The sends, reads and workflow calls of a seat go through the main thread of the child. At most `taskSendPool` of them per seat wait for the database at once.

A shared async child watches its event loop. When the loop is blocked, the child names the task that held it: see [A Blocked Event Loop](/horsies/workers/shared-async-tasks/#a-blocked-event-loop). The child marks the task of each synchronous part of a body and of `runtime.spawn` work on both runtimes. The part of a body after an `await` is marked through `v8.promiseHooks`, which only Node has. On Bun, a block after an `await` names no task: it takes the path of a block that no one task held.

## Child Mode

The worker starts each child with this command line:

```text
<runtime> <runtime flags> <entry file> --horsies-child <fd> <worker pid>
```

- `<runtime>` is the runtime of the worker (`process.execPath`). The environment and the working directory reach each child unchanged.
- Each runtime flag of the worker reaches the children, except the file watch flags (`--watch`, `--watch-path`, `--watch-preserve-output`, `--hot`) and the inspector flags (`--inspect`, `--inspect-brk`, `--inspect-wait`, `--inspect-port`, `--inspect-publish-uid`). The worker writes one line on stderr for each flag that it leaves out.
- `NODE_OPTIONS` is part of the environment: its flags reach each child as they are. Put a flag that a child must not get on the command line of the worker.
- `cli.run(app)` reads `--horsies-child` before any other argument and serves the child until the worker stops it. The child ends its process itself, with the exit code that the worker reads. See [CLI Reference](/horsies/typescript/cli/#exit-codes).

The worker refuses to start, and exits with code 1, when the child command cannot be built:

| Start of the worker | Why |
|---------------------|-----|
| Code from `-e`, `--eval`, `-p` or `--print` | A child has no entry file to start again |
| A REPL, or an entry file read from stdin | A child cannot read the entry again |
| An entry path that is not absolute | The child command needs the absolute path |
| A Bun compiled executable, or a Node single executable application | Run the entry file with the runtime |

### Every Line Before `cli.run` Runs in Each Child

Each child runs the entry file from its first line. Build the app there (config, implementations, providers, start hooks), and start nothing else: each child would start it again.

- In a child, sends stay suppressed until the child serves: a send at module level returns `SEND_SUPPRESSED` and writes nothing. The `check` command suppresses sends too, and `HORSIES_SUPPRESS_SENDS=1` suppresses them in any process.
- A child does not wait for its event loop to empty before it ends: a pool that a provider opens does not keep the child alive.
- A child installs handlers of `SIGINT` and `SIGTERM` that do nothing. A signal sent to every process of the group stops the worker, and the worker drains its children.

## App State

`app.provide(factory)` adds a provider of app state and returns its key. A body reads the value with `runtime.state(key)`.

<!-- ts-check: alone -->
```ts
import { cli, defineTask, h, Horsies } from "horsies";
import pg from "pg";

const countOrders = defineTask({
  name: "count_orders",
  input: { customerId: h.uuid() },
  output: h.int(),
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { kind: "isolatedChildren", count: 2 } } },
  tasks: [countOrders],
});

if (!created.ok) {
  process.exit(1);
}

const app = created.value;

const OrdersDb = app.provide(() => new pg.Pool({ connectionString: process.env["ORDERS_DATABASE_URL"] }));

app.onChildStart(async () => {
  const pool = new pg.Pool({ connectionString: process.env["ORDERS_DATABASE_URL"], max: 1 });

  await pool.query("SELECT 1");
  await pool.end();
});

app.implement(countOrders, async ({ customerId }, runtime) => {
  const result = await runtime
    .state(OrdersDb)
    .query<{ count: number }>("SELECT count(*)::int AS count FROM orders WHERE customer_id = $1", [customerId]);

  return runtime.ok(result.rows[0]?.count ?? 0);
});

process.exitCode = await cli.run(app);
```

| Item | Does |
|------|------|
| `app.provide<T>(factory: () => T \| Promise<T>): StateKey<Awaited<T>>` | Adds the provider and returns its key. Calls nothing |
| `runtime.state<T>(key: StateKey<T>): T` | The value that the provider built in this child |

- The factory runs in each task child, a replacement child included, before the child reports ready. The providers run in registration order, before the `onChildStart` hooks.
- Each factory runs for at most 10 s. A factory that throws, rejects or runs past its bound ends the child with exit code 173, and the worker stops with `CHILD_HOOK_FAILED`. The logged cause names the provider by its position and its function name: `provide #1 (anonymous)`.
- The worker process and a producer never run a factory. `TaskRuntime.forTest` takes the state of a test: see [Testing](/horsies/typescript/operations/testing/).
- A key of another app in `runtime.state` is a defect (`ClientDefect`).
- A call of `app.provide` after `cli.run` serves the child is a defect (`ClientDefect`). Add every provider before `cli.run`.
- A defect follows the defect rule: in a worker process it is written to stderr and the process aborts; in another process it is thrown.

## Child Start Hooks

`app.onChildStart(hook)` runs a sync or async hook once in each task child, a replacement child included.

| Item | Does |
|------|------|
| `app.onChildStart(hook: () => void \| Promise<void>): void` | Adds the hook. Calls nothing |

- The hooks run after the providers and before the child takes its first task, in registration order. The first failure ends the rest.
- Each hook runs for at most 10 s. A hook that throws, rejects or runs past its bound ends the child with exit code 173, and the worker stops with `CHILD_HOOK_FAILED`. The logged cause names the hook by its position and its function name, for example `onChildStart #2 checkCache` for the second hook, a function named `checkCache`.
- Only task children run the hooks: the worker process and a producer never call them.
- A call of `app.onChildStart` after `cli.run` serves the child is a defect (`ClientDefect`).
- Work that a hook starts and leaves running belongs to no task.

To give task bodies a value that each child builds, use `app.provide`, not a hook.

## Run a Worker

`<entry> worker --profile <name>` runs a worker through `cli.run(app)`. The command, its flags and its exit codes are on [CLI Reference](/horsies/typescript/cli/#worker). The command runs these steps in order:

1. The check of the app, without the schedules. A refused check writes its report on stderr.
2. The command that starts the children (see [Child Mode](#child-mode)).
3. The worker manifest, the profile and the connection settings.
4. The worker process of the core, which connects, starts the children and claims. `SIGTERM` or `SIGINT` stops it after the drain.

Nothing starts before step 4. The command exits with code 1 and writes the cause on stderr when:

| Cause | Nothing started |
|-------|-----------------|
| The check refused the app: for example, a listed contract with no implementation in this entry (`HRS-300`) | Yes |
| The child command cannot be built | Yes |
| A config value is outside the bound of its worker manifest field | Yes |
| The profile set is refused, or `--profile` names no profile of the app; the report lists the profile names | Yes |
| The connection settings fail the database config check | Yes |
| The core of this process refused the call | Yes |
| The start failed, for example with `CHILD_HOOK_FAILED`; the worker claimed nothing | The worker claimed nothing |
| The worker was ready, then failed; its running tasks drained first | No |

## Things to Avoid

**Do not start servers, timers or loops at module level of the entry file.** Each worker child runs the entry again and starts them again.

**Do not implement a task in a module that the worker entry does not import.** The worker start refuses a listed contract that has no implementation in its entry (`HRS-300`).

**Do not set a memory limit at or below the child baseline.** The worker start fails. See [Memory Recycling](/horsies/workers/concurrency/#memory-recycling).

**Do not start the worker with a flag in `NODE_OPTIONS` that a child must not get.** `NODE_OPTIONS=--inspect` makes each child bind the debugger port of the worker. Give the flag on the command line of the worker.
