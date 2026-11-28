---
title: Thread Seats
summary: Run SHARED_THREAD tasks of a TypeScript app in shared thread children on Node - the sharedThread profile field, one worker_threads isolate per seat, providers per seat, the seat relay and its bounds, the heap cap of a seat and the memory bound of the child.
related: [worker-setup, ../tasks/defining-tasks, ../tasks/cancellation, ../cli, ../../workers/shared-thread-tasks, ../../workers/execution-modes, ../../workers/concurrency]
tags: [typescript, worker, SHARED_THREAD, sharedThread, seats, worker_threads, isolate, SEAT_RELAY_FULL, memory, exit code 81]
---

# Thread Seats

A shared thread child runs `SHARED_THREAD` tasks. It is one process with one `worker_threads` isolate per seat, and each seat runs one task at a time. The main thread of the child runs no task body. The behaviour of the class (the deadline, the kill, the routing) is on [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/). This page holds the API of the TypeScript client.

## Define the Children

Set `sharedThread` on a worker profile:

```ts
import type { WorkerProfileInput } from "horsies";

export const workerProfiles: Readonly<Record<string, WorkerProfileInput>> = {
  default: {
    isolated: { kind: "isolatedChildren", count: 2 },
    sharedThread: {
      kind: "sharedThreadChildren",
      count: 2,
      seats: 4,
      recycle: { afterPerSeat: { kind: "taskLimit", tasks: 200 }, above: { kind: "memoryLimit", mb: 2048 } },
      taskSendPool: { kind: "connections", connections: 4 },
    },
  },
};
```

This profile runs `ISOLATED` tasks in 2 isolated children, and `SHARED_THREAD` tasks on 2 × 4 = 8 seats.

| Type | Fields |
|------|--------|
| `SharedThreadChildren` | `kind: "sharedThreadChildren"`, `count`, `seats`, `recycle?: SharedRecycleInput`, `pool?: SharedPoolInput`, `taskSendPool?: PoolSize` |
| `NoSharedThreadChildren` | `kind: "noSharedThreadChildren"` |

| Field | Bounds | Default |
|-------|--------|---------|
| `count` | 1 to 1,024 | Required |
| `seats` | 1 to 256 | Required |
| `recycle.afterPerSeat` | `TaskLimit` | 100 tasks per seat |
| `recycle.above` | `MemoryLimit`, `mb` 1 to 16,777,216 | No limit |
| `taskSendPool` | `PoolSize`, 1 to 1,000 connections | 2 connections |

- A profile that leaves out `sharedThread` has no shared thread children. A worker with none runs `SHARED_THREAD` tasks in isolated children. See [What Runs Where](/horsies/workers/shared-thread-tasks/#what-runs-where).
- A field outside its bound is refused at `Horsies.create` with `HRS-218` and the path of the field.
- `SharedRecycleInput`, `SharedPoolInput` and `PoolSize` are the types of [Worker Setup](/horsies/typescript/workers/worker-setup/#recycling).

## Node Only

Shared thread children run on Node 26. Node caps the JavaScript heap of each `worker_threads` isolate (`resourceLimits`), so a seat at its cap ends alone. Bun ignores `resourceLimits`.

A worker started with `bun` refuses a profile with `SHARED_THREAD` children at its start. The refusal is `sharedThreadUnavailable`. The worker writes the cause on stderr, exits with code 1, and starts nothing:

```text
workerProfiles["default"].sharedThread: SHARED_THREAD children run on Node only: ...
```

To run the profile on Bun, run its tasks in isolated or shared async children.

## Write a Thread Task

Declare the task with `execution: "SHARED_THREAD"` and a `timeoutMs`. The body can be sync or async. It gets a `ThreadTaskRuntime`: the members of `TaskRuntime` without `spawn`.

<!-- ts-check: alone -->
```ts
import { gzipSync } from "node:zlib";

import { cli, defineTask, h, Horsies } from "horsies";

const compressReport = defineTask({
  name: "compress_report",
  execution: "SHARED_THREAD",
  timeoutMs: 30_000,
  input: { sections: h.array(h.string()) },
  output: h.string(),
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: {
    default: { sharedThread: { kind: "sharedThreadChildren", count: 2, seats: 4 } },
  },
  tasks: [compressReport],
});

if (!created.ok) {
  process.exit(1);
}

const app = created.value;

app.implement(compressReport, ({ sections }, { ok, cancelState }) => {
  const parts: Buffer[] = [];

  for (const section of sections) {
    const state = cancelState();

    switch (state.kind) {
      case "requested":
        return ok("");
      case "notRequested":
        parts.push(gzipSync(section));
        break;
    }
  }

  return ok(Buffer.concat(parts).toString("base64"));
});

process.exitCode = await cli.run(app);
```

- A sync body holds its isolate until it returns. It reads `cancelState()` between its steps. The read needs no turn of the event loop: the main thread writes the cause into a shared cell of the seat.
- An async body passes `runtime.signal` to the calls that it awaits. The signal fires at the deadline and at an operator cancel. See [Cancellation](/horsies/typescript/tasks/cancellation/).
- A `SHARED_THREAD` contract takes no `cancelGraceMs`. A contract with one, or with no `timeoutMs`, fails typecheck.

## What Runs in a Seat

Each seat isolate runs the app's entry file from its first line, as each child does. Then `cli.run(app)` serves the seat.

| Item | Where it runs |
|------|---------------|
| The task bodies | In the seat isolates, one run per seat at a time |
| `app.provide` factories | In each seat isolate, in registration order, before the seat takes a run. Each seat has its own value. A replacement isolate runs them again |
| `app.onChildStart` hooks | On the main thread of the child, once per child |
| The addon | On the main thread of the child only. A seat isolate never loads it |

- The boot of the seat isolates is one start hook of the child: it runs for at most 10 s. A provider that throws or rejects, or a boot past its bound, ends the child with exit code 173, and the worker stops with `CHILD_HOOK_FAILED`.
- A provider value belongs to one seat. A pool that a provider opens is opened once per seat: size it for one run at a time.
- Each isolate has its own globals. An OpenTelemetry SDK that the entry file sets before `cli.run`, or that a provider sets, reaches each seat. See [Tracing Setup](/horsies/typescript/observability/tracing-setup/#shared-thread-seats).
- A throw outside every run, or `process.exit` in a seat, ends that isolate. Its run ends `WORKER_CRASHED`, and a new isolate takes the seat. The retry policy of the task decides the retry.
- The main thread does not watch the event loop of a seat. A blocked seat names no culprit.

## The Seat Relay

A seat isolate has no addon. Its sends, its reads, its workflow calls, its monitoring and health reads, its cancels and its reruns go to the main thread of the child as messages. The main thread runs each call on its own core, and sends the answer back. The calls keep their types and their error unions.

The relay of each seat has two bounds:

| Bound | Value | When it is full |
|-------|-------|-----------------|
| Calls that wait for the database at once | `taskSendPool` of the profile: 2 connections by default | The next call waits in the isolate, in order |
| Calls that wait in the isolate | 10,000 (the core's `MAX_RUN_WORK`) | The next call fails at once with `SEAT_RELAY_FULL` |

- The sends of every seat of the child share the `taskSendPool` of the child. A seat can hold at most that many calls at the database at once.
- `SEAT_RELAY_FULL` is `retryable: true`. Nothing was sent, so nothing was read or written. Call again.
- Each relayed call has the member `seatRelayFull` in its error union: a send (`TaskSendError`), a read (`ReadError`), a workflow start and handle call, a monitoring or health read, a ping, a cancel and a rerun. See [Sending Tasks](/horsies/typescript/tasks/sending-tasks/#tasksenderror).
- The prepare of a send needs no database. It goes at once and takes no place of the first bound. At most 10,000 prepares wait for their answer.

## Memory

### The Heap Cap of a Seat

The JavaScript heap of each seat isolate has the heap limit of the main isolate of its child. The worker profile has no member for it.

- `--max-old-space-size` on the worker command sets the limit. Each child gets the flag of the worker, so each seat gets the same limit. Without the flag, the limit is the default of V8.
- A seat at its cap ends alone with `ERR_WORKER_OUT_OF_MEMORY`. Its run ends `WORKER_CRASHED`, the child lives, and a new isolate takes the seat.
- The cap does not cover `ArrayBuffer` and `Buffer` memory. That memory of every seat counts toward the whole child.

```bash
node --max-old-space-size=512 main.ts worker --profile default
```

### The Memory Bound of the Child

`recycle.above` bounds the resident memory of the whole child, the seat isolates included:

1. After a task, the worker reads the resident memory of the child. At or above the bound, the worker replaces the child: it takes no new task, its running tasks end, and a new child starts.
2. While the child holds runs, its main thread reads the resident memory of the child every 100 ms. At or above the bound, the child writes one log line with the bound and the measured value, and ends with exit code 81.
3. The worker replaces a child that ended with code 81 as a memory recycle. Each run that the child held is lost with it: the run ends `WORKER_CRASHED`, and the retry policy of its task decides the retry.

With no `recycle.above`, the child reads its memory at no time. The worker start fails when `recycle.above` is at or below the measured child baseline. See [Memory Recycling](/horsies/workers/concurrency/#memory-recycling).

## Things to Avoid

**Do not start the profile on Bun.** The worker refuses it with `sharedThreadUnavailable`.

**Do not keep large buffers in a seat.** The heap cap of a seat does not cover `Buffer` memory. The buffers of all seats grow the whole child, until `recycle.above` ends it with every run in it.

**Do not size a provider pool for the whole child.** Each seat runs the provider, so the child opens one pool per seat.

**Do not run a sync loop without a read of `cancelState()`.** A sync body holds its isolate. It stops at its deadline only where it reads the cancel state.
