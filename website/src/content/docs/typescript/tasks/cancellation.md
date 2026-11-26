---
title: Cancellation
summary: How a cancel and a deadline reach a task body through runtime.signal and runtime.cancelState(), what happens to a body that does not stop, the cancel grace, and the work that runtime.spawn tracks.
related: [defining-tasks, errors, ../operations/testing, ../../workers/shared-async-tasks, ../../workers/execution-modes, ../../monitoring/action-semantics]
tags: [typescript, cancel, cancellation, deadline, timeout, signal, AbortSignal, cancelState, spawn, cancelGraceMs]
---

# Cancellation

A cancel reaches a task body through its runtime: `runtime.signal` fires, and `runtime.cancelState()` names the cause. JavaScript cannot stop a Promise from outside. A body stops only where it passes the signal to the calls that it awaits, or where it reads the cancel state between its steps.

## When the Signal Fires

| Cause | When | Classes |
|-------|------|---------|
| `"OPERATOR"` | An operator cancels the running task: `app.monitoring.cancelTask` with the policy `"includeRunning"`, or the web UI. See [Monitoring Queries](/horsies/typescript/monitoring/monitoring-queries/) | Every class |
| `"DEADLINE"` | The run reaches its `timeoutMs` | `SHARED_ASYNC` |

- The reason of the signal is `{ cause }`. The first cause stays: a deadline after an operator cancel does not change it.
- An operator cancel moves the task to `CANCELLED` at once. What the body returns after the cancel is dropped. A cancel that comes before the body starts: the body does not run. The rules of each task status are on [Action Semantics](/horsies/monitoring/action-semantics/#cancelling-a-running-task).
- At its deadline an `ISOLATED` run gets no signal: the worker kills its child and stores `TASK_TIMEOUT`.
- A `SHARED_ASYNC` run whose deadline fired ends as `TASK_TIMEOUT`, whatever the body returns after the signal.

## Pass the Signal

Give `runtime.signal` to each call that takes an `AbortSignal`: `fetch`, `setTimeout` of `node:timers/promises`, and the clients that take a signal. The call then rejects with the signal's reason, and the body ends.

```ts
import { setTimeout as sleep } from "node:timers/promises";
import { defineTask, h, Horsies } from "horsies";

const syncFeed = defineTask({
  name: "sync_feed",
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
  cancelGraceMs: 2_000,
  input: { feedUrl: h.string(), pages: h.int() },
  output: h.int(),
});

const importBatches = defineTask({
  name: "import_batches",
  timeoutMs: 600_000,
  cancelGraceMs: 5_000,
  input: { batchIds: h.array(h.string()) },
  output: h.int(),
});

const warmCaches = defineTask({
  name: "warm_caches",
  execution: "SHARED_ASYNC",
  timeoutMs: 60_000,
  input: { urls: h.array(h.string()) },
  output: h.int(),
  errors: ["TOO_MUCH_WORK"],
});

const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [syncFeed, importBatches, warmCaches],
});

if (!created.ok) {
  throw new Error(created.error.refusals.map((refusal) => refusal.message).join("\n"));
}

const app = created.value;

app.implement(syncFeed, async ({ feedUrl, pages }, runtime) => {
  let items = 0;

  for (let page = 1; page <= pages; page += 1) {
    const response = await fetch(`${feedUrl}?page=${page}`, { signal: runtime.signal });
    const body: unknown = await response.json();

    items += Array.isArray(body) ? body.length : 0;
    await sleep(100, undefined, { signal: runtime.signal });
  }

  return syncFeed.ok(items);
});
```

A body that holds the thread between steps reads `runtime.cancelState()`. It reads the cause with no turn of the event loop:

```ts
function importBatch(batchId: string): void {
  void batchId; // stands in for one step of the work
}

app.implement(importBatches, ({ batchIds }, runtime) => {
  let done = 0;

  for (const batchId of batchIds) {
    const state = runtime.cancelState();

    switch (state.kind) {
      case "requested":
        // What the task returns after an operator cancel is dropped.
        return importBatches.ok(done);
      case "notRequested":
        importBatch(batchId);
        done += 1;
        break;
    }
  }

  return importBatches.ok(done);
});
```

| `cancelState()` | Means |
|-----------------|-------|
| `{ kind: "notRequested" }` | No cancel and no deadline yet |
| `{ kind: "requested", cause }` | The signal fired; `cause` is `"OPERATOR"` or `"DEADLINE"` |

## A Body That Does Not Stop

`cancelGraceMs` is the time that a body gets to stop after the signal. It is from 100 to 10,000 ms.

- **`ISOLATED`**: the body runs until it returns, or until the kill at its `timeoutMs`. With `cancelGraceMs`, the worker kills the child `cancelGraceMs` after an operator cancel when the body has not ended; the task stays `CANCELLED`. Without it, an operator cancel kills nothing.
- **`SHARED_ASYNC`** (default grace 1,000 ms): a body that has not settled `cancelGraceMs` after the signal keeps its seat. Its child takes no new task, and the worker kills the child when no run in it has time left. No retry runs next to the live body. The steps, and the stored `cancel_end`, are on [Shared Async Tasks](/horsies/workers/shared-async-tasks/#the-timeout) and [An Operator Cancel](/horsies/workers/shared-async-tasks/#an-operator-cancel).
- **`SHARED_THREAD`** takes no `cancelGraceMs`.

So a `SHARED_ASYNC` body stops at its deadline only where it passes `runtime.signal`. Many I/O libraries take no signal. Declare a task whose libraries take no signal `ISOLATED`, the default: at the deadline the worker kills its child, and the task never holds a shared seat past its deadline.

## Tracked Work

`runtime.spawn(name, work)` starts work that belongs to the run. `work` is a function that takes its own signal and returns a Promise. `spawn` gives `Result<Promise<T>, SpawnRefused>`: the Promise settles with the work.

```ts
app.implement(warmCaches, async ({ urls }, runtime) => {
  const started: Promise<Response>[] = [];

  for (const url of urls) {
    const spawned = runtime.spawn(`warm ${url}`, (signal) => fetch(url, { signal }));

    if (!spawned.ok) {
      return warmCaches.err({ code: "TOO_MUCH_WORK", message: spawned.error.kind });
    }

    started.push(spawned.value);
  }

  const responses = await Promise.allSettled(started);

  return warmCaches.ok(responses.filter((response) => response.status === "fulfilled").length);
});
```

- The signal of the work fires at the deadline, at an operator cancel, and at the stop of left work, with the reason `{ cause: "LEFT_WORK" }`.
- The run waits for its work after the body returns: 1 s in an `ISOLATED` child, `cancelGraceMs` in a shared async child. Then the work gets the `LEFT_WORK` signal. The outcome stays what the body returned. See [A Return That Leaves Work](/horsies/workers/shared-async-tasks/#a-return-that-leaves-work).
- Work that does not end after its signal keeps the run's seat until it ends or the child is replaced.
- Work may spawn more work after the body returned: it belongs to the same run.
- A Promise that the body starts with no `spawn` and does not await is not tracked: the run does not wait for it, and its signal is not the run's.

| `SpawnRefused` | Cause | Nothing starts |
|----------------|-------|----------------|
| `{ kind: "runEnded" }` | The run is over: its body returned and its work ended or was stopped | Yes |
| `{ kind: "tooMuchWork", bound }` | The run holds `bound` (10,000) pieces of work | Yes |

`ThreadTaskRuntime`, the runtime of a `SHARED_THREAD` body, has no `spawn`: a call fails typecheck.

## In a Test

`TaskRuntime.forTest` gives a runtime whose signal never fires: `cancelState()` is `{ kind: "notRequested" }`, and `spawn` runs the work at once and is never refused. See [Testing](/horsies/typescript/operations/testing/).

## Things to Avoid

**Don't catch the abort to go on working.** The run still ends as `TASK_TIMEOUT` or `CANCELLED`, and in a shared async child the body holds its seat past the grace.

<!-- ts-check: skip elided -- a Wrong / Correct pair of statements -->
```ts
// Wrong: the body goes on after the signal
try {
  await fetch(url, { signal: runtime.signal });
} catch {
  await fetch(url);
}

// Correct: let the rejection end the body
await fetch(url, { signal: runtime.signal });
```

**Don't start work with no `spawn` and no `await`.** The run does not wait for it and does not signal it, so it runs on into the next task of the child.

**Don't declare `SHARED_ASYNC` for a body that cannot pass the signal.** Keep it `ISOLATED`.
