---
title: Event Handlers
summary: Register typed handlers of the worker's lifecycle events with app.onEvent and start hooks of the handler child with app.onHandlerChildStart; all handlers of the handler child share one event loop.
related: [../workers/worker-setup, ../observability/tracing-setup, ../cli, ../../observability/events, ../../workers/worker-architecture]
tags: [typescript, events, onEvent, onHandlerChildStart, AttemptEnded, EventHandler, handler child]
---

# Event Handlers

The worker emits one event at each step of a task and of its children. A handler takes the events of one event type. The handlers run in the handler child of the worker: the app's entry file again in child mode, as for the task children. A handler never runs in the worker process, so a handler that fails or blocks does not change a task outcome and does not delay a claim. The event contract and the behaviour of the handler child are on [Events](/horsies/observability/events/).

## Register a Handler

<!-- ts-check: alone -->
```ts
import { type AttemptEnded, cli, Horsies, type TaskClaimed } from "horsies";

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { kind: "isolatedChildren", count: 2 } } },
  tasks: [],
});

if (!created.ok) {
  process.exit(1);
}

const app = created.value;

function howItEnded(event: AttemptEnded): string {
  switch (event.end.kind) {
    case "reportedByChild":
      return event.end.result.kind;
    case "decidedByWorker":
      return event.end.reason;
    case "notKnown":
      return `not known (child ${String(event.end.childPid)})`;
  }
}

app.onEvent("AttemptEnded", { name: "audit.attempt_ended" }, (event) => {
  console.log(`task ${event.taskId} attempt ${String(event.attempt)} ${howItEnded(event)} after ${event.runDuration.toString()}`);
});

app.onEvent("TaskClaimed", { name: "audit.task_claimed" }, async (event: TaskClaimed) => {
  await fetch("https://audit.internal/claims", {
    method: "POST",
    body: JSON.stringify({ taskId: event.taskId, queueWaitMs: event.queueWait.total("milliseconds") }),
    signal: AbortSignal.timeout(2_000),
  });
});

process.exitCode = await cli.run(app);
```

Register every handler before `cli.run(app)`. The worker reads the handlers from the app to start its handler child, and the handler child runs the same entry file.

| Item | Does |
|------|------|
| `app.onEvent<T extends EventTypeName>(eventType: T, options: EventHandlerOptions, handler: EventHandler<T>): void` | Adds the handler of the events of `eventType`. Calls nothing |
| `EventHandlerOptions` | `{ name: string }` |
| `EventHandler<T>` | `(event: EventOf<T>) => void \| Promise<void>`: sync or async |
| `EventOf<T>` | The member of `AnyEvent` whose `kind` is `T` |

- The handler takes the events of its type only. The type of `event` comes from `eventType`, so a later event type changes no handler.
- `name` names the handler in the logs, and is the `handler` label of its metrics.
- `app.onEvent` never throws. A refused registration is recorded: `check` reports it, and a worker refuses to start while the app holds one.
- A call of `app.onEvent` after `cli.run` serves the handler child is a defect (`ClientDefect`).

<!-- members: HorsiesErrorCode subset -->

| Code | Refused |
|------|---------|
| `HRS-222` | The event type is not an event type of the contract. TypeScript refuses it at typecheck |
| `HRS-223` | The handler is not a function |
| `HRS-225` | The name is not a text, is empty, has more than 255 characters, or is `(unknown)` |
| `HRS-224` | An earlier handler has the name |
| `HRS-226` | The app has 32 handlers, the most |

## Event Types

`EventTypeName` holds 11 names. Each event has `kind` (its type name) and `at`, the wall-clock time of the worker at the step, as a `Temporal.Instant`.

| `kind` | Emitted when |
|--------|--------------|
| `"TaskClaimed"` | The worker claimed a task |
| `"ClaimReleased"` | A claimed task went back to the queue before it reached a child |
| `"AttemptPlaced"` | A task reached a child |
| `"AttemptNotStarted"` | The child stopped before it set the task `RUNNING`; no task code ran |
| `"AttemptEnded"` | An attempt ended: the child reported its end, the worker ended it, or the worker did not see how it ended (`end: AttemptEnd`) |
| `"AttemptRecorded"` | The worker recorded the end of an attempt (`record: AttemptRecord`) |
| `"TaskRecovered"` | A recovery pass of the worker changed a task |
| `"ChildStarted"` | A child of a slot sent its ready frame |
| `"ChildReplaced"` | A slot replaced its child |
| `"SlotDown"` | A slot is down after a boot failure that can clear |
| `"SlotRecovered"` | A slot that was down serves again |

An event holds ids, names, codes, numbers and times. It never holds a task input, a result or an error message. The types are generated from the core's event catalog, so the Python and Rust clients read the same events.

### AttemptEnded

| Field | Type |
|-------|------|
| `kind` | `"AttemptEnded"` |
| `at` | `Temporal.Instant` |
| `taskId`, `taskName`, `queue` | `string` |
| `attempt` | `number`, from 1 |
| `execution` | `Execution`: the class of the child that ran the attempt |
| `runDuration` | `Temporal.Duration` |
| `end` | `AttemptEnd` |

`AttemptEnd` is told apart by `kind`:

| `kind` | Fields | Means |
|--------|--------|-------|
| `"reportedByChild"` | `result: ChildResult`, `start: AttemptStart` | The child reported the end of the attempt |
| `"decidedByWorker"` | `reason: WorkerDecision` | The worker ended the attempt itself: its child is gone |
| `"notKnown"` | `childPid`, `value` | The worker did not see how the attempt ended |

| `ChildResult` `kind` | Fields |
|----------------------|--------|
| `"completed"` | none |
| `"failed"` | `errorCode: AttemptErrorCode`: `{ kind: "builtIn", code }`, `{ kind: "custom", code }` or `{ kind: "customOverBound" }` |
| `"workerFailure"` | none |
| `"timedOut"` | none |
| `"cancelled"` | `end: OperatorCancelEnd`: `"RETURNED_AFTER_SIGNAL"`, `"DROPPED_AFTER_GRACE"` or `"BEFORE_BODY"` |

`WorkerDecision` is `"TIMED_OUT"`, `"TASK_BLOCKED_LOOP"`, `"NEIGHBOUR_BLOCKED_LOOP"`, `"CHILD_LOST"`, `"KILLED_AFTER_CANCEL"` or `"LOST_AFTER_CANCEL"`. The meaning of each end is on [The End of an Attempt](/horsies/observability/events/#the-end-of-an-attempt). With tracing on, the handler child makes the span of each `decidedByWorker` attempt: see [Tracing Setup](/horsies/typescript/observability/tracing-setup/#spans-of-the-attempts-that-the-worker-ended).

## Delivery

- The core gives each handler a queue of 10,000 events. When the queue is full, the next event drops for that handler only and counts in `horsies.telemetry.handler.dropped{handler}`.
- A handler takes one event at a time, in the order of the worker. The core waits for the handler to return, or for its Promise to settle, before it gives the next event.
- A throw or a rejection counts in `horsies.telemetry.handler.errors{handler}` and is logged with the handler's name and the error type and message. The handler takes its next event.
- Delivery is at most once: an event that drops is not sent again. The history tables are the record of each task.
- A handler may send tasks and start workflows. The handler child holds no task capacity, so a handler may also wait for a result.

### One Event Loop

All event handlers of a handler child share one JavaScript event loop. The worker-ended spans of the child run on it too.

- A handler that holds the loop with sync work delays every other handler and the spans. The queue bound of each handler still holds, and no task is affected.
- While one handler awaits I/O, the other handlers run.
- Give each call to another system a timeout (`AbortSignal.timeout`). A Promise that never settles stops its handler: its queue fills and its events drop.

## Handler Child Start Hooks

<!-- ts-check: alone -->
```ts
import { cli, Horsies } from "horsies";
import pg from "pg";

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { kind: "isolatedChildren", count: 2 } } },
  tasks: [],
});

if (!created.ok) {
  process.exit(1);
}

const app = created.value;

const audit = new pg.Pool({ connectionString: process.env["AUDIT_DATABASE_URL"], max: 2 });

app.onHandlerChildStart(async () => {
  await audit.query("SELECT 1");
});

app.onEvent("TaskRecovered", { name: "audit.task_recovered" }, async (event) => {
  await audit.query("INSERT INTO recovered_tasks (task_id, recovered_at) VALUES ($1, $2)", [event.taskId, event.at.toString()]);
});

process.exitCode = await cli.run(app);
```

| Item | Does |
|------|------|
| `app.onHandlerChildStart(hook: HandlerChildStartHook): void` | Adds a start hook of the handler child. Calls nothing |
| `HandlerChildStartHook` | `() => void \| Promise<void>`: sync or async |

- A hook runs once in each handler child, a replacement child included, before the handlers start.
- The hooks run in registration order. The first failure ends the rest.
- Each hook runs for at most 10 s.
- A hook that throws, rejects or runs past its bound ends the handler child with exit code 173. The worker keeps the handler child down until the worker restarts, and runs its tasks: the handler child never stops the worker. The logged cause names the hook by its position and its function name: `onHandlerChildStart #1 (anonymous)`.
- A hook that is not a function is refused with `HRS-214`. A call after `cli.run` serves the handler child is a defect (`ClientDefect`).
- The handler child runs only these hooks. A task child runs the hooks of `app.onChildStart`: see [Worker Setup](/horsies/typescript/workers/worker-setup/#child-start-hooks).

## The Handler Child

The worker starts its handler child when the app has a handler, a handler child start hook, or `workerEndedSpans: "makeSpans"` with tracing on. With none, it starts no handler child. The start, the boot failures, the stop and the state of the child in `horsies_worker_states.telemetry` are on [The Handler Child](/horsies/observability/events/#the-handler-child).

- The handler child runs the entry file from its first line, as a task child does. Every line before `cli.run` runs there too.
- It starts on the runtime of the worker: Node or Bun.

## Things to Avoid

**Do not use a handler as the record of a task.** An event can drop. Read the outcome of a task from a result read or from the monitoring reads.

**Do not run long sync work in a handler.** All handlers share one event loop: the sync work of one handler delays the events of every handler.

**Do not await a call with no timeout.** A Promise that never settles stops its handler, and its events drop once 10,000 wait.
