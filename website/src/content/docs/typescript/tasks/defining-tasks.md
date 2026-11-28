---
title: Defining Tasks
summary: Define the contract of a task with defineTask, list it in Horsies.create, write its body with app.implement, choose its execution class and retry policy, and read the run's state, cancel and fencing token from the runtime.
related: [wire-types, errors, cancellation, sending-tasks, ../configuration/app-config, ../workers/worker-setup, ../../workers/execution-modes, ../../tasks/retry-policy, ../../concepts/fencing-token]
tags: [typescript, task, defineTask, implement, contract, execution, retry, TaskRuntime, fencing token]
---

# Defining Tasks

A task has two parts:

- The **contract**, from `defineTask`: the name, the schemas of the input and the output, the error codes, the execution class and the retry policy. It holds everything that a sender needs, has no dependencies and does no I/O. An API server that only sends imports the contracts and not the worker code.
- The **implementation**, from `app.implement`: the body that a worker runs for the contract.

## Define a Contract

```ts
import { defineTask, h } from "horsies";

const validateOrder = defineTask({
  name: "validate_order",
  input: {
    orderId: h.uuid(),
    lines: h.array(h.object({ sku: h.string(), quantity: h.int(), unitPriceCents: h.int() })),
  },
  output: h.object({ orderId: h.uuid(), totalCents: h.int() }),
  errorCodes: ["EMPTY_ORDER", "LIMIT_EXCEEDED"],
});
```

- `input` has one schema per parameter. A send stores the input as an object keyed by the parameter names: `{"orderId": "...", "lines": [...]}`. A task with no parameters has `input: {}` and stores `{}`.
- A schema is an `h.*` wire type or a zod 4 schema. See [Wire Types](/horsies/typescript/tasks/wire-types/).
- A parameter name is a letter or `_`, then letters, digits and `_`. The names `workflow_context` and `workflow_meta`, and a name that only the engine writes (`__h_...`, `__horsies_...`), are refused.
- `output` is always present: `.optional()` and `.default()` on it are refused.
- `errorCodes` lists the task's own codes as literals, with no `as const`. Without `errorCodes`, the task may return any code that is not a built-in name. See [Task Errors](/horsies/typescript/tasks/errors/).

`defineTask` never throws and refuses nothing. `Horsies.create` checks each listed contract and reports what it breaks: see [The Task List](#the-task-list).

### Fields

| Field | Type | Rule |
|-------|------|------|
| `name` | `string` | The task name. Each send stores it, and a worker finds the implementation by it. Not empty |
| `queue` | `string` | One of `customQueues` in the custom queue mode. Left out in the default queue mode |
| `input` | `{ [parameter]: schema }` | One schema per parameter |
| `output` | schema | The schema of the value of `ok` |
| `errorCodes` | `readonly string[]` | The task's own codes. A built-in name, or a list of the type `string[]`, fails typecheck |
| `execution` | `"ISOLATED"` (default), `"SHARED_ASYNC"`, `"SHARED_THREAD"` | See [Execution Class](#execution-class) |
| `timeoutMs` | `number` | The deadline of a run, from 1,000 to 4,294,967,295. Required for `SHARED_ASYNC` and `SHARED_THREAD` |
| `cancelGraceMs` | `number` | From 100 to 10,000. Not taken by `SHARED_THREAD`. See [Cancellation](/horsies/typescript/tasks/cancellation/) |
| `retryPolicy` | `RetryPolicy` | See [Retry Policy](#retry-policy) |
| `workflowContext` | `boolean` | `true`: a workflow node of the task may name `workflowContextFrom`. See [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/) |

`goodUntil` is not a contract field: set a deadline per send. See [Sending Tasks](/horsies/typescript/tasks/sending-tasks/).

## Implement the Task

```ts
import { Horsies } from "horsies";

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [validateOrder],
});

if (!created.ok) {
  throw new Error(created.error.refusals.map((refusal) => refusal.message).join("\n"));
}

const app = created.value;

app.implement(validateOrder, ({ orderId, lines }, { ok, err }) => {
  if (lines.length === 0) {
    return err({ code: "EMPTY_ORDER", message: "the order has no lines" });
  }

  const totalCents = lines.reduce((sum, line) => sum + line.quantity * line.unitPriceCents, 0);

  if (totalCents > 1_000_000) {
    return err({ code: "LIMIT_EXCEEDED", message: "the order is over the limit", data: { totalCents } });
  }

  return ok({ orderId, totalCents });
});
```

- The body takes `(input, runtime)`. `input` has every parameter, with each default filled. `runtime` holds the members of the [Task Runtime](#task-runtime) of the run, and `ok` and `err`. A body may destructure it, or leave it out.
- The body returns `ok(value)` or `err({ code, message?, data? })`, from its second argument, or a Promise of one. `ok` and `err` are bound to the contract. The contract has no `ok` and no `err`: a value of the wrong type, or a code that `errorCodes` does not list, fails typecheck at its own line.
- A thrown error is stored as `UNHANDLED_EXCEPTION`, or as a declared code through the `exceptionMapper` option. See [Task Errors](/horsies/typescript/tasks/errors/#thrown-errors).
- The worker decodes the input with the contract's types, and checks that the value of `ok` encodes and reads back. See [Task Errors](/horsies/typescript/tasks/errors/#what-the-library-records).
- `app.implement` never throws. It records a refusal for a contract that the app does not list (`HRS-300`) and for a second implementation of a contract (`HRS-301`). `check` reports each refusal, and a worker refuses to start while the app holds one, or while a listed contract has no implementation. See [CLI Reference](/horsies/typescript/cli/).

Put the `app.implement` calls in the module that the worker runs. A producer that only sends needs `Horsies.create` with the contracts, and no implementation.

## Execution Class

`execution` declares how a worker runs the task. Every send stores the class. The behaviour of each class, and how to choose one, is on [Choosing an Execution Mode](/horsies/workers/execution-modes/).

| `execution` | Body | Runtime | `timeoutMs` | `cancelGraceMs` |
|-------------|------|---------|-------------|-----------------|
| `"ISOLATED"` (default) | sync or async | `TaskRuntime` | optional | optional: the kill after an operator cancel |
| `"SHARED_ASYNC"` | async only | `TaskRuntime` | required | optional, default 1,000 |
| `"SHARED_THREAD"` | sync or async | `ThreadTaskRuntime` (no `spawn`) | required | not taken |

The static types hold these rules: a shared class without `timeoutMs`, `cancelGraceMs` on `SHARED_THREAD`, and a sync body for `SHARED_ASYNC` fail typecheck. A contract that a cast takes past the types is refused at `Horsies.create` (`HRS-109`, `HRS-110`, `HRS-111`).

<!-- ts-check: alone -->
```ts
import { defineTask, h, Horsies } from "horsies";

const fetchQuote = defineTask({
  name: "fetch_quote",
  execution: "SHARED_ASYNC",
  timeoutMs: 10_000,
  cancelGraceMs: 2_000,
  input: { symbol: h.string() },
  output: h.string(),
  errorCodes: ["QUOTE_UNAVAILABLE"],
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [fetchQuote],
});

if (!created.ok) {
  throw new Error(created.error.refusals.map((refusal) => refusal.message).join("\n"));
}

const app = created.value;

app.implement(fetchQuote, async ({ symbol }, { ok, err, signal }) => {
  const response = await fetch(`https://quotes.example.com/${encodeURIComponent(symbol)}`, { signal });

  if (!response.ok) {
    return err({ code: "QUOTE_UNAVAILABLE", message: `status ${response.status}` });
  }

  return ok(await response.text());
});
```

Rules of `SHARED_ASYNC`:

- The runs of a shared async child share one event loop. A body stops at its deadline only where it passes `runtime.signal` to the call that it awaits. A body whose libraries take no signal runs on past its deadline. Declare such a task `ISOLATED`: at the deadline the worker kills its child. See [Cancellation](/horsies/typescript/tasks/cancellation/).
- Keep CPU work out of `SHARED_ASYNC`: it blocks the loop of every run of the child. A block of the loop ends the child; see [A Blocked Event Loop](/horsies/workers/shared-async-tasks/#a-blocked-event-loop).
- On Node, the child names the run that blocks the loop, also after its first `await`. On Bun, a block after the body's first `await` names no run: each run of the child before its deadline takes the crash path.

### Custom Queues

In the custom queue mode, every contract names one of `customQueues`. A contract with no `queue`, or with a queue that is not in the list, is refused at `Horsies.create`.

<!-- ts-check: alone -->
```ts
import { defineTask, h, Horsies } from "horsies";

const chargeCard = defineTask({
  name: "charge_card",
  queue: "payments",
  input: { orderId: h.uuid(), amountCents: h.int() },
  output: h.string(),
});

const sendReceipt = defineTask({
  name: "send_receipt",
  queue: "email",
  input: { orderId: h.uuid() },
  output: h.null(),
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  queueMode: "custom",
  customQueues: [{ name: "payments" }, { name: "email" }],
  tasks: [chargeCard, sendReceipt],
});
```

The queue fields are on [App Config](/horsies/typescript/configuration/app-config/), and the queue modes on [Queue Modes](/horsies/concepts/queue-modes/).

## Retry Policy

`retryPolicy` names when a failed run runs again. `retryFor` lists the codes that start a retry: a code of the task's `errorCodes`, or a library code (`TASK_TIMEOUT`, `WORKER_CRASHED`, ...). A custom code that `errorCodes` does not list fails typecheck.

```ts
const fetchPrices = defineTask({
  name: "fetch_prices",
  timeoutMs: 5_000,
  input: { symbol: h.string() },
  output: h.string(),
  errorCodes: ["RATE_LIMITED", "UPSTREAM_DOWN"],
  retryPolicy: {
    kind: "exponential",
    baseSeconds: 30,
    maxRetries: 5,
    maxDelaySeconds: 600,
    retryFor: ["RATE_LIMITED", "TASK_TIMEOUT", "WORKER_CRASHED"],
  },
});
```

| `kind` | Fields | Delay before retry n |
|--------|--------|----------------------|
| `"fixed"` | `intervalsSeconds` (one delay per retry; its length is the number of retries), `jitter?`, `retryFor` | `intervalsSeconds[n - 1]` |
| `"exponential"` | `baseSeconds`, `maxRetries`, `jitter?`, `maxDelaySeconds?`, `retryFor` | `baseSeconds * 2^(n - 1)`, at most `maxDelaySeconds` |

- `jitter` is `true` when left out.
- A policy has 1 to 20 retries; each interval and `baseSeconds` is 1 to 86,400 seconds; `maxDelaySeconds` is greater than 0. `Horsies.create` refuses a policy that breaks a rule (`HRS-102`).
- When a retry starts and how the delay is computed is on [Retry Policy](/horsies/tasks/retry-policy/).

## The Task List

`Horsies.create({ ..., tasks })` takes every contract of the app. A send of a contract that the list does not hold is refused. `create` checks each contract in list order and returns every refusal: `created.error.refusals`, each with `code`, `path` (`tasks[2] charge_card`), `message` and `help`.

| Refusal | Code |
|---------|------|
| A second contract with a listed name, or one contract listed twice | `HRS-301` |
| A `queue` in the default queue mode, or no `queue` in the custom queue mode | `HRS-200` |
| A `queue` that is not in `customQueues` | `HRS-103` |
| A task name that is empty or holds a lone surrogate; a timeout below 1,000; a retry policy that breaks a rule | `HRS-102` |
| A parameter name that breaks a rule; a schema that has no stored form | `HRS-105`. See [Wire Types](/horsies/typescript/tasks/wire-types/#refusals-at-horsiescreate) |
| A declared code that is a built-in or reserved name | `HRS-212` |
| A declared code that is empty or not `UPPER_SNAKE_CASE` | `HRS-112` |
| A declared code given twice | `HRS-113` |
| A shared class without `timeoutMs` | `HRS-109` |
| `cancelGraceMs` on `SHARED_THREAD` | `HRS-110` |
| `cancelGraceMs` outside 100 to 10,000 | `HRS-111` |

The codes are on the shared [Errors Reference](/horsies/tasks/errors/#task-definition-hrs-100-hrs-199).

## Task Runtime

The second argument of a body is one frozen object for its run. Each member is bound to the run, so a destructured member works.

| Member | Gives |
|--------|-------|
| `taskId` | `TaskId`: the id of the task |
| `fencingToken` | `{ taskId, value }`: `value` is an `AttemptNumber`. See [Fencing Token](#fencing-token) |
| `signal` | An `AbortSignal` that fires at an operator cancel, and at the deadline of a `SHARED_ASYNC` run. See [Cancellation](/horsies/typescript/tasks/cancellation/) |
| `cancelState()` | `{ kind: "notRequested" }` or `{ kind: "requested", cause }`, `cause` `"OPERATOR"` or `"DEADLINE"` |
| `spawn(name, (signal) => work)` | `Result<Promise<T>, SpawnRefused>`: starts work that belongs to the run. Not on `ThreadTaskRuntime` |
| `state(key)` | The value that the provider of `key` built in this process |
| `workflowContext` | Only for a task with `workflowContext: true`: `{ kind: "context", context }` or `{ kind: "noContext" }` |
| `ok(value)`, `err({ code, message?, data? })` | The constructors of the result of the body |

`app.provide(factory)` returns the key of one value of app state. The factory runs in each task child before the child takes its first task. See [Worker Setup](/horsies/typescript/workers/worker-setup/). A test builds a runtime with `TaskRuntime.forTest`. See [Testing](/horsies/typescript/operations/testing/).

### Fencing Token

A stalled attempt can run on after the worker retries its task. horsies refuses the late writes of that attempt to its own tables. It cannot refuse its writes to your systems. Write the fencing token of the run with each such write, and make the store refuse a write whose token is lower than the token that it holds for the task id. The guarantee and its limits are on [Fencing Token](/horsies/concepts/fencing-token/).

- Two reads in one attempt give the same token.
- An attempt that runs after another attempt of the same task gives a greater value.
- The value depends on no clock. Compare the values of one task id; do not depend on the number itself.

<!-- ts-check: alone -->
```ts
import pg from "pg";
import { defineTask, h, Horsies } from "horsies";

const recordPayment = defineTask({
  name: "record_payment",
  input: { orderId: h.string(), cents: h.int() },
  output: h.string(),
  errorCodes: ["LATER_ATTEMPT", "STORE_FAILED"],
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [recordPayment],
});

if (!created.ok) {
  throw new Error(created.error.refusals.map((refusal) => refusal.message).join("\n"));
}

const app = created.value;

const Payments = app.provide(() => new pg.Pool({ connectionString: process.env["PAYMENTS_DATABASE_URL"] }));

// CREATE TABLE payments (task_id uuid PRIMARY KEY, fence integer NOT NULL,
//                        order_id text NOT NULL, cents bigint NOT NULL);
app.implement(
  recordPayment,
  async ({ orderId, cents }, { ok, err, fencingToken, state }) => {
    const { taskId, value } = fencingToken;
    // One conditional write: the row changes only when no later attempt of this task wrote it.
    const written = await state(Payments).query(
      "INSERT INTO payments (task_id, fence, order_id, cents) VALUES ($1, $2, $3, $4) " +
        "ON CONFLICT (task_id) DO UPDATE " +
        "SET fence = EXCLUDED.fence, order_id = EXCLUDED.order_id, cents = EXCLUDED.cents " +
        "WHERE payments.fence <= EXCLUDED.fence",
      [taskId, value, orderId, cents],
    );

    return written.rowCount === 0
      ? err({ code: "LATER_ATTEMPT", message: "a later attempt of this task holds the fence" })
      : ok(orderId);
  },
  { exceptionMapper: [[pg.DatabaseError, "STORE_FAILED"]] },
);
```

A rerun, and a workflow node that a resume starts again, get a new task id: their value starts again at 1.

## Things to Avoid

**Don't return a plain value from a body.** The worker stores a return that is neither `ok(value)` nor `err(failure)` as `RETURN_TYPE_MISMATCH`, and the static types refuse it.

<!-- ts-check: skip elided -- a Wrong / Correct pair of statements -->
```ts
// Wrong
app.implement(greet, ({ name }) => `hello, ${name}`);

// Correct
app.implement(greet, ({ name }, { ok }) => ok(`hello, ${name}`));
```

**Don't declare a task `SHARED_ASYNC` when its libraries take no signal.** Its body runs on past the deadline and holds its seat. Keep it `ISOLATED`, the default.

**Don't import the worker module in an API server.** Import the contracts and `Horsies.create`; the implementations belong to the worker's entry.

## API Reference

### `defineTask(definition): TaskContract<P, O, E, X, C>`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `definition` | `TaskDefinition & ExecutionRules<X>` | Yes | The fields of [Fields](#fields) |

**Returns:** a frozen `TaskContract` with `kind` and `name`. It has no `ok` and no `err`: those are members of the second argument of the body. `err` throws `TypeError` for a built-in code that a cast takes past the types.

### `app.implement(contract, body, options?): Implementation<P, O, E, X>`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contract` | `TaskContract` | Yes | A contract of the app's task list |
| `body` | `BodyFor<P, O, E, X>` | Yes | `(input, runtime) => result`, or a Promise of it |
| `options.exceptionMapper` | `readonly [ErrorClass, E][]` | No | Thrown error classes and their codes. See [Task Errors](/horsies/typescript/tasks/errors/#thrown-errors) |

**Returns:** the `Implementation`. Its `run(input, runtime)` runs the body with no worker; see [Testing](/horsies/typescript/operations/testing/).
