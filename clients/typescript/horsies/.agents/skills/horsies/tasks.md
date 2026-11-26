# horsies (TypeScript) — Tasks

Reference for defining, sending and reading tasks with the `horsies`
package: the contract, the wire types, the zod adapter, the stored input,
the implementation, `TaskRuntime`, app state, the fencing token, the cancel
signal, retry policies, the sends, the reads and tests of a body. Rerun is
in `monitoring.md`.

## The Contract

```ts
import { defineTask, h } from "horsies";

const OrderLine = h.object({ sku: h.string(), quantity: h.int() });

export const validateOrder = defineTask({
  name: "validate_order",
  input: { orderId: h.uuid(), lines: h.array(OrderLine), note: h.string().optional() },
  output: h.object({ orderId: h.uuid(), lineCount: h.int(), validatedAt: h.instant() }),
  errors: ["EMPTY_ORDER", "UNKNOWN_SKU", "STOCK_SERVICE_DOWN"],
  timeoutMs: 30_000,
  retry: {
    kind: "exponential",
    baseSeconds: 30,
    maxRetries: 5,
    autoRetryFor: ["STOCK_SERVICE_DOWN", "WORKER_CRASHED"],
  },
});
```

`defineTask` returns a frozen `TaskContract`. It does no I/O and throws for
no rule of the definition: `Horsies.create` checks every listed contract and
reports each refusal with its `HRS-nnn` code.

| Field | Value | Rule |
|---|---|---|
| `name` | `string` | The task name. Each send stores it, and a worker finds the implementation by it. Not empty and no lone surrogate (`HRS-102`); unique in the app (`HRS-301`) |
| `queue` | `string` | A queue of the custom queue mode. In the default queue mode a task that names a queue is refused (`HRS-200`). In the custom queue mode every task names a queue (`HRS-200`), and the queue is one of `customQueues` (`HRS-103`) |
| `input` | one schema per parameter | An `h.*` wire type or a zod 4 schema per parameter. `{}` for a task with no parameters |
| `output` | a schema | A wire type that is always present (no `.optional()`), or a zod 4 schema |
| `errors` | `["CODE", ...]` | The codes that the task may return, as literals. Left out: any code that is not a built-in name |
| `execution` | `"ISOLATED"` (default), `"SHARED_ASYNC"` | How a worker runs the task (Execution Classes) |
| `timeoutMs` | 1,000 to 4,294,967,295 | The deadline of a run. Required for `SHARED_ASYNC` (typecheck). Below 1,000 is `HRS-102` |
| `cancelGraceMs` | 100 to 10,000 | See The Cancel Signal |
| `retry` | a `RetryPolicy` | See Retry Policy |
| `workflowContext` | `true` | A workflow node of the task may name `ctxFrom`, and the body reads `runtime.workflowContext` (see `workflows.md`, The Workflow Context) |

- `errors` takes literal codes; no `as const` is needed. A list of the type
  `string[]` fails typecheck, because its codes are not known to the types.
- A built-in code name in `errors` fails typecheck: `BUILT_IN_TASK_CODES`
  and `WORKFLOW_STOPPED` are reserved.
- `Horsies.create` checks each declared code with the core's rule: a code
  that is empty, not `UPPER_SNAKE_CASE` or an exception class name is
  `HRS-112`; a code given twice is `HRS-113`.
- The class and the options are stored with every send. Each `send` of the
  contract carries them.

## Execution Classes

```ts
export const importFeed = defineTask({
  name: "import_feed",
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
  cancelGraceMs: 2_000,
  input: { feedUrl: h.string() },
  output: h.int(),
});
```

| Class | Body | Runs |
|---|---|---|
| `ISOLATED` | sync or async | One task at a time in a child process of its own. At its `timeoutMs` the worker kills the child |
| `SHARED_ASYNC` | async only (typecheck) | Many runs on the one event loop of a shared async child. `timeoutMs` is required |

- A shared async run stops at its deadline only where its code passes
  `runtime.signal` (or reads `runtime.cancelState()`). A promise cannot be
  stopped from outside. Put a body whose libraries take no `AbortSignal` in
  `ISOLATED`: there the worker kills the child at the deadline.
- Keep CPU work out of `SHARED_ASYNC`: a body that blocks the event loop
  blocks every run of its child. See The Cancel Signal.
- The behaviour of each class is on the shared docs pages
  `workers/execution-modes` and `workers/shared-async-tasks`.

## Wire Types

The `h.*` builders give a wire type: the stored form of a value and its
TypeScript types. The stored form is the form of the Python client, so a
value reads back to the same value.

| Builder | Value type | Stored as |
|---|---|---|
| `h.null()` | `null` | `null` |
| `h.boolean()` | `boolean` | `true`, `false` |
| `h.string()` | `string` | A JSON string. A lone surrogate is refused |
| `h.int()` | `number` | An integer in the safe range (abs value at most 2^53 - 1) |
| `h.bigint()` | `bigint` | An integer of up to 4,300 digits, exact |
| `h.float()` | `number` | A finite float in Python's float text: `7.0`, `1e-05`, `1e+16`, `-0.0` |
| `h.uuid()` | `string` | A UUID in the lower-case 8-4-4-4-12 form (`crypto.randomUUID()` gives it) |
| `h.decimal()` | `Decimal` | A JSON string: Python's `str(Decimal)` text (`"19.90"`, `"1E+3"`) |
| `h.instant()` | `Temporal.Instant` | UTC: `2026-08-12T09:10:12.654321Z` |
| `h.offsetDateTime()` | `Temporal.ZonedDateTime` | With its fixed offset: `2026-08-12T09:10:12.654321+02:00`. A named zone, `UTC` too, is refused |
| `h.plainDateTime()` | `Temporal.PlainDateTime` | `2026-08-12T09:10:12.654321` |
| `h.plainDate()` | `Temporal.PlainDate` | `2026-08-12` |
| `h.plainTime()` | `Temporal.PlainTime` | `09:10:12.654321` |
| `h.json()` | `JsonValue` | Untyped JSON. A `bigint` and a safe integer are integers; another `number` is a float |
| `h.literal(a, b, ...)` | the listed values | Strings, safe integers, booleans or `null` |
| `h.enum(["a", "b"])` | the listed strings | The string |
| `h.object({ ... })` | an object of the members | A JSON object in declaration order. An undeclared key is refused |
| `h.array(item)` | `T[]` | A JSON array |
| `h.tuple([a, b])` | `[A, B]` | A JSON array with one value per item type |
| `h.record(value)` | `Readonly<Record<string, T>>` | A JSON object in the value's own key order |
| `h.union([a, b])` | `A \| B` | Primitive members, at most one per stored JSON type |
| `h.union(tag, [objects])` | the member objects | Object members told apart by the string literal member `tag` |
| `h.taskOutcome(contract)` | `TaskOutcome<O, E>` of the contract | A whole task parameter only, which a workflow node fills (see `workflows.md`) |

| Modifier | Does |
|---|---|
| `.optional()` | An object member or a parameter that a value may leave out. The key is left out of the stored text; it is never `null` |
| `.nullable()` | `null` or the value |
| `.default(value)` | A sender may leave the member out; a reader fills `value`. The sender never stores the default |

```ts
import { Decimal } from "horsies";

const Invoice = h.object({
  invoiceId: h.uuid(),
  issuedAt: h.instant(),
  dueOn: h.plainDate(),
  total: h.decimal(),
  lines: h.array(h.object({ sku: h.string(), count: h.int() })),
  status: h.enum(["draft", "sent", "paid"]),
  note: h.string().optional(),
  reminders: h.int().default(3),
  discount: h.decimal().nullable(),
  tags: h.record(h.string()),
});

export type InvoiceValue = h.Infer<typeof Invoice>;

const Payment = h.union("method", [
  h.object({ method: h.literal("card"), last4: h.string() }),
  h.object({ method: h.literal("bank"), iban: h.string() }),
]);

export function paymentText(payment: h.Infer<typeof Payment>): string {
  switch (payment.method) {
    case "card":
      return `card ending ${payment.last4}`;
    case "bank":
      return `bank account ${payment.iban}`;
  }
}

export function totalText(text: string): string {
  const total = Decimal.parse(text);

  return total.ok ? total.value.toString() : `not a decimal: ${total.error.kind}`;
}
```

- `h.Infer<typeof W>` is the value that a body and a reader get.
- `Decimal` has no arithmetic. `Decimal.parse(text)` returns
  `Result<Decimal, DecimalRefused>` (`NotADecimal`, `NotFinite`); it takes
  and refuses the same texts as Python's `Decimal`. `toString()` is the
  stored text; `1.10` and `1.1` are two values.
- A time stores no fraction for a whole second, 6 digits for a whole
  microsecond and 9 digits for a part below the microsecond. A date-time
  outside the years 1 to 9999 is refused.
- `Horsies.create` refuses an integer-like member name of `h.object` (such
  as `"10"`): JavaScript puts such a key first in an object, so its
  declaration order cannot hold. `h.record` keeps the value's own key
  order (integer-like keys first).
- `Horsies.create` refuses `h.union` with `h.int()` and `h.float()`: one
  `number` cannot say which it is.
- The published declarations carry the Temporal lib reference. A project
  with the default `lib` needs no lib setting for the `Temporal` types.

## The zod Adapter

`input` and `output` take a zod 4 schema in place of a wire type. The main
entry recognises a zod schema by its Standard Schema marker; the adapter
maps it to a wire type at `Horsies.create`. The entry `horsies/zod` gives
`hz`: zod codecs for the kinds that zod cannot express exactly, and
`hz.wire` for an `h.*` wire type inside a zod schema.

```ts
import { z } from "zod";
import { hz } from "horsies/zod";

const Shipment = z.object({
  shipmentId: z.uuid(),
  shippedAt: hz.instant(),
  weightKg: hz.decimal(),
  carrier: z.enum(["dhl", "ups"]),
  parcels: z.int().default(1),
  label: hz.wire(h.string()).optional(),
});

export const recordShipment = defineTask({
  name: "record_shipment",
  input: { shipment: Shipment, warehouse: h.string() },
  output: z.object({ recorded: z.boolean() }),
});
```

| `hz` | zod value type |
|---|---|
| `hz.instant()`, `hz.offsetDateTime()`, `hz.plainDateTime()`, `hz.plainDate()`, `hz.plainTime()` | The Temporal type, stored as the `h.*` kind's text |
| `hz.decimal()` | `Decimal` |
| `hz.wire(w)` | The value of the required wire type `w`; put `.optional()` on the result, not on `w` |

- A body and a reader get zod's output type; a sender may leave out a
  member that zod marks `.optional()`, `.default()` or `.prefault()`.
- The adapter refuses a zod form that has no stored form, with `HRS-105`:
  `z.any()`, `z.unknown()`; a one-way change (`.transform()`,
  `z.preprocess()`, `z.coerce.*()`, `.trim()`, `.toLowerCase()`);
  `.catch()`; `z.date()` (a `Date` holds milliseconds only: use
  `hz.instant()`); `z.looseObject()`, `.passthrough()`, `.catchall()`;
  `z.map()`, `z.set()`, `z.function()`, `z.promise()`; a union with an
  object member that is not a discriminated union; a union of an integer
  and a float; `z.intersection()`; `.optional()` or `.default()` outside an
  object member or a parameter; a function default; `z.lazy()`; and a zod
  definition, field, format or check that the installed horsies does not
  know.
- The supported zod range is the peer range of the package
  (`>=4.1.13 <4.7.0`). A zod 3 schema is refused with `Zod3Schema`, which
  names zod 4 as the fix. A Standard Schema of another vendor is refused.
- The main entry imports no zod. A bundled producer with zod contracts
  works.

## The Stored Input

Each parameter is stored under its name, as Python stores keyword
arguments.

| `input` | Send | Stored input |
|---|---|---|
| `{}` | `{}` | `{}` |
| `{ a: h.int(), b: h.int() }` | `{ a: 2, b: 3 }` | `{"a": 2, "b": 3}` |
| `{ order: Order }` | `{ order: { ... } }` | `{"order": {...}}` |
| `{ limit: h.int().default(10) }` | `{}` | `{}`; the body gets `limit: 10` |

- A parameter name is a letter or `_`, then letters, digits and `_`
  (`HRS-105`). The names `workflow_ctx` and `workflow_meta`, and names that
  only the engine writes (`__h_...`, `__horsies_...`), are refused.
- A sender may leave out an `.optional()` or defaulted parameter. A body
  gets every parameter: a default is filled, an optional one may be absent.
- A parameter declared `h.taskOutcome(contract)` is not sent. A task with a
  required `h.taskOutcome` parameter cannot be sent: `app.send` takes a type that no
  value has, so only a workflow node runs it.
- The reader refuses an undeclared key at every depth. A stored input that
  does not decode with the parameters fails the run with
  `WORKER_SERIALIZATION_ERROR`; the message names the field path and the
  kind of the failure, never a value.

## The Implementation

The worker entry builds the app and implements every listed contract:

```ts
import { Horsies } from "horsies";

const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [validateOrder, importFeed, recordShipment],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`${refusal.code} ${refusal.message}`);
  }

  process.exit(1);
}

const app = created.value;

class StockServiceDown extends Error {}

export const validateOrderBody = app.implement(
  validateOrder,
  async ({ orderId, lines }, { ok, err }) => {
    if (lines.length === 0) {
      return err({ code: "EMPTY_ORDER", message: "the order has no lines" });
    }

    const unknownLine = lines.find((line) => !line.sku.startsWith("SKU-"));

    if (unknownLine !== undefined) {
      return err({ code: "UNKNOWN_SKU", data: { sku: unknownLine.sku } });
    }

    return ok({ orderId, lineCount: lines.length, validatedAt: Temporal.Now.instant() });
  },
  { exceptions: [[StockServiceDown, "STOCK_SERVICE_DOWN"]] },
);
```

- `app.implement(contract, body, options)` returns the `Implementation`. It
  never throws and does no I/O. A contract that the app does not list
  (`HRS-300`) and a second implementation of one contract (`HRS-301`) are
  refusals that `check` and the worker start report.
- The body is `(input, runtime) => result`. `input` is the decoded input;
  `runtime` holds the members of the `TaskRuntime` of the run and the
  contract's `ok` and `err`. A body may destructure it
  (`{ ok, err, signal }`) or leave it out. A `SHARED_ASYNC` body returns a
  Promise.
- `ok(value)` is the success, with a value of the contract's `output` type.
  `err({ code, message, data })` is a failure with a declared code;
  `message` and `data` may be left out. `data` is a `JsonValue`. A wrong
  value or code fails typecheck at the line of the `ok` or `err` call. The
  contract has no `ok` and no `err`.
- `exceptions` maps thrown error classes to declared codes. The first entry
  whose class the thrown value is an instance of (a subclass too) gives the
  code. An undeclared code in the map fails typecheck.

What horsies records:

| The body | Stored error |
|---|---|
| Returns `ok(value)` and the value encodes and reads back | No error: the value |
| Returns `ok(value)` and the value does not read back with `output` | `WORKER_SERIALIZATION_ERROR` |
| Returns `err(...)` | The custom code, the message and the data |
| Returns `err` with a code that the core refuses (no `errors` list) | `RETURN_TYPE_MISMATCH`; the message names the task and the rule |
| Returns another value | `RETURN_TYPE_MISMATCH`: `Task <name> returned <type> but must return ok(value) or err({ code })` |
| Throws or rejects with a mapped class | The mapped custom code |
| Throws or rejects with another value | `UNHANDLED_EXCEPTION`: the message `Unhandled exception in task <name>: <type>: <text>`, `data.exception_type`, and the exception record |
| Calls `err` with a built-in code (untyped code) | `err` throws `TypeError`, stored as `UNHANDLED_EXCEPTION` |
| Times out, crashes, is cancelled, expires | The built-in code: `TASK_TIMEOUT`, `WORKER_CRASHED`, `TASK_CANCELLED`, `TASK_EXPIRED`, ... (shared docs page `tasks/errors`) |

- Error data that is not strict JSON (a value that is not JSON-native, NaN,
  an infinity, more than 300 levels, an integer of more than 4,300 digits)
  is replaced with `{"data_refused": "<cause>"}`. The code and the message
  are kept.

## `TaskRuntime`

The members of the second argument of a body, with `ok` and `err`. The
second argument is one frozen object per run; each member is an own
property and each function member is bound to its run, so a destructured
member works. The worker's child gives it; `TaskRuntime.forTest` gives a
runtime to a test, and `implementation.run` adds `ok` and `err`.

| Member | Gives |
|---|---|
| `ok(value)` | The success of the body: a value of the contract's `output` type |
| `err({ code, message, data })` | The error of the body: a code of the contract's `errors` |
| `taskId` | The `TaskId` of the task |
| `fencingToken` | `{ taskId, value }`: `value` is the attempt number (Fencing Token) |
| `signal` | An `AbortSignal` that fires at an operator cancel, and at the deadline of a `SHARED_ASYNC` run |
| `cancelState()` | `{ kind: "notRequested" }` or `{ kind: "requested", cause: "DEADLINE" \| "OPERATOR" }`, with no turn of the event loop |
| `spawn(name, (signal) => work)` | `Result<Promise<T>, SpawnRefused>`: work that belongs to the run |
| `state(key)` | The value that the provider of `key` built in this process (App State) |
| `workflowContext` | Only for a task with `workflowContext: true` (typecheck): `{ kind: "context", context }` or `{ kind: "noContext" }` (`workflows.md`, The Workflow Context) |

## App State

```ts
import { Pool } from "pg";

const Reports = app.provide(() => new Pool({ connectionString: process.env["REPORTS_URL"] }));

app.onChildStart(() => {
  console.log(`child ${process.pid} is ready`);
});

export const exportReport = defineTask({
  name: "export_report",
  input: { reportId: h.uuid() },
  output: h.null(),
  errors: ["STALE_ATTEMPT"],
});

app.implement(exportReport, async ({ reportId }, { ok, err, state, fencingToken }) => {
  const pool = state(Reports);
  const written = await pool.query(
    "UPDATE report_exports SET fence = $2, exported_at = now() WHERE report_id = $1 AND fence <= $2",
    [reportId, fencingToken.value],
  );

  return written.rowCount === 0
    ? err({ code: "STALE_ATTEMPT", message: "a later attempt holds the fence" })
    : ok(null);
});
```

- `app.provide(factory)` adds a provider and returns its `StateKey`. The
  call runs nothing. The factory runs in each task child, before the child
  reports ready, in registration order and before the `onChildStart` hooks,
  for at most 10 s. It may return a Promise; the key's type is the resolved
  value.
- `app.onChildStart(hook)` runs `hook` once in each task child, a
  replacement child too, after the providers, for at most 10 s.
- A factory or a hook that throws, rejects or runs past its bound stops the
  worker with `CHILD_HOOK_FAILED` (exit code 173).
- `runtime.state(key)` with a key of another app throws `ClientDefect`.
- `provide` and `onChildStart` after `cli.run` serves a child throw
  `ClientDefect`. Call them in the entry, before `cli.run`.
- Each child builds its own state: every child runs the entry again.

## Fencing Token

A stalled attempt can run on after the worker retries its task. Write the
fencing token with each write to another system, and make that write
conditional: the store refuses a token lower than the one it holds for the
task id (the `UPDATE ... WHERE fence <= $2` above). Two reads in one attempt
give the same token; a later attempt of the same task gives a greater value;
the value depends on no clock. Compare values of one task id only.

- `token.value` is an `AttemptNumber`: a whole number from 1.
- Limits: a rerun or a workflow node that a resume starts again gets a new
  task id and starts again at 1; a stale attempt with no later attempt, or
  one that writes before the later attempt took the fence, is not refused.
  See the shared docs page `concepts/fencing-token`.

## The Cancel Signal

`runtime.signal` fires for two causes, and the first cause stays. Its
`reason` is `{ cause: "OPERATOR" }` or `{ cause: "DEADLINE" }`.

- `OPERATOR`: an operator cancel of the running task
  (`app.monitoring.cancelTask`, or the web UI), in every execution class.
  The task ends `CANCELLED`; a waiting `handle.get` gives the outcome
  `TASK_CANCELLED`. What the body returns after the cancel is dropped. A
  cancel before the body starts: the body does not run.
- `DEADLINE`: the deadline of a `SHARED_ASYNC` run, at its `timeoutMs`. The
  run ends `TASK_TIMEOUT` whatever the body returns. An `ISOLATED` run is
  killed at its deadline with no signal.

<!-- ts-check: alone -->
```ts
import { defineTask, h, type Horsies } from "horsies";

export const syncFeed = defineTask({
  name: "sync_feed",
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
  cancelGraceMs: 2_000,
  input: { feedUrl: h.string() },
  output: h.int(),
  errors: ["FEED_UNREACHABLE"],
});

export const indexFeeds = defineTask({
  name: "index_feeds",
  execution: "SHARED_ASYNC",
  timeoutMs: 60_000,
  input: { feedUrls: h.array(h.string()) },
  output: h.int(),
  errors: ["WORK_REFUSED"],
});

export const importBatches = defineTask({
  name: "import_batches",
  timeoutMs: 600_000,
  cancelGraceMs: 5_000,
  input: { batchIds: h.array(h.string()) },
  output: h.int(),
});

function importBatch(batchId: string): void {
  console.log(`imported ${batchId}`);
}

export function implementFeeds(app: Horsies): void {
  app.implement(syncFeed, async ({ feedUrl }, { ok, err, signal }) => {
    const response = await fetch(feedUrl, { signal });

    if (!response.ok) {
      return err({ code: "FEED_UNREACHABLE", message: `HTTP ${response.status}` });
    }

    const items: unknown = await response.json();

    return ok(Array.isArray(items) ? items.length : 0);
  });

  app.implement(indexFeeds, async ({ feedUrls }, { ok, err, spawn }) => {
    const counts: Promise<number>[] = [];

    for (const feedUrl of feedUrls) {
      const started = spawn(`fetch ${feedUrl}`, async (signal) => {
        const response = await fetch(feedUrl, { signal });

        return response.ok ? 1 : 0;
      });

      if (!started.ok) {
        return err({ code: "WORK_REFUSED", message: started.error.kind });
      }

      counts.push(started.value);
    }

    const reached = await Promise.all(counts);

    return ok(reached.reduce((sum, count) => sum + count, 0));
  });

  app.implement(importBatches, ({ batchIds }, { ok, cancelState }) => {
    let done = 0;

    for (const batchId of batchIds) {
      if (cancelState().kind === "requested") {
        break;
      }

      importBatch(batchId);
      done += 1;
    }

    return ok(done);
  });
}
```

- `SHARED_ASYNC`: the body stops only where it passes `runtime.signal` (to
  `fetch`, to a driver that takes a signal) or reads `cancelState()`.
  `cancelGraceMs` (default 1,000) is the time that the body and its spawned
  work have after the signal to end. A body that does not end keeps its seat
  in the child until it ends or the child is replaced; the attempt still
  ends `TASK_TIMEOUT`.
- `ISOLATED`: the body runs until it returns, or until its `timeoutMs` kill.
  With `cancelGraceMs` on the task, the worker kills the child
  `cancelGraceMs` after an operator cancel. With no `cancelGraceMs`, no kill.
- A sync body reads `cancelState()` between the steps of its work: it reads
  with no turn of the event loop.
- `TaskRuntime.forTest` gives a signal that never fires and the state
  `notRequested`.

`runtime.spawn(name, work)`:

- `work` gets its own signal. It fires at the deadline, at an operator
  cancel, and at the stop of the work that the body left after it returned
  (`reason` `{ cause: "LEFT_WORK" }`).
- The work belongs to the run: the run's work set counts it until its
  Promise settles. Work that a body spawned may spawn more work after the
  body returned.
- A refusal starts nothing: `{ kind: "runEnded" }` after the run is over,
  `{ kind: "tooMuchWork", bound }` at 10,000 pieces of work.
- A plain Promise that the body does not await is not tracked.

A blocked event loop: a shared async child that does not turn its loop for
5 s is blocked, and the worker replaces the child. On Node the child names
the run that held the loop, also after an `await`. On Bun a block after the
body's first `await` names no run: every run of the child takes the crash
path (`WORKER_CRASHED`). The rules are on the shared docs page
`workers/shared-async-tasks`.

## Retry Policy

```ts
export const fetchPrices = defineTask({
  name: "fetch_prices",
  input: { symbol: h.string() },
  output: h.decimal(),
  errors: ["RATE_LIMITED"],
  timeoutMs: 5_000,
  retry: {
    kind: "fixed",
    intervalsSeconds: [30, 60, 120],
    autoRetryFor: ["RATE_LIMITED", "TASK_TIMEOUT", "WORKER_CRASHED"],
  },
});
```

| `kind` | Fields | Delay of retry n |
|---|---|---|
| `"fixed"` | `intervalsSeconds` (one delay per retry), `jitter?`, `autoRetryFor` | `intervalsSeconds[n - 1]` |
| `"exponential"` | `baseSeconds`, `maxRetries`, `jitter?`, `maxDelaySeconds?`, `autoRetryFor` | `baseSeconds * 2^(n - 1)` |

- `autoRetryFor` has at least one code: a declared code of the task or a
  built-in code. Another code fails typecheck.
- `jitter` is `true` when left out, as in the Python client.
  `maxDelaySeconds` caps the delay after the backoff and the jitter.
- Each interval is 1 to 86,400 s, and a policy has 1 to 20 retries. The
  core refuses another value at `Horsies.create` (`HRS-102`).
- When a retry starts is on the shared docs page `tasks/retry-policy`.

## Sends

```ts
export async function sendValidation(orderId: string): Promise<void> {
  const sent = await app.send(
    validateOrder,
    { orderId, lines: [{ sku: "SKU-1", quantity: 2 }] },
    {
      idempotencyKey: `validate:${orderId}`,
      goodUntil: Temporal.Now.instant().add({ minutes: 10 }),
      delaySeconds: 5,
      retention: { kind: "days", days: 30 },
    },
  );

  if (sent.ok) {
    console.log(`sent ${sent.value.taskId}`);

    return;
  }

  switch (sent.error.code) {
    case "ENQUEUE_FAILED":
      if (sent.error.retryable) {
        const again = await app.retrySend(sent.error);

        console.log(again.ok ? `sent ${again.value.taskId}` : again.error.message);
      }

      break;
    case "IDEMPOTENCY_KEY_CONFLICT": {
      // Retryable: a retry after the key's reservation ends is accepted.
      const again = await app.retrySend(sent.error);

      console.warn(again.ok ? `sent ${again.value.taskId}` : `task ${sent.error.ownerTaskId} holds the key`);
      break;
    }
    case "VALIDATION_FAILED":
    case "PAYLOAD_TOO_LARGE":
    case "PAYLOAD_MISMATCH":
    case "SEND_SUPPRESSED":
      console.warn(sent.error.message);
      break;
  }
}
```

`app.send(contract, input, options)` runs these steps in order and stops at
the first refusal. Every refusal before the enqueue writes nothing.

1. The app lists the contract. Else `VALIDATION_FAILED`.
2. Send suppression: `SEND_SUPPRESSED`.
3. The options pass their checks. Else `VALIDATION_FAILED`.
4. The input encodes with the parameters and reads back to the same text.
   Else `VALIDATION_FAILED`.
5. The core prepares the task id, the options and the retention, and checks
   the payload bound: an input over the config's reject bound is
   `PAYLOAD_TOO_LARGE`.
6. One enqueue through the core.

A task body may send. The sends that the entry makes at module level in a
task child are suppressed: the child lifts the suppression when it serves.

| `SendOptions` | Does |
|---|---|
| `idempotencyKey` | 1 to 255 UTF-8 bytes. Stores the task once per key within the reservation window (24 hours). A second send of the same command gives the task that holds the key |
| `goodUntil` | A `Temporal.Instant`: the task expires (`TASK_EXPIRED`) when no worker started it by then. Cut to the microsecond, toward the past |
| `delaySeconds` | Whole seconds, 0 to 4,294,967,295, before a worker may start the task |
| `retention` | `{ kind: "days", days }`: a period that the app registers; `{ kind: "forever" }`. Left out: the queue's retention, else 30 days |

`TaskSendError<O, E>`, told apart by `code`:

| `code` | Fields | When |
|---|---|---|
| `SEND_SUPPRESSED` | `message` | The process does not send now: a task child before it serves, the `check` command, or `HORSIES_SUPPRESS_SENDS=1` |
| `VALIDATION_FAILED` | `message`, `exception` | A rule refused the send. Nothing was written |
| `PAYLOAD_TOO_LARGE` | `message`, `sizeBytes`, `limitBytes` (`bigint`) | The input is over the reject bound. Nothing was written |
| `PAYLOAD_MISMATCH` | `taskId`, `payload`, `message` | The task id exists with another command or key. Nothing was written |
| `IDEMPOTENCY_KEY_CONFLICT` | `taskId`, `payload`, `message`, `ownerTaskId`, `retryable: true` | Another task holds the key with another command. Nothing was written. The conflict clears when the key's reservation ends |
| `ENQUEUE_FAILED` | `taskId`, `payload`, `message`, `retryable`, `exception` | The enqueue failed. The row may or may not exist. `retryable` is the classification of the cause |
| `SEAT_RELAY_FULL` | `message`, `retryable: true`, `exception` | A `SHARED_THREAD` body only: the seat relay holds its bound of waiting calls. Nothing was sent; send again |

- `exception` is the cause that the send met: the server's error, no answer,
  a broken read contract, a failed precondition, or a rule of the client.
- `payload` is not enumerable, so `JSON.stringify` of an error never holds
  the input.

`app.retrySend(error)` takes `ENQUEUE_FAILED` and `IDEMPOTENCY_KEY_CONFLICT`
(typecheck; another code from untyped code is `VALIDATION_FAILED`). It sends
the same task id and payload again, as a repeat: a task that an earlier call
wrote answers with its id, also after it finished, and does not run again. A
key conflict before the key's reservation ends gives the same conflict again.
It takes an error of another app instance too, when this app lists its task.

With `resendOnTransientErr: true` in the config, a retryable
`ENQUEUE_FAILED` is sent again up to 3 times, after 200, 400 and 800 ms, as
a repeat. A key conflict is not sent again: only `retrySend` sends it again.

## Reads

```ts
import { TaskId, type ReadError } from "horsies";

function readErrorText(error: ReadError): string {
  switch (error.kind) {
    case "waitTimeout":
      return `the task is still ${error.status}`;
    case "taskNotFound":
      return `no record holds the task (${error.reason})`;
    case "resultNotAvailable":
      return `the task is ${error.status} and stores no result`;
    case "storedResultUnreadable":
      return error.cause;
    case "unknownUserCode":
      return `the stored code ${error.code} is not declared`;
    case "waitInsideTask":
      return "a result wait inside a task body";
    case "database":
      return `${error.code}, retryable: ${String(error.retryable)}`;
    case "invalidInput":
      return `waitMs is a whole number from 0 to ${error.maxMs}`;
    case "seatRelayFull":
      return error.message;
  }
}

export async function readValidation(text: string): Promise<void> {
  const taskId = TaskId.parse(text);

  if (!taskId.ok) {
    console.error(taskId.error.detail);

    return;
  }

  const handle = app.taskHandle(validateOrder, taskId.value);
  const read = await handle.get({ waitMs: 5_000 });

  if (!read.ok) {
    console.error(readErrorText(read.error));

    return;
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(`${outcome.value.orderId}: ${outcome.value.lineCount} lines`);
      break;
    case "failed": {
      const error = outcome.error;

      switch (error.kind) {
        case "custom":
          switch (error.code) {
            case "EMPTY_ORDER":
            case "UNKNOWN_SKU":
              console.log(`rejected: ${error.code}`);
              break;
            case "STOCK_SERVICE_DOWN":
              console.log("the stock service did not answer");
              break;
          }

          break;
        case "builtIn":
          console.log(`horsies recorded ${error.code}`);
          break;
      }

      break;
    }
  }

  const info = await handle.info();

  if (info.ok) {
    console.log(info.value.status, info.value.attempts.length);
  }
}
```

`handle.get({ waitMs })` waits for the task's end, at most `waitMs`
(whole milliseconds, 0 to 4,294,967,295). Left out: no bound.

| `TaskOutcome<O, E>` | The task |
|---|---|
| `{ kind: "succeeded", value }` | Returned this value |
| `{ kind: "failed", error }` | Returned this error, or horsies recorded a code (`TASK_CANCELLED`, `TASK_EXPIRED`, `TASK_TIMEOUT`, `WORKER_CRASHED`, ...) |

`TaskError<E>` has `kind`, `code`, `message` (`null` when none), `data`
(`null` when none), `exception` (the `ExceptionRecord` of a thrown value, or
`null`) and `subWorkflow` (the failed child of a sub-workflow node, or
`null`). `kind` is `"custom"` with `code: E` (a code that the task declares),
or `"builtIn"` with `code: BuiltInTaskCode` (a code of horsies).

| `ReadError` `kind` | `code` | Fields | When | Kept by the handle |
|---|---|---|---|---|
| `waitTimeout` | `WAIT_TIMEOUT` | `status` | The task had not ended when the wait ended; it may still run | No |
| `taskNotFound` | `TASK_NOT_FOUND` | `reason` | Neither the live table nor the history holds the task | No |
| `resultNotAvailable` | `RESULT_NOT_AVAILABLE` | `status` | The task ended and stores no result | Yes |
| `storedResultUnreadable` | - | `cause` | The stored result does not read with the contract's types | Yes |
| `unknownUserCode` | - | `code` (the stored code) | The stored custom code is not in the contract's `errors` | Yes |
| `waitInsideTask` | `WAIT_INSIDE_TASK` | - | A result wait in a worker child. Nothing was read | No |
| `database` | the broker code | `retryable`, `message` | The database or the connection failed | No |
| `invalidInput` | `INVALID_INPUT` | `maxMs` | `waitMs` outside its range. Nothing was read | No |
| `seatRelayFull` | `SEAT_RELAY_FULL` | `retryable: true`, `message` | A `SHARED_THREAD` body only: the seat relay holds its bound of waiting calls. Nothing was read | No |

- `reason` is `PREDATES_RETAINED_HISTORY` (retention can have removed the
  task), `WITHIN_RETAINED_HISTORY` (no removed history can have held it), or
  `NOT_CLASSIFIED` (the id is not a UUIDv7).
- A `CANCELLED` task reads as the outcome `TASK_CANCELLED`, never as a read
  error.
- The handle keeps its first final read (an outcome, or a failure marked
  "Yes"). A later `get` gives it with no I/O.
- `handle.info()` gives the `TaskInfo`: status, queue, priority, retry
  counts, the times, the worker, the error code, the failed reason, every
  attempt newest first, and the execution class of a live task. A fact that
  the row may not hold is `Recorded<T>`: `{ kind: "recorded", value }` or
  `{ kind: "notRecorded" }`. The info is not kept.
- `app.taskHandle(contract, taskId)` gives a typed handle of a stored id,
  with no I/O. The contract gives the types only; the app need not list it.
  `TaskId.parse(text)` takes the lower-case 8-4-4-4-12 form and refuses
  another text with `notCanonicalUuid`.

## Test a Body

```ts
import { AttemptNumber, TaskRuntime } from "horsies";

export async function testEmptyOrder(): Promise<boolean> {
  const taskId = TaskId.parse("0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b");
  const attempt = AttemptNumber.parse(1);

  if (!taskId.ok || !attempt.ok) {
    return false;
  }

  const runtime = TaskRuntime.forTest({ taskId: taskId.value, attempt: attempt.value });
  const outcome = await validateOrderBody.run({ orderId: taskId.value, lines: [] }, runtime);

  return outcome.kind === "failed" && outcome.error.code === "EMPTY_ORDER";
}
```

- `implementation.run(input, runtime)` runs the body with no worker, no
  database and no addon. The body gets the members of `runtime` and the
  contract's `ok` and `err`; the runtime of `TaskRuntime.forTest` holds no
  `ok` and no `err`. The input and the output are checked with the
  contract's types, as a worker checks them, and a thrown value goes
  through the exception map. It gives the outcome as a task read gives it.
- `TaskRuntime.forTest({ taskId, attempt, state })`: `state` pairs each key
  that `app.provide` returned with a value of the key's type
  (`[[Reports, fakePool]]`); a key with no entry throws `ClientDefect`.
  `spawn` runs its work at once and is never refused.
- `TaskRuntime.forTest({ ..., workflowContext })`: the stored form of a
  workflow context: `workflow_id`, `task_index`, `task_name`, and
  `results_by_id`, the stored result of each node by node id
  (`{ __h_task_result__: true, ok: <value>, err: null }`). Left out, the
  runtime holds `{ kind: "noContext" }`. A form that does not read throws
  `TypeError` with the member and the rule. The runtime fits the body of a
  task with or without `workflowContext: true`.
- A custom code that the core's rules would refuse is not checked by `run`:
  that check needs the addon.

## Common Pitfalls

**Start nothing in the entry before `cli.run(app)`.** Every worker child
runs the entry again. A server, a timer or a loop started there starts again
in each child.

**Wait for no result in a task body.** A result wait in a worker child is
refused with `waitInsideTask`. Pass the result with a workflow
(`h.taskOutcome`).

<!-- ts-check: skip elided -- a Wrong / Correct pair of statements -->
```ts
// WRONG: in a task body
const total = await otherHandle.get();

// CORRECT: a workflow node receives the upstream outcome
// input: { total: h.taskOutcome(computeTotals) }
// w.task(report, { total: totalsNode }, { id: "report" })
```

**Pass `runtime.signal` in a `SHARED_ASYNC` body.** A body that does not
pass it runs on past its deadline and keeps its seat. Put bodies whose
libraries take no signal in `ISOLATED`.

**Start run work with `runtime.spawn`, not with a Promise that the body
does not await.** Such a Promise is not tracked and gets no signal.

**Rename a parameter only when nothing stored holds the old name.** The
parameter name is the stored key. A queued task or a workflow node with the
old key fails with `WORKER_SERIALIZATION_ERROR`. Add a parameter with
`.optional()` or `.default(value)`, so that stored inputs without it read.

**List every contract.** `app.send` of a contract that `Horsies.create`
did not list is `VALIDATION_FAILED`, and `app.implement` of it is a refusal
(`HRS-300`).

**Do not drop a `Result`.** Read `ok` on every `Result`, or discard it with
`void`. The lint rule `horsies/no-dropped-result` reports a dropped one.

## All Key Imports

<!-- ts-check: alone -->
```ts
import {
  // Contracts and wire types
  defineTask, h, Decimal, BUILT_IN_TASK_CODES,
  type TaskContract, type TaskDefinition, type ExecutionClass, type RetryPolicy,
  type CustomFailure, type JsonValue, type Wire,
  // Results and outcomes
  type Result, type Ok, type Err, type TaskOutcome, type TaskError,
  type BuiltInTaskCode, type ExceptionRecord,
  // Implementations and the runtime
  type Implementation, type ImplementOptions, type ExceptionMapping, type ResultConstructors,
  TaskRuntime, AttemptNumber, type CancelState, type CancelCause, type FencingToken,
  type SpawnRefused, type StateKey, type WorkflowTaskRuntime, type StoredWorkflowContext,
  // Sends
  type SendOptions, type Retention, type TaskSendError, type RetryableSendError,
  // Reads
  TaskId, type TaskHandle, type ReadError, type TaskInfo, type TaskAttempt, type Recorded,
} from "horsies";
import { hz } from "horsies/zod";
```
