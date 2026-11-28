---
title: Sending Tasks
summary: Send a task with app.send, now or after a delay, with per-send options, and handle the closed union TaskSendError and resend.
related: [defining-tasks, retrieving-results, ../configuration/app-config, ../../tasks/errors]
tags: [typescript, send, TaskSendOptions, TaskSendError, resend, idempotency, retention]
---

# Sending Tasks

`app.send(contract, input, options)` sends one task of a contract that the app lists. It returns `Promise<Result<TaskHandle<O, E>, TaskSendError<O, E>>>`: the handle of the new task, or the send error. `O` is the output type of the contract and `E` its code type. A send never throws for an expected failure: each failure is a member of `TaskSendError`. Each member has a `kind` and a `code`.

## Send a Task

```ts
import { defineTask, h, Horsies } from "horsies";

const sendReceipt = defineTask({
  name: "send_receipt",
  input: { orderId: h.string() },
  output: h.null(),
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [sendReceipt],
});

if (!created.ok) {
  throw new Error(created.error.refusals.map((refusal) => `${refusal.code} ${refusal.path}: ${refusal.message}`).join("\n"));
}

const app = created.value;

export async function sendReceipts(): Promise<void> {
  const now = await app.send(sendReceipt, { orderId: "order-1042" });
  const later = await app.send(sendReceipt, { orderId: "order-1043" }, { delaySeconds: 300 });

  if (now.ok && later.ok) {
    console.log(now.value.taskId, later.value.taskId);
  }
}
```

- The input is an object with one member per parameter of the contract. A parameter with a default or `.optional()` can be left out. See [Wire Types](/horsies/typescript/tasks/wire-types/).
- The typecheck refuses an input with a missing, unknown or wrong-type member.
- A contract with a required `h.taskOutcome(contract)` parameter takes the outcome of an upstream task. Its input type is a text that names the rule, so the typecheck refuses every send of it: only a workflow node runs it. See [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/).
- The app sends only the contracts of its `tasks` list. See [App Config](/horsies/typescript/configuration/app-config/).

## What a Send Does

A send runs these steps in order, and stops at the first refusal. Every refusal before the enqueue writes nothing.

1. The list check: a contract that the app does not list returns `VALIDATION_FAILED`.
2. The suppression check: a suppressed send returns `SEND_SUPPRESSED`. See [Suppressed Sends](#suppressed-sends).
3. The options check: the first option that breaks its rule, in the order `idempotencyKey`, `goodUntil`, `delaySeconds`, `retention`, returns `VALIDATION_FAILED`.
4. The upstream check: a contract with a required `h.taskOutcome(contract)` parameter returns `VALIDATION_FAILED`.
5. The read-back of the input: the client encodes the input to text, decodes the text with the contract's types, and encodes that value again. A refusal, or two different texts, returns `VALIDATION_FAILED`.
6. The core's prepare step: the retention of the send (`VALIDATION_FAILED` for a period that the app does not register), the payload bound (`PAYLOAD_TOO_LARGE`), a new task id, and the priority of the task's queue.
7. One enqueue through the core.

A send on a `worker_threads` thread that the app code started throws `AddonOutsideMainThread`: the addon loads only on the main thread of a process.

## Send Options

```ts
import type { TaskSendOptions } from "horsies";

function invoiceOptions(invoiceId: number): TaskSendOptions {
  return {
    idempotencyKey: `invoice-${invoiceId}`,
    goodUntil: Temporal.Now.instant().add({ hours: 1 }),
    retention: { kind: "forever" },
  };
}

export async function sendInvoiceReceipt(invoiceId: number): Promise<void> {
  const sent = await app.send(sendReceipt, { orderId: `invoice-${invoiceId}` }, invoiceOptions(invoiceId));

  if (!sent.ok) {
    console.error(sent.error.code, sent.error.message);
  }
}
```

| Option | Type | Does |
|--------|------|------|
| `delaySeconds` | `number` | Whole seconds from 0 to 4,294,967,295 before a worker may start the task. Left out: no delay |
| `goodUntil` | `Temporal.Instant` | The task expires if no worker starts it before this instant. An instant of the years 1 to 9999; a part below the microsecond is cut toward the past |
| `idempotencyKey` | `string` | A key of 1 to 255 UTF-8 bytes, with no lone surrogate. A second send of the same command with the key in its reservation window gives the task that holds the key |
| `retention` | `SendRetention` | The retention of the task's history record. Left out: the retention of the task's queue, else the 30-day default |

A delay is the only start option. A send has no start time.

`SendRetention`:

| Value | History record kept |
|-------|---------------------|
| `{ kind: "period", days }` | For a period of whole days that the app registers: the 30-day default, a period of `retention.queueRetention`, or a period of `retention.retentionPeriods`. Another period returns `VALIDATION_FAILED`, and the message names the allowed periods |
| `{ kind: "forever" }` | Forever |

The meaning of the reservation window and of retention is on [Retention Config](/horsies/configuration/retention-config/) and [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#make-the-send-repeatable-with-an-idempotency-key).

## TaskSendError

`TaskSendError<O, E>` is a closed union. Its members are told apart by `kind`, the variant name. Each `code` names the same state. Each member holds only the facts of its state.

| `kind` | `code` | Type | Members | `resend` |
|--------|--------|------|---------|----------|
| `suppressed` | `SEND_SUPPRESSED` | `TaskSendSuppressed` | `message` | Refused by the typecheck |
| `validationFailed` | `VALIDATION_FAILED` | `TaskSendValidationFailed` | `message`, `cause` | Refused by the typecheck |
| `enqueueFailed` | `ENQUEUE_FAILED` | `RetryableSendError<O, E>` | `taskId`, `payload`, `message`, `retryable`, `cause` | Accepted |
| `payloadMismatch` | `PAYLOAD_MISMATCH` | `TaskSendPayloadMismatch` | `taskId`, `payload`, `message` | Refused by the typecheck |
| `payloadTooLarge` | `PAYLOAD_TOO_LARGE` | `TaskSendPayloadTooLarge` | `message`, `sizeBytes`, `limitBytes` (both `bigint`) | Refused by the typecheck |
| `idempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `TaskSendIdempotencyKeyConflict` | `taskId`, `payload`, `message`, `ownerTaskId`, `retryable: false` | Refused by the typecheck |
| `seatRelayFull` | `SEAT_RELAY_FULL` | `SeatRelayFull` | `message`, `retryable: true`, `cause` | Refused by the typecheck |

- `ENQUEUE_FAILED`: the row may or may not exist. `retryable` is the classification of the cause: `true` when the condition may clear. `cause` is the cause that the send met, or `{ kind: "noCause" }`.
- `PAYLOAD_MISMATCH`: the task id exists with another command or idempotency key. Nothing was written.
- `IDEMPOTENCY_KEY_CONFLICT`: another task (`ownerTaskId`) holds the key with another command. Nothing was written. `retryable` is always `false`: a resend under this task id meets the same reservation.
- `SEAT_RELAY_FULL`: a `SHARED_THREAD` body only. The seat relay holds its bound of waiting calls. Nothing was sent. Send again.
- `cause` of `VALIDATION_FAILED` is the cause that the send met, or `{ kind: "noCause" }`.
- `payload` is a `TaskSendPayload`: the task name, the queue, the priority, the input text, the sent instant and the options of the send. It is not enumerable, so `JSON.stringify` of an error does not write the input.
- `message` is not enumerable either.

The meaning of each code is on the [Errors Reference](/horsies/tasks/errors/#send-error-codes).

### Handle Every Code

<!-- ts-check: alone -->
```ts
import { defineTask, h, type Horsies, type TaskHandle } from "horsies";

const chargeCard = defineTask({
  name: "charge_card",
  input: { orderId: h.string(), amountCents: h.int() },
  output: h.string(),
  errorCodes: ["CARD_DECLINED"],
});

function assertNever(value: never): never {
  throw new TypeError(`no case for ${String(value)}`);
}

export async function charge(app: Horsies, orderId: string): Promise<TaskHandle<string, "CARD_DECLINED"> | undefined> {
  const sent = await app.send(chargeCard, { orderId, amountCents: 4_200 }, { idempotencyKey: `charge-${orderId}` });

  if (sent.ok) {
    return sent.value;
  }

  switch (sent.error.kind) {
    case "enqueueFailed":
      console.warn(`the row of ${sent.error.taskId} may exist; retryable: ${String(sent.error.retryable)}`);
      return undefined;
    case "idempotencyKeyConflict":
      console.warn(`the key is held by ${sent.error.ownerTaskId}`);
      return undefined;
    case "validationFailed":
    case "payloadTooLarge":
    case "payloadMismatch":
    case "suppressed":
    case "seatRelayFull":
      console.warn(sent.error.message);
      return undefined;
    default:
      return assertNever(sent.error);
  }
}
```

A `switch` that leaves out a `kind` fails the typecheck at `assertNever`.

### Resend a Failed Send

<!-- ts-check: alone -->
```ts
import { defineTask, h, type Horsies, type Result, type TaskHandle, type TaskSendError } from "horsies";

const sendReceipt = defineTask({
  name: "send_receipt",
  input: { orderId: h.string() },
  output: h.null(),
});

export async function sendWithOneRetry(
  app: Horsies,
  orderId: string,
): Promise<Result<TaskHandle<null, string>, TaskSendError<null, string>>> {
  const sent = await app.send(sendReceipt, { orderId });

  if (!sent.ok && sent.error.kind === "enqueueFailed" && sent.error.retryable) {
    return app.resend(sent.error);
  }

  return sent;
}
```

`app.resend(error)` sends the payload of an `ENQUEUE_FAILED` error again, under the same task id, as a repeat. A task that an earlier call wrote answers with its task id, also after it finished, and does not run again.

- The typecheck accepts only `ENQUEUE_FAILED`. At run time any other error from untyped code returns `VALIDATION_FAILED`.
- The error of another app instance is accepted when this app lists a contract with the task name of `error.payload`. Else the call returns `VALIDATION_FAILED`.
- The suppression check runs again: a suppressed process returns `SEND_SUPPRESSED`.
- The handle reads with the types of the error's contract: `O` and `E` come from the send that failed.

### Resend on Transient Errors

With `resendOnTransientError: true` in the config, a send whose enqueue fails with a retryable `ENQUEUE_FAILED` is sent again with the same task id and payload, as a repeat. The first resend waits 200 ms, each later wait doubles up to 2,000 ms, and at most 3 resends run. With the option on, `resend` resends the same way. The answer of a repeat is on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#resend-transient-failures).

## Payload Size

The `payload` field of the config sets two bounds on the UTF-8 bytes of the input text:

| Field | Type | Does |
|-------|------|------|
| `warnBytes` | `PayloadBound` | A send above this size logs a warning, once per task name and payload kind in a process |
| `rejectBytes` | `PayloadBound` | A send above this size returns `PAYLOAD_TOO_LARGE` before any row is written |

`PayloadBound` is `{ kind: "bytes", bytes }` or `{ kind: "noBound" }`. A field left out has the core's default. See [App Config](/horsies/typescript/configuration/app-config/).

## Suppressed Sends

A suppressed send returns `SEND_SUPPRESSED` and writes nothing. Sends are suppressed:

| Where | Until |
|-------|-------|
| A task child of a worker | `cli.run` starts to serve the child: a send at module level of the app entry does not run once per child |
| The `check` command | The end of the process |
| Any process with `HORSIES_SUPPRESS_SENDS=1` | The variable changes: it is read at each send, with the whitespace around the value ignored |

See [Worker Setup](/horsies/typescript/workers/worker-setup/) and [Testing](/horsies/typescript/operations/testing/#suppress-sends).

## Things to Avoid

**Do not resend `idempotencyKeyConflict` or `payloadMismatch`.** Nothing was written, and a repeat meets the same row. Choose a new key or a new command.

**Do not read `retryable` as "safe to repeat".** `retryable: true` means that the cause may clear. The row of an `ENQUEUE_FAILED` may exist; `resend` is safe because it repeats the same task id.
