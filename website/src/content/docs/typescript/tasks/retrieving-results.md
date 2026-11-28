---
title: Retrieving Results
summary: Read a task's outcome with handle.get as Result<TaskOutcome<O, E>, ReadError>, read its info, and build a handle from a stored task id.
related: [sending-tasks, errors, ../../concepts/task-lifecycle]
tags: [typescript, result, TaskHandle, TaskOutcome, ReadError, TaskInfo, TaskId]
---

# Retrieving Results

A read of a task's result returns `Promise<Result<TaskOutcome<O, E>, ReadError>>`. The outer error is the read that failed: the wait ended, the task is not held, the stored data does not read, the database failed. The inner value is what the task ended with.

## Read the Outcome

```ts
import { type Decimal, defineTask, h, type TaskHandle } from "horsies";

const validateOrder = defineTask({
  name: "validate_order",
  input: { orderId: h.uuid() },
  output: h.object({ orderId: h.uuid(), total: h.decimal() }),
  errorCodes: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

type ValidateOrderHandle = TaskHandle<{ orderId: string; total: Decimal }, "EMPTY_ORDER" | "INVALID_EMAIL">;

function assertNever(value: never): never {
  throw new TypeError(`no case for ${String(value)}`);
}

export async function printOutcome(handle: ValidateOrderHandle): Promise<void> {
  const read = await handle.get({ waitMs: 30_000 });

  if (!read.ok) {
    console.warn(`the read failed: ${read.error.kind}`);
    return;
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(`validated ${outcome.value.orderId}: ${outcome.value.total.toString()}`);
      break;
    case "failed":
      switch (outcome.error.kind) {
        case "custom":
          console.log(`refused with ${outcome.error.code}`);
          break;
        case "builtIn":
          console.log(`horsies recorded ${outcome.error.code}`);
          break;
        default:
          assertNever(outcome.error);
      }
      break;
    default:
      assertNever(outcome);
  }
}
```

`handle.get(options)` waits while the task has not finished:

| `GetOptions.waitMs` | Wait |
|------------------------|------|
| Left out | Until the task ends |
| A whole number of milliseconds from 0 to 4,294,967,295 | Up to that time |
| Any other number | No wait and no read: the read error `invalidInput`, with `maxMs` |

`TaskOutcome<O, E>`:

| `kind` | Members | The task |
|--------|---------|----------|
| `"succeeded"` | `value: O` | Returned this value |
| `"failed"` | `error: TaskError<E>` | Returned this error, or horsies recorded a code |

`TaskError<E>` is told apart by `kind`:

| `kind` | `code` | The error |
|--------|--------|-----------|
| `"custom"` | `E` | A code that the contract declares in `errorCodes` |
| `"builtIn"` | `BuiltInTaskCode` | A code that horsies records, for example `TASK_CANCELLED`, `TASK_EXPIRED`, `TASK_TIMEOUT` or `WORKER_CRASHED` |

Both variants have these members:

| Member | Type | Holds |
|--------|------|-------|
| `message` | `string \| null` | The message, or `null` |
| `data` | `JsonValue` | The error data, or `null` |
| `exception` | `ExceptionRecord \| null` | The record of a thrown error, or `null` |
| `subWorkflow` | `SubWorkflowFailure \| null` | The failed child workflow of a sub-workflow node, or `null` |

A task that was cancelled reads as the outcome `failed` with the built-in code `TASK_CANCELLED`, also when its row stores no result. The codes and their meaning are on [Task Errors](/horsies/typescript/tasks/errors/) and the [Errors Reference](/horsies/tasks/errors/#task-error-codes).

## ReadError

`ReadError` is a closed union, told apart by `kind`.

| `kind` | `code` | Members | When | A later read |
|--------|--------|---------|------|--------------|
| `"waitTimeout"` | `WAIT_TIMEOUT` | `status`: `PENDING`, `CLAIMED` or `RUNNING` | The task had not finished when the wait ended. It may still run | May find it finished |
| `"taskNotFound"` | `TASK_NOT_FOUND` | `reason`: `TaskAbsenceReason` | Neither the live table nor the history holds the task. A handle built from a stored id can race the commit of its send | May find it |
| `"resultNotAvailable"` | `RESULT_NOT_AVAILABLE` | `status`: `COMPLETED`, `FAILED` or `EXPIRED` | The task finished and stores no result | Kept: gives the same |
| `"storedResultUnreadable"` | `RESULT_DESERIALIZATION_ERROR` | `cause` | The stored result does not read with the contract's types. `cause` holds no value | Kept: gives the same |
| `"unknownCustomCode"` | `UNKNOWN_CUSTOM_CODE` | `storedCode` | The stored error has a custom code that the contract's `errorCodes` does not list | Kept: gives the same |
| `"waitInsideTask"` | `WAIT_INSIDE_TASK` | - | A wait in a process that runs task bodies. Nothing was read. Compose the tasks with a workflow | - |
| `"databaseError"` | `DATABASE_ERROR` | `databaseCode`, `retryable`, `message` | The database or the connection failed. `retryable` is the classification of the cause | May succeed |
| `"invalidInput"` | `INVALID_INPUT` | `maxMs` | `waitMs` is not a whole number of milliseconds from 0 to `maxMs`. Nothing was read | - |
| `"seatRelayFull"` | `SEAT_RELAY_FULL` | `retryable: true`, `message` | A `SHARED_THREAD` body only: the seat relay holds its bound of waiting calls. Nothing was read. Read again | May succeed |

`TaskAbsenceReason` tells why the task is held nowhere:

| Value | Meaning |
|-------|---------|
| `"PREDATES_RETAINED_HISTORY"` | The task is older than the oldest history that retention keeps: retention can have removed it |
| `"WITHIN_RETAINED_HISTORY"` | No removed history can have held the task: it did not exist, or its history was removed out of band |
| `"NOT_CLASSIFIED"` | The id is not a UUIDv7, so it tells no time of birth |

The handle keeps its first final read: an outcome, `resultNotAvailable`, `storedResultUnreadable` and `unknownCustomCode`. A later `get` gives it with no I/O. The `waitMs` check runs first, also on a handle that keeps a read.

## Task Info

```ts
export async function printAttempts(handle: ValidateOrderHandle): Promise<void> {
  const info = await handle.info();

  if (!info.ok) {
    console.warn(`the read failed: ${info.error.kind}`);
    return;
  }

  console.log(info.value.status, info.value.queue, `retry ${info.value.retryCount} of ${info.value.maxRetries}`);

  for (const attempt of info.value.attempts) {
    const code = attempt.errorCode.kind === "recorded" ? attempt.errorCode.value : "no error code";
    console.log(attempt.attempt, attempt.outcome, code);
  }
}
```

`handle.info()` returns the task's `TaskInfo`. The info is not kept: a status changes. Its read errors are the `ReadError` kinds `taskNotFound` and `databaseError`.

| Member | Type | Holds |
|--------|------|-------|
| `taskId`, `taskName`, `queue` | `TaskId`, `string`, `string` | The task |
| `status` | `TaskStatus` | `PENDING`, `CLAIMED`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` or `EXPIRED` |
| `priority`, `retryCount`, `maxRetries` | `number` | The priority of the send and the retry counts |
| `dueAt` | `Temporal.Instant` | When the task is due to start |
| `sentAt`, `claimedAt`, `startedAt`, `completedAt`, `failedAt`, `nextRetryAt` | `Recorded<Temporal.Instant>` | Each time, or `notRecorded` |
| `workerHostname`, `workerPid`, `workerProcessName` | `Recorded<string>`, `Recorded<number>`, `Recorded<string>` | The worker, or `notRecorded` |
| `errorCode`, `failedReason` | `Recorded<string>` | The error code and the failed reason, or `notRecorded` |
| `attempts` | `readonly TaskAttempt[]` | Every attempt, newest first |
| `execution` | `ExecutionFact` | `{ kind: "class", execution }` for a live task; `{ kind: "notKeptInHistory" }` for a task read from history |

`Recorded<T>` is `{ kind: "recorded", value: T }` or `{ kind: "notRecorded" }`. A `TaskAttempt` has `attempt`, `outcome` (`COMPLETED`, `FAILED`, `WORKER_FAILURE` or `CANCELLED`), `willRetry`, `startedAt`, `finishedAt`, and the `Recorded` `errorCode`, `errorMessage`, `failedReason`, `workerId`, `workerHostname`, `workerPid` and `workerProcessName`.

## Reads by Task Id

A process that did not send the task reads it by its id. `TaskId.parse(text)` checks a stored text: a `TaskId` is a UUID in the lower-case 8-4-4-4-12 form. `app.taskHandle(contract, taskId)` gives a typed handle with no I/O.

<!-- ts-check: alone -->
```ts
import { defineTask, h, type Horsies, TaskId } from "horsies";

const validateOrder = defineTask({
  name: "validate_order",
  input: { orderId: h.uuid() },
  output: h.object({ orderId: h.uuid(), total: h.decimal() }),
  errorCodes: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

export async function readStoredOrder(app: Horsies, storedId: string): Promise<string> {
  const taskId = TaskId.parse(storedId);

  if (!taskId.ok) {
    return taskId.error.detail;
  }

  const read = await app.taskHandle(validateOrder, taskId.value).get({ waitMs: 5_000 });

  if (!read.ok) {
    return read.error.kind;
  }

  return read.value.kind === "succeeded" ? read.value.value.total.toString() : "the task failed";
}
```

| Call | Returns |
|------|---------|
| `TaskId.parse(text)` | `Result<TaskId, IdRefused>`. `IdRefused` is `{ kind: "notCanonicalUuid", detail }`; `detail` names the rule and never holds the text |
| `app.taskHandle(contract, taskId)` | A `TaskHandle<O, E>` of the task. Starts nothing. The contract gives the types only: the app need not list it |

A plain `string` is not a `TaskId`: the typecheck refuses it. A handle of a contract that does not compile reads `storedResultUnreadable`, and its `cause` names the refusals.

## Things to Avoid

**Do not wait for a result inside a task body.** The read returns the read error `waitInsideTask` and reads nothing. Compose the tasks with a workflow. See [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/).

**Do not treat a read error as the task's outcome.** `waitTimeout` and `databaseError` say nothing about the task: it may still run or may have finished.
