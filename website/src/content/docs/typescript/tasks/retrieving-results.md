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
  errors: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

type ValidateOrderHandle = TaskHandle<{ orderId: string; total: Decimal }, "EMPTY_ORDER" | "INVALID_EMAIL">;

function assertNever(value: never): never {
  throw new TypeError(`no case for ${String(value)}`);
}

export async function printOutcome(handle: ValidateOrderHandle): Promise<void> {
  const read = await handle.get({ timeoutMs: 30_000 });

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
      switch (outcome.error.code.kind) {
        case "user":
          console.log(`refused with ${outcome.error.code.code}`);
          break;
        case "library":
          console.log(`the library recorded ${outcome.error.code.code}`);
          break;
        default:
          assertNever(outcome.error.code);
      }
      break;
    default:
      assertNever(outcome);
  }
}
```

`handle.get(options)` waits while the task has not finished:

| `GetOptions.timeoutMs` | Wait |
|------------------------|------|
| Left out | Until the task ends |
| A whole number of milliseconds from 0 to 4,294,967,295 | Up to that time |
| Any other number | No wait and no read: the read error `invalidWait`, with `maxMs` |

`TaskOutcome<O, E>`:

| `kind` | Members | The task |
|--------|---------|----------|
| `"succeeded"` | `value: O` | Returned this value |
| `"failed"` | `error: TaskError<E>` | Returned this error, or the library recorded it |

`TaskError<E>`:

| Member | Type | Holds |
|--------|------|-------|
| `code` | `TaskErrorCode<E>` | `{ kind: "user", code: E }` for a code of the contract, or `{ kind: "library", code: LibraryTaskCode }` for a code that the library records, for example `TASK_CANCELLED`, `TASK_EXPIRED`, `TASK_TIMEOUT` or `WORKER_CRASHED` |
| `message` | `string \| null` | The message, or `null` |
| `data` | `JsonValue` | The error data, or `null` |
| `exception` | `ExceptionRecord \| null` | The record of a thrown error, or `null` |
| `subWorkflow` | `SubWorkflowFailure \| null` | The failed child workflow of a sub-workflow node, or `null` |

A task that was cancelled reads as the outcome `failed` with the library code `TASK_CANCELLED`, also when its row stores no result. The codes and their meaning are on [Task Errors](/horsies/typescript/tasks/errors/) and the [Errors Reference](/horsies/tasks/errors/#task-error-codes).

## ReadError

`ReadError` is a closed union, told apart by `kind`.

| `kind` | Members | When | A later read |
|--------|---------|------|--------------|
| `"waitTimedOut"` | `status`: `PENDING`, `CLAIMED` or `RUNNING` | The task had not finished when the wait ended | May find it finished |
| `"taskNotFound"` | `absence`: `TaskAbsence` | Neither the live table nor the history holds the task. A handle built from a stored id can race the commit of its send | May find it |
| `"resultNotAvailable"` | `status`: `COMPLETED`, `FAILED` or `EXPIRED` | The task finished and stores no result | Kept: gives the same |
| `"storedResultUnreadable"` | `cause` | The stored result does not read with the contract's types. `cause` holds no value | Kept: gives the same |
| `"unknownUserCode"` | `code` | The stored error has a user code that the contract's `errors` does not list | Kept: gives the same |
| `"waitInsideTask"` | - | A wait in a process that runs task bodies. Nothing was read. Compose the tasks with a workflow | - |
| `"database"` | `code`: `BrokerErrorCode`, `retryable`, `message` | The database or the connection failed. `retryable` is the classification of the cause | May succeed |
| `"invalidWait"` | `maxMs` | `timeoutMs` is not a whole number of milliseconds from 0 to `maxMs`. Nothing was read | - |

`TaskAbsence` tells why the task is held nowhere:

| Value | Meaning |
|-------|---------|
| `"predatesRetainedHistory"` | The task is older than the oldest history that retention keeps: retention can have removed it |
| `"withinRetainedHistory"` | No removed history can have held the task: it did not exist, or its history was removed out of band |
| `"notClassified"` | The id is not a UUIDv7, so it tells no time of birth |

The handle keeps its first final read: an outcome, `resultNotAvailable`, `storedResultUnreadable` and `unknownUserCode`. A later `get` gives it with no I/O. The `timeoutMs` check runs first, also on a handle that keeps a read.

## Task Info

```ts
export async function printAttempts(handle: ValidateOrderHandle): Promise<void> {
  const info = await handle.info();

  if (!info.ok) {
    console.warn(`the read failed: ${info.error.kind}`);
    return;
  }

  console.log(info.value.status, info.value.queueName, `retry ${info.value.retryCount} of ${info.value.maxRetries}`);

  for (const attempt of info.value.attempts) {
    const code = attempt.errorCode.kind === "recorded" ? attempt.errorCode.value : "no error code";
    console.log(attempt.attempt, attempt.outcome, code);
  }
}
```

`handle.info()` returns the task's `TaskInfo`. The info is not kept: a status changes. Its read errors are `taskNotFound` and `database`.

| Member | Type | Holds |
|--------|------|-------|
| `taskId`, `taskName`, `queueName` | `TaskId`, `string`, `string` | The task |
| `status` | `TaskStatus` | `PENDING`, `CLAIMED`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` or `EXPIRED` |
| `priority`, `retryCount`, `maxRetries` | `number` | The priority of the send and the retry counts |
| `enqueuedAt` | `Temporal.Instant` | When the row was written |
| `sentAt`, `claimedAt`, `startedAt`, `completedAt`, `failedAt`, `nextRetryAt` | `Recorded<Temporal.Instant>` | Each time, or `notRecorded` |
| `workerHostname`, `workerPid`, `workerProcessName` | `Recorded<string>`, `Recorded<number>`, `Recorded<string>` | The worker, or `notRecorded` |
| `errorCode`, `failedReason` | `Recorded<string>` | The error code and the failed reason, or `notRecorded` |
| `attempts` | `readonly TaskAttempt[]` | Every attempt, newest first |
| `execution` | `ExecutionFact` | `{ kind: "class", execution }` for a live task; `{ kind: "notKeptInHistory" }` for a task read from history |

`Recorded<T>` is `{ kind: "recorded", value: T }` or `{ kind: "notRecorded" }`. A `TaskAttempt` has `attempt`, `outcome` (`COMPLETED`, `FAILED`, `WORKER_FAILURE` or `CANCELLED`), `willRetry`, `startedAt`, `finishedAt`, and the recorded `errorCode`, `errorMessage`, `failedReason`, `workerId`, `workerHostname`, `workerPid` and `workerProcessName`.

## Reads by Task Id

A process that did not send the task reads it by its id. `TaskId.parse(text)` checks a stored text: a `TaskId` is a UUID in the lower-case 8-4-4-4-12 form. `app.taskHandle(contract, taskId)` gives a typed handle with no I/O.

<!-- ts-check: alone -->
```ts
import { defineTask, h, type Horsies, TaskId } from "horsies";

const validateOrder = defineTask({
  name: "validate_order",
  input: { orderId: h.uuid() },
  output: h.object({ orderId: h.uuid(), total: h.decimal() }),
  errors: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

export async function readStoredOrder(app: Horsies, storedId: string): Promise<string> {
  const taskId = TaskId.parse(storedId);

  if (!taskId.ok) {
    return taskId.error.detail;
  }

  const read = await app.taskHandle(validateOrder, taskId.value).get({ timeoutMs: 5_000 });

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

**Do not wait for a result inside a task body.** The read returns `waitInsideTask` and reads nothing. Compose the tasks with a workflow. See [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/).

**Do not treat a read error as the task's outcome.** `waitTimedOut` and `database` say nothing about the task: it may still run or may have finished.
