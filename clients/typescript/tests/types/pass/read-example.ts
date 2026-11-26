// Compile-pass case: a read through `app.taskHandle`: a read error switch over every
// kind, the outcome switch, and the two-level code with a full match on the declared user codes.

import { defineTask, h, type Horsies, type ReadError, TaskId } from "../../../horsies/src/index.ts";

function assertNever(value: never): never {
  throw new TypeError(`unexpected: ${String(value)}`);
}

const validateOrder = defineTask({
  name: "validate_order",
  input: { orderId: h.uuid() },
  output: h.object({ orderId: h.uuid(), total: h.decimal() }),
  errors: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

function describeReadError(error: ReadError): string {
  switch (error.kind) {
    case "waitTimedOut":
      return error.status;
    case "taskNotFound":
      return error.absence;
    case "resultNotAvailable":
      return error.status;
    case "storedResultUnreadable":
      return error.cause;
    case "unknownUserCode":
      return error.code;
    case "waitInsideTask":
      return "inside a task";
    case "database":
      return `${error.code} ${String(error.retryable)}`;
    case "invalidWait":
      return String(error.maxMs);
    default:
      return assertNever(error);
  }
}

function userCodeText(code: "EMPTY_ORDER" | "INVALID_EMAIL"): string {
  switch (code) {
    case "EMPTY_ORDER":
      return "empty";
    case "INVALID_EMAIL":
      return "email";
    default:
      return assertNever(code);
  }
}

export async function readOrder(app: Horsies, text: string): Promise<string> {
  const taskId = TaskId.parse(text);

  if (!taskId.ok) {
    return taskId.error.kind;
  }

  const handle = app.taskHandle(validateOrder, taskId.value);
  const read = await handle.get({ timeoutMs: 5_000 });

  if (!read.ok) {
    return describeReadError(read.error);
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      return outcome.value.total.toString();
    case "failed":
      switch (outcome.error.code.kind) {
        case "user":
          return userCodeText(outcome.error.code.code);

        case "library":
          return outcome.error.code.code;
        default:
          return assertNever(outcome.error.code);
      }

    default:
      return assertNever(outcome);
  }
}

export async function infoStatus(app: Horsies, taskId: TaskId): Promise<string> {
  const info = await app.taskHandle(validateOrder, taskId).info();

  return info.ok ? `${info.value.status} ${String(info.value.attempts.length)}` : describeReadError(info.error);
}
