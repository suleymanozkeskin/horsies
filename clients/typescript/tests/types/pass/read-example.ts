// Compile-pass case: a read through `app.taskHandle`: a read error switch over every
// kind with the catalog code of each, the outcome switch, and the tag of the task error with a full match on the
// declared custom codes.

import { defineTask, h, type Horsies, type ReadError, type TaskAbsenceReason, TaskId } from "../../../horsies/src/index.ts";

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
    case "waitTimeout": {
      const code: "WAIT_TIMEOUT" = error.code;

      return `${code} ${error.status}`;
    }

    case "taskNotFound": {
      const code: "TASK_NOT_FOUND" = error.code;
      const reason: TaskAbsenceReason = error.reason;

      return `${code} ${reason}`;
    }

    case "resultNotAvailable": {
      const code: "RESULT_NOT_AVAILABLE" = error.code;

      return `${code} ${error.status}`;
    }

    case "storedResultUnreadable":
      return error.cause;
    case "unknownUserCode":
      return error.code;
    case "waitInsideTask": {
      const code: "WAIT_INSIDE_TASK" = error.code;

      return code;
    }

    case "database":
      return `${error.code} ${String(error.retryable)}`;
    case "invalidInput": {
      const code: "INVALID_INPUT" = error.code;

      return `${code} ${String(error.maxMs)}`;
    }

    case "seatRelayFull": {
      const code: "SEAT_RELAY_FULL" = error.code;
      const retryable: true = error.retryable;

      return `${code} ${String(retryable)} ${error.message}`;
    }

    default:
      return assertNever(error);
  }
}

function customCodeText(code: "EMPTY_ORDER" | "INVALID_EMAIL"): string {
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
  const read = await handle.get({ waitMs: 5_000 });

  if (!read.ok) {
    return describeReadError(read.error);
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      return outcome.value.total.toString();
    case "failed":
      switch (outcome.error.kind) {
        case "custom":
          return customCodeText(outcome.error.code);

        case "builtIn":
          return outcome.error.code;
        default:
          return assertNever(outcome.error);
      }

    default:
      return assertNever(outcome);
  }
}

export async function infoStatus(app: Horsies, taskId: TaskId): Promise<string> {
  const info = await app.taskHandle(validateOrder, taskId).info();

  return info.ok ? `${info.value.status} ${String(info.value.attempts.length)}` : describeReadError(info.error);
}
