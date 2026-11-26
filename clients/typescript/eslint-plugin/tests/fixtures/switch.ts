// A user's switch statements over the kinds of a read error. Each line that ends with an `expect` comment gets the
// report of that rule and message id; no other line gets a report (tests/typescript-7-project.test.ts).

import type { ReadError } from "horsies";

/** A switch that misses the kinds of `ReadError` other than `database` and `waitTimeout`. */
export function retryableOf(error: ReadError): boolean {
  switch (error.kind) { // expect: @typescript-eslint/switch-exhaustiveness-check switchIsNotExhaustive
    case "database":
      return error.retryable;
    case "waitTimeout":
      return true;
    default:
      return false;
  }
}

/** A switch with a case for each kind of `ReadError`, and a `never` check. */
export function readErrorText(error: ReadError): string {
  switch (error.kind) {
    case "waitTimeout":
      return `the task is ${error.status}`;
    case "taskNotFound":
      return `no task: ${error.reason}`;
    case "resultNotAvailable":
      return `no result: ${error.status}`;
    case "resultDeserializationError":
      return `the stored result does not read: ${error.cause}`;
    case "unknownCustomCode":
      return `an unknown custom code: ${error.code}`;
    case "waitInsideTask":
      return "a wait inside a task";
    case "database":
      return `the database failed: ${error.message}`;
    case "invalidInput":
      return `a wait over ${error.maxMs} ms`;
    case "seatRelayFull":
      return `the seat relay sent nothing: ${error.message}`;
    default: {
      const unreachable: never = error;

      return unreachable;
    }
  }
}
