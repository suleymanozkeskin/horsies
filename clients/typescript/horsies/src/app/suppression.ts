// Sends suppressed while a process prepares to serve. Every line before `cli.run` runs again in each child process
// and each seat isolate, so a send at module level would run once per start. `Horsies.create` starts with sends
// suppressed in a task child, in a seat isolate and in the `check` command; `cli.run` lifts the suppression when the
// child or the seat starts to serve. `HORSIES_SUPPRESS_SENDS=1` suppresses too (as the Python and Rust clients).

import { isMainThread, workerData } from "node:worker_threads";

/** The argument that starts a task child (`cli.run`, child mode). */
export const CHILD_MODE_FLAG = "--horsies-child";

/** The command that checks the app and runs no task. */
export const CHECK_COMMAND = "check";

/** The `workerData` member that marks a seat isolate that horsies started. */
export const SEAT_MARKER_KEY = "horsiesSeat";

/** The environment variable that suppresses sends in any process. */
export const SUPPRESS_SENDS_VARIABLE = "HORSIES_SUPPRESS_SENDS";

/** The value of `SUPPRESS_SENDS_VARIABLE` that suppresses. */
const SUPPRESS_SENDS_ON = "1";

/** Why sends are suppressed. A child and a seat lift it when they serve; the others stay. */
export type SuppressionReason = "taskChildBeforeServe" | "seatBeforeServe" | "checkCommand" | "environment";

/** Whether this process may send. */
export type SendSuppression =
  | { readonly kind: "open" }
  | { readonly kind: "suppressed"; readonly reason: SuppressionReason };

/** The facts of this process that decide the suppression at `Horsies.create`. */
export type ProcessStart = {
  readonly argumentAfterScript: string | undefined;
  readonly seatIsolate: boolean;
  readonly suppressVariable: string | undefined;
};

/** The start facts of this process. Effects: reads `process.argv`, `process.env` and `workerData`. */
export function processStart(): ProcessStart {
  return {
    argumentAfterScript: process.argv[2],
    seatIsolate: !isMainThread && isSeatMarked(workerData),
    suppressVariable: process.env[SUPPRESS_SENDS_VARIABLE],
  };
}

/** Whether `workerData` (any value that the starter of the thread gave) is an object with the seat marker. */
function isSeatMarked(data: object | null | undefined): boolean {
  return data !== null && data !== undefined && Object(data) === data && SEAT_MARKER_KEY in data;
}

/** The suppression that a process starts with. Pure. */
export function initialSuppression(start: ProcessStart): SendSuppression {
  if (start.suppressVariable === SUPPRESS_SENDS_ON) {
    return { kind: "suppressed", reason: "environment" };
  }

  if (start.seatIsolate) {
    return { kind: "suppressed", reason: "seatBeforeServe" };
  }

  switch (start.argumentAfterScript) {
    case CHILD_MODE_FLAG:
      return { kind: "suppressed", reason: "taskChildBeforeServe" };
    case CHECK_COMMAND:
      return { kind: "suppressed", reason: "checkCommand" };
    default:
      return { kind: "open" };
  }
}

/** The suppression after the process starts to serve: a child or a seat opens; the others stay. Pure. */
export function servingSuppression(current: SendSuppression): SendSuppression {
  switch (current.kind) {
    case "open":
      return current;
    case "suppressed":
      switch (current.reason) {
        case "taskChildBeforeServe":
        case "seatBeforeServe":
          return { kind: "open" };
        case "checkCommand":
        case "environment":
          return current;
        default: {
          const unexpected: never = current.reason;

          throw new TypeError(`servingSuppression: a reason outside its type: ${String(unexpected)}`);
        }
      }

    default: {
      const unexpected: never = current;

      throw new TypeError(`servingSuppression: a suppression outside its type: ${String(unexpected)}`);
    }
  }
}

/** The suppression of one app: read at each send, lifted once by `cli.run`. */
export class SuppressionState {
  #current: SendSuppression;

  constructor(initial: SendSuppression) {
    this.#current = initial;
  }

  current(): SendSuppression {
    return this.#current;
  }

  /** The process starts to serve (`cli.run`, a task child or a seat isolate). */
  startServing(): void {
    this.#current = servingSuppression(this.#current);
  }
}
