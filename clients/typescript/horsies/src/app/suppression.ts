// Sends suppressed while a process prepares to serve. Every line before `cli.run` runs again in each child process,
// each seat isolate and each scheduler start, so a send at module level would run once per start. `Horsies.create`
// starts with sends suppressed in a task child, in a seat isolate, in the scheduler and in the `check` command;
// `cli.run` lifts the suppression when the child, the seat or the scheduler starts to serve. The Python scheduler
// suppresses sends while it loads the task modules, and lifts the suppression before it serves.
// `HORSIES_SUPPRESS_SENDS=1` suppresses too: read at each send, the value trimmed, as the Python and Rust clients read
// it.

import { isMainThread, workerData } from "node:worker_threads";

/** The argument that starts a task child (`cli.run`, child mode). */
export const CHILD_MODE_FLAG = "--horsies-child";

/** The command that checks the app and runs no task. */
export const CHECK_COMMAND = "check";

/** The command that runs the scheduler. */
export const SCHEDULER_COMMAND = "scheduler";

/** The `workerData` member that marks a seat isolate that horsies started. */
export const SEAT_MARKER_KEY = "horsiesSeat";

/** The environment variable that suppresses sends in any process. */
export const SUPPRESS_SENDS_VARIABLE = "HORSIES_SUPPRESS_SENDS";

/** The value of `SUPPRESS_SENDS_VARIABLE` that suppresses, after a trim of the whitespace around it. */
const SUPPRESS_SENDS_ON = "1";

/** A read of one environment variable. */
export type VariableRead = (name: string) => string | undefined;

/** Why sends are suppressed. A child, a seat and the scheduler lift it when they serve; the others stay. */
export type SuppressionReason =
  | "taskChildBeforeServe"
  | "seatBeforeServe"
  | "schedulerBeforeServe"
  | "checkCommand"
  | "environment";

/** Whether this process may send. */
export type SendSuppression =
  | { readonly kind: "open" }
  | { readonly kind: "suppressed"; readonly reason: SuppressionReason };

/** The facts of this process that decide the suppression at `Horsies.create`. */
export type ProcessStart = {
  readonly argumentAfterScript: string | undefined;
  readonly seatIsolate: boolean;
};

/** The start facts of this process. Effects: reads `process.argv`, `process.env` and `workerData`. */
export function processStart(): ProcessStart {
  return {
    argumentAfterScript: process.argv[2],
    seatIsolate: !isMainThread && isSeatMarked(workerData),
  };
}

/** Whether `workerData` (any value that the starter of the thread gave) is an object with the seat marker. */
function isSeatMarked(data: object | null | undefined): boolean {
  return data !== null && data !== undefined && Object(data) === data && SEAT_MARKER_KEY in data;
}

/** The suppression that a process starts with. Pure. */
export function initialSuppression(start: ProcessStart): SendSuppression {
  if (start.seatIsolate) {
    return { kind: "suppressed", reason: "seatBeforeServe" };
  }

  switch (start.argumentAfterScript) {
    case CHILD_MODE_FLAG:
      return { kind: "suppressed", reason: "taskChildBeforeServe" };
    case SCHEDULER_COMMAND:
      return { kind: "suppressed", reason: "schedulerBeforeServe" };
    case CHECK_COMMAND:
      return { kind: "suppressed", reason: "checkCommand" };
    default:
      return { kind: "open" };
  }
}

/** The suppression after the process starts to serve: a child, a seat or the scheduler opens; the others stay. Pure. */
export function servingSuppression(current: SendSuppression): SendSuppression {
  switch (current.kind) {
    case "open":
      return current;
    case "suppressed":
      switch (current.reason) {
        case "taskChildBeforeServe":
        case "seatBeforeServe":
        case "schedulerBeforeServe":
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

/** Whether a value of `SUPPRESS_SENDS_VARIABLE` asks for suppression: `1`, with the whitespace around it ignored. */
export function suppressionRequested(value: string | undefined): boolean {
  return value?.trim() === SUPPRESS_SENDS_ON;
}

/** The suppression of one app: read at each send, lifted once by `cli.run`. */
export class SuppressionState {
  #current: SendSuppression;
  readonly #variable: VariableRead;

  constructor(initial: SendSuppression, variable: VariableRead) {
    this.#current = initial;
    this.#variable = variable;
  }

  /** The suppression now: `SUPPRESS_SENDS_VARIABLE` first (read at this call), then the state of the process. */
  current(): SendSuppression {
    return suppressionRequested(this.#variable(SUPPRESS_SENDS_VARIABLE))
      ? { kind: "suppressed", reason: "environment" }
      : this.#current;
  }

  /** The process starts to serve (`cli.run`: a task child, a seat isolate or the scheduler). */
  startServing(): void {
    this.#current = servingSuppression(this.#current);
  }
}
