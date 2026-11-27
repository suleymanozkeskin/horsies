// The outcome of a run whose seat isolate ended before the run's result: an uncaught throw outside every run,
// `process.exit` in the isolate, or the isolate at its memory cap (`ERR_WORKER_OUT_OF_MEMORY`).
//
// The run ends with the core's crash outcome, `WORKER_CRASHED`, as a run whose child process ended does: the message
// names the seat, the exit code and the error that ended the isolate (the core's form: `... exited with code <n>
// before the task ended`), the data holds the task id, and no exception record is stored. A retry policy that names
// `WORKER_CRASHED` retries it, as in the other classes.

import { errorDataPart } from "../child/body.ts";
import { type ThrownRecord, thrownRecord } from "../child/thrown-error.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { SeatOutcome } from "./protocol.ts";

/** The type name of an isolate that ended with no error. */
export const WORKER_THREAD_EXIT_TYPE = "WorkerThreadExit";

/** How a seat isolate ended: with the error that it reported, or with its exit code only. */
export type IsolateEnd =
  | { readonly kind: "error"; readonly cause: unknown; readonly exitCode: number }
  | { readonly kind: "exit"; readonly exitCode: number };

/** The record of the error that ended an isolate. Pure. */
export function isolateEndRecord(end: IsolateEnd): ThrownRecord {
  switch (end.kind) {
    case "error":
      return thrownRecord(end.cause);
    case "exit": {
      const message = `the seat isolate ended with exit code ${String(end.exitCode)}`;

      return { type: WORKER_THREAD_EXIT_TYPE, message, repr: message, traceback: "" };
    }

    default:
      return unreachable(end, "isolateEndRecord");
  }
}

/** The run of task `taskId` on seat `seat`, whose isolate ended with `end`. */
export type EndedRun = { readonly seat: number; readonly taskId: string; readonly end: IsolateEnd };

/** The stored outcome of a run whose isolate ended: `WORKER_CRASHED`. Pure. */
export function isolateEndOutcome(run: EndedRun): SeatOutcome {
  const record = isolateEndRecord(run.end);

  return {
    kind: "error",
    parts: {
      code: { kind: "builtIn", code: "WORKER_CRASHED" },
      message: {
        kind: "text",
        text: `Seat isolate ${String(run.seat)} exited with code ${String(run.end.exitCode)} before the task ended: ${record.type}: ${record.message}`,
      },
      data: errorDataPart({ task_id: run.taskId }),
      exception: { kind: "noException" },
    },
  };
}
