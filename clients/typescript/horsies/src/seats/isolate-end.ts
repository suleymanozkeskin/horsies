// The outcome of a run whose seat isolate ended before the run's result: an uncaught throw in the isolate,
// `process.exit` there, or an isolate out of memory (`ERR_WORKER_OUT_OF_MEMORY`, with the memory cap of the isolate).
//
// The run ends `UNHANDLED_EXCEPTION`, with the message form of a thrown value in a body
// (`Unhandled exception in task <name>: <type>: <text>`), `data.exception_type`, and the exception record of the error
// that ended the isolate. An isolate that ended with no error has the type `WorkerThreadExit` and its exit code.

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

/** The exception record of `end`. Pure. */
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

/** The stored outcome of the run of `taskName` on an isolate that ended with `end`. Pure. */
export function isolateEndOutcome(taskName: string, end: IsolateEnd): SeatOutcome {
  const record = isolateEndRecord(end);

  return {
    kind: "error",
    parts: {
      code: { kind: "library", code: "UNHANDLED_EXCEPTION" },
      message: { kind: "text", text: `Unhandled exception in task ${taskName}: ${record.type}: ${record.message}` },
      data: errorDataPart({ exception_type: record.type }),
      exception: {
        kind: "record",
        className: record.type,
        module: "",
        message: record.message,
        repr: record.repr,
        traceback: record.traceback,
      },
    },
  };
}
