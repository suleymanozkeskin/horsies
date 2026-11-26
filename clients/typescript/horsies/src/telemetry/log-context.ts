// The log context of task code: `currentTask()` gives the task whose body runs in the current async context, with
// the ids of its attempt span, so a log library can put them on each record (a pino mixin, a winston format). The
// Python client's `TaskContextFilter` reads the same facts.
//
// The run comes from the run store of the child (the body call and its spawned work run inside it). The span ids come
// from the span id store, which the attempt-span observer sets around the body call.

import { currentRun } from "../child/run-store.ts";
import { unreachable } from "../wire/unreachable.ts";
import { attemptSpanStore } from "./span-id-store.ts";

/** The ids of the attempt span of a run: 32 and 16 lower-case hex digits. */
export type SpanIds = { readonly kind: "spanIds"; readonly traceId: string; readonly spanId: string };

/** The run has no attempt span: the app does not trace, or the SDK gave a span that is not valid. */
export type NoSpanIds = { readonly kind: "noSpanIds" };

/** The task whose body runs in the current async context, in a worker child. `attempt` is from 1. */
export type RunningTask = {
  readonly kind: "running";
  readonly taskId: string;
  readonly taskName: string;
  readonly attempt: number;
  readonly span: SpanIds | NoSpanIds;
};

/** No task body runs in the current async context. */
export type NoRunningTask = { readonly kind: "noTask" };

/** What `currentTask()` gives. */
export type CurrentTask = RunningTask | NoRunningTask;

const NO_SPAN_IDS: NoSpanIds = Object.freeze({ kind: "noSpanIds" });

const NO_TASK: NoRunningTask = Object.freeze({ kind: "noTask" });

/**
 * The task whose body runs in the current async context: inside a body (also after an `await`, in a timer and in the
 * work of `runtime.spawn`) `running`, else `noTask`. Reads two async local stores; never throws.
 */
export function currentTask(): CurrentTask {
  const run = currentRun();

  switch (run.kind) {
    case "noRun":
      return NO_TASK;
    case "run":
      return Object.freeze({
        kind: "running",
        taskId: run.mark.taskId,
        taskName: run.mark.taskName,
        attempt: run.mark.attempt,
        span: attemptSpanStore.getStore() ?? NO_SPAN_IDS,
      });
    default:
      return unreachable(run, "currentTask");
  }
}
