// The tasks and the workflow of the `worker_ended_spans` instance (Python
// `clients/python/tests/e2e/tasks/instance_worker_ended_spans.py`): an isolated sleeper that the worker kills at its
// deadline, a one-node workflow of that sleeper, and a shared async sleeper that its child ends at its deadline. The
// `SHARED_THREAD` tasks run in the seat isolates of a thread child: one that holds its isolate for the time it gets,
// and one that holds its isolate past its deadline, then returns.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../../horsies/src/index.ts";
import { createOnce, markerErrorText } from "../markers.ts";
import { holdThread, sleepFor } from "./waits.ts";

/** The deadline of the sleepers. */
export const SLEEPER_TIMEOUT_MS = 1_000;

/** How long a sleeper waits: far past its deadline. */
const SLEEP_MS = 60_000;

/** Holds its thread past its deadline: the worker kills its child at the deadline. */
export const endedSpansSleeper = defineTask({
  name: "e2e_worker_ended_spans_sleeper",
  input: {},
  output: h.string(),
  timeoutMs: SLEEPER_TIMEOUT_MS,
});

/** Passes the signal into its wait: its child ends the run at the deadline and lives. */
export const endedSpansSharedSleeper = defineTask({
  name: "e2e_worker_ended_spans_shared_sleeper",
  input: {},
  output: h.string(),
  execution: "SHARED_ASYNC",
  timeoutMs: SLEEPER_TIMEOUT_MS,
});

/** The deadline of the thread hold: past every hold of the suite. */
const THREAD_HOLD_TIMEOUT_MS = 30_000;

/** How long the thread overrun holds its isolate: past its deadline, `SLEEPER_TIMEOUT_MS`. */
export const THREAD_OVERRUN_HOLD_MS = 2_500;

/** Writes the marker `<token>-started`, then holds its seat isolate for `holdMs`. */
export const endedSpansThreadHold = defineTask({
  name: "e2e_worker_ended_spans_thread_hold",
  input: { token: h.string(), holdMs: h.int() },
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_HOLD_TIMEOUT_MS,
});

/** Holds its seat isolate past its deadline, then returns: the run ends `TASK_TIMEOUT` at that return. */
export const endedSpansThreadOverrun = defineTask({
  name: "e2e_worker_ended_spans_thread_overrun",
  input: {},
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: SLEEPER_TIMEOUT_MS,
});

/** One node of the isolated sleeper. */
export const endedSpansFlow = defineWorkflow(
  { name: "e2e_worker_ended_spans_flow", definitionKey: "tests.e2e.worker_ended_spans.flow.v1", params: {}, output: h.string() },
  (w) => {
    const sleeper = w.task(endedSpansSleeper, {}, { id: "sleeper" });

    return { output: sleeper };
  },
);

/** Every task contract of this module. */
export const ENDED_SPANS_CONTRACTS = [
  endedSpansSleeper,
  endedSpansSharedSleeper,
  endedSpansThreadHold,
  endedSpansThreadOverrun,
] as const;

/** The implementations of this module on `app`. */
export function implementEndedSpans(app: Horsies): void {
  app.implement(endedSpansSleeper, (_input, { ok }) => {
    holdThread(SLEEP_MS);

    return ok("woke");
  });
  app.implement(endedSpansSharedSleeper, async (_input, { ok, signal }) => {
    await sleepFor(SLEEP_MS, signal);

    return ok("woke");
  });
  app.implement(endedSpansThreadHold, ({ token, holdMs }, { ok }) => {
    const created = createOnce(`${token}-started`, "started");

    if (!created.ok) {
      throw new Error(`the started marker: ${markerErrorText(created.error)}`);
    }

    holdThread(holdMs);

    return ok("held");
  });
  app.implement(endedSpansThreadOverrun, (_input, { ok }) => {
    holdThread(THREAD_OVERRUN_HOLD_MS);

    return ok("late");
  });
}
