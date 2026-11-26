// The tasks and the workflow of the `worker_ended_spans` instance (Python
// `clients/python/tests/e2e/tasks/instance_worker_ended_spans.py`): an isolated sleeper that the worker kills at its
// deadline, a one-node workflow of that sleeper, and a shared async sleeper that its child ends at its deadline.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../../horsies/src/index.ts";
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

/** One node of the isolated sleeper. */
export const endedSpansFlow = defineWorkflow(
  { name: "e2e_worker_ended_spans_flow", definitionKey: "tests.e2e.worker_ended_spans.flow.v1", params: {}, output: h.string() },
  (w) => {
    const sleeper = w.task(endedSpansSleeper, {}, { id: "sleeper" });

    return { output: sleeper };
  },
);

/** Every task contract of this module. */
export const ENDED_SPANS_CONTRACTS = [endedSpansSleeper, endedSpansSharedSleeper] as const;

/** The implementations of this module on `app`. */
export function implementEndedSpans(app: Horsies): void {
  app.implement(endedSpansSleeper, () => {
    holdThread(SLEEP_MS);

    return endedSpansSleeper.ok("woke");
  });
  app.implement(endedSpansSharedSleeper, async (_input, runtime) => {
    await sleepFor(SLEEP_MS, runtime.signal);

    return endedSpansSharedSleeper.ok("woke");
  });
}
