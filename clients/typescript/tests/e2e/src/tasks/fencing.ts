// Tasks that return the fencing token of their run (Rust `clients/rust/tests/worker/src/tasks/fencing.rs`, Python
// `clients/python/tests/e2e/test_fencing_token.py`): one per execution class that the TypeScript worker runs, one that
// reads it twice and in work that it started, and one that a retry runs again.

import { defineTask, h, type Horsies, type TaskRuntime } from "#horsies";
import { appendLine, markerErrorText, nextAttempt } from "../markers.ts";

/** A fencing token that a body read: the task id and the value. */
const fence = h.object({ taskId: h.string(), value: h.int() });

/** A fencing token as a test reads it. */
export type Fence = { readonly taskId: string; readonly value: number };

const FENCE_CODES = ["WORK_FAILED", "TRANSIENT", "MARKER_FAILED"] as const;

/** The token of an isolated async run. */
export const fenceIsolatedAsync = defineTask({ name: "e2e_fence_isolated_async", input: {}, output: fence });

/** The token of an isolated run whose body holds its thread (a sync body). */
export const fenceIsolatedSync = defineTask({ name: "e2e_fence_isolated_sync", input: {}, output: fence });

/** The token of a shared async run. */
export const fenceSharedAsync = defineTask({
  name: "e2e_fence_shared_async",
  input: {},
  output: fence,
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
});

/** The token of a shared thread run, read in its seat isolate. */
export const fenceSharedThread = defineTask({
  name: "e2e_fence_shared_thread",
  input: {},
  output: fence,
  execution: "SHARED_THREAD",
  timeoutMs: 30_000,
});

/** Two reads of the token in the body, then a read in work that the body started with `runtime.spawn`. */
export const fenceReadsTwice = defineTask({ name: "e2e_fence_reads_twice", input: {}, output: h.array(fence), errorCodes: FENCE_CODES });

/** The first run for `marker` fails with `TRANSIENT`; its retry returns its token and the counted run. */
export const fenceRetry = defineTask({
  name: "e2e_fence_retry",
  input: { marker: h.string() },
  output: h.object({ fence, countedRun: h.int() }),
  errorCodes: FENCE_CODES,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["TRANSIENT"] },
});

/** The suffix of the marker that each run of `retryFacts` appends a line to: `<attempt> <maxRetries> <retriesLeft>`. */
export const RETRY_FACTS_MARKER_SUFFIX = "-retry-facts";

/** The retry facts of a run: the first run for `marker` fails with `TRANSIENT`; both runs write their facts to a marker. */
export const retryFacts = defineTask({
  name: "e2e_retry_facts",
  input: { marker: h.string() },
  output: h.object({ attempt: h.int(), maxRetries: h.int(), retriesLeft: h.int() }),
  errorCodes: FENCE_CODES,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["TRANSIENT"] },
});

/** Every contract of this module. */
export const FENCING_CONTRACTS = [
  fenceIsolatedAsync,
  fenceIsolatedSync,
  fenceSharedAsync,
  fenceSharedThread,
  fenceReadsTwice,
  fenceRetry,
  retryFacts,
] as const;

function fenceOf(runtime: Pick<TaskRuntime, "fencingToken">): Fence {
  return { taskId: runtime.fencingToken.taskId, value: runtime.fencingToken.value };
}

/** The implementations of this module on `app`. */
export function implementFencing(app: Horsies): void {
  app.implement(fenceIsolatedAsync, async (_input, runtime) => runtime.ok(fenceOf(runtime)));
  app.implement(fenceIsolatedSync, (_input, runtime) => runtime.ok(fenceOf(runtime)));
  app.implement(fenceSharedAsync, async (_input, runtime) => runtime.ok(fenceOf(runtime)));
  app.implement(fenceSharedThread, (_input, runtime) => runtime.ok(fenceOf(runtime)));
  app.implement(fenceReadsTwice, async (_input, runtime) => {
    const first = fenceOf(runtime);
    const second = fenceOf(runtime);
    const work = runtime.spawn("read_fence", async () => fenceOf(runtime));

    if (!work.ok) {
      return runtime.err({ code: "WORK_FAILED", message: `spawn refused: ${work.error.kind}` });
    }

    return runtime.ok([first, second, await work.value]);
  });
  app.implement(fenceRetry, ({ marker }, runtime) => {
    const read = fenceOf(runtime);
    const counted = nextAttempt(marker);

    if (!counted.ok) {
      return runtime.err({ code: "MARKER_FAILED", message: markerErrorText(counted.error) });
    }

    return counted.value === 1
      ? runtime.err({ code: "TRANSIENT", message: `run 1 read the fence value ${String(read.value)}` })
      : runtime.ok({ fence: read, countedRun: counted.value });
  });
  app.implement(retryFacts, ({ marker }, runtime) => {
    const facts = { attempt: runtime.attempt, maxRetries: runtime.maxRetries, retriesLeft: runtime.retriesLeft };
    const written = appendLine(`${marker}${RETRY_FACTS_MARKER_SUFFIX}`, `${String(facts.attempt)} ${String(facts.maxRetries)} ${String(facts.retriesLeft)}`);

    if (!written.ok) {
      return runtime.err({ code: "MARKER_FAILED", message: markerErrorText(written.error) });
    }

    return facts.attempt === 1 ? runtime.err({ code: "TRANSIENT", message: "run 1 fails" }) : runtime.ok(facts);
  });
}
