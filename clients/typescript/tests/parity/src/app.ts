// The TypeScript app of the parity runs (`crossclient/parity/runner.py`).
//
// The app has the task names, input field names, codes, messages, options, profiles and workflows of the Python parity
// app (`crossclient/parity/app.py`) and of the Rust one (`clients/rust/tests/parity/src/lib.rs`), so the same scenario
// stores the same rows on each client. The runner sets `HORSIES_PARITY_DATABASE_URL` for each process of a side: the
// worker, its children, the scheduler and the send process.
//
// Profiles, one child each, so the tasks of a scenario start in send order:
// - `isolated-1`: one isolated child.
// - `shared-async-1x4`: one shared async child with 4 seats.
// - `shared-thread-1x4`: one shared thread child with 4 seats (Node only: a worker on Bun refuses it at its start).
//
// The schedule `parity_slots` sends `parity.add` (2 + 3) every hour and catches up missed runs, at most two slots per
// check. Only the scheduler process of the `schedule_slots` scenario runs it.
//
// Every send retains its input (`retainRerunInputDefault`), so a failed task can be rerun. Each task ends the same way
// on every run: no task reads the clock, a random source, or the state of another task.

import {
  defineTask,
  defineWorkflow,
  h,
  Horsies,
  type HorsiesConfig,
  type ScheduleEntry,
  type ScheduleSection,
  type TaskOutcome,
  type WorkerProfileInput,
} from "../../../horsies/src/index.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";

/** The database of this side of the run. */
export const DATABASE_URL_ENV = "HORSIES_PARITY_DATABASE_URL";

export const ISOLATED = "isolated-1";
export const SHARED_ASYNC = "shared-async-1x4";
export const SHARED_THREAD = "shared-thread-1x4";

/** A profile of the parity app. */
export type ProfileName = typeof ISOLATED | typeof SHARED_ASYNC | typeof SHARED_THREAD;

/** The deadline of the tasks that run past it. */
const SHORT_TIMEOUT_MS = 1_000;

/** How long a task that must time out waits: far past `SHORT_TIMEOUT_MS`. */
export const PAST_THE_DEADLINE_MS = 30_000;

/** The deadline of a shared task that ends in time. */
const SHARED_TIMEOUT_MS = 30_000;

/** How long a stopping worker waits for its tasks. */
const DRAIN_TIMEOUT_MS = 5_000;

/** The broker pool of the app (Python `pool_size=5, max_overflow=5`). */
const POOL_SIZE = 5;
const POOL_OVERFLOW = 5;

/** The seats of each shared child. */
const SHARED_SEATS = 4;

/** The output of a task that returns after its deadline: the worker stores the timeout, not this value. */
const LATE = "late";

export const CHAIN_NAME = "parity_chain";
export const CHAIN_DEFINITION_KEY = "crossclient.parity.chain.v1";
export const CHILD_NAME = "parity_child";
export const CHILD_DEFINITION_KEY = "crossclient.parity.child.v1";
export const PARENT_NAME = "parity_parent";
export const PARENT_DEFINITION_KEY = "crossclient.parity.parent.v1";

/** A workflow that the send process starts. */
export type WorkflowName = typeof CHAIN_NAME | typeof PARENT_NAME;

/** The schedule of the `schedule_slots` scenario. */
export const SCHEDULE_NAME = "parity_slots";

/** Longer than any run: a slot is due only after the runner moves the schedule back. */
const SCHEDULE_INTERVAL = Temporal.Duration.from({ hours: 1 });

/** The most slots that one check of the scheduler sends. */
const SCHEDULE_MAX_CATCH_UP_RUNS = 2;

/** The wait between two checks of the scheduler, in seconds. */
const SCHEDULE_CHECK_INTERVAL_S = 1;

/** The broker URL of the second app of `send_retry`: nothing listens on port 1, so a connect is refused at once. */
export const REFUSED_BROKER_URL = "postgresql://postgres@127.0.0.1:1/parity_refused";

/**
 * The pool checkout wait of the app on `REFUSED_BROKER_URL`, in seconds. The pool tries a refused connect again until
 * this wait ends, so it bounds the send that must fail. That app writes nothing, so the setting leaves no stored row.
 */
export const REFUSED_BROKER_CHECKOUT_WAIT_S = 1;

// ---- Isolated ----

/** A plain success. */
export const add = defineTask({ name: "parity.add", input: { a: h.int(), b: h.int() }, output: h.int() });

/** A task error with no retry. */
export const refuse = defineTask({
  name: "parity.refuse",
  input: { value: h.int() },
  output: h.int(),
  errorCodes: ["REFUSED"],
});

/**
 * A task error that the policy retries until the retries run out: two retries one second apart for the code
 * `TRANSIENT`, with jitter (left out: on), as Python's `RetryPolicy.fixed` gives by default.
 */
export const transient = defineTask({
  name: "parity.transient",
  input: { value: h.int() },
  output: h.int(),
  errorCodes: ["TRANSIENT"],
  retryPolicy: { kind: "fixed", intervalsSeconds: [1, 1], autoRetryFor: ["TRANSIENT"] },
});

/** An isolated task that the parent kills at its deadline. */
export const sleepPastDeadline = defineTask({
  name: "parity.sleep_past_deadline",
  input: {},
  output: h.string(),
  timeoutMs: SHORT_TIMEOUT_MS,
});

/** Twice the value of its upstream node. */
export const double = defineTask({
  name: "parity.double",
  input: { upstream: h.taskOutcome(add) },
  output: h.int(),
  errorCodes: ["UPSTREAM_FAILED"],
});

// ---- Shared async ----

export const addAsync = defineTask({
  name: "parity.add_async",
  input: { a: h.int(), b: h.int() },
  output: h.int(),
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TIMEOUT_MS,
});

export const refuseAsync = defineTask({
  name: "parity.refuse_async",
  input: { value: h.int() },
  output: h.int(),
  errorCodes: ["REFUSED"],
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TIMEOUT_MS,
});

export const transientAsync = defineTask({
  name: "parity.transient_async",
  input: { value: h.int() },
  output: h.int(),
  errorCodes: ["TRANSIENT"],
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TIMEOUT_MS,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1, 1], autoRetryFor: ["TRANSIENT"] },
});

/**
 * Ends at the cancel signal of its deadline, within the grace: `cancel_end` is `RETURNED_AFTER_SIGNAL`, as the Python
 * task whose sleep the cancel ends and the Rust task that waits on `rt.cancelled()`.
 */
export const asyncPastDeadline = defineTask({
  name: "parity.async_past_deadline",
  input: {},
  output: h.string(),
  execution: "SHARED_ASYNC",
  timeoutMs: SHORT_TIMEOUT_MS,
});

// ---- Shared thread ----

export const addThread = defineTask({
  name: "parity.add_thread",
  input: { a: h.int(), b: h.int() },
  output: h.int(),
  execution: "SHARED_THREAD",
  timeoutMs: SHARED_TIMEOUT_MS,
});

export const refuseThread = defineTask({
  name: "parity.refuse_thread",
  input: { value: h.int() },
  output: h.int(),
  errorCodes: ["REFUSED"],
  execution: "SHARED_THREAD",
  timeoutMs: SHARED_TIMEOUT_MS,
});

export const transientThread = defineTask({
  name: "parity.transient_thread",
  input: { value: h.int() },
  output: h.int(),
  errorCodes: ["TRANSIENT"],
  execution: "SHARED_THREAD",
  timeoutMs: SHARED_TIMEOUT_MS,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1, 1], autoRetryFor: ["TRANSIENT"] },
});

/** A thread seat that cannot stop at its deadline: the child is killed when no run of it has time left. */
export const threadPastDeadline = defineTask({
  name: "parity.thread_past_deadline",
  input: {},
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: SHORT_TIMEOUT_MS,
});

// ---- Workflows ----

/** Two nodes: the second takes the result of the first (Python `parity_chain`). */
export const parityChain = defineWorkflow(
  { name: CHAIN_NAME, definitionKey: CHAIN_DEFINITION_KEY, params: {}, output: h.int(), onError: "fail" },
  (w) => {
    const added = w.task(add, { a: 2, b: 3 }, { nodeId: "add" });

    return { output: w.task(double, { upstream: added }, { nodeId: "double" }) };
  },
);

/** The child of `parity_parent`: one node, its output (Python `ParityChild`). */
export const parityChild = defineWorkflow(
  { name: CHILD_NAME, definitionKey: CHILD_DEFINITION_KEY, params: {}, output: h.int(), onError: "fail" },
  (w) => ({ output: w.task(add, { a: 2, b: 3 }, { nodeId: "add" }) }),
);

/** A sub-workflow node of `parity_child`, then a node that takes the child's output (Python `parity_parent`). */
export const parityParent = defineWorkflow(
  { name: PARENT_NAME, definitionKey: PARENT_DEFINITION_KEY, params: {}, output: h.int(), onError: "fail" },
  (w) => {
    const child = w.subWorkflow(parityChild, {}, { nodeId: "child" });

    return { output: w.task(double, { upstream: child }, { nodeId: "double" }) };
  },
);

// ---- The app ----

/** Every task contract of the app. */
export const TASKS = [
  add,
  refuse,
  transient,
  sleepPastDeadline,
  double,
  addAsync,
  refuseAsync,
  transientAsync,
  asyncPastDeadline,
  addThread,
  refuseThread,
  transientThread,
  threadPastDeadline,
] as const;

/** Every workflow contract of the app. */
export const WORKFLOWS = [parityChain, parityChild, parityParent] as const;

/** The worker profiles. A class that a profile leaves out has no children. */
export const PROFILES: { readonly [Name in ProfileName]: WorkerProfileInput } = {
  [ISOLATED]: { isolated: { kind: "isolatedChildren", count: 1 }, drainTimeoutMs: DRAIN_TIMEOUT_MS },
  [SHARED_ASYNC]: { sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: SHARED_SEATS }, drainTimeoutMs: DRAIN_TIMEOUT_MS },
  [SHARED_THREAD]: { sharedThread: { kind: "sharedThreadChildren", count: 1, seats: SHARED_SEATS }, drainTimeoutMs: DRAIN_TIMEOUT_MS },
};

/** The schedule `parity_slots`: each slot sends `parity.add` with 2 and 3 (Python `TaskSchedule` of `app.py`). */
export const PARITY_SCHEDULE: ScheduleEntry<typeof add> = {
  name: SCHEDULE_NAME,
  task: add,
  input: { a: 2, b: 3 },
  pattern: { kind: "interval", every: SCHEDULE_INTERVAL },
  timezone: "UTC",
  catchUpMissed: true,
  maxCatchUpRuns: SCHEDULE_MAX_CATCH_UP_RUNS,
};

/** The schedule section of the parity app (Python `ScheduleConfig(check_interval_seconds=1)`). */
export const SCHEDULE_SECTION: ScheduleSection = { checkIntervalSeconds: SCHEDULE_CHECK_INTERVAL_S };

/** The config of the parity app on `databaseUrl`. Pure. */
export function parityConfig(databaseUrl: string): HorsiesConfig {
  return {
    broker: {
      databaseUrl: { kind: "value", value: databaseUrl },
      poolSize: POOL_SIZE,
      maxOverflow: POOL_OVERFLOW,
      retainRerunInputDefault: "keep",
    },
    workerProfiles: PROFILES,
    tasks: TASKS,
    workflows: WORKFLOWS,
  };
}

/**
 * The config of the second app of `send_retry`: the contracts of the parity app on `REFUSED_BROKER_URL`, with the
 * checkout wait `REFUSED_BROKER_CHECKOUT_WAIT_S`. Pure.
 */
export function refusedBrokerConfig(): HorsiesConfig {
  const config = parityConfig(REFUSED_BROKER_URL);

  return { ...config, broker: { ...config.broker, poolTimeoutSeconds: REFUSED_BROKER_CHECKOUT_WAIT_S } };
}

/**
 * The parity app on `databaseUrl` with the implementation of every task and the schedule, or the text of every refusal
 * of `Horsies.create`. On a main thread the core checks the config, each contract and the schedule (the addon loads;
 * nothing connects). A refusal of the schedule reaches `check` and the scheduler start, which report it.
 */
export function buildApp(databaseUrl: string): Result<Horsies, string> {
  const created = Horsies.create(parityConfig(databaseUrl));

  if (!created.ok) {
    return err(created.error.refusals.map((refused) => `${refused.code} ${refused.path}: ${refused.message}`).join("; "));
  }

  implementTasks(created.value);
  created.value.schedules([PARITY_SCHEDULE], SCHEDULE_SECTION);

  return ok(created.value);
}

/** The bodies of the tasks: each one by its contract. Calls no addon function. */
function implementTasks(app: Horsies): void {
  app.implement(add, ({ a, b }, { ok }) => ok(a + b));
  app.implement(refuse, ({ value }, { err }) => err({ code: "REFUSED", message: refusedMessage(value) }));
  app.implement(transient, ({ value }, { err }) => err({ code: "TRANSIENT", message: transientMessage(value) }));
  app.implement(sleepPastDeadline, (_input, { ok }) => {
    holdThread(PAST_THE_DEADLINE_MS);

    return ok(LATE);
  });
  app.implement(double, ({ upstream }, { ok, err }) => {
    const doubled = doubledValue(upstream);

    return doubled.ok ? ok(doubled.value) : err({ code: "UPSTREAM_FAILED", message: doubled.error });
  });

  app.implement(addAsync, async ({ a, b }, { ok }) => ok(a + b));
  app.implement(refuseAsync, async ({ value }, { err }) => err({ code: "REFUSED", message: refusedMessage(value) }));
  app.implement(transientAsync, async ({ value }, { err }) =>
    err({ code: "TRANSIENT", message: transientMessage(value) }),
  );
  app.implement(asyncPastDeadline, async (_input, { ok, signal }) => {
    await waitOrSignal(PAST_THE_DEADLINE_MS, signal);

    return ok(LATE);
  });

  app.implement(addThread, ({ a, b }, { ok }) => ok(a + b));
  app.implement(refuseThread, ({ value }, { err }) => err({ code: "REFUSED", message: refusedMessage(value) }));
  app.implement(transientThread, ({ value }, { err }) =>
    err({ code: "TRANSIENT", message: transientMessage(value) }),
  );
  app.implement(threadPastDeadline, (_input, { ok }) => {
    holdThread(PAST_THE_DEADLINE_MS);

    return ok(LATE);
  });
}

/** The message of the `REFUSED` error of `value` (Python `f'refused {value}'`). Pure. */
export function refusedMessage(value: number): string {
  return `refused ${String(value)}`;
}

/** The message of the `TRANSIENT` error of `value` (Python `f'transient {value}'`). Pure. */
export function transientMessage(value: number): string {
  return `transient ${String(value)}`;
}

/** The message of a `double` node whose upstream failed. */
export const UPSTREAM_FAILED_MESSAGE = "the upstream failed";

/** Twice the value of a succeeded upstream, or the message of a failed one. Pure. */
export function doubledValue(upstream: TaskOutcome<number, string>): Result<number, string> {
  switch (upstream.kind) {
    case "succeeded":
      return ok(upstream.value * 2);
    case "failed":
      return err(UPSTREAM_FAILED_MESSAGE);
    default: {
      const unexpected: never = upstream;

      throw new TypeError(`doubledValue: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

// ---- The waits of the bodies ----

/** The cell that `holdThread` waits on: no code writes it, so each wait ends at its time limit. */
const IDLE_CELL = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));

/** Hold the thread for `milliseconds`, as a Python `time.sleep`: no callback, timer or Promise of the thread runs. */
export function holdThread(milliseconds: number): void {
  // The only outcome of a wait on a cell that no code writes is `timed-out`.
  const outcome = Atomics.wait(IDLE_CELL, 0, 0, milliseconds);

  if (outcome !== "timed-out") {
    throw new TypeError(`holdThread: a wait on the idle cell ended with ${outcome}`);
  }
}

/** How a `waitOrSignal` ended: its time passed, or the signal fired first. */
export type WaitEnd = "timePassed" | "signalled";

/** Wait `milliseconds` on the event loop, or until `signal` fires. A signal that fired already ends the wait at once. */
export async function waitOrSignal(milliseconds: number, signal: AbortSignal): Promise<WaitEnd> {
  if (signal.aborted) {
    return "signalled";
  }

  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve("timePassed");
    }, milliseconds);

    function onAbort(): void {
      clearTimeout(timer);
      resolve("signalled");
    }

    signal.addEventListener("abort", onAbort, { once: true });
  });
}
