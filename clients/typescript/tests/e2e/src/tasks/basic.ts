// Task fundamentals (Rust `clients/rust/tests/worker/src/tasks/basic.rs`, Python `clients/python/tests/e2e/tasks/basic.py`).
// Each task is `ISOLATED`, the default class, on the default queue. A body that holds its thread mirrors a Rust
// blocking body; an async body mirrors a Rust async body.

import { defineTask, h, type Horsies } from "../../../../horsies/src/index.ts";
import { createOnce, markerErrorText, unixMillis } from "../markers.ts";
import { holdThread, runtimeName, sleepFor } from "./waits.ts";

/** The longest result wait that `e2e_wait_inside_task` tries. */
const INSIDE_WAIT_MS = 1_000;

/** The deadline of the timeout sleepers. */
export const SLEEPER_TIMEOUT_MS = 2_000;

/** The readiness task of a worker with an isolated child. */
export const healthcheck = defineTask({ name: "e2e_healthcheck", input: {}, output: h.string() });

export const simple = defineTask({ name: "e2e_simple", input: { x: h.int() }, output: h.int() });

/** Python `e2e_kwargs`: members that a sender may leave out; the body applies Python's defaults. */
export const kwargs = defineTask({
  name: "e2e_kwargs",
  input: { required: h.int(), optional: h.string().optional(), multiplier: h.int().optional() },
  output: h.string(),
});

/** A returned error with a code, a message and data. */
export const deliberateError = defineTask({ name: "e2e_error", input: {}, output: h.int(), errors: ["DELIBERATE_ERROR"] });

/** A code that no retry policy names. */
export const noRetry = defineTask({ name: "e2e_no_retry", input: {}, output: h.string(), errors: ["PERMANENT"] });

/**
 * A body that returns a built-in code. The static types refuse the literal, so the code is a `string`; `err` throws
 * `TypeError` at run time, and the worker stores the throw as `UNHANDLED_EXCEPTION`.
 */
export const builtInCode = defineTask({ name: "e2e_builtin_code", input: {}, output: h.string() });

export const complexResult = defineTask({
  name: "e2e_complex_result",
  input: {},
  output: h.object({ value: h.int(), nested: h.record(h.array(h.int())) }),
});

/** Holds its thread for `durationMs`. */
export const slow = defineTask({ name: "e2e_slow", input: { durationMs: h.int() }, output: h.string() });

/** Holds its thread past its 2 s deadline: the worker kills its child at the deadline. */
export const timeoutSleeper = defineTask({
  name: "e2e_timeout_sleeper",
  input: { durationMs: h.int() },
  output: h.string(),
  timeoutMs: SLEEPER_TIMEOUT_MS,
});

/** Waits on the event loop past its 2 s deadline. */
export const asyncTimeoutSleeper = defineTask({
  name: "e2e_async_timeout_sleeper",
  input: { durationMs: h.int() },
  output: h.string(),
  timeoutMs: SLEEPER_TIMEOUT_MS,
});

/** A 1 s deadline with one retry on `TASK_TIMEOUT`. */
export const timeoutRetrySleeper = defineTask({
  name: "e2e_timeout_retry_sleeper",
  input: { durationMs: h.int() },
  output: h.string(),
  timeoutMs: 1_000,
  retry: { kind: "fixed", intervalsSeconds: [1], jitter: false, autoRetryFor: ["TASK_TIMEOUT"] },
});

/** The process id of the child that runs it, after `sleepMs`. */
export const pid = defineTask({ name: "e2e_pid", input: { sleepMs: h.int() }, output: h.int() });

/** Throws a `TypeError`: the worker stores `UNHANDLED_EXCEPTION` with its record; the child lives. */
export const throws = defineTask({ name: "e2e_throws", input: { text: h.string() }, output: h.int() });

/** Ends its own child with `process.abort()` after the row is RUNNING. */
export const abort = defineTask({ name: "e2e_abort", input: {}, output: h.string() });

/** Ends its own child with `process.exit(3)` after the row is RUNNING. */
export const exitsChild = defineTask({ name: "e2e_exit", input: {}, output: h.string() });

/** Runs once per token; a second run of a token fails with `DOUBLE_EXECUTION`. */
export const idempotent = defineTask({
  name: "e2e_idempotent",
  input: { token: h.string() },
  output: h.string(),
  errors: ["DOUBLE_EXECUTION", "MARKER_FAILED"],
});

/** Runs once per token, then waits `durationMs`. */
export const slowIdempotent = defineTask({
  name: "e2e_slow_idempotent",
  input: { token: h.string(), durationMs: h.int() },
  output: h.string(),
  errors: ["DOUBLE_EXECUTION", "MARKER_FAILED"],
});

/** Sends a task, then waits for it: whether the wait is refused with `waitInsideTask`. */
export const waitInsideTask = defineTask({ name: "e2e_wait_inside_task", input: {}, output: h.boolean(), errors: ["SEND_FAILED"] });

/** Sends `e2e_simple` from its body; the output is the sent task id. */
export const asyncSends = defineTask({ name: "e2e_async_sends", input: { x: h.int() }, output: h.string(), errors: ["SEND_FAILED"] });

/** Holds its thread for `durationMs` and reports when it ran: `<pid>:<start ms>:<end ms>`. */
export const stamp = defineTask({ name: "e2e_stamp", input: { durationMs: h.int() }, output: h.string() });

/** The runtime and the process id of the child that runs it. */
export const runtimeOfChild = defineTask({
  name: "e2e_runtime_of_child",
  input: {},
  output: h.object({ runtime: h.enum(["node", "bun"]), pid: h.int() }),
});

/** Writes its process id to the marker `<token>-pid`, then holds its thread for `durationMs`. */
export const pidThenHold = defineTask({
  name: "e2e_pid_then_hold",
  input: { token: h.string(), durationMs: h.int() },
  output: h.string(),
  errors: ["MARKER_FAILED"],
});

/** Holds its thread for `seconds`: longer than the drain timeout of every test profile. */
export const outlivesDrain = defineTask({ name: "e2e_outlives_drain", input: { seconds: h.int() }, output: h.string() });

/** Every contract of this module. */
export const BASIC_CONTRACTS = [
  healthcheck,
  simple,
  kwargs,
  deliberateError,
  noRetry,
  builtInCode,
  complexResult,
  slow,
  timeoutSleeper,
  asyncTimeoutSleeper,
  timeoutRetrySleeper,
  pid,
  throws,
  abort,
  exitsChild,
  idempotent,
  slowIdempotent,
  waitInsideTask,
  asyncSends,
  stamp,
  runtimeOfChild,
  pidThenHold,
  outlivesDrain,
] as const;

/** The implementations of this module on `app`. */
export function implementBasic(app: Horsies): void {
  app.implement(healthcheck, (_input, { ok }) => ok("ready"));
  app.implement(simple, ({ x }, { ok }) => ok(x * 2));
  app.implement(kwargs, ({ required, optional, multiplier }, { ok }) =>
    ok(`${String(required * (multiplier ?? 1))}_${optional ?? "default"}`),
  );
  app.implement(deliberateError, (_input, { err }) =>
    err({ code: "DELIBERATE_ERROR", message: "This is intentional", data: { key: "value" } }),
  );
  app.implement(noRetry, (_input, { err }) => err({ code: "PERMANENT", message: "not retryable" }));
  app.implement(builtInCode, (_input, { err }) => {
    const code: string = "TASK_EXCEPTION";

    return err({ code, message: "boom" });
  });
  app.implement(complexResult, (_input, { ok }) => ok({ value: 42, nested: { a: [1, 2, 3], b: [4, 5] } }));
  implementSleepers(app);
  implementProcessEnds(app);
  implementMarkers(app);
  implementSends(app);
}

function implementSleepers(app: Horsies): void {
  app.implement(slow, ({ durationMs }, { ok }) => {
    holdThread(durationMs);

    return ok(`slept_${String(durationMs)}`);
  });
  app.implement(timeoutSleeper, ({ durationMs }, { ok }) => {
    holdThread(durationMs);

    return ok(`slept_${String(durationMs)}`);
  });
  app.implement(asyncTimeoutSleeper, async ({ durationMs }, { ok }) => {
    await sleepFor(durationMs, "noSignal");

    return ok(`slept_${String(durationMs)}`);
  });
  app.implement(timeoutRetrySleeper, async ({ durationMs }, { ok }) => {
    await sleepFor(durationMs, "noSignal");

    return ok(`slept_${String(durationMs)}`);
  });
  app.implement(pid, async ({ sleepMs }, { ok }) => {
    await sleepFor(sleepMs, "noSignal");

    return ok(process.pid);
  });
  app.implement(stamp, ({ durationMs }, { ok }) => {
    const start = unixMillis();

    holdThread(durationMs);

    return ok(`${String(process.pid)}:${String(start)}:${String(unixMillis())}`);
  });
  app.implement(outlivesDrain, ({ seconds }, { ok }) => {
    holdThread(seconds * 1_000);

    return ok("ended");
  });
  app.implement(runtimeOfChild, (_input, { ok }) => ok({ runtime: runtimeName(), pid: process.pid }));
}

function implementProcessEnds(app: Horsies): void {
  app.implement(throws, ({ text }) => {
    throw new TypeError(`e2e boom: ${text}`);
  });
  app.implement(abort, () => {
    process.abort();
  });
  app.implement(exitsChild, () => {
    process.exit(3);
  });
}

function implementMarkers(app: Horsies): void {
  app.implement(idempotent, ({ token }, { ok, err }) => {
    const created = createOnce(`idempotent-${token}`, "executed");

    if (!created.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(created.error) });
    }

    return created.value === "created"
      ? ok(`executed:${token}`)
      : err({ code: "DOUBLE_EXECUTION", message: `Token ${token} already executed` });
  });
  app.implement(slowIdempotent, async ({ token, durationMs }, { ok, err }) => {
    const created = createOnce(`idempotent-${token}`, "executed");

    if (!created.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(created.error) });
    }

    if (created.value === "existed") {
      return err({ code: "DOUBLE_EXECUTION", message: `Token ${token} already executed` });
    }

    await sleepFor(durationMs, "noSignal");

    return ok(`executed:${token}`);
  });
  app.implement(pidThenHold, ({ token, durationMs }, { ok, err }) => {
    const created = createOnce(`${token}-pid`, String(process.pid));

    if (!created.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(created.error) });
    }

    holdThread(durationMs);

    return ok("ended");
  });
}

function implementSends(app: Horsies): void {
  app.implement(waitInsideTask, async (_input, { ok, err }) => {
    const sent = await app.send(simple, { x: 1 });

    if (!sent.ok) {
      return err({ code: "SEND_FAILED", message: `${sent.error.code}: ${sent.error.message}` });
    }

    const read = await sent.value.get({ waitMs: INSIDE_WAIT_MS });

    return ok(!read.ok && read.error.kind === "waitInsideTask");
  });
  app.implement(asyncSends, async ({ x }, { ok, err }) => {
    const sent = await app.send(simple, { x });

    return sent.ok
      ? ok(sent.value.taskId)
      : err({ code: "SEND_FAILED", message: `${sent.error.code}: ${sent.error.message}` });
  });
}
