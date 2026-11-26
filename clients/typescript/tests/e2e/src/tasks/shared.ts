// `SHARED_ASYNC` tasks (Rust `clients/rust/tests/worker/src/tasks/classes.rs`, Python
// `clients/python/tests/e2e/tasks/shared_async_tasks.py`), and the cases of the TypeScript child: a body that passes
// the signal or ignores it, a blocked event loop before and after an `await`, a throw in a callback that a body
// scheduled, and a Promise that no body awaits.

import { defineTask, h, type Horsies } from "../../../../horsies/src/index.ts";
import { appendLine, createOnce, markerErrorText, unixMillis } from "../markers.ts";
import { simple } from "./basic.ts";
import { holdThread, PAST_THE_DEADLINE_MS, sleepFor } from "./waits.ts";

/** The deadline of the tasks that run past it: short, to keep the tests short (Python `STOP_TIMEOUT_MS`). */
export const STOP_TIMEOUT_MS = 2_000;

/** How long `e2e_shared_blocks_once` holds the event loop: far past the 5 s block limit of a shared async child. */
export const BLOCK_DURATION_MS = 30_000;

/** How long the bodies that leave a callback or a Promise behind run after they left it. */
const LEFT_BEHIND_WAIT_MS = 300;

/** When the left callback or Promise throws, after the body started. */
const LEFT_BEHIND_THROW_MS = 50;

const SHARED_CODES = ["INTERRUPTED", "MARKER_FAILED", "SEND_FAILED", "WORK_FAILED"] as const;

/** `<pid>:<start ms>:<end ms>` of a body that ran from `start` to now. */
function stampSince(start: number): string {
  return `${String(process.pid)}:${String(start)}:${String(unixMillis())}`;
}

/** The readiness task of a worker whose only children are shared async. */
export const sharedHealthcheck = defineTask({
  name: "e2e_shared_healthcheck",
  input: {},
  output: h.string(),
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
});

/** Waits `milliseconds` on the event loop of its child; reports `<pid>:<start ms>:<end ms>`. */
export const sharedSleep = defineTask({
  name: "e2e_shared_sleep",
  input: { milliseconds: h.int() },
  output: h.string(),
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
});

/** Waits far past its 2 s deadline and does not pass the signal: its run keeps its seat until its child is killed. */
export const sharedHang = defineTask({
  name: "e2e_shared_hang",
  input: {},
  output: h.string(),
  execution: "SHARED_ASYNC",
  timeoutMs: STOP_TIMEOUT_MS,
  cancelGraceMs: 500,
});

/** Passes the signal into its wait: it returns at the deadline, within its grace. Writes the cause to `<token>-cause`. */
export const sharedCooperative = defineTask({
  name: "e2e_shared_cooperative",
  input: { token: h.string() },
  output: h.string(),
  errorCodes: SHARED_CODES,
  execution: "SHARED_ASYNC",
  timeoutMs: STOP_TIMEOUT_MS,
  cancelGraceMs: 2_000,
});

/** The first attempt of `token` holds the event loop before its first `await`; the retry returns its process id. */
export const sharedBlocksOnce = defineTask({
  name: "e2e_shared_blocks_once",
  input: { token: h.string() },
  output: h.int(),
  errorCodes: SHARED_CODES,
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["TASK_BLOCKED_LOOP"] },
});

/**
 * The first attempt of `token` holds the event loop after an `await`. On Node the child names the run (promise hooks);
 * on Bun no method names it, so each run of the child takes the crash path. The policy retries both ends.
 */
export const sharedBlocksAfterAwait = defineTask({
  name: "e2e_shared_blocks_after_await",
  input: { token: h.string() },
  output: h.int(),
  errorCodes: SHARED_CODES,
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["TASK_BLOCKED_LOOP", "WORKER_CRASHED"] },
});

/** Waits `milliseconds` on the event loop, with one retry on `WORKER_CRASHED`: the neighbour of a block on Bun. */
export const sharedSleepRetried = defineTask({
  name: "e2e_shared_sleep_retried",
  input: { milliseconds: h.int() },
  output: h.string(),
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["WORKER_CRASHED"] },
});

/** The first attempt of `token` ignores its deadline and every cancel; the retry returns its process id. */
export const sharedStuckOnce = defineTask({
  name: "e2e_shared_stuck_once",
  input: { token: h.string() },
  output: h.int(),
  errorCodes: SHARED_CODES,
  execution: "SHARED_ASYNC",
  timeoutMs: STOP_TIMEOUT_MS,
  cancelGraceMs: 100,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["TASK_TIMEOUT"] },
});

/** Sends `e2e_simple` from a shared async body; the output is the sent task id. */
export const sharedSends = defineTask({
  name: "e2e_shared_sends",
  input: { x: h.int() },
  output: h.string(),
  errorCodes: SHARED_CODES,
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
});

/** Schedules a callback that throws, then returns its process id: the child lives. */
export const sharedDetachedThrow = defineTask({
  name: "e2e_shared_detached_throw",
  input: {},
  output: h.int(),
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
});

/** Leaves a Promise that rejects and that it does not await, then returns its process id: the child lives. */
export const sharedUntrackedRejection = defineTask({
  name: "e2e_shared_untracked_rejection",
  input: {},
  output: h.int(),
  execution: "SHARED_ASYNC",
  timeoutMs: 30_000,
});

/**
 * Spawns work that waits for its own signal and writes the reason's cause to `<token>-work`; the body awaits the
 * work. At the deadline the work's signal fires with `DEADLINE`, so the run ends within its grace.
 */
export const sharedSpawnAtDeadline = defineTask({
  name: "e2e_shared_spawn_at_deadline",
  input: { token: h.string() },
  output: h.string(),
  errorCodes: SHARED_CODES,
  execution: "SHARED_ASYNC",
  timeoutMs: STOP_TIMEOUT_MS,
  cancelGraceMs: 2_000,
});

/** Every contract of this module. */
export const SHARED_CONTRACTS = [
  sharedHealthcheck,
  sharedSleep,
  sharedHang,
  sharedCooperative,
  sharedBlocksOnce,
  sharedBlocksAfterAwait,
  sharedSleepRetried,
  sharedStuckOnce,
  sharedSends,
  sharedDetachedThrow,
  sharedUntrackedRejection,
  sharedSpawnAtDeadline,
] as const;

/** The implementations of this module on `app`. */
export function implementShared(app: Horsies): void {
  app.implement(sharedHealthcheck, async (_input, { ok }) => ok("ready"));
  app.implement(sharedSleep, async ({ milliseconds }, { ok }) => {
    const start = unixMillis();

    await sleepFor(milliseconds, "noSignal");

    return ok(stampSince(start));
  });
  app.implement(sharedSleepRetried, async ({ milliseconds }, { ok }) => {
    const start = unixMillis();

    await sleepFor(milliseconds, "noSignal");

    return ok(stampSince(start));
  });
  app.implement(sharedHang, async (_input, { ok }) => {
    await sleepFor(PAST_THE_DEADLINE_MS, "noSignal");

    return ok("never");
  });
  app.implement(sharedCooperative, async ({ token }, { ok, err, signal }) => {
    const end = await sleepFor(PAST_THE_DEADLINE_MS, signal);

    if (end.kind === "slept") {
      return ok("never");
    }

    const written = appendLine(`${token}-cause`, end.reason);

    return written.ok
      ? err({ code: "INTERRUPTED", message: `cancelled: ${end.reason}` })
      : err({ code: "MARKER_FAILED", message: markerErrorText(written.error) });
  });
  implementBlocks(app);
  implementLeftBehind(app);
}

function implementBlocks(app: Horsies): void {
  app.implement(sharedBlocksOnce, async ({ token }, { ok, err }) => {
    const first = createOnce(`${token}-blocked`, String(process.pid));

    if (!first.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(first.error) });
    }

    if (first.value === "created") {
      holdThread(BLOCK_DURATION_MS);
    }

    return ok(process.pid);
  });
  app.implement(sharedBlocksAfterAwait, async ({ token }, { ok, err }) => {
    await sleepFor(LEFT_BEHIND_THROW_MS, "noSignal");

    const first = createOnce(`${token}-blocked`, String(process.pid));

    if (!first.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(first.error) });
    }

    if (first.value === "created") {
      holdThread(BLOCK_DURATION_MS);
    }

    return ok(process.pid);
  });
  app.implement(sharedStuckOnce, async ({ token }, { ok, err }) => {
    const first = createOnce(`${token}-stuck`, String(process.pid));

    if (!first.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(first.error) });
    }

    if (first.value === "created") {
      await sleepFor(PAST_THE_DEADLINE_MS, "noSignal");
    }

    return ok(process.pid);
  });
}

function implementLeftBehind(app: Horsies): void {
  app.implement(sharedSends, async ({ x }, { ok, err }) => {
    const sent = await app.send(simple, { x });

    return sent.ok
      ? ok(sent.value.taskId)
      : err({ code: "SEND_FAILED", message: `${sent.error.code}: ${sent.error.message}` });
  });
  app.implement(sharedDetachedThrow, async (_input, { ok }) => {
    setTimeout(() => {
      throw new Error("e2e detached throw");
    }, LEFT_BEHIND_THROW_MS);
    await sleepFor(LEFT_BEHIND_WAIT_MS, "noSignal");

    return ok(process.pid);
  });
  app.implement(sharedUntrackedRejection, async (_input, { ok }) => {
    const rejecting = async (): Promise<never> => {
      await sleepFor(LEFT_BEHIND_THROW_MS, "noSignal");

      throw new Error("e2e untracked rejection");
    };

    // The body does not await this Promise: no run tracks it, and its rejection reaches `unhandledRejection`.
    void rejecting();
    await sleepFor(LEFT_BEHIND_WAIT_MS, "noSignal");

    return ok(process.pid);
  });
  app.implement(sharedSpawnAtDeadline, async ({ token }, { ok, err, spawn }) => {
    const work = spawn("wait_for_signal", async (signal) => {
      const end = await sleepFor(PAST_THE_DEADLINE_MS, signal);
      const cause = end.kind === "aborted" ? end.reason : "slept";
      const written = appendLine(`${token}-work`, cause);

      return written.ok ? cause : markerErrorText(written.error);
    });

    if (!work.ok) {
      return err({ code: "WORK_FAILED", message: `spawn refused: ${work.error.kind}` });
    }

    return ok(await work.value);
  });
}
