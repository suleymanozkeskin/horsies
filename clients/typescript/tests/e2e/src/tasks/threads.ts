// `SHARED_THREAD` tasks (Rust `clients/rust/tests/worker/src/tasks/classes.rs`, Python
// `clients/python/tests/e2e/tasks/shared_thread_tasks.py`), and the cases of the seat isolates of a TypeScript thread
// child: a sync and an async body, the cancel cell of a sync body and the signal of an async body, a send through the
// relay with no addon in the isolate, CPU work, a block of the child's main thread, and memory past the bound of the
// child.
//
// Every body runs in a seat isolate of a thread child, or on the main thread of an isolated child when the worker
// retries a timed-out thread run there. A body that holds its thread holds its isolate only: the main thread of the
// child, which hands the runs to the isolates, keeps turning.

import { randomFillSync } from "node:crypto";
import { BroadcastChannel, isMainThread, threadId } from "node:worker_threads";

import { defineTask, h, type Horsies } from "#horsies";
import { addonLoads } from "../addon-loads.ts";
import { entryThread } from "../entry-thread.ts";
import { appendLine, createOnce, markerErrorText, nextAttempt, unixMillis } from "../markers.ts";
import { simple } from "./basic.ts";
import { holdThread, PAST_THE_DEADLINE_MS, sleepFor } from "./waits.ts";

/** The deadline of the thread tasks that run past it (Rust and Python `e2e_thread_hang`). */
export const THREAD_STOP_TIMEOUT_MS = 2_000;

/** The deadline of the thread tasks that end in time. */
const THREAD_TIMEOUT_MS = 30_000;

/** The deadline of `e2e_thread_overrun`, and the interval and the count of its lines: 6 s of lines (Rust `OVERRUN_TICK`). */
export const OVERRUN_TIMEOUT_MS = 1_000;
const OVERRUN_TICK_MS = 100;
const OVERRUN_TICKS = 60;

/** How often a sync body that waits for its cancel reads the cancel cell. */
const CANCEL_READ_MS = 20;

/** The most reads of a sync body that waits for its cancel: they last far past every deadline of the suite. */
const CANCEL_READS = PAST_THE_DEADLINE_MS / CANCEL_READ_MS;

/** How long a CPU-heavy body computes: past the 5 s block limit of the main thread of a child (core `LOOP_BLOCK_LIMIT`). */
export const CPU_WORK_MS = 6_000;

/** The channel on which a thread body asks the main thread of its child to block. */
export const MAIN_BLOCK_CHANNEL = "horsies-e2e-block-main";

/** How long the main thread blocks at the ask: far past the 5 s block limit. */
const MAIN_BLOCK_MS = 30_000;

/** How long the asking body holds its isolate after the ask: past the block limit, so its run is in flight at the kill. */
const ASKER_HOLD_MS = 10_000;

/** The size of one chunk that `e2e_thread_holds_memory` keeps, in MiB, and the most chunks it keeps: 512 MiB at most. */
const HELD_CHUNK_MB = 16;
const HELD_CHUNKS = 32;

/** The memory that `e2e_thread_holds_memory` keeps at most, in MiB: above the bound of `thread-memory-bound-1x1`. */
export const HELD_MEMORY_MB = HELD_CHUNK_MB * HELD_CHUNKS;

/** The pause after each chunk, so the child passes its bound by a few chunks only before the memory watch reads it. */
const HELD_CHUNK_PAUSE_MS = 20;

/** How long `e2e_thread_holds_memory` holds its isolate after the last chunk: far past the memory watch interval. */
const HELD_MEMORY_HOLD_MS = 20_000;

const BYTES_PER_MIB = 1024 * 1024;

const THREAD_CODES = ["INTERRUPTED", "MARKER_FAILED", "SEND_FAILED", "NOT_IN_SEAT"] as const;

/** Where a body ran: its process, its thread, and whether the thread is a seat isolate. */
const place = h.object({ pid: h.int(), threadId: h.int(), thread: h.enum(["main", "seatIsolate"]) });

/** Where a body ran and when: wall-clock milliseconds since 1970 at its start and its end. */
const stamp = h.object({
  pid: h.int(),
  threadId: h.int(),
  thread: h.enum(["main", "seatIsolate"]),
  startMs: h.int(),
  endMs: h.int(),
});

/** Where a body ran, as a test reads it. */
export type Place = { readonly pid: number; readonly threadId: number; readonly thread: "main" | "seatIsolate" };

/** Where a body ran and when, as a test reads it. */
export type Stamp = Place & { readonly startMs: number; readonly endMs: number };

/** The place of this body. */
function placeHere(): Place {
  return { pid: process.pid, threadId, thread: entryThread(isMainThread) };
}

/** The stamp of a body of this thread that started at `startMs`. */
function stampSince(startMs: number): Stamp {
  return { ...placeHere(), startMs, endMs: unixMillis() };
}

/** The readiness task of a worker whose only children are shared thread (Rust `e2e_thread_healthcheck`). */
export const threadHealthcheck = defineTask({
  name: "e2e_thread_healthcheck",
  input: {},
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
});

/** Holds its isolate for `milliseconds` (Rust and Python `e2e_thread_sleep`). */
export const threadSleep = defineTask({
  name: "e2e_thread_sleep",
  input: { milliseconds: h.int() },
  output: stamp,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
});

/** Holds its isolate far past its 2 s deadline and reads no cancel: the worker kills its child (Rust `e2e_thread_hang`). */
export const threadHang = defineTask({
  name: "e2e_thread_hang",
  input: {},
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_STOP_TIMEOUT_MS,
});

/**
 * Runs 6 s against a 1 s deadline, with three retries on `TASK_TIMEOUT` (Rust `e2e_thread_overrun`). Each run appends
 * `start <attempt> <ms>`, then `tick <attempt> <ms>` every 100 ms, to the marker `<token>-runs`: a test reads whether a
 * run went on after the next one started.
 */
export const threadOverrun = defineTask({
  name: "e2e_thread_overrun",
  input: { token: h.string() },
  output: h.string(),
  errorCodes: THREAD_CODES,
  execution: "SHARED_THREAD",
  timeoutMs: OVERRUN_TIMEOUT_MS,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1, 1, 1], jitter: false, retryFor: ["TASK_TIMEOUT"] },
});

/** A sync body: where it ran. */
export const threadPlace = defineTask({
  name: "e2e_thread_place",
  input: {},
  output: place,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
});

/** An async body that awaits a timer of its isolate: where it ran. */
export const threadPlaceAsync = defineTask({
  name: "e2e_thread_place_async",
  input: {},
  output: place,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
});

/**
 * A sync body that holds its isolate and reads its cancel cell (`runtime.cancelState()`) every 20 ms. At the cancel it
 * writes the cause to `<token>-cause` and returns `INTERRUPTED`.
 */
export const threadSyncCooperative = defineTask({
  name: "e2e_thread_sync_cooperative",
  input: { token: h.string() },
  output: h.string(),
  errorCodes: THREAD_CODES,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_STOP_TIMEOUT_MS,
});

/** An async body that passes `runtime.signal` into its wait. At the fire it writes the cause to `<token>-cause`. */
export const threadAsyncCooperative = defineTask({
  name: "e2e_thread_async_cooperative",
  input: { token: h.string() },
  output: h.string(),
  errorCodes: THREAD_CODES,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_STOP_TIMEOUT_MS,
});

/** Computes for `milliseconds` with no wait: CPU work in its isolate. The count of its turns shows the work. */
export const threadCpuWork = defineTask({
  name: "e2e_thread_cpu_work",
  input: { milliseconds: h.int() },
  output: h.object({ stamp, turns: h.int() }),
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
});

/** Sends `e2e_simple` through the seat relay, then gives the addon loads of its isolate and the sent task id. */
export const threadSendsFromSeat = defineTask({
  name: "e2e_thread_sends_from_seat",
  input: { x: h.int() },
  output: h.object({ place, addonLoads: h.int(), sentId: h.string() }),
  errorCodes: THREAD_CODES,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
});

/**
 * The first run of `token` writes its process id to `<token>-blocked`, asks the main thread of its child to block
 * (`MAIN_BLOCK_CHANNEL`), and holds its isolate; a retry returns its process id. The policy retries the crash path.
 */
export const threadBlocksMain = defineTask({
  name: "e2e_thread_blocks_main",
  input: { token: h.string() },
  output: h.int(),
  errorCodes: THREAD_CODES,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["WORKER_CRASHED"] },
});

/**
 * The first run keeps `Buffer` chunks (outside the JavaScript heap of its isolate) up to `HELD_MEMORY_MB`, then holds
 * its isolate, and writes its pid to `<token>-held`; the memory watch of the child ends the child at its bound. The
 * retry, on `WORKER_CRASHED`, gives the pid of its child.
 */
export const threadHoldsMemory = defineTask({
  name: "e2e_thread_holds_memory",
  input: { token: h.string() },
  output: h.int(),
  errorCodes: THREAD_CODES,
  execution: "SHARED_THREAD",
  timeoutMs: THREAD_TIMEOUT_MS,
  retryPolicy: { kind: "fixed", intervalsSeconds: [1], jitter: false, retryFor: ["WORKER_CRASHED"] },
});

/** Every contract of this module. */
export const THREAD_CONTRACTS = [
  threadHealthcheck,
  threadSleep,
  threadHang,
  threadOverrun,
  threadPlace,
  threadPlaceAsync,
  threadSyncCooperative,
  threadAsyncCooperative,
  threadCpuWork,
  threadSendsFromSeat,
  threadBlocksMain,
  threadHoldsMemory,
] as const;

/**
 * On the main thread of a child: block the thread for `MAIN_BLOCK_MS` at each ask on `MAIN_BLOCK_CHANNEL`. The channel
 * does not keep the child alive.
 */
export function blockMainThreadOnAsk(): void {
  const channel = new BroadcastChannel(MAIN_BLOCK_CHANNEL);

  channel.addEventListener("message", () => {
    holdThread(MAIN_BLOCK_MS);
  });
  channel.unref();
}

/** Keep `HELD_CHUNKS` chunks of random bytes (pages that the OS cannot share or compress away), one after the other. */
function keepChunks(): Buffer[] {
  const chunks: Buffer[] = [];

  for (let chunk = 0; chunk < HELD_CHUNKS; chunk += 1) {
    chunks.push(randomFillSync(Buffer.allocUnsafe(HELD_CHUNK_MB * BYTES_PER_MIB)));
    holdThread(HELD_CHUNK_PAUSE_MS);
  }

  return chunks;
}

/** Compute for `milliseconds` with no wait; the count of turns. */
function computeFor(milliseconds: number): number {
  const end = performance.now() + milliseconds;
  let turns = 0;

  while (performance.now() < end) {
    turns += 1;
  }

  return turns;
}

/** The implementations of this module on `app`. */
export function implementThreads(app: Horsies): void {
  app.implement(threadHealthcheck, (_input, { ok }) => ok("ready"));
  app.implement(threadSleep, ({ milliseconds }, { ok }) => {
    const start = unixMillis();

    holdThread(milliseconds);

    return ok(stampSince(start));
  });
  app.implement(threadHang, (_input, { ok }) => {
    holdThread(PAST_THE_DEADLINE_MS);

    return ok("never");
  });
  app.implement(threadOverrun, ({ token }, { ok, err }) => {
    const attempt = nextAttempt(token);

    if (!attempt.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(attempt.error) });
    }

    const runs = `${token}-runs`;

    const line = (kind: "start" | "tick"): ReturnType<typeof appendLine> =>
      appendLine(runs, `${kind} ${String(attempt.value)} ${String(unixMillis())}`);

    const started = line("start");

    if (!started.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(started.error) });
    }

    for (let tick = 0; tick < OVERRUN_TICKS; tick += 1) {
      holdThread(OVERRUN_TICK_MS);

      const written = line("tick");

      if (!written.ok) {
        return err({ code: "MARKER_FAILED", message: markerErrorText(written.error) });
      }
    }

    return ok(`ran ${String(attempt.value)}`);
  });
  app.implement(threadPlace, (_input, { ok }) => ok(placeHere()));
  app.implement(threadPlaceAsync, async (_input, { ok }) => {
    await sleepFor(CANCEL_READ_MS, "noSignal");

    return ok(placeHere());
  });
  app.implement(threadCpuWork, ({ milliseconds }, { ok }) => {
    const start = unixMillis();
    const turns = computeFor(milliseconds);

    return ok({ stamp: stampSince(start), turns });
  });
  implementCancels(app);
  implementSeatCalls(app);
}

function implementCancels(app: Horsies): void {
  app.implement(threadSyncCooperative, ({ token }, { ok, err, cancelState }) => {
    for (let read = 0; read < CANCEL_READS; read += 1) {
      const state = cancelState();

      if (state.kind === "requested") {
        const written = appendLine(`${token}-cause`, state.cause);

        return written.ok
          ? err({ code: "INTERRUPTED", message: `cancelled: ${state.cause}` })
          : err({ code: "MARKER_FAILED", message: markerErrorText(written.error) });
      }

      holdThread(CANCEL_READ_MS);
    }

    return ok("never");
  });
  app.implement(threadAsyncCooperative, async ({ token }, { ok, err, signal }) => {
    const end = await sleepFor(PAST_THE_DEADLINE_MS, signal);

    if (end.kind === "slept") {
      return ok("never");
    }

    const written = appendLine(`${token}-cause`, end.reason);

    return written.ok
      ? err({ code: "INTERRUPTED", message: `cancelled: ${end.reason}` })
      : err({ code: "MARKER_FAILED", message: markerErrorText(written.error) });
  });
}

function implementSeatCalls(app: Horsies): void {
  app.implement(threadSendsFromSeat, async ({ x }, { ok, err }) => {
    const sent = await app.send(simple, { x });

    if (!sent.ok) {
      return err({ code: "SEND_FAILED", message: `${sent.error.code}: ${sent.error.message}` });
    }

    const loads = addonLoads();

    switch (loads.kind) {
      case "mainThread":
        return err({ code: "NOT_IN_SEAT", message: "the body ran on the main thread of its child" });
      case "counted":
        return ok({ place: placeHere(), addonLoads: loads.loads, sentId: sent.value.taskId });
    }
  });
  app.implement(threadHoldsMemory, ({ token }, { ok, err }) => {
    const first = createOnce(`${token}-held`, String(process.pid));

    if (!first.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(first.error) });
    }

    if (first.value === "created") {
      const chunks = keepChunks();

      holdThread(HELD_MEMORY_HOLD_MS);
      // The chunks stay reachable until the hold ends.
      void chunks.length;
    }

    return ok(process.pid);
  });
  app.implement(threadBlocksMain, ({ token }, { ok, err }) => {
    const first = createOnce(`${token}-blocked`, String(process.pid));

    if (!first.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(first.error) });
    }

    if (first.value === "created") {
      const channel = new BroadcastChannel(MAIN_BLOCK_CHANNEL);

      channel.postMessage("block");
      holdThread(ASKER_HOLD_MS);
      channel.close();
    }

    return ok(process.pid);
  });
}
