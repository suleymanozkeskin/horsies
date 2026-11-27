// The addon load count of the worker entry (`src/addon-loads.ts`): a seat isolate counts each `process.dlopen` call of
// its thread, and the main thread has no count. No addon, no database and no process.

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Worker } from "node:worker_threads";

import { addonLoads } from "../src/addon-loads.ts";

/** The longest wait for the answer of the worker thread. */
const WORKER_ANSWER_BOUND_MS = 10_000;

/** The answer of a worker thread that counts its loads, within `WORKER_ANSWER_BOUND_MS`. */
async function workerAnswer(): Promise<unknown> {
  const worker = new Worker(new URL("addon-loads-worker.ts", import.meta.url));
  let timer: ReturnType<typeof setTimeout> | undefined;

  const answer = new Promise<unknown>((resolve, reject) => {
    worker.once("message", resolve);
    worker.once("error", reject);
  });

  const bound = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`no answer of the worker thread within ${String(WORKER_ANSWER_BOUND_MS)} ms`));
    }, WORKER_ANSWER_BOUND_MS);
  });

  try {
    return await Promise.race([answer, bound]);
  } finally {
    clearTimeout(timer);
    await worker.terminate();
  }
}

describe("the addon load count of the worker entry", () => {
  test("the main thread has no count", () => {
    assert.deepEqual(addonLoads(), { kind: "mainThread" });
  });

  test("a seat isolate counts each process.dlopen call of its thread, also one that fails", async () => {
    assert.deepEqual(await workerAnswer(), {
      before: { kind: "counted", loads: 0 },
      opened: "refused",
      after: { kind: "counted", loads: 1 },
    });
  });
});
