// The thread rule of the worker entry (`src/entry-thread.ts`): the steps that run once per process run on the main
// thread only, and a seat isolate (a worker thread) runs none of them. No addon, no database and no process.

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { isMainThread, Worker } from "node:worker_threads";

import { entryThread, runProcessSteps } from "../src/entry-thread.ts";

/** The longest wait for the answer of the worker thread. */
const WORKER_ANSWER_BOUND_MS = 10_000;

/** The answer of a worker thread that runs the rule of the entry, within `WORKER_ANSWER_BOUND_MS`. */
async function workerAnswer(): Promise<unknown> {
  const worker = new Worker(new URL("entry-thread-worker.ts", import.meta.url));
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

describe("the thread rule of the worker entry", () => {
  test("the main thread runs the steps of the process once", () => {
    let ran = 0;

    assert.equal(
      runProcessSteps("main", () => {
        ran += 1;
      }),
      "ran",
    );
    assert.equal(ran, 1);
  });

  test("a seat isolate runs none of the steps", () => {
    let ran = 0;

    assert.equal(
      runProcessSteps("seatIsolate", () => {
        ran += 1;
      }),
      "skipped",
    );
    assert.equal(ran, 0);
  });

  test("the thread of the test is the main thread", () => {
    assert.equal(entryThread(isMainThread), "main");
  });

  test("a worker thread is a seat isolate and runs none of the steps", async () => {
    assert.deepEqual(await workerAnswer(), { thread: "seatIsolate", steps: "skipped", ran: 0 });
  });
});
