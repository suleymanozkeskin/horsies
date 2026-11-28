// Runs in a `worker_threads` isolate of `entry-thread.test.ts`, as a seat isolate runs the worker entry: it posts the
// thread that the rule of the entry reads and whether the steps of the process ran.

import { isMainThread, parentPort } from "node:worker_threads";

import { entryThread, runProcessSteps } from "../src/entry-thread.ts";

const thread = entryThread(isMainThread);
let ran = 0;

const steps = runProcessSteps(thread, () => {
  ran += 1;
});

parentPort?.postMessage({ thread, steps, ran });
