// Runs in a `worker_threads` isolate of `addon-loads.test.ts`, as a seat isolate runs the worker entry: it posts the
// count of addon loads of its thread before and after one `process.dlopen` call of a file that does not exist.

import { parentPort } from "node:worker_threads";

import { addonLoads } from "../src/addon-loads.ts";

const before = addonLoads();
let opened: "opened" | "refused" = "opened";

try {
  process.dlopen({ exports: {} }, "/nonexistent/horsies-e2e-unit.node");
} catch {
  opened = "refused";
}

parentPort?.postMessage({ before, opened, after: addonLoads() });
