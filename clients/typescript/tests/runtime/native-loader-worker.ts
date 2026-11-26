// Runs in a `worker_threads` isolate of `native-loader.test.ts`: it calls the loader and posts the
// kind of the outcome.

import { parentPort } from "node:worker_threads";

import { loadAddon } from "../../horsies/src/native/load.ts";

parentPort?.postMessage(loadAddon().kind);
