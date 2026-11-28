// Runs in a `worker_threads` isolate of `native-loader.test.ts`: it calls the loader that its worker
// data names (`package`: `loadAddon`; `dev`: `loadDevAddon`) and posts the kind of the outcome.

import { parentPort, workerData } from "node:worker_threads";

import { loadDevAddon } from "../../horsies/src/native/dev-addon.ts";
import { loadAddon } from "../../horsies/src/native/load.ts";

/** The path that the `dev` case names: absolute, so the dev entry does not refuse it. */
const DEV_BUILD_PATH = "/nonexistent/horsies-loader-check/horsies.node";

parentPort?.postMessage(workerData === "dev" ? loadDevAddon(DEV_BUILD_PATH).kind : loadAddon().kind);
