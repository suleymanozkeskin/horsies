// The functions of the `worker` area of the addon through the call wrapper: a throw or a rejection of a call is a
// defect under the defect rule, and a thrown envelope is its error.

import { nativeValue, nativeValueAsync } from "../call/envelope.ts";
import type { CoreHandle } from "../handle/core-handle.ts";
import { workerAddon } from "./addon.ts";
import type {
  ListenAddressCheck,
  LiveCheckOutcome,
  LogBacklog,
  NativePreparedWorker,
  WorkerPreparation,
  WorkerRunEnded,
} from "./addon.ts";

/** The core's check of a `HOST:PORT` text. Pure: starts no core. */
export function parseListenAddress(text: string): ListenAddressCheck {
  return nativeValue("parseListenAddress", () => workerAddon().parseListenAddress(text));
}

/**
 * The worker of the accepted app config text `appConfigJson` and the start members `startJson`, or why it does not
 * start. Pure: starts no core. Throws `ManifestRefused` for a start member that the core's parser refuses (a defect of
 * the package) and `ClientDefect` for a config text that the core does not accept again.
 */
export function prepareWorker(appConfigJson: string, startJson: string): WorkerPreparation {
  return nativeValue("prepareWorker", () => workerAddon().prepareWorker(appConfigJson, startJson));
}

/**
 * Run `worker` until a stop request or a failure. The sub-workflow children of its workflow steps build through the
 * child spec source of `handle`. Starts the core of the process. A second run of one worker throws `ClientDefect`.
 */
export async function runWorker(worker: NativePreparedWorker, handle: CoreHandle): Promise<WorkerRunEnded> {
  return nativeValueAsync("runWorker", () => workerAddon().runWorker(worker, handle.native));
}

/** Ask the run of `worker` to stop. Idempotent; returns at once. */
export function requestWorkerStop(worker: NativePreparedWorker): void {
  nativeValue("requestStop", () => worker.requestStop());
}

/** The live steps of `check --live` on `handle` for the worker profiles of `appConfigJson`. Connects and reads. */
export async function checkLive(handle: CoreHandle, appConfigJson: string): Promise<LiveCheckOutcome> {
  return nativeValueAsync("checkLive", () => workerAddon().checkLive(handle.native, appConfigJson));
}

/** Whether core log records wait in the channel of this process. Starts nothing. */
export function workerLogBacklog(): LogBacklog {
  return nativeValue("workerLogBacklog", () => workerAddon().workerLogBacklog());
}
