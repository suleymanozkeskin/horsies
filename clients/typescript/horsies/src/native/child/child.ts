// The functions of the `child` area of the addon through the call wrapper: a throw of a call is a defect under the
// defect rule, and a thrown envelope is its error.

import { nativeValue, nativeValueAsync } from "../call/envelope.ts";
import type { NativeTaskError } from "../sends/addon.ts";
import { childAddon } from "./addon.ts";
import type {
  BodySettle,
  ChildArgumentsCheck,
  ChildHostPlan,
  ChildPost,
  ChildServeEnd,
  RunCancelStateRecord,
  SpawnEnd,
  SpawnRegistration,
  StartHookCall,
} from "./addon.ts";

/** The core's read of the child-mode arguments (the process arguments after the entry script). Pure: starts nothing. */
export function childArguments(processArguments: readonly string[]): ChildArgumentsCheck {
  return nativeValue("childArguments", () => childAddon().childArguments([...processArguments]));
}

/**
 * Serve as a child with `plan` until the stop frame or the end of the parent; resolves with the exit code. `post` and
 * `hook` never throw: a throw into Node-API ends the process. A second serve in a process throws `ClientDefect`.
 */
export async function serveChild(
  plan: ChildHostPlan,
  post: (post: ChildPost) => void,
  hook: (callId: bigint, call: StartHookCall) => void,
): Promise<ChildServeEnd> {
  return nativeValueAsync("serveChild", async () => childAddon().serveChild(plan, post, hook));
}

/** End the run `runKey` with the stored text `json` of its value. */
export function settleBodyValue(runKey: bigint, json: string): BodySettle {
  return nativeValue("settleBodyValue", () => childAddon().settleBodyValue(runKey, json));
}

/** End the run `runKey` with the task error `error`. */
export function settleBodyError(runKey: bigint, error: NativeTaskError): BodySettle {
  return nativeValue("settleBodyError", () => childAddon().settleBodyError(runKey, error));
}

/** The cancel state of the run `runKey`, read while the JavaScript thread runs a sync body. */
export function runCancelStateOf(runKey: bigint): RunCancelStateRecord {
  return nativeValue("runCancelStateOf", () => childAddon().runCancelStateOf(runKey));
}

/** Register spawned work of the run `runKey`, named `name` in the reports. */
export function registerSpawn(runKey: bigint, name: string): SpawnRegistration {
  return nativeValue("registerSpawn", () => childAddon().registerSpawn(runKey, name));
}

/** The spawned work `workKey` settled: it leaves its run. */
export function endSpawn(workKey: bigint): SpawnEnd {
  return nativeValue("endSpawn", () => childAddon().endSpawn(workKey));
}

/** The loop of the JavaScript main thread turned now. */
export function loopTurned(): void {
  nativeValue("loopTurned", () => childAddon().loopTurned());
}

/** The loop runs code of the run of task `taskId` now. */
export function loopHold(taskId: string): void {
  nativeValue("loopHold", () => childAddon().loopHold(taskId));
}

/** The loop runs code of no run now. */
export function loopRelease(): void {
  nativeValue("loopRelease", () => childAddon().loopRelease());
}

/**
 * End this `SHARED_THREAD` child at its memory bound: the addon writes one stderr line that names `boundMb` and
 * `measuredMb` (MiB), then ends the process with exit code 81 (`MEMORY_LIMIT`) through the core's immediate exit. Does
 * not return.
 */
export function endAtMemoryLimit(boundMb: number, measuredMb: number): never {
  return childAddon().endAtMemoryLimit(boundMb, measuredMb);
}
