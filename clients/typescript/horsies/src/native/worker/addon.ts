// The functions of the `worker` area of the addon, their records, and the accessor.
//
// - `parseListenAddress` and `prepareWorker` are pure: they start no core, so a task child that runs the entry
//   before `cli.run` still lets the child host shape its process runtime.
// - `runWorker` is a service run on the core's process runtime: it starts the core of the process.
// - `checkLive` is an operation on a core handle: it connects and reads.
// - `workerLogBacklog` reads counters of the log channel; it starts nothing.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";
import type { CoreRefusal } from "../sends/addon.ts";

/** The check of a `--metrics-listen` value: the host (an IPv6 address without brackets) and port, or the core's refusal text. */
export type ListenAddressCheck =
  | { readonly kind: "address"; readonly host: string; readonly port: number }
  | { readonly kind: "refused"; readonly cause: string };

/** The worker of one worker manifest, not run yet. It runs once. */
export interface NativePreparedWorker {
  /** Ask the run to stop. Idempotent; returns at once. A request before the run ends the run after its start. */
  requestStop(): NativeEnvelope<undefined>;
}

/**
 * How the preparation of a worker ended:
 * - `prepared`: the worker of the profile;
 * - `manifestBound`: a config value outside the bound of its manifest field; `path` names the field as the config
 *   writes it, and the value is not kept;
 * - `profileRefused`: the core's report on the profile set or the requested profile;
 * - `connectionSettingsRefused`: the broker config check refuses the connection settings;
 * - `sharedThreadUnavailable`: the profile starts `SHARED_THREAD` children, and the addon has no thread seat isolates.
 */
export type WorkerPreparation =
  | { readonly kind: "prepared"; readonly worker: NativePreparedWorker }
  | { readonly kind: "manifestBound"; readonly path: string; readonly min: bigint; readonly max: bigint }
  | { readonly kind: "profileRefused"; readonly refusals: CoreRefusal[] }
  | { readonly kind: "connectionSettingsRefused"; readonly cause: string }
  | { readonly kind: "sharedThreadUnavailable"; readonly profile: string };

/** Why a worker start failed, as the core names it. */
export type WorkerBootCauseName =
  | "SCHEMA_REFUSED"
  | "CLIENT_FAMILY_REFUSED"
  | "STATEMENT_POOLED_URL"
  | "SESSION_NOT_CAPABLE"
  | "START_TIMED_OUT"
  | "DATABASE_UNAVAILABLE"
  | "DATABASE_REFUSED"
  | "BOOT_ATTEMPTS_EXHAUSTED"
  | "CHILD_HOOK_FAILED"
  | "PRELOAD_REFUSED"
  | "BASELINE_OVER_LIMIT"
  | "PLACEMENT_REFUSED"
  | "SPAWNER_START_FAILED"
  | "CONFIG_REFUSED"
  | "LISTENER_REFUSED";

/** Why a worker that was ready failed, as the core names it. */
export type WorkerFailCauseName =
  | "SERVICE_LOOP_DIED"
  | "RUNNER_HEARTBEAT_LOST"
  | "POOL_FAILED"
  | "BASELINE_OVER_LIMIT"
  | "CLAIM_FAILED";

/** How a worker run ended. `message` names the subject and the cause. */
export type WorkerRunEnded =
  | { readonly kind: "stopped" }
  | { readonly kind: "bootFailed"; readonly cause: WorkerBootCauseName; readonly message: string }
  | { readonly kind: "failed"; readonly cause: WorkerFailCauseName; readonly message: string };

/** The connectivity step of the live check that refused. */
export type ConnectivityStep = "CONNECT" | "POOL_SETTINGS" | "MAIN_URL" | "SESSION_URL" | "LISTENER";

/**
 * How the live check ended: every step passed; the first connectivity step that refused, with the core's cause; or
 * one refusal per worker profile that does not fit the server's connection limit (`HRS-219`).
 */
export type LiveCheckOutcome =
  | { readonly kind: "passed" }
  | { readonly kind: "connectivityRefused"; readonly step: ConnectivityStep; readonly message: string }
  | { readonly kind: "connectionsRefused"; readonly refusals: CoreRefusal[] };

/** Whether core log records wait in the channel: none, some that the forwarder hands on, or some that stay. */
export type LogBacklog = "empty" | "waiting" | "noForwarder";

/** The functions of the `worker` area. */
export interface WorkerAddon {
  parseListenAddress(text: string): NativeEnvelope<ListenAddressCheck>;
  prepareWorker(appConfigJson: string, startJson: string): NativeEnvelope<WorkerPreparation>;
  runWorker(worker: NativePreparedWorker, handle: NativeCoreHandle): Promise<NativeEnvelope<WorkerRunEnded>>;
  checkLive(handle: NativeCoreHandle, appConfigJson: string): Promise<NativeEnvelope<LiveCheckOutcome>>;
  workerLogBacklog(): NativeEnvelope<LogBacklog>;
}

/** The `worker` functions of the addon. Throws as `requireAddon` does. */
export function workerAddon(): WorkerAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as WorkerAddon;
}
