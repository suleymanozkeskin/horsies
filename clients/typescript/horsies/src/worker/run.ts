// The worker run of the `worker` command (the model: the Rust client's `run_worker`, `worker.rs`). In order:
//
// 1. The panic rule of a worker process and the parent's log level, before the first core call.
// 2. The static check without the schedules: a refusal prints the `worker-startup` report.
// 3. The child command of this process.
// 4. The start members and `prepareWorker`: the manifest bounds, the profile, the connection settings, and no
//    `SHARED_THREAD` children on a runtime other than Node (Node caps the JavaScript heap of a worker thread with
//    `resourceLimits`; Bun ignores them, so a seat isolate out of heap would not end alone).
// 5. The run on the core's process runtime, with the core handle that `Horsies.create` built for the app (so the
//    child spec source that the app registers there builds the sub-workflow children), and the stop on `SIGTERM`
//    and `SIGINT`. The stop handlers stay until the process ends: `cli.run` ends it after the log wait.
//
// Each failure is one `WorkerRunError` variant; nothing starts before step 5. A defect of the package follows the
// defect rule (`raiseDefect`): in a worker process it aborts. The JavaScript thread stays free during the run: the
// core runs on its own threads, and the run's Promise keeps the event loop alive.

import type { MetricsEndpoint, ProfileRequest, WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import { type CoreChecks, refusalOfCore } from "../app/core-checks.ts";
import { type CheckReport, checkReport, reportText } from "../check/report.ts";
import { staticCheck } from "../check/static-check.ts";
import type {
  NativePreparedWorker,
  SeatIsolateMemory,
  WorkerBootCauseName,
  WorkerFailCauseName,
  WorkerPreparation,
  WorkerRunEnded,
} from "../native/worker/addon.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import { setPanicRule } from "../native/process-core/process-core.ts";
import { checkAppConfig, checkTaskOptions, parseDeclaredErrorCodes } from "../native/sends/sends.ts";
import { prepareWorker, requestWorkerStop, runWorker } from "../native/worker/worker.ts";
import { appCoreHandle } from "../app/core-access.ts";
import { configureCommandLogging } from "../logging.ts";
import { ClientDefect, CoreError } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import {
  childCommand,
  childCommandRefusedText,
  type ChildCommandRefused,
  type LaunchFacts,
  processLaunchFacts,
} from "./child-command.ts";
import { childLaunch, eventHandlers, startMembersText, taskManifests, workerEndedSpans } from "./start-members.ts";

/** How a worker starts: the profile that it runs, the log level of the process and its children, the metrics endpoint. */
export type WorkerStart = {
  readonly profile: ProfileRequest;
  readonly logLevel: WorkerLogLevel;
  readonly metricsEndpoint: MetricsEndpoint;
};

/** The worker ran until a stop request and drained. */
export type WorkerStopped = { readonly kind: "stopped" };

/** Why a worker run ended without a stop request. The `worker` command exits 1 for each. */
export type WorkerRunError =
  /** The static check refused the app (the `worker-startup` report). Nothing started. */
  | { readonly kind: "checkRefused"; readonly report: CheckReport }
  /** The command that starts the children cannot be built. Nothing started. */
  | { readonly kind: "childCommandRefused"; readonly refused: ChildCommandRefused }
  /** A config value is outside the bound of its manifest field; the value is not kept. Nothing started. */
  | { readonly kind: "manifestBound"; readonly path: string; readonly min: bigint; readonly max: bigint }
  /** The profile set or the requested profile is refused (the core's report). Nothing started. */
  | { readonly kind: "profileRefused"; readonly report: CheckReport }
  /** The connection settings fail the broker config check. Nothing started. */
  | { readonly kind: "connectionSettingsRefused"; readonly cause: string }
  /**
   * The profile starts `SHARED_THREAD` children, and the runtime of this process is not Node, so it does not cap the
   * JavaScript heap of a worker thread (Bun ignores `resourceLimits`). Nothing started.
   */
  | { readonly kind: "sharedThreadUnavailable"; readonly profile: string }
  /** The core of this process refused the call: it could not start, it stopped, or this process is a fork. */
  | { readonly kind: "coreRefused"; readonly error: CoreError }
  /** The start failed; the worker claimed nothing. */
  | { readonly kind: "bootFailed"; readonly cause: WorkerBootCauseName; readonly message: string }
  /** The worker was ready, then failed; its running tasks drained first. */
  | { readonly kind: "failed"; readonly cause: WorkerFailCauseName; readonly message: string };

/** What the worker run does that has effects. The process gives the addon's; a test gives fakes. */
export interface WorkerRunDeps {
  /** Set the default threshold of the core log of this process, and the app's log format. */
  setLogLevel(level: WorkerLogLevel): void;
  /** Set the panic rule of a worker process (`ABORT_PROCESS`). */
  setWorkerPanicRule(): void;
  readonly coreChecks: CoreChecks;
  launchFacts(): LaunchFacts;
  /** Write one line to stderr. */
  writeLine(line: string): void;
  prepare(appConfigJson: string, startJson: string): WorkerPreparation;
  run(worker: NativePreparedWorker): Promise<WorkerRunEnded>;
  requestStop(worker: NativePreparedWorker): void;
  /** Call `requestStop` at the first stop signal. The handlers stay until the process ends. */
  stopOnSignals(requestStop: () => void): void;
}

/** Run a worker of `context` as `start` says, until a stop request or a failure. */
export async function runWorkerOf(
  context: AppContext,
  start: WorkerStart,
  deps: WorkerRunDeps,
): Promise<Result<WorkerStopped, WorkerRunError>> {
  deps.setWorkerPanicRule();
  deps.setLogLevel(start.logLevel);

  const prepared = preparedWorker(context, start, deps);

  return prepared.ok ? runPrepared(prepared.value, deps) : prepared;
}

/** Steps 2 to 4: the check, the child command, the preparation. Nothing starts. */
export function preparedWorker(
  context: AppContext,
  start: WorkerStart,
  deps: WorkerRunDeps,
): Result<NativePreparedWorker, WorkerRunError> {
  const checked = staticCheck(context, deps.coreChecks, "withoutSchedules", "worker-startup");

  if (checked.kind === "refused") {
    return err({ kind: "checkRefused", report: checked.report });
  }

  const built = childCommand(deps.launchFacts());

  if (!built.ok) {
    return err({ kind: "childCommandRefused", refused: built.error });
  }

  for (const flag of built.value.dropped) {
    deps.writeLine(`horsies worker: the children start without the runtime flag ${flag}`);
  }

  const startJson = startMembersText({
    profile: start.profile,
    metricsEndpoint: start.metricsEndpoint,
    tasks: taskManifests(context),
    handlers: eventHandlers(context.telemetry.handlers()),
    workerEndedSpans: workerEndedSpans(context.telemetry.tracing),
    childLaunch: childLaunch(built.value.command, start.logLevel),
  });

  return preparationResult(() => deps.prepare(context.config.coreConfigJson, startJson));
}

/** The prepared worker, or the error of a refused preparation or of a core refusal. */
function preparationResult(prepare: () => WorkerPreparation): Result<NativePreparedWorker, WorkerRunError> {
  const prepared = withCoreRefusal(prepare);

  if (!prepared.ok) {
    return prepared;
  }

  const preparation = prepared.value;

  switch (preparation.kind) {
    case "prepared":
      return ok(preparation.worker);
    case "manifestBound":
      return err({ kind: "manifestBound", path: preparation.path, min: preparation.min, max: preparation.max });
    case "profileRefused": {
      const [first, ...rest] = preparation.refusals.map((refused) => refusalOfCore("workerProfiles", refused));

      if (first === undefined) {
        return raiseDefect(new ClientDefect({ subject: "prepareWorker", rule: "a profile refusal holds at least one refusal" }));
      }

      return err({ kind: "profileRefused", report: checkReport("worker-startup", first, rest) });
    }

    case "connectionSettingsRefused":
      return err({ kind: "connectionSettingsRefused", cause: preparation.cause });
    case "sharedThreadUnavailable":
      return err({ kind: "sharedThreadUnavailable", profile: preparation.profile });
    default:
      return unreachable(preparation);
  }
}

/**
 * Step 5: the run, with the stop handlers installed first. The handlers stay after the run: a later signal during
 * the log wait of `cli.run` writes its line only, and the default action of the signal does not end the process.
 */
async function runPrepared(worker: NativePreparedWorker, deps: WorkerRunDeps): Promise<Result<WorkerStopped, WorkerRunError>> {
  deps.stopOnSignals(() => deps.requestStop(worker));

  const ended = await withCoreRefusalAsync(async () => deps.run(worker));

  return ended.ok ? outcomeOf(ended.value) : ended;
}

/** The outcome of a run end. Pure. */
export function outcomeOf(ended: WorkerRunEnded): Result<WorkerStopped, WorkerRunError> {
  switch (ended.kind) {
    case "stopped":
      return ok({ kind: "stopped" });
    case "bootFailed":
      return err({ kind: "bootFailed", cause: ended.cause, message: ended.message });
    case "failed":
      return err({ kind: "failed", cause: ended.cause, message: ended.message });
    default:
      return unreachable(ended);
  }
}

/** The value of `call`, or the error of a core refusal that it threw. A defect passes on. */
function withCoreRefusal<T>(call: () => T): Result<T, WorkerRunError> {
  try {
    return ok(call());
  } catch (cause) {
    if (cause instanceof CoreError) {
      return err({ kind: "coreRefused", error: cause });
    }

    throw cause;
  }
}

/** The value of `call`, or the error of a core refusal that it rejected with. A defect passes on. */
async function withCoreRefusalAsync<T>(call: () => Promise<T>): Promise<Result<T, WorkerRunError>> {
  try {
    return ok(await call());
  } catch (cause) {
    if (cause instanceof CoreError) {
      return err({ kind: "coreRefused", error: cause });
    }

    throw cause;
  }
}

/** The text of a worker run error, as the Rust client writes it; a report is its report text. Pure. */
export function workerRunErrorText(error: WorkerRunError): string {
  switch (error.kind) {
    case "checkRefused":
    case "profileRefused":
      return reportText(error.report);
    case "childCommandRefused":
      return `the child command cannot be built: ${childCommandRefusedText(error.refused)}`;
    case "manifestBound":
      return `${error.path}: outside ${String(error.min)} to ${String(error.max)}, the bound of its worker manifest field`;
    case "connectionSettingsRefused":
      return `the connection settings are refused: ${error.cause}`;
    case "sharedThreadUnavailable":
      return `workerProfiles[${JSON.stringify(error.profile)}].sharedThread: SHARED_THREAD children run on Node only: Node caps the JavaScript heap of each seat isolate (resourceLimits), and this runtime does not (Bun ignores resourceLimits), so a seat isolate at that heap's end would not end alone; run the worker on Node, or run the profile's tasks in isolated or shared async children`;
    case "coreRefused":
      return `the core refused the worker: ${error.error.name}: ${error.error.message}`;
    case "bootFailed":
      return `worker start failed (${error.cause}): ${error.message}`;
    case "failed":
      return `worker failed (${error.cause}): ${error.message}`;
    default:
      return unreachable(error);
  }
}

/** The worker functions of the addon that the worker run calls. A test gives fakes. */
export interface NativeWorkerCalls {
  prepareWorker(appConfigJson: string, startJson: string, seatIsolates: SeatIsolateMemory): WorkerPreparation;
  runWorker(worker: NativePreparedWorker, handle: CoreHandle): Promise<WorkerRunEnded>;
  requestWorkerStop(worker: NativePreparedWorker): void;
}

/** The worker functions of the addon. */
export const NATIVE_WORKER_CALLS: NativeWorkerCalls = Object.freeze({ prepareWorker, runWorker, requestWorkerStop });

/**
 * The worker run of this process: the addon, the process facts and stderr; `stopOnSignals` from the command. The run
 * gets the core handle that `Horsies.create` built for the app (`appCoreHandle`).
 */
export function processWorkerRunDeps(
  context: AppContext,
  stopOnSignals: (requestStop: () => void) => void,
  native: NativeWorkerCalls = NATIVE_WORKER_CALLS,
): WorkerRunDeps {
  return {
    setLogLevel: (level) => configureCommandLogging(level, context.config.logFormat),
    setWorkerPanicRule: () => setPanicRule("ABORT_PROCESS"),
    coreChecks: { checkAppConfig, parseDeclaredErrorCodes, checkTaskOptions },
    launchFacts: processLaunchFacts,
    writeLine: (line) => {
      process.stderr.write(`${line}\n`);
    },
    prepare: (appConfigJson, startJson) =>
      native.prepareWorker(appConfigJson, startJson, seatIsolateMemoryOf(runtimeNameOf(process.versions))),
    run: async (worker) => native.runWorker(worker, appCoreHandle(context.core)),
    requestStop: (worker) => native.requestWorkerStop(worker),
    stopOnSignals,
  };
}

/** The runtime of a process, by the version facts that it reports (`process.versions`). */
export type RuntimeName = "node" | "bun" | "deno" | "unknown";

/** The version facts of a runtime that tell it apart: Bun and Deno also report a `node` version. */
export type RuntimeVersions = { readonly node?: string | undefined; readonly bun?: string | undefined; readonly deno?: string | undefined };

/** The runtime of `versions`: Bun or Deno by their own version fact; Node when only the Node version is there. Pure. */
export function runtimeNameOf(versions: RuntimeVersions): RuntimeName {
  if (versions.bun !== undefined) {
    return "bun";
  }

  if (versions.deno !== undefined) {
    return "deno";
  }

  return versions.node === undefined ? "unknown" : "node";
}

/**
 * Whether the worker threads of `runtime` have a cap of their JavaScript heap: `CAPPED` on Node only (its
 * `resourceLimits` are measured); Bun ignores `resourceLimits`, and no other runtime is in the support promise. Pure.
 */
export function seatIsolateMemoryOf(runtime: RuntimeName): SeatIsolateMemory {
  switch (runtime) {
    case "node":
      return "CAPPED";
    case "bun":
    case "deno":
    case "unknown":
      return "NOT_CAPPED";
    default:
      return unreachable(runtime);
  }
}
