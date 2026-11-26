// The worker run of the `worker` command (the model: the Rust client's `run_worker`, `worker.rs`). In order:
//
// 1. The panic rule of a worker process and the parent's log level, before the first core call.
// 2. The static check without the schedules: a refusal prints the `worker-startup` report.
// 3. The child command of this process.
// 4. The start members and `prepareWorker`: the manifest bounds, the profile and the connection settings.
// 5. The run on the core's process runtime, with the stop on `SIGTERM` and `SIGINT` while it runs.
//
// Each failure is one `WorkerRunError` variant; nothing starts before step 5. The JavaScript thread stays free
// during the run: the core runs on its own threads, and the run's Promise keeps the event loop alive.

import type { MetricsEndpoint, ProfileRequest, WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import { type CoreChecks, refusalOfCore } from "../app/core-checks.ts";
import { type CheckReport, checkReport, reportText } from "../check/report.ts";
import { staticCheck } from "../check/static-check.ts";
import type { NativePreparedWorker, WorkerBootCauseName, WorkerFailCauseName, WorkerPreparation, WorkerRunEnded } from "../native/worker/addon.ts";
import type { HandleProfile } from "../native/handle/addon.ts";
import { buildCoreHandle, type CoreHandle } from "../native/handle/core-handle.ts";
import { setPanicRule } from "../native/process-core/process-core.ts";
import { checkAppConfig, checkTaskOptions, parseDeclaredErrorCodes } from "../native/sends/sends.ts";
import { prepareWorker, requestWorkerStop, runWorker } from "../native/worker/worker.ts";
import { configureLogging } from "../logging.ts";
import { ClientDefect, CoreError } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
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
  /** The core of this process refused the call: it could not start, it stopped, or this process is a fork. */
  | { readonly kind: "coreRefused"; readonly error: CoreError }
  /** The start failed; the worker claimed nothing. */
  | { readonly kind: "bootFailed"; readonly cause: WorkerBootCauseName; readonly message: string }
  /** The worker was ready, then failed; its running tasks drained first. */
  | { readonly kind: "failed"; readonly cause: WorkerFailCauseName; readonly message: string };

/** The stop handlers of one run, removed when it ends. */
export interface StopHandlers {
  remove(): void;
}

/** What the worker run does that has effects. The process gives the addon's; a test gives fakes. */
export interface WorkerRunDeps {
  /** Set the thresholds of the core log of this process. */
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
  /** Call `requestStop` at the first stop signal, until `remove`. */
  stopOnSignals(requestStop: () => void): StopHandlers;
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
    // The app hub holds no event handler and no tracing setting, so the worker starts no handler child.
    handlers: eventHandlers([]),
    workerEndedSpans: workerEndedSpans("none"),
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
        throw new TypeError("preparedWorker: the addon refused the profile with no refusal");
      }

      return err({ kind: "profileRefused", report: checkReport("worker-startup", first, rest) });
    }

    case "connectionSettingsRefused":
      return err({ kind: "connectionSettingsRefused", cause: preparation.cause });
    default:
      return unreachable(preparation, "preparedWorker");
  }
}

/** Step 5: the run, with the stop handlers while it runs. */
async function runPrepared(worker: NativePreparedWorker, deps: WorkerRunDeps): Promise<Result<WorkerStopped, WorkerRunError>> {
  const handlers = deps.stopOnSignals(() => deps.requestStop(worker));

  try {
    const ended = await withCoreRefusalAsync(async () => deps.run(worker));

    return ended.ok ? outcomeOf(ended.value) : ended;
  } finally {
    handlers.remove();
  }
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
      return unreachable(ended, "outcomeOf");
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
    case "coreRefused":
      return `the core refused the worker: ${error.error.name}: ${error.error.message}`;
    case "bootFailed":
      return `worker start failed (${error.cause}): ${error.message}`;
    case "failed":
      return `worker failed (${error.cause}): ${error.message}`;
    default:
      return unreachable(error, "workerRunErrorText");
  }
}

/** The worker run of this process: the addon, the process facts and stderr; `stopOnSignals` from the command. */
export function processWorkerRunDeps(
  context: AppContext,
  stopOnSignals: (requestStop: () => void) => StopHandlers,
): WorkerRunDeps {
  return {
    setLogLevel: (level) => configureLogging({ levels: { default: level } }),
    setWorkerPanicRule: () => setPanicRule("ABORT_PROCESS"),
    coreChecks: { checkAppConfig, parseDeclaredErrorCodes, checkTaskOptions },
    launchFacts: processLaunchFacts,
    writeLine: (line) => {
      process.stderr.write(`${line}\n`);
    },
    prepare: prepareWorker,
    run: async (worker) => runWorker(worker, appCoreHandle(context, "WORKER")),
    requestStop: requestWorkerStop,
    stopOnSignals,
  };
}

/**
 * A core handle of the app's producer manifest with the pool profile `profile`. It starts nothing until an operation
 * runs. The worker run takes the child spec source of the app's workflows from it: with none registered, each
 * sub-workflow node fails as not listed (`runWorker`).
 */
export function appCoreHandle(context: AppContext, profile: HandleProfile): CoreHandle {
  const checked = checkAppConfig(context.config.coreConfigJson);

  switch (checked.kind) {
    case "accepted":
      return buildCoreHandle(checked.manifestJson, profile);
    case "refused":
      throw new ClientDefect({ subject: "appCoreHandle", rule: "the config check refuses the config that Horsies.create accepted" });
    default:
      return unreachable(checked, "appCoreHandle");
  }
}
