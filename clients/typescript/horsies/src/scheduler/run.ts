// The scheduler run of the `scheduler` command (the model: the Rust client's `run_scheduler`, `scheduler.rs`). In
// order:
//
// 1. The log level of the process, before the first core call.
// 2. The schedule declaration of the app (`app.schedules`): none is `notConfigured`.
// 3. The master switch of the declaration's section: off is `disabled`.
// 4. The static check with the schedules: a refusal is the `scheduler-startup` report. A refusal of a schedule, of
//    the section or of the core's manifest parser is in this report.
// 5. The stop on `SIGTERM` and `SIGINT`; the sends of the process open (module code ran with sends suppressed);
//    then the run of the scheduler that the declaration prepared, on the core's process runtime, with the core
//    handle that `Horsies.create` built for the app.
//
// Each failure is one `SchedulerRunError` variant; nothing starts before step 5. The core owns the scheduler loop
// and its SQL. The stop handlers stay until the process ends: `cli.run` ends it after the log wait.

import type { WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import type { CoreChecks } from "../app/core-checks.ts";
import { type CheckReport, reportText } from "../check/report.ts";
import { staticCheck } from "../check/static-check.ts";
import { configureLogging } from "../logging.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { NativePreparedScheduler, SchedulerRunEnded } from "../native/scheduler/addon.ts";
import { requestSchedulerStop, runScheduler } from "../native/scheduler/scheduler.ts";
import { checkAppConfig, checkTaskOptions, parseDeclaredErrorCodes } from "../native/sends/sends.ts";
import { type DeclaredScheduler, scheduleDeclaration } from "../schedules/app-member.ts";
import { ClientDefect, CoreError } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { appHandle } from "../worker/run.ts";

/** How a scheduler starts: the log level of the process. */
export type SchedulerStart = { readonly logLevel: WorkerLogLevel };

/** The scheduler ran until a stop request. */
export type SchedulerStopped = { readonly kind: "stopped" };

/** Why a scheduler run ended without a stop request. The `scheduler` command exits 1 for each. */
export type SchedulerRunError =
  /** The app makes no `app.schedules` call. Nothing started. */
  | { readonly kind: "notConfigured" }
  /** The master switch of the `app.schedules` section is off. Nothing started. */
  | { readonly kind: "disabled" }
  /** The static check refused the app (the `scheduler-startup` report). Nothing started. */
  | { readonly kind: "checkRefused"; readonly report: CheckReport }
  /** The core's start refused `schedule` before any state write; `reason` names the schedule and the cause. */
  | { readonly kind: "startRefused"; readonly schedule: string; readonly reason: string }
  /** The connect, the schema install or the client family check failed; no schedule state was written. */
  | { readonly kind: "startFailed"; readonly message: string; readonly retryable: boolean }
  /** The core of this process refused the call: it could not start, it stopped, or this process is a fork. */
  | { readonly kind: "coreRefused"; readonly error: CoreError };

/** What the scheduler run does that has effects. The process gives the addon's; a test gives fakes. */
export interface SchedulerRunDeps {
  /** Set the thresholds of the core log of this process. */
  setLogLevel(level: WorkerLogLevel): void;
  readonly coreChecks: CoreChecks;
  run(scheduler: NativePreparedScheduler): Promise<SchedulerRunEnded>;
  requestStop(scheduler: NativePreparedScheduler): void;
  /** Call `requestStop` at the first stop signal. The handlers stay until the process ends. */
  stopOnSignals(requestStop: () => void): void;
}

/** Run the scheduler of `context` as `start` says, until a stop request, a refused start or a failed start. */
export async function runSchedulerOf(
  context: AppContext,
  start: SchedulerStart,
  deps: SchedulerRunDeps,
): Promise<Result<SchedulerStopped, SchedulerRunError>> {
  deps.setLogLevel(start.logLevel);

  const prepared = preparedScheduler(context, deps.coreChecks);

  return prepared.ok ? runPrepared(context, prepared.value, deps) : prepared;
}

/** Steps 2 to 4: the declaration, the master switch, the check. Nothing starts. */
export function preparedScheduler(context: AppContext, checks: CoreChecks): Result<NativePreparedScheduler, SchedulerRunError> {
  const declaration = scheduleDeclaration(context);

  switch (declaration.kind) {
    case "notDeclared":
      return err({ kind: "notConfigured" });
    case "declared": {
      if (!declaration.section.enabled) {
        return err({ kind: "disabled" });
      }

      const checked = staticCheck(context, checks, "withSchedules", "scheduler-startup");

      return checked.kind === "refused" ? err({ kind: "checkRefused", report: checked.report }) : ok(checkedScheduler(declaration.scheduler));
    }

    default:
      return unreachable(declaration);
  }
}

/**
 * The scheduler of a declaration that passed the static check. A refused declaration has its refusals in the sink, so
 * the check refuses first; a declaration with no prepared scheduler is one of a worker thread, where `cli.run` runs no
 * command. Either is a defect: `ClientDefect`.
 */
function checkedScheduler(scheduler: DeclaredScheduler): NativePreparedScheduler {
  switch (scheduler.kind) {
    case "prepared":
      return scheduler.scheduler;
    case "refused":
    case "notPrepared":
      return raiseDefect(
        new ClientDefect({ subject: "scheduler", rule: `a declaration that passes the check has a prepared scheduler, not ${scheduler.kind}` }),
      );
    default:
      return unreachable(scheduler);
  }
}

/** Step 5: the stop handlers, the sends of the process open, then the run. */
async function runPrepared(
  context: AppContext,
  scheduler: NativePreparedScheduler,
  deps: SchedulerRunDeps,
): Promise<Result<SchedulerStopped, SchedulerRunError>> {
  deps.stopOnSignals(() => deps.requestStop(scheduler));
  context.suppression.startServing();

  try {
    return outcomeOf(await deps.run(scheduler));
  } catch (cause) {
    // A refusal of the core of this process is an outcome; a defect passes on.
    if (cause instanceof CoreError) {
      return err({ kind: "coreRefused", error: cause });
    }

    throw cause;
  }
}

/** The outcome of a run end. Pure. */
export function outcomeOf(ended: SchedulerRunEnded): Result<SchedulerStopped, SchedulerRunError> {
  switch (ended.kind) {
    case "stopped":
      return ok({ kind: "stopped" });
    case "startRefused":
      return err({ kind: "startRefused", schedule: ended.schedule, reason: ended.reason });
    case "startFailed":
      return err({ kind: "startFailed", message: ended.message, retryable: ended.retryable });
    default:
      return unreachable(ended);
  }
}

/** The text of a scheduler run error, as the Rust client writes it; a report is its report text. Pure. */
export function schedulerRunErrorText(error: SchedulerRunError): string {
  switch (error.kind) {
    case "notConfigured":
      return "no schedules: the app makes no app.schedules call before cli.run";
    case "disabled":
      return "the scheduler is disabled: the app.schedules section has enabled: false";
    case "checkRefused":
      return reportText(error.report);
    case "startRefused":
      return error.reason;
    case "startFailed":
      return `the scheduler start failed: ${error.message}`;
    case "coreRefused":
      return `the core refused the scheduler: ${error.error.name}: ${error.error.message}`;
    default:
      return unreachable(error);
  }
}

/** The scheduler functions of the addon that the run calls. A test gives fakes. */
export interface NativeSchedulerCalls {
  runScheduler(scheduler: NativePreparedScheduler, handle: CoreHandle): Promise<SchedulerRunEnded>;
  requestSchedulerStop(scheduler: NativePreparedScheduler): void;
}

/** The scheduler functions of the addon. */
export const NATIVE_SCHEDULER_CALLS: NativeSchedulerCalls = Object.freeze({ runScheduler, requestSchedulerStop });

/**
 * The scheduler run of this process: the addon and the log; `stopOnSignals` from the command. The run gets the core
 * handle that `Horsies.create` built for the app (`appHandle`).
 */
export function processSchedulerRunDeps(
  context: AppContext,
  stopOnSignals: (requestStop: () => void) => void,
  native: NativeSchedulerCalls = NATIVE_SCHEDULER_CALLS,
): SchedulerRunDeps {
  return {
    setLogLevel: (level) => configureLogging({ levels: { default: level } }),
    coreChecks: { checkAppConfig, parseDeclaredErrorCodes, checkTaskOptions },
    run: async (scheduler) => native.runScheduler(scheduler, appHandle(context)),
    requestStop: (scheduler) => native.requestSchedulerStop(scheduler),
    stopOnSignals,
  };
}
