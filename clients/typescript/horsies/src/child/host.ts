// Child mode of `cli.run`: the app entry, started again by the worker with `--horsies-child <fd> <pid>`, serves as a
// task child (or the handler child) on the core's child host.
//
// Steps:
// 1. The core reads the arguments. Bad arguments print the cause and end the process with 73.
// 2. The panic rule of a worker process: a defect goes to stderr, then to the log sink, then the process aborts.
// 3. The process events: no-op `SIGINT` and `SIGTERM`; the detached throws and rejections (`process-events.ts`).
// 4. The plan: each implementation by its task name and body form; the start hooks (the child's own, the providers,
//    the `onChildStart` hooks); the producer manifest of the app; the host name; the body facts; the flush bound. The
//    handler slot: the event handlers, the start hooks of the handler child and the worker-ended span maker.
// 5. The serve: sends from task code are no longer suppressed; the core posts each body, cancel and stop request to
//    the post dispatcher, and runs each start hook through the hook runner. The turn timer starts at hook 0 (a host
//    that started); the culprit method of Node starts there in a shared async child.
// 6. The exit rule: the flush of the body observer, at most its bound, then the end of the process with the host's
//    code. The child never waits for its event loop to empty.

import { hostname } from "node:os";

import type { Horsies } from "../app.ts";
import type { AppContext } from "../app/context.ts";
import { appInternals } from "../app/internals.ts";
import { addonSettlement, settlingCallback, type CallbackSettlement } from "../native/call/callback.ts";
import { reportDefect } from "../native/call/defect.ts";
import {
  childArguments,
  endSpawn,
  loopHold,
  loopRelease,
  loopTurned,
  registerSpawn,
  runCancelStateOf,
  serveChild,
  settleBodyError,
  settleBodyValue,
} from "../native/child/child.ts";
import { setPanicRule } from "../native/process-core/process-core.ts";
import { fillHandlerChild } from "../native/handler-child/handler-child.ts";
import { fillHandlerChildOf, type HandlerChildNative } from "../events/handler-child.ts";
import type { TracingSetting } from "../telemetry/tracing-config.ts";
import { buildTaskError, checkAppConfig } from "../native/sends/sends.ts";
import type {
  BodyFactsMode,
  ChildArgumentsCheck,
  ChildHostPlan,
  ChildPost,
  ChildServeEnd,
  StartHookCall,
} from "../native/child/addon.ts";
import type { AppConfigCheck } from "../native/sends/addon.ts";
import type { PanicRule } from "../thrown-errors.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type ChildMembersView, servedMembers } from "./app-members.ts";
import { type BodyObserver, bodyObserverOf, NO_BODY_OBSERVER } from "./body-observer.ts";
import { CHILD_EXIT_FLUSH_BOUND_MS, ChildEnd } from "./exit.ts";
import {
  type CulpritMethod,
  type LoopNative,
  loopMarksOf,
  processTurnTimer,
  runtimeCulpritMethod,
  startCulpritMarks,
  startTurns,
  type TurnTimer,
} from "./loop-marks.ts";
import { installProcessEvents, processEventHooks, type ProcessEventHooks, UNCAUGHT_EXIT_CODE } from "./process-events.ts";
import { thrownRecord } from "./thrown-error.ts";
import { currentRun } from "./run-store.ts";
import { RunTable } from "./run-table.ts";
import { bodyFormOf, hookRunner, postDispatcher, type ServeNative, startHookNames } from "./serve.ts";
import { StateValues } from "./state.ts";

/** The calls of the addon in child mode. */
export interface ChildNative extends ServeNative, LoopNative, HandlerChildNative {
  childArguments(processArguments: readonly string[]): ChildArgumentsCheck;
  serveChild(
    plan: ChildHostPlan,
    post: (post: ChildPost) => void,
    hook: (callId: bigint, call: StartHookCall) => void,
  ): Promise<ChildServeEnd>;
  setPanicRule(rule: PanicRule): void;
  checkAppConfig(appConfigJson: string): AppConfigCheck;
}

/** What child mode does that has effects. */
export interface ChildModeDeps {
  readonly native: ChildNative;
  readonly events: ProcessEventHooks;
  readonly timer: TurnTimer;
  readonly culprit: CulpritMethod;
  /** Where the start hook runner settles its calls. */
  readonly settlement: CallbackSettlement;
  readonly hostname: string;
  readonly writeLine: (line: string) => void;
  /** Wait `milliseconds`; the wait does not keep the process alive. */
  readonly wait: (milliseconds: number) => Promise<void>;
  readonly exit: (code: number) => never;
  /** The defect rule: it never throws. */
  readonly reportDefect: (defect: CoreDefect) => void;
}

/**
 * Serve `app` as a child with the process arguments after the entry script; the process ends at the child's end. It
 * never rejects: a defect of the package goes to the defect rule (stderr, the log sink, the abort), and any other
 * failure of child mode is written with its type and message, and the child ends with `CHILD_MODE_FAILED_EXIT_CODE`
 * through the exit rule. So an entry that does not await `cli.run` (a CommonJS entry) does not exit 0 or hang.
 */
export async function runChildMode(app: Horsies, processArguments: readonly string[], deps: ChildModeDeps): Promise<never> {
  const observer = observerOfApp(app);
  const childEnd = new ChildEnd({ flush: async () => observer.flush(), wait: deps.wait, exit: deps.exit });

  let ended: ChildModeEnd;

  try {
    ended = await childMode(app, processArguments, deps, childEnd);
  } catch (cause) {
    return failedChildMode(cause, deps, childEnd);
  }

  switch (ended.kind) {
    case "badArguments":
      return deps.exit(ended.code);
    case "served":
      return childEnd.end(ended.code);
    default:
      return unreachable(ended, "runChildMode");
  }
}

/** How child mode ended, before the end of the process: bad arguments (no exit rule), or the host's exit code. */
type ChildModeEnd = { readonly kind: "badArguments"; readonly code: number } | { readonly kind: "served"; readonly code: number };

/** The exit code of a child whose child mode failed outside every run: the code of an uncaught throw there. */
export const CHILD_MODE_FAILED_EXIT_CODE: number = UNCAUGHT_EXIT_CODE;

/** The body observer of `app`, or the one that does nothing when `app` is not an app (child mode then fails). */
function observerOfApp(app: Horsies): BodyObserver {
  try {
    return bodyObserverOf(appInternals(app));
  } catch {
    return NO_BODY_OBSERVER;
  }
}

/** The end of a failed child mode: the defect rule, or one stderr line; then the exit with 1. */
async function failedChildMode(cause: unknown, deps: ChildModeDeps, childEnd: ChildEnd): Promise<never> {
  if (cause instanceof CoreDefect) {
    deps.reportDefect(cause);
  } else {
    const record = thrownRecord(cause);
    const traceback = record.traceback === "" ? "" : `\n${record.traceback}`;

    deps.writeLine(`horsies child: child mode failed: ${record.type}: ${record.message}${traceback}`);
  }

  return childEnd.end(CHILD_MODE_FAILED_EXIT_CODE);
}

/** Step 1, then steps 2 to 6 for arguments that start a child. */
async function childMode(
  app: Horsies,
  processArguments: readonly string[],
  deps: ChildModeDeps,
  childEnd: ChildEnd,
): Promise<ChildModeEnd> {
  const checked = deps.native.childArguments(processArguments);

  switch (checked.kind) {
    case "badArguments":
      deps.writeLine(`horsies child: ${checked.cause}`);

      return { kind: "badArguments", code: checked.code };
    case "child":
      return servedChild(appInternals(app), processArguments, deps, childEnd);
    default:
      return unreachable(checked, "runChildMode");
  }
}

/** Steps 2 to 6. */
async function servedChild(
  context: AppContext,
  processArguments: readonly string[],
  deps: ChildModeDeps,
  childEnd: ChildEnd,
): Promise<ChildModeEnd> {
  deps.native.setPanicRule("ABORT_PROCESS");
  installProcessEvents({
    hooks: deps.events,
    writeLine: deps.writeLine,
    end: (code) => {
      void childEnd.end(code);
    },
    currentRun,
  });

  const members = servedMembers(context);
  const states = new StateValues(members.providers.map((provider) => provider.key));
  const marks = loopMarksOf(deps.native);
  const table = new RunTable(deps.native, marks, states);

  const post = postDispatcher({
    implementations: context.implementations,
    table,
    observer: bodyObserverOf(context),
    native: deps.native,
    marks,
    reportDefect: deps.reportDefect,
  });

  // Hook 0 runs only in a host that started, after it made the loop: the turns start there.
  const hooks = hookRunner(members, states, (facts) => {
    // The turns and the culprit marks run until the process ends.
    void startTurns(deps.native, deps.timer);

    if (facts.kind === "sharedAsync") {
      void startCulpritMarks(deps.culprit, marks);
    }
  });

  const plan = childPlan(context, members, processArguments, deps);

  // The handler slot of the core, which a handler child reads at the serve.
  fillHandlerChildOf(context, deps.native, deps.settlement);

  // Task code may send and start workflows from here on: the providers, the hooks and the bodies.
  context.suppression.startServing();

  const end = await deps.native.serveChild(plan, post, settlingCallback(hooks, deps.settlement));

  return { kind: "served", code: end.code };
}

/** The body facts that a child of an app with `tracing` records: the facts of each run for its spans, or none. Pure. */
export function bodyFactsMode(tracing: TracingSetting): BodyFactsMode {
  switch (tracing.kind) {
    case "otel":
      return "SPANS";
    case "none":
      return "NO_SCOPE";
    default:
      return unreachable(tracing, "bodyFactsMode");
  }
}

/** The plan of the child of `context`. A config that the core refuses again is a defect: `ClientDefect`. */
export function childPlan(
  context: AppContext,
  members: ChildMembersView,
  processArguments: readonly string[],
  deps: Pick<ChildModeDeps, "native" | "hostname">,
): ChildHostPlan {
  const checked = deps.native.checkAppConfig(context.config.coreConfigJson);

  if (checked.kind === "refused") {
    throw new ClientDefect({ subject: "childPlan", rule: "the config check refuses the config that Horsies.create accepted" });
  }

  return {
    tasks: [...context.implementations].map(([name, parts]) => ({ name, form: bodyFormOf(parts.contract) })),
    startHooks: startHookNames(members),
    producerManifestJson: checked.manifestJson,
    hostname: deps.hostname,
    bodyFacts: bodyFactsMode(context.telemetry.tracing),
    exitFlushBoundMs: CHILD_EXIT_FLUSH_BOUND_MS,
    processArguments: [...processArguments],
  };
}

/** The addon calls of child mode in this process. */
export function processChildNative(): ChildNative {
  return {
    childArguments,
    serveChild,
    settleBodyValue,
    settleBodyError,
    runCancelStateOf,
    registerSpawn,
    endSpawn,
    loopTurned,
    loopHold,
    loopRelease,
    buildTaskError,
    setPanicRule,
    checkAppConfig,
    fillHandlerChild,
  };
}

/** The dependencies of child mode in this process. */
export function processChildModeDeps(): ChildModeDeps {
  return {
    native: processChildNative(),
    events: processEventHooks(),
    timer: processTurnTimer,
    culprit: runtimeCulpritMethod(),
    settlement: addonSettlement,
    hostname: hostname(),
    writeLine: (line) => {
      process.stderr.write(`${line}\n`);
    },
    // The bound timer is referenced: an unref'd one lets Node end an awaiting entry with 13 before the exit.
    wait: async (milliseconds) =>
      new Promise((resolve) => {
        setTimeout(resolve, milliseconds);
      }),
    exit: (code) => process.exit(code),
    reportDefect,
  };
}
