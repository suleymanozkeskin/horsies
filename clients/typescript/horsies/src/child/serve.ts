// The dispatchers of a served child: the post dispatcher, which the core calls with each body, each cancel and each
// stop request of spawned work, and the start hook runner, which the core calls with the index of each start hook.
//
// - `runBody`: find the implementation, enter the run in the run table, run the body through the adapter, then settle
//   the run: `settleBodyValue` with the stored text, or `settleBodyError` with the core's task error. An `err` code
//   that the core refuses is stored as `RETURN_TYPE_MISMATCH` (`refusedCodeParts`).
// - `cancelRun` and `stopWork` abort the signals of the run table.
// - The post dispatcher never throws: a throw into Node-API ends the process. A defect of the package goes to the
//   defect rule (in a child: stderr, the log sink, then the abort).
// - Hook 0 is the child's own: it reads the start facts of the child (its kind). Then the providers, in registration
//   order (each keeps its value in the state values), then the `onChildStart` hooks.

import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { compileContract } from "../contract/compile.ts";
import { contractRecord } from "../contract/definition.ts";
import { taskIdFromCore } from "../contract/ids.ts";
import type { ImplementationParts } from "../contract/implementation.ts";
import type { BodySettle, ChildPost, StartFactsRecord, StartHookCall, StartHookEnd } from "../native/child/addon.ts";
import type { NativeTaskError, TaskErrorBuild, TaskErrorParts } from "../native/sends/addon.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { ChildMembersView } from "./app-members.ts";
import { attemptFromCore } from "./attempt.ts";
import { refusedCodeParts, runBody, type StoredBodyOutcome } from "./body.ts";
import type { BodyObserver } from "./body-observer.ts";
import type { LoopMarks, RunNative, RunTable } from "./run-table.ts";
import { taskRuntimeOf } from "./runtime.ts";
import type { StateValues } from "./state.ts";

/** The name of the child's own start hook, the first of the plan. */
export const CHILD_KIND_HOOK_NAME = "horsies: the kind of the child";

/** The calls of the addon that the post dispatcher makes. */
export interface ServeNative extends RunNative {
  settleBodyValue(runKey: bigint, json: string): BodySettle;
  settleBodyError(runKey: bigint, error: NativeTaskError): BodySettle;
  buildTaskError(parts: TaskErrorParts): TaskErrorBuild;
}

/** What the post dispatcher reads and changes. */
export interface ChildServe {
  readonly implementations: ReadonlyMap<string, ImplementationParts>;
  readonly table: RunTable;
  readonly observer: BodyObserver;
  readonly native: ServeNative;
  readonly marks: LoopMarks;
  /** The defect rule: it never throws. */
  readonly reportDefect: (defect: CoreDefect) => void;
}

/** The post dispatcher of a child. It never throws. */
export function postDispatcher(serve: ChildServe): (post: ChildPost) => void {
  return (post) => {
    try {
      dispatch(serve, post);
    } catch (cause) {
      serve.reportDefect(defectOf("childPost", cause));
    }
  };
}

function dispatch(serve: ChildServe, post: ChildPost): void {
  switch (post.kind) {
    case "runBody":
      startRun(serve, post);

      return;
    case "cancelRun":
      serve.table.cancel(post.runKey, post.cause);

      return;
    case "stopWork":
      serve.table.stopWork(post.workKey, post.cause);

      return;
    default:
      unreachable(post, "childPost");
  }
}

/** Start the run of a `runBody` post; its settle comes when the body ends. */
function startRun(serve: ChildServe, post: Extract<ChildPost, { kind: "runBody" }>): void {
  const implementation = serve.implementations.get(post.taskName);

  if (implementation === undefined) {
    throw new ClientDefect({ subject: "runBody", rule: `the core posted the task ${post.taskName}, which has no implementation` });
  }

  const compiled = compileContract(implementation.contract);

  if (!compiled.ok) {
    throw new ClientDefect({ subject: "runBody", rule: `the contract of ${post.taskName} does not compile in the child` });
  }

  const taskId = taskIdFromCore(post.taskId, "runBody");
  const attempt = attemptFromCore(post.attempt, "runBody");
  const controls = serve.table.enter({ runKey: post.runKey, taskId, taskName: post.taskName, attempt });

  void runBody({
    // The core's child host reads each run input with its strict read rule (`build_task_input` in its `prepare`)
    // before it calls the body; an input that the rule refuses ends the run with WORKER_SERIALIZATION_ERROR and is
    // never posted.
    inputJson: jsonTextCheckedByCore(post.inputJson),
    implementation,
    compiled: compiled.value,
    runtime: taskRuntimeOf(controls),
    mark: { taskId, taskName: post.taskName, attempt },
    marks: serve.marks,
    observation: serve.observer.enter(post.facts),
  }).then(
    (outcome) => {
      serve.table.leave(post.runKey);
      settleRun(serve, post.runKey, post.taskName, outcome);
    },
    (cause: unknown) => {
      serve.reportDefect(defectOf("runBody", cause));
    },
  );
}

/** Settle the run `runKey` with its stored outcome. A defect goes to the defect rule. */
function settleRun(serve: ChildServe, runKey: bigint, taskName: string, outcome: StoredBodyOutcome): void {
  try {
    switch (outcome.kind) {
      case "value":
        settledOnce(serve.native.settleBodyValue(runKey, outcome.json));

        return;
      case "error":
        settledOnce(serve.native.settleBodyError(runKey, builtError(serve.native, taskName, outcome.parts)));

        return;
      default:
        unreachable(outcome, "settleRun");
    }
  } catch (cause) {
    serve.reportDefect(defectOf("settleRun", cause));
  }
}

/** The core's task error of `parts`; an `err` code that the core refuses gives `RETURN_TYPE_MISMATCH`. */
function builtError(native: ServeNative, taskName: string, parts: TaskErrorParts): NativeTaskError {
  const built = native.buildTaskError(parts);

  switch (built.kind) {
    case "built":
      return built.error;
    case "refused": {
      const replaced = native.buildTaskError(refusedCodeParts(taskName, parts.code.code, built.cause));

      if (replaced.kind === "built") {
        return replaced.error;
      }

      throw new ClientDefect({ subject: "settleRun", rule: `the core refuses the library error of a refused code: ${replaced.cause}` });
    }

    default:
      return unreachable(built, "builtError");
  }
}

/** A run settles once: `unknownRun` is a defect of the package. */
function settledOnce(settle: BodySettle): void {
  switch (settle) {
    case "settled":
      return;
    case "unknownRun":
      throw new ClientDefect({ subject: "settleRun", rule: "a run was settled twice" });
    default:
      unreachable(settle, "settledOnce");
  }
}

/** A thrown value as a defect: a `CoreDefect` stays itself. */
function defectOf(subject: string, cause: unknown): CoreDefect {
  if (cause instanceof CoreDefect) {
    return cause;
  }

  const facts = thrownFacts(cause);

  return new ClientDefect({ subject, rule: `${facts.name}: ${facts.message}` });
}

/**
 * The start hook runner of a child: hook 0 gives the start facts to `onKind`; then each provider builds its value
 * and keeps it in `states`; then each `onChildStart` hook runs. A throw or a rejection is the hook's failure.
 */
export function hookRunner(
  members: ChildMembersView,
  states: StateValues,
  onKind: (facts: StartFactsRecord) => void,
): (call: StartHookCall) => Promise<StartHookEnd> {
  return async (call) => {
    if (call.index === 0) {
      onKind(call.facts);

      return "ended";
    }

    const position = call.index - 1;
    const provider = members.providers[position];

    if (provider !== undefined) {
      states.keep(provider.key, await provider.factory());

      return "ended";
    }

    const hook = members.hooks[position - members.providers.length];

    if (hook === undefined) {
      throw new ClientDefect({ subject: "startHook", rule: `the core called the start hook ${String(call.index)}, which the plan does not hold` });
    }

    await hook.hook();

    return "ended";
  };
}

/** The names of the start hooks of the plan, in run order: the child's own, the providers, the hooks. */
export function startHookNames(members: ChildMembersView): string[] {
  return [CHILD_KIND_HOOK_NAME, ...members.providers.map((provider) => provider.name), ...members.hooks.map((hook) => hook.name)];
}

/** The body form of the task of `contract`: the core's async form, or its blocking form for `SHARED_THREAD`. */
export function bodyFormOf(contract: object): "ASYNC" | "BLOCKING" {
  const execution = contractRecord(contract).execution;

  switch (execution) {
    case "ISOLATED":
    case "SHARED_ASYNC":
      return "ASYNC";
    case "SHARED_THREAD":
      return "BLOCKING";
    default:
      return unreachable(execution, "bodyFormOf");
  }
}
