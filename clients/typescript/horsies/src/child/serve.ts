// The dispatchers of a served child: the post dispatcher, which the core calls with each body, each cancel and each
// stop request of spawned work, and the start hook runner, which the core calls with the index of each start hook.
//
// - `runBody`: find the implementation, enter the run in the run table, run the body through the adapter, then settle
//   the run: `settleBodyValue` with the stored text, or `settleBodyError` with the core's task error. An `err` code
//   that the core refuses is stored as `RETURN_TYPE_MISMATCH` (`refusedCodeParts`).
// - `cancelRun` and `stopWork` abort the signals of the run table; `cancelRun` also reaches the body observation of the
//   run (its `cancelled`), so the observer can end the run's span at the core's decision.
// - The post dispatcher never throws: a throw into Node-API ends the process. A defect of the package goes to the
//   defect rule (in a child: stderr, the log sink, then the abort).
// - Hook 0 is the child's own: it reads the start facts of the child (its kind); in a `SHARED_THREAD` child it boots the
//   seat isolates. Then the providers, in registration order (each keeps its value in the state values), then the
//   `onChildStart` hooks. A `SHARED_THREAD` child runs its providers in each seat isolate, not on its main thread.

import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { compileContract } from "../contract/compile.ts";
import { contractRecord } from "../contract/definition.ts";
import { taskIdFromCore } from "../contract/ids.ts";
import type { ImplementationParts } from "../contract/implementation.ts";
import type {
  BodySettle,
  ChildPost,
  StartFactsRecord,
  StartHookCall,
  StartHookEnd,
  TaskContextUse,
} from "../native/child/addon.ts";
import type { NativeTaskError, TaskErrorBuild, TaskErrorParts } from "../native/sends/addon.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import type { SeatOutcome } from "../seats/protocol.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { ChildMembersView } from "./app-members.ts";
import { attemptFromCore } from "./attempt.ts";
import { refusedCodeParts, runBody, type StoredBodyOutcome } from "./body.ts";
import type { BodyObservation, BodyObserver } from "./body-observer.ts";
import type { LoopMarks, RunNative, RunTable } from "./run-table.ts";
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

/** The body observation of each run whose body has not settled, by run key: at most the seats of the child. */
type RunObservations = Map<bigint, BodyObservation>;

/** The post dispatcher of a child. It never throws. */
export function postDispatcher(serve: ChildServe): (post: ChildPost) => void {
  const observations: RunObservations = new Map();

  return (post) => {
    try {
      dispatch(serve, observations, post);
    } catch (cause) {
      serve.reportDefect(defectOf("childPost", cause));
    }
  };
}

function dispatch(serve: ChildServe, observations: RunObservations, post: ChildPost): void {
  switch (post.kind) {
    case "runBody":
      startRun(serve, observations, post);

      return;
    case "cancelRun":
      serve.table.cancel(post.runKey, post.cause);
      observations.get(post.runKey)?.cancelled(post.cause);

      return;
    case "stopWork":
      serve.table.stopWork(post.workKey, post.cause);

      return;
    default:
      unreachable(post, "childPost");
  }
}

/** Start the run of a `runBody` post; its settle comes when the body ends. */
function startRun(serve: ChildServe, observations: RunObservations, post: Extract<ChildPost, { kind: "runBody" }>): void {
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
  const observation = serve.observer.enter(post.facts);

  observations.set(post.runKey, observation);

  void runBody({
    start: {
      kind: "posted",
      // The core's child host reads each run input with its strict read rule (`build_task_input` in its `prepare`)
      // before it calls the body; an input that the rule refuses ends the run with WORKER_SERIALIZATION_ERROR and is
      // never posted.
      inputJson: jsonTextCheckedByCore(post.inputJson),
      controls,
    },
    implementation,
    compiled: compiled.value,
    mark: { taskId, taskName: post.taskName, attempt },
    marks: serve.marks,
    observation,
  }).then(
    (outcome) => {
      observations.delete(post.runKey);
      serve.table.leave(post.runKey);
      settleRun(serve, post.runKey, post.taskName, outcome);
    },
    (cause: unknown) => {
      serve.reportDefect(defectOf("runBody", cause));
    },
  );
}

/**
 * Settle the run `runKey` with its stored outcome: the outcome of a body on this thread, or of a body in a seat
 * isolate. A defect goes to the defect rule.
 */
export function settleRun(serve: ChildServe, runKey: bigint, taskName: string, outcome: StoredBodyOutcome | SeatOutcome): void {
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

      throw new ClientDefect({ subject: "settleRun", rule: `the core refuses the built-in error of a refused code: ${replaced.cause}` });
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
 * The start hook runner of a child: hook 0 gives the start facts to `onKind` and waits for it; then each provider
 * builds its value and keeps it in `states` (a `SHARED_THREAD` child skips them: each seat isolate runs them); then
 * each `onChildStart` hook runs. A throw or a rejection is the hook's failure.
 */
export function hookRunner(
  members: ChildMembersView,
  states: StateValues,
  onKind: (facts: StartFactsRecord) => Promise<void>,
): (call: StartHookCall) => Promise<StartHookEnd> {
  return async (call) => {
    if (call.index === 0) {
      await onKind(call.facts);

      return "ended";
    }

    const position = call.index - 1;
    const provider = members.providers[position];

    if (provider !== undefined) {
      await providerHook(provider, states, call.facts);

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

/** Run `provider` on this thread and keep its value; a `SHARED_THREAD` child runs it in each seat isolate instead. */
async function providerHook(provider: ChildMembersView["providers"][number], states: StateValues, facts: StartFactsRecord): Promise<void> {
  switch (facts.kind) {
    case "isolated":
    case "sharedAsync":
    case "handler":
      states.keep(provider.key, await provider.factory());

      return;
    case "sharedThread":
      return;
    default:
      unreachable(facts, "providerHook");
  }
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

/**
 * The context use of the task of `contract` in the child host's registry: `TAKES_CONTEXT` for `workflowContext: true`,
 * so the core writes the node's workflow context into the run's input; else `NO_CONTEXT`, and the core removes it.
 */
export function taskContextUseOf(contract: object): TaskContextUse {
  const context = contractRecord(contract).context;

  switch (context) {
    case "takesContext":
      return "TAKES_CONTEXT";
    case "noContext":
      return "NO_CONTEXT";
    default:
      return unreachable(context, "taskContextUseOf");
  }
}

/**
 * The custom codes that the contract of a task declares, for the core's check of the upstream codes of an `args_from`
 * result: the `errorCodes` in their order, or none for a contract with no `errorCodes` (the core then checks no code of
 * the task; an empty `errorCodes` list also declares none). Effects: none.
 */
export function declaredCodesOf(contract: object): string[] {
  const codes = contractRecord(contract).codes;

  switch (codes.kind) {
    case "open":
      return [];
    case "declared":
      return [...codes.codes];
    default:
      return unreachable(codes, "declaredCodesOf");
  }
}
