// The functions of the `child` area of the addon, their records, and the accessor.
//
// - `childArguments` is pure: the core's read of the child-mode arguments.
// - `serveChild` runs the core's child host on a thread of the addon until the stop frame or the end of the parent,
//   and resolves with the exit code. The core posts each body, each cancel and each stop request of spawned work to
//   `post`, and runs the start hooks through `hook` (the callback primitive).
// - The settle calls, the cancel state read, the spawn calls and the loop marks are sync calls of the JavaScript
//   thread during the serve.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeTaskError } from "../sends/addon.ts";

/** Why the cancel signal of a run fired. */
export type CancelCauseName = "DEADLINE" | "OPERATOR";

/** Why a cancel round asks a piece of spawned work to stop: the run's signal after the grace, or the stop of left work. */
export type WorkStopCauseName = "DEADLINE" | "OPERATOR" | "LEFT_WORK";

/** The execution class of the child that runs a body. */
export type BodyExecution = "ISOLATED" | "SHARED_ASYNC" | "SHARED_THREAD";

/** The workflow node that links a run, as its start read it. */
export type BodyWorkflowRecord =
  | { readonly kind: "notInWorkflow" }
  | {
      readonly kind: "inWorkflow";
      readonly workflowId: string;
      readonly nodeIndex: number;
      readonly rootWorkflowId: string;
    };

/** One entry of a trace carrier, as the send's propagator wrote it. */
export interface CarrierEntry {
  readonly key: string;
  readonly value: string;
}

/** The trace carrier that the send stored on the task row; the entries in key order. */
export type BodyCarrierRecord = { readonly kind: "noCarrier" } | { readonly kind: "carrier"; readonly entries: CarrierEntry[] };

/** The facts of one run for the body observer: none when the app does not trace its bodies. `attempt` is from 1. */
export type BodyFactsRecord =
  | { readonly kind: "notRecorded" }
  | {
      readonly kind: "recorded";
      readonly taskId: string;
      readonly taskName: string;
      readonly queueName: string;
      readonly attempt: number;
      readonly workerId: string;
      readonly execution: BodyExecution;
      readonly workflow: BodyWorkflowRecord;
      readonly carrier: BodyCarrierRecord;
    };

/**
 * What the core posts to the JavaScript main thread of a child:
 * - `runBody`: run the body of `taskName` for the run `runKey` with the stored input text; `attempt` is the fencing
 *   token's value, from 1, and `maxRetries` is the retry budget of the task row, from 0 to 20;
 * - `cancelRun`: the cancel signal of the run fired;
 * - `stopWork`: a cancel round asks the spawned work `workKey` to stop.
 */
export type ChildPost =
  | {
      readonly kind: "runBody";
      readonly runKey: bigint;
      readonly taskName: string;
      readonly inputJson: string;
      readonly taskId: string;
      readonly attempt: number;
      readonly maxRetries: number;
      readonly facts: BodyFactsRecord;
    }
  | { readonly kind: "cancelRun"; readonly runKey: bigint; readonly cause: CancelCauseName }
  | { readonly kind: "stopWork"; readonly workKey: bigint; readonly cause: WorkStopCauseName };

/** How a settle call ended: `unknownRun` when the run was settled before. */
export type BodySettle = "settled" | "unknownRun";

/** The cancel state of a run as its watch saw it; `runEnded` once the run settled. */
export type RunCancelStateRecord =
  | { readonly kind: "notRequested" }
  | { readonly kind: "requested"; readonly cause: CancelCauseName }
  | { readonly kind: "runEnded" };

/** How a registration of spawned work ended. Nothing was registered unless `registered`. */
export type SpawnRegistration =
  | { readonly kind: "registered"; readonly workKey: bigint }
  | { readonly kind: "runEnded" }
  | { readonly kind: "tooMuchWork"; readonly bound: number };

/** How the end of spawned work ended: `unknownWork` when it ended before. */
export type SpawnEnd = "ended" | "unknownWork";

/** How the child host runs the body of a task: `ASYNC` for `ISOLATED` and `SHARED_ASYNC`, `BLOCKING` for `SHARED_THREAD`. */
export type BodyForm = "ASYNC" | "BLOCKING";

/** Whether the child records the facts of its task bodies. */
export type BodyFactsMode = "NO_SCOPE" | "SPANS";

/**
 * Whether the child host writes the workflow context into the input of a task: `TAKES_CONTEXT` for a task with
 * `workflowContext: true` (the core then writes the `workflow_context` member for a node with `workflowContextFrom`),
 * `NO_CONTEXT` for any other task (the core removes the member).
 */
export type TaskContextUse = "TAKES_CONTEXT" | "NO_CONTEXT";

/** One task of the child, by its name. */
export interface ChildTaskEntry {
  readonly name: string;
  readonly form: BodyForm;
  readonly context: TaskContextUse;
  /** The `errorCodes` of the contract, in its order; empty for a contract with no `errorCodes`. */
  readonly errorCodes: string[];
}

/** What the package gives the child host. */
export interface ChildHostPlan {
  readonly tasks: ChildTaskEntry[];
  /** The name of each start hook in run order: the providers, then the `onChildStart` hooks. */
  readonly startHooks: string[];
  readonly producerManifestJson: string;
  readonly hostname: string;
  readonly bodyFacts: BodyFactsMode;
  /** The package's longest wait for its telemetry flush before the exit, in milliseconds (at most 8,000). */
  readonly exitFlushBoundMs: number;
  /** The process arguments after the entry script: the child flag, the socket descriptor, the worker pid. */
  readonly processArguments: string[];
}

/** The read of the child-mode arguments: a child, or the cause of bad arguments and the exit code (73). */
export type ChildArgumentsCheck =
  | { readonly kind: "child" }
  | { readonly kind: "badArguments"; readonly cause: string; readonly code: number };

/** How the child host ended, as the core names it. */
export type ChildExitName =
  | "STOPPED"
  | "PARENT_GONE"
  | "BAD_PARENT_FRAME"
  | "BAD_ARGUMENTS"
  | "BOOT_FAILED"
  | "PROTOCOL_MISMATCH"
  | "CHILD_HOOK_FAILED"
  | "CORE_STOPPED";

/** The end of the serve: the exit code of the process and its name. */
export interface ChildServeEnd {
  readonly code: number;
  readonly exit: ChildExitName;
}

/** The facts of the child that a start hook runs in. */
export type StartFactsRecord =
  | { readonly kind: "isolated" }
  | { readonly kind: "sharedAsync"; readonly seats: number }
  | { readonly kind: "sharedThread"; readonly seats: number }
  | { readonly kind: "handler" };

/** One call of the hook dispatcher: the index of the hook in the plan, and the facts of the child. */
export interface StartHookCall {
  readonly index: number;
  readonly facts: StartFactsRecord;
}

/** The answer of a hook that ended. */
export type StartHookEnd = "ended";

/** The functions of the `child` area. */
export interface ChildAddon {
  childArguments(processArguments: string[]): NativeEnvelope<ChildArgumentsCheck>;
  serveChild(
    plan: ChildHostPlan,
    post: (post: ChildPost) => void,
    hook: (callId: bigint, call: StartHookCall) => void,
  ): Promise<NativeEnvelope<ChildServeEnd>>;
  settleBodyValue(runKey: bigint, json: string): NativeEnvelope<BodySettle>;
  settleBodyError(runKey: bigint, error: NativeTaskError): NativeEnvelope<BodySettle>;
  runCancelStateOf(runKey: bigint): NativeEnvelope<RunCancelStateRecord>;
  registerSpawn(runKey: bigint, name: string): NativeEnvelope<SpawnRegistration>;
  endSpawn(workKey: bigint): NativeEnvelope<SpawnEnd>;
  loopTurned(): NativeEnvelope<undefined>;
  loopHold(taskId: string): NativeEnvelope<undefined>;
  loopRelease(): NativeEnvelope<undefined>;
}

/** The `child` functions of the addon. Throws as `requireAddon` does. */
export function childAddon(): ChildAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as ChildAddon;
}
