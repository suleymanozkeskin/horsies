// The core operations of workflow starts and handles in this thread: the addon on a main thread, the seat relay in a
// seat isolate, or no core in any other worker thread (where an operation is a defect).
//
// A start runs in two calls. `prepare` converts a checked spec to spec data (with the core's facts) and runs the core's
// parse, with no I/O; `start` parses that text again and writes the tree. A seat isolate sends each call to its child's
// main thread, which runs the same steps. So a spec that the conversion or the parse refuses is refused before the
// start observer opens its span, as in the Rust client.

import { threadId } from "node:worker_threads";

import { DEFAULT_QUEUE_ROUTE } from "../app/contract-list.ts";
import type { CoreAccess, SeatRelay } from "../app/core-access.ts";
import type { SeatRelayFullOutcome } from "../seats/relay-full.ts";
import type { QueueSetup } from "../config/queues.ts";
import { addonSettlement, addonTake, settlingCallback, takingCallback } from "../native/call/callback.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { NativeSendRules, NativeTaskOptions, ProcessRoleName, WaitBound } from "../native/sends/addon.ts";
import { processRole, storedOptionsText } from "../native/sends/sends.ts";
import { sendPriority } from "../send/send.ts";
import type { TraceArgument } from "../native/values/records.ts";
import type {
  ChildSpecCall,
  HandleChange,
  HandleDone,
  NodeResultRead,
  NodeResultsRead,
  NodesRead,
  WorkflowStartOutcome,
  WorkflowStatusRead,
  WorkflowWaitRead,
} from "../native/workflows/addon.ts";
import * as native from "../native/workflows/workflows.ts";
import { AddonOutsideMainThread, ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { childSpecAnswer } from "./child-spec.ts";
import type { CheckedSpec, NodeDeadline } from "./findings.ts";
import type { WorkflowRegistry } from "./registry.ts";
import type { ConversionFacts } from "./spec-data.ts";
import { specDataTextOf } from "./spec-data.ts";
import type { SpecTextRefused } from "./spec-text.ts";
import type { WorkflowsSeatOperation, WorkflowsSeatReply } from "./seat-operations.ts";

/** One start of a prepared spec data text: a new start, or a retry of a start that failed. */
export type StartRequest = {
  readonly specData: string;
  readonly workflowId: string;
  readonly sentAtMicros: bigint;
  readonly trace: TraceArgument;
};

/** The parse of the core refused the spec data; nothing was sent. */
export type SpecParseRefused = { readonly kind: "specRefused"; readonly fieldPath: string; readonly cause: string };

/**
 * Why a spec has no spec data that the core takes:
 * - `textRefused`: a string of the spec has no text (a lone surrogate);
 * - `specRefused`: the core's parse refused the spec data.
 */
export type SpecRefusal = { readonly kind: "textRefused"; readonly refused: SpecTextRefused } | SpecParseRefused;

/** The end of one prepare call: the spec data text that the core's parse takes, or why the spec has none. */
export type SpecPrepare = { readonly kind: "prepared"; readonly specData: string } | SpecRefusal;

/**
 * The end of one start call:
 * - `outcome`: the core's outcome, with the spec data text that it started;
 * - `specRefused`: the core's parse refused the spec data; nothing was sent (a text that `prepare` gave parses).
 */
export type StartCall = { readonly kind: "outcome"; readonly specData: string; readonly outcome: WorkflowStartOutcome } | SpecParseRefused;

/**
 * The core operations of workflow starts and handles. Each is async, so every form has one shape. `Relay` is what a
 * relayed operation adds to its answer: nothing on a main thread (`never`), and `seatRelayFull` in a seat isolate,
 * whose relay sends nothing past its bound. `waitEnd` and `processRole` are never relayed.
 */
export interface WorkflowCoreOf<Relay> {
  /** The spec data of `spec` that the core's parse takes, or why it has none. No I/O. */
  prepare(spec: CheckedSpec): Promise<SpecPrepare | Relay>;
  /** Parse the spec data of `request` and write the tree. */
  start(request: StartRequest): Promise<StartCall | Relay>;
  status(workflowId: string): Promise<WorkflowStatusRead | Relay>;
  nodeResults(workflowId: string): Promise<NodeResultsRead | Relay>;
  nodes(workflowId: string): Promise<NodesRead | Relay>;
  cancel(workflowId: string): Promise<HandleDone | Relay>;
  pause(workflowId: string): Promise<HandleChange | Relay>;
  resume(workflowId: string): Promise<HandleChange | Relay>;
  waitEnd(workflowId: string, wait: WaitBound): Promise<WorkflowWaitRead>;
  nodeResult(workflowId: string, nodeId: string): Promise<NodeResultRead | Relay>;
  /** Whether this process runs task bodies: a wait for a result is refused there. */
  processRole(): Promise<ProcessRoleName>;
}

/** The workflow core of a main thread: the addon's answers. */
export type AddonWorkflowCore = WorkflowCoreOf<never>;

/** The workflow core of a thread: in a seat isolate, a relayed operation can answer `seatRelayFull`. */
export type WorkflowCore = WorkflowCoreOf<SeatRelayFullOutcome>;

/** The facts of the app that the main-thread core reads. */
export type MainThreadFacts = {
  readonly handle: CoreHandle;
  readonly rules: NativeSendRules;
  /** The checked options of each listed task, by task name. */
  readonly options: ReadonlyMap<string, NativeTaskOptions>;
  /** The queues that the app admits: the class of each is read once. */
  readonly queueNames: readonly string[];
  readonly resendOnTransientErr: boolean;
};

/** The conversion facts of a main thread, from the core. Reads the queue classes and priorities once. */
export function addonConversionFacts(facts: MainThreadFacts): ConversionFacts {
  const classes = native.queueRetentionClasses(facts.rules, facts.queueNames);
  const byQueue = new Map(facts.queueNames.map((name, index) => [name, classes[index]]));
  const priorities = native.queuePriorities(facts.rules, facts.queueNames);
  const priorityByQueue = new Map(facts.queueNames.map((name, index) => [name, priorities[index]]));

  return Object.freeze({
    taskOptionsText: (taskName: string, deadline: NodeDeadline): string => {
      const options = facts.options.get(taskName);

      if (options === undefined) {
        throw new ClientDefect({ subject: "taskOptionsText", rule: `the task ${taskName} has no checked options` });
      }

      return deadline.kind === "until"
        ? native.nodeTaskOptionsText(options, deadline.epochMicros)
        : storedOptionsText(options);
    },
    queueClass: (queueName: string): string => {
      const found = byQueue.get(queueName);

      if (found === undefined) {
        throw new ClientDefect({ subject: "queueClass", rule: `the queue ${queueName} is not a queue of the app` });
      }

      return found;
    },
    queuePriority: (queueName: string): number => {
      const found = priorityByQueue.get(queueName);

      if (found === undefined) {
        throw new ClientDefect({ subject: "queuePriority", rule: `the queue ${queueName} is not a queue of the app` });
      }

      return sendPriority(found);
    },
  });
}

/** The core operations on a main thread: the addon on `facts.handle`. */
export function addonWorkflowCore(facts: MainThreadFacts, conversion: ConversionFacts): AddonWorkflowCore {
  const handle = facts.handle;

  const start = async (request: StartRequest): Promise<StartCall> => {
    const prepared = native.prepareClientSpec(request.specData);

    switch (prepared.kind) {
      case "refused":
        return { kind: "specRefused", fieldPath: prepared.fieldPath, cause: prepared.cause };
      case "prepared": {
        const outcome = await native.startWorkflow(
          handle,
          prepared.spec,
          request.workflowId,
          request.sentAtMicros,
          request.trace,
          facts.resendOnTransientErr,
        );

        return { kind: "outcome", specData: request.specData, outcome };
      }

      default:
        return unreachable(prepared, "addonWorkflowCore.start");
    }
  };

  const core: AddonWorkflowCore = {
    prepare: async (spec) => preparedSpec(spec, conversion),
    start,
    status: async (workflowId) => native.workflowStatus(handle, workflowId),
    nodeResults: async (workflowId) => native.workflowNodeResults(handle, workflowId),
    nodes: async (workflowId) => native.workflowNodes(handle, workflowId),
    cancel: async (workflowId) => native.cancelWorkflow(handle, workflowId),
    pause: async (workflowId) => native.pauseWorkflow(handle, workflowId),
    resume: async (workflowId) => native.resumeWorkflow(handle, workflowId),
    waitEnd: async (workflowId, wait) => native.waitWorkflowEnd(handle, workflowId, wait),
    nodeResult: async (workflowId, nodeId) => native.nodeResult(handle, workflowId, nodeId),
    processRole: async () => processRole(),
  };

  return Object.freeze(core);
}

/** The spec data of `spec` and the core's parse of it, with no I/O. Pure apart from the addon's pure parse. */
function preparedSpec(spec: CheckedSpec, conversion: ConversionFacts): SpecPrepare {
  const text = specDataTextOf(spec, conversion);

  if (!text.ok) {
    return { kind: "textRefused", refused: text.error };
  }

  const parsed = native.prepareClientSpec(text.value);

  switch (parsed.kind) {
    case "refused":
      return { kind: "specRefused", fieldPath: parsed.fieldPath, cause: parsed.cause };
    case "prepared":
      return { kind: "prepared", specData: text.value };
    default:
      return unreachable(parsed, "preparedSpec");
  }
}

/** The main-thread side of one seat operation: the same calls as a main-thread call. */
export async function serveWorkflowsSeatOperation(
  core: AddonWorkflowCore,
  operation: WorkflowsSeatOperation,
): Promise<WorkflowsSeatReply> {
  switch (operation.kind) {
    case "prepare":
      return { kind: "prepare", prepared: await core.prepare(operation.spec) };
    case "start":
      return { kind: "start", call: await core.start(operation.request) };
    case "status":
      return { kind: "status", read: await core.status(operation.workflowId) };
    case "nodeResults":
      return { kind: "nodeResults", read: await core.nodeResults(operation.workflowId) };
    case "nodes":
      return { kind: "nodes", read: await core.nodes(operation.workflowId) };
    case "cancel":
      return { kind: "cancel", done: await core.cancel(operation.workflowId) };
    case "pause":
      return { kind: "pause", change: await core.pause(operation.workflowId) };
    case "resume":
      return { kind: "resume", change: await core.resume(operation.workflowId) };
    case "nodeResult":
      return { kind: "nodeResult", read: await core.nodeResult(operation.workflowId, operation.nodeId) };
    default:
      return unreachable(operation, "serveWorkflowsSeatOperation");
  }
}

/**
 * The core operations in a seat isolate: each goes through the relay. A seat isolate runs task bodies, so a wait for
 * a result is refused there before any message (`processRole`), and `waitEnd` is never relayed.
 */
export function relayedWorkflowCore(relay: SeatRelay): WorkflowCore {
  const ask = async (operation: WorkflowsSeatOperation): Promise<WorkflowsSeatReply | SeatRelayFullOutcome> => {
    const answer = await relay.request("workflows", operation);

    return answer.kind === "reply" ? answer.reply : answer;
  };

  const wrong = (asked: WorkflowsSeatOperation["kind"], reply: WorkflowsSeatReply): never => {
    throw new ClientDefect({ subject: "seat relay", rule: `the reply to ${asked} is ${reply.kind}` });
  };

  const core: WorkflowCore = {
    prepare: async (spec) =>
      fullOr(await ask({ kind: "prepare", spec }), (reply) => (reply.kind === "prepare" ? reply.prepared : wrong("prepare", reply))),
    start: async (request) =>
      fullOr(await ask({ kind: "start", request }), (reply) => (reply.kind === "start" ? reply.call : wrong("start", reply))),
    status: async (workflowId) =>
      fullOr(await ask({ kind: "status", workflowId }), (reply) => (reply.kind === "status" ? reply.read : wrong("status", reply))),
    nodeResults: async (workflowId) =>
      fullOr(await ask({ kind: "nodeResults", workflowId }), (reply) =>
        reply.kind === "nodeResults" ? reply.read : wrong("nodeResults", reply),
      ),
    nodes: async (workflowId) =>
      fullOr(await ask({ kind: "nodes", workflowId }), (reply) => (reply.kind === "nodes" ? reply.read : wrong("nodes", reply))),
    cancel: async (workflowId) =>
      fullOr(await ask({ kind: "cancel", workflowId }), (reply) => (reply.kind === "cancel" ? reply.done : wrong("cancel", reply))),
    pause: async (workflowId) =>
      fullOr(await ask({ kind: "pause", workflowId }), (reply) => (reply.kind === "pause" ? reply.change : wrong("pause", reply))),
    resume: async (workflowId) =>
      fullOr(await ask({ kind: "resume", workflowId }), (reply) => (reply.kind === "resume" ? reply.change : wrong("resume", reply))),
    waitEnd: async () => {
      throw new ClientDefect({ subject: "seat relay", rule: "a seat isolate refuses a wait before it asks the core" });
    },
    nodeResult: async (workflowId, nodeId) =>
      fullOr(await ask({ kind: "nodeResult", workflowId, nodeId }), (reply) =>
        reply.kind === "nodeResult" ? reply.read : wrong("nodeResult", reply),
      ),
    processRole: async () => "RUNS_TASK_BODIES",
  };

  return Object.freeze(core);
}

/** `answer` when the relay sent nothing (`seatRelayFull`), else the reply that `read` reads. */
function fullOr<T>(
  answer: WorkflowsSeatReply | SeatRelayFullOutcome,
  read: (reply: WorkflowsSeatReply) => T,
): T | SeatRelayFullOutcome {
  return answer.kind === "seatRelayFull" ? answer : read(answer);
}

/** The core operations in a worker thread with no core: each throws `AddonOutsideMainThread`. */
export function noWorkflowCore(): AddonWorkflowCore {
  const refuse = async (): Promise<never> => {
    throw new AddonOutsideMainThread({ threadId });
  };

  return Object.freeze({
    prepare: refuse,
    start: refuse,
    status: refuse,
    nodeResults: refuse,
    nodes: refuse,
    cancel: refuse,
    pause: refuse,
    resume: refuse,
    waitEnd: refuse,
    nodeResult: refuse,
    processRole: refuse,
  });
}

/**
 * The core operations of an app on a main thread: the conversion facts read once from the core, and the app's child
 * spec source registered on `facts.handle`, so the core's workflow steps in this process build sub-workflow children
 * with the app's builds. Effects: reads the queue classes; registers the source.
 */
export function mainThreadWorkflowCore(facts: MainThreadFacts, registry: WorkflowRegistry): AddonWorkflowCore {
  const conversion = addonConversionFacts(facts);

  const source = takingCallback(
    settlingCallback((call: ChildSpecCall) => childSpecAnswer(registry, conversion, call), addonSettlement),
    addonTake,
  );

  const definitionKeys = registry.workflows.map((listed) => listed.record.definitionKey);
  const registered = native.registerChildSpecSource(facts.handle, source, definitionKeys);

  if (registered === "alreadySet") {
    throw new ClientDefect({ subject: "registerChildSpecSource", rule: "a new core handle holds a child spec source" });
  }

  return addonWorkflowCore(facts, conversion);
}

/** The queues that an app admits: the default queue, or the custom queues. Pure. */
export function admittedQueueNames(queues: QueueSetup): readonly string[] {
  switch (queues.kind) {
    case "default":
      return [DEFAULT_QUEUE_ROUTE.queueName];
    case "custom":
      return [...queues.queues.keys()];
    default:
      return unreachable(queues, "admittedQueueNames");
  }
}

/** The workflow core of a thread's core access: the producer's on a main thread, the relay's in a seat isolate. */
export function workflowCoreOf(access: CoreAccess): WorkflowCore {
  switch (access.kind) {
    case "addon":
      return access.producer.workflows;
    case "seatRelay":
      return relayedWorkflowCore(access.relay);
    case "noCore":
      return noWorkflowCore();
    default:
      return unreachable(access, "workflowCoreOf");
  }
}
