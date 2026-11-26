// The core operations of workflow starts and handles in this thread: the addon on a main thread, the seat relay in a
// seat isolate, or no core in any other worker thread (where an operation is a defect).
//
// A start sends a checked spec: on a main thread the conversion to spec data (with the core's facts), the core's parse
// and the start run here; a seat isolate sends the checked spec to its child's main thread, which runs the same steps.

import { threadId } from "node:worker_threads";

import { DEFAULT_QUEUE_ROUTE } from "../app/contract-list.ts";
import type { CoreAccess, SeatRelay } from "../app/core-access.ts";
import type { QueueSetup } from "../config/queues.ts";
import { addonSettlement, settlingCallback } from "../native/call/callback.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { NativeSendRules, NativeTaskOptions, ProcessRoleName, WaitBound } from "../native/sends/addon.ts";
import { processRole, storedOptionsText } from "../native/sends/sends.ts";
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

/** One start of a checked spec. */
export type StartRequest = {
  readonly spec: CheckedSpec;
  readonly workflowId: string;
  readonly sentAtMicros: bigint;
  readonly trace: TraceArgument;
};

/** One start of a spec data text again (a retry of a start that failed). */
export type RestartRequest = {
  readonly specData: string;
  readonly workflowId: string;
  readonly sentAtMicros: bigint;
  readonly trace: TraceArgument;
};

/**
 * The end of one start call:
 * - `outcome`: the core's outcome, with the spec data text that it started;
 * - `textRefused`: a string of the spec has no text (a lone surrogate); nothing was sent;
 * - `specRefused`: the core's parse refused the spec data; nothing was sent.
 */
export type StartCall =
  | { readonly kind: "outcome"; readonly specData: string; readonly outcome: WorkflowStartOutcome }
  | { readonly kind: "textRefused"; readonly refused: SpecTextRefused }
  | { readonly kind: "specRefused"; readonly fieldPath: string; readonly cause: string };

/** The core operations of workflow starts and handles. Each is async, so every form has one shape. */
export interface WorkflowCore {
  start(request: StartRequest): Promise<StartCall>;
  restart(request: RestartRequest): Promise<StartCall>;
  status(workflowId: string): Promise<WorkflowStatusRead>;
  nodeResults(workflowId: string): Promise<NodeResultsRead>;
  nodes(workflowId: string): Promise<NodesRead>;
  cancel(workflowId: string): Promise<HandleDone>;
  pause(workflowId: string): Promise<HandleChange>;
  resume(workflowId: string): Promise<HandleChange>;
  waitEnd(workflowId: string, wait: WaitBound): Promise<WorkflowWaitRead>;
  nodeResult(workflowId: string, nodeId: string): Promise<NodeResultRead>;
  /** Whether this process runs task bodies: a wait for a result is refused there. */
  processRole(): Promise<ProcessRoleName>;
}

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

/** The conversion facts of a main thread, from the core. Reads the queue classes once. */
export function addonConversionFacts(facts: MainThreadFacts): ConversionFacts {
  const classes = native.queueRetentionClasses(facts.rules, facts.queueNames);
  const byQueue = new Map(facts.queueNames.map((name, index) => [name, classes[index]]));

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
  });
}

/** The core operations on a main thread: the addon on `facts.handle`. */
export function addonWorkflowCore(facts: MainThreadFacts, conversion: ConversionFacts): WorkflowCore {
  const handle = facts.handle;

  const startText = async (specData: string, request: Omit<RestartRequest, "specData">): Promise<StartCall> => {
    const prepared = native.prepareClientSpec(specData);

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

        return { kind: "outcome", specData, outcome };
      }

      default:
        return unreachable(prepared, "startText");
    }
  };

  const core: WorkflowCore = {
    start: async (request) => {
      const text = specDataTextOf(request.spec, conversion);

      return text.ok ? startText(text.value, request) : { kind: "textRefused", refused: text.error };
    },
    restart: async (request) => startText(request.specData, request),
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

/** The main-thread side of one seat operation: the same calls as a main-thread call. */
export async function serveWorkflowsSeatOperation(
  core: WorkflowCore,
  operation: WorkflowsSeatOperation,
): Promise<WorkflowsSeatReply> {
  switch (operation.kind) {
    case "start":
      return { kind: "start", call: await core.start(operation.request) };
    case "restart":
      return { kind: "restart", call: await core.restart(operation.request) };
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
  const ask = async (operation: WorkflowsSeatOperation): Promise<WorkflowsSeatReply> =>
    relay.request("workflows", operation);

  const wrong = (asked: WorkflowsSeatOperation["kind"], reply: WorkflowsSeatReply): never => {
    throw new ClientDefect({ subject: "seat relay", rule: `the reply to ${asked} is ${reply.kind}` });
  };

  const core: WorkflowCore = {
    start: async (request) => {
      const reply = await ask({ kind: "start", request });

      return reply.kind === "start" ? reply.call : wrong("start", reply);
    },
    restart: async (request) => {
      const reply = await ask({ kind: "restart", request });

      return reply.kind === "restart" ? reply.call : wrong("restart", reply);
    },
    status: async (workflowId) => {
      const reply = await ask({ kind: "status", workflowId });

      return reply.kind === "status" ? reply.read : wrong("status", reply);
    },
    nodeResults: async (workflowId) => {
      const reply = await ask({ kind: "nodeResults", workflowId });

      return reply.kind === "nodeResults" ? reply.read : wrong("nodeResults", reply);
    },
    nodes: async (workflowId) => {
      const reply = await ask({ kind: "nodes", workflowId });

      return reply.kind === "nodes" ? reply.read : wrong("nodes", reply);
    },
    cancel: async (workflowId) => {
      const reply = await ask({ kind: "cancel", workflowId });

      return reply.kind === "cancel" ? reply.done : wrong("cancel", reply);
    },
    pause: async (workflowId) => {
      const reply = await ask({ kind: "pause", workflowId });

      return reply.kind === "pause" ? reply.change : wrong("pause", reply);
    },
    resume: async (workflowId) => {
      const reply = await ask({ kind: "resume", workflowId });

      return reply.kind === "resume" ? reply.change : wrong("resume", reply);
    },
    waitEnd: async () => {
      throw new ClientDefect({ subject: "seat relay", rule: "a seat isolate refuses a wait before it asks the core" });
    },
    nodeResult: async (workflowId, nodeId) => {
      const reply = await ask({ kind: "nodeResult", workflowId, nodeId });

      return reply.kind === "nodeResult" ? reply.read : wrong("nodeResult", reply);
    },
    processRole: async () => "RUNS_TASK_BODIES",
  };

  return Object.freeze(core);
}

/** The core operations in a worker thread with no core: each throws `AddonOutsideMainThread`. */
export function noWorkflowCore(): WorkflowCore {
  const refuse = async (): Promise<never> => {
    throw new AddonOutsideMainThread({ threadId });
  };

  return Object.freeze({
    start: refuse,
    restart: refuse,
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
export function mainThreadWorkflowCore(facts: MainThreadFacts, registry: WorkflowRegistry): WorkflowCore {
  const conversion = addonConversionFacts(facts);

  const source = settlingCallback(
    (call: ChildSpecCall) => childSpecAnswer(registry, conversion, call),
    addonSettlement,
  );

  const registered = native.registerChildSpecSource(facts.handle, source);

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
