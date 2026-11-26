// How an area reaches the core in this thread: the addon on a main thread, the seat relay in a seat isolate, or no
// core in any other worker thread (where a core operation is a defect).

import type {
  EnqueueOutcome,
  EnqueueRequest,
  PreparedSend,
  ProcessRoleName,
  ResultRead,
  SendFacts,
  SendKind,
  TaskInfoRead,
  WaitBound,
} from "../native/sends/addon.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { TraceArgument } from "../native/values/records.ts";
import type { WorkflowCore } from "../workflow/core.ts";

/**
 * The core operations of sends and task reads. On a main thread the addon runs them; in a seat isolate the relay
 * sends each to the main thread of its child, whose app holds the same contracts. Each operation is async, so both
 * forms have one shape. Values that cross are plain data.
 */
export interface ProducerCore {
  /** The prepared facts of one send of the listed task `taskName`, with the task's checked options. */
  prepareSend(taskName: string, facts: SendFacts): Promise<PreparedSend>;
  enqueue(request: EnqueueRequest, sendKind: SendKind, trace: TraceArgument): Promise<EnqueueOutcome>;
  getResult(taskId: string, wait: WaitBound): Promise<ResultRead>;
  taskInfo(taskId: string): Promise<TaskInfoRead>;
  processRole(): Promise<ProcessRoleName>;
  /** The core operations of workflow starts and handles. */
  readonly workflows: WorkflowCore;
}

/**
 * The operations of each area that a seat isolate sends to its child's main thread, by area name. An area adds its
 * entry in its own file (declaration merging); the seats package serves each one on the main thread.
 */
export interface SeatOperationMap {
  readonly sends: { readonly operation: SendsSeatOperation; readonly reply: SendsSeatReply };
}

/** One send or task read that a seat isolate asks for. */
export type SendsSeatOperation =
  | { readonly kind: "prepareSend"; readonly taskName: string; readonly facts: SendFacts }
  | {
      readonly kind: "enqueue";
      readonly request: EnqueueRequest;
      readonly sendKind: SendKind;
      readonly trace: TraceArgument;
    }
  | { readonly kind: "taskInfo"; readonly taskId: string };

/** The answer to a `SendsSeatOperation`, of the same `kind`. */
export type SendsSeatReply =
  | { readonly kind: "prepareSend"; readonly prepared: PreparedSend }
  | { readonly kind: "enqueue"; readonly outcome: EnqueueOutcome }
  | { readonly kind: "taskInfo"; readonly read: TaskInfoRead };

/** The channel of a seat isolate to its child's main thread. */
export interface SeatRelay {
  request<A extends keyof SeatOperationMap>(
    area: A,
    operation: SeatOperationMap[A]["operation"],
  ): Promise<SeatOperationMap[A]["reply"]>;
}

/**
 * The core of an app on a main thread: its send and read operations, and the one core handle that `Horsies.create`
 * built for the app. The worker run and the live check use that handle, so the child spec source that the app
 * registers on it is the one that the worker serves.
 */
export type AddonCore = { readonly producer: ProducerCore; readonly handle: CoreHandle };

/** How this thread reaches the core. */
export type CoreAccess =
  | ({ readonly kind: "addon" } & AddonCore)
  | { readonly kind: "seatRelay"; readonly relay: SeatRelay }
  | { readonly kind: "noCore" };
