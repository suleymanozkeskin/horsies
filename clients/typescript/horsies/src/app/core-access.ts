// How an area reaches the core in this thread: the addon on a main thread, the seat relay in a seat isolate, or no
// core in any other worker thread (where a core operation is a defect).

import { threadId } from "node:worker_threads";

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
import { unreachable } from "../native/call/never.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { AddonOutsideMainThread } from "../thrown-errors.ts";

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
 * How this thread reaches the core. On a main thread, `handle` is the app's core handle (the facades call the addon
 * with it) and `producer` runs the sends and the task reads on the same handle.
 */
export type CoreAccess =
  | { readonly kind: "addon"; readonly handle: CoreHandle; readonly producer: ProducerCore }
  | { readonly kind: "seatRelay"; readonly relay: SeatRelay }
  | { readonly kind: "noCore" };

/**
 * The app's core handle of a main thread, for an area that calls the addon with it (the facades). A seat isolate and
 * any other worker thread have none: `AddonOutsideMainThread`.
 */
export function appCoreHandle(core: CoreAccess): CoreHandle {
  switch (core.kind) {
    case "addon":
      return core.handle;
    case "seatRelay":
    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    default:
      return unreachable(core);
  }
}
