// How an area reaches the core in this thread: the addon on a main thread, the seat relay in a seat isolate, or no
// core in any other worker thread (where a core operation is a defect).

import { isMainThread, threadId } from "node:worker_threads";

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
import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import type { FacadesSeatOperation, FacadesSeatReply } from "../monitoring/seat-calls.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { AddonOutsideMainThread, ClientDefect } from "../thrown-errors.ts";

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
  /** The monitoring and health reads, the database ping, the cancel and the rerun (`monitoring/seat-calls.ts`). */
  readonly facades: { readonly operation: FacadesSeatOperation; readonly reply: FacadesSeatReply };
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

/**
 * The answer to a `SendsSeatOperation`, of the same `kind`; or, to a prepare past the bound of the seat relay,
 * `seatRelayFull` with the text that names the bound (nothing was sent).
 */
export type SendsSeatReply =
  | { readonly kind: "prepareSend"; readonly prepared: PreparedSend }
  | { readonly kind: "seatRelayFull"; readonly message: string }
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

/** The thread of a call: the main thread of the process, or a `worker_threads` isolate (with its id). */
export type CallThread = { readonly kind: "main" } | { readonly kind: "worker"; readonly threadId: number };

/**
 * The core handle that `Horsies.create` built for the app, for each part that calls the addon with it: the facades,
 * the worker run, the live check.
 * - Off the main thread, `AddonOutsideMainThread`: the addon runs on the main thread of a process only.
 * - On the main thread, a core access that is not the addon is a defect (`ClientDefect`): `Horsies.create` gives
 *   every app of a main thread the addon core.
 */
export function appCoreHandle(core: CoreAccess): CoreHandle {
  return coreHandleOnThread(core, isMainThread ? { kind: "main" } : { kind: "worker", threadId });
}

/** The app's core handle of `core` for a call on `thread`; the refusals of `appCoreHandle`. */
export function coreHandleOnThread(core: CoreAccess, thread: CallThread): CoreHandle {
  switch (thread.kind) {
    case "worker":
      throw new AddonOutsideMainThread({ threadId: thread.threadId });
    case "main":
      return mainThreadCoreHandle(core);
    default:
      return unreachable(thread);
  }
}

/** The handle of the addon core of a main thread; another core access there is a defect. */
function mainThreadCoreHandle(core: CoreAccess): CoreHandle {
  switch (core.kind) {
    case "addon":
      return core.handle;
    case "seatRelay":
    case "noCore":
      return raiseDefect(
        new ClientDefect({
          subject: "appCoreHandle",
          rule: `an app on the main thread holds the addon core, not the ${core.kind} access`,
        }),
      );
    default:
      return unreachable(core);
  }
}
