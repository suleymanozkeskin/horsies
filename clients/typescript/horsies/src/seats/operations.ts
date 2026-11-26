// The operations of the seat isolates, served on the main thread of a `SHARED_THREAD` child: each area's operation
// runs on the main thread's own core of the app, as the same call of that thread would run it.
//
// - `sends`: the prepared facts of a send, an enqueue, a task info read (`ProducerCore`).
// - `workflows`: a workflow start, restart, read or handle change (`ProducerCore.workflows`).
// - A core call that throws a `CoreError` (a process-state refusal) answers `refused` with the facts of the error; the
//   relay of the isolate throws the same class. A defect of the package rejects: the pool gives it to the defect rule.

import type { CoreAccess, ProducerCore, SendsSeatOperation, SendsSeatReply } from "../app/core-access.ts";
import {
  ClientDefect,
  CoreError,
  type CoreStartFailed,
  type CoreStopped,
  type ForkedAfterCoreStart,
  type TooManyPendingCalls,
} from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { serveWorkflowsSeatOperation } from "../workflow/core.ts";
import type { RelayedRefusal, SeatAnswer, SeatReply, SeatRequest } from "./protocol.ts";

/** Run `request` on the core of the main thread, `core`, and give its answer. Rejects only with a defect. */
export async function serveSeatOperation(core: CoreAccess, request: SeatRequest): Promise<SeatAnswer> {
  const producer = producerOf(core);

  try {
    return { kind: "reply", reply: await areaReply(producer, request) };
  } catch (cause) {
    if (cause instanceof CoreError) {
      return { kind: "refused", refusal: relayedRefusal(cause) };
    }

    throw cause;
  }
}

/** The producer of the main thread's app. Another core access is a defect: a child's main thread holds the addon. */
function producerOf(core: CoreAccess): ProducerCore {
  switch (core.kind) {
    case "addon":
      return core.producer;
    case "seatRelay":
    case "noCore":
      throw new ClientDefect({ subject: "seat operation", rule: "the main thread of a child reaches the core through the addon" });
    default:
      return unreachable(core, "producerOf");
  }
}

async function areaReply(producer: ProducerCore, request: SeatRequest): Promise<SeatReply> {
  const area = request.area;

  switch (area) {
    case "sends":
      return { area, reply: await sendsReply(producer, request.operation) };
    case "workflows":
      return { area, reply: await serveWorkflowsSeatOperation(producer.workflows, request.operation) };
    default:
      return unreachable(area, "areaReply");
  }
}

async function sendsReply(producer: ProducerCore, operation: SendsSeatOperation): Promise<SendsSeatReply> {
  switch (operation.kind) {
    case "prepareSend":
      return { kind: "prepareSend", prepared: await producer.prepareSend(operation.taskName, operation.facts) };
    case "enqueue":
      return {
        kind: "enqueue",
        outcome: await producer.enqueue(operation.request, operation.sendKind, operation.trace),
      };
    case "taskInfo":
      return { kind: "taskInfo", read: await producer.taskInfo(operation.taskId) };
    default:
      return unreachable(operation, "sendsReply");
  }
}

/** The facts of `error`, by its code. Pure. */
export function relayedRefusal(error: CoreError): RelayedRefusal {
  switch (error.code) {
    case "CORE_STOPPED": {
      // SAFETY: CORE_STOPPED is the code of `CoreStopped` only.
      const stopped = error as CoreStopped;

      return { code: error.code, subject: stopped.subject, cause: stopped.cause, stateAfter: stopped.stateAfter };
    }

    case "CORE_START_FAILED":
      // SAFETY: CORE_START_FAILED is the code of `CoreStartFailed` only.
      return { code: error.code, cause: (error as CoreStartFailed).cause };
    case "TOO_MANY_PENDING_CALLS": {
      // SAFETY: TOO_MANY_PENDING_CALLS is the code of `TooManyPendingCalls` only.
      const pending = error as TooManyPendingCalls;

      return { code: error.code, subject: pending.subject, bound: pending.bound };
    }

    case "FORKED_AFTER_CORE_START": {
      // SAFETY: FORKED_AFTER_CORE_START is the code of `ForkedAfterCoreStart` only.
      const forked = error as ForkedAfterCoreStart;

      return { code: error.code, corePid: forked.corePid, thisPid: forked.thisPid };
    }

    default:
      return unreachable(error.code, "relayedRefusal");
  }
}
