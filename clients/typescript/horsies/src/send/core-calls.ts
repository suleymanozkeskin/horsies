// The core operations of the send and read paths, through the core access of this thread: the addon's producer on a
// main thread, the relay in a seat isolate. A core operation in any other worker thread is a defect.

import { threadId } from "node:worker_threads";

import type { CoreAccess, SeatRelay, SendsSeatOperation, SendsSeatReply } from "../app/core-access.ts";
import type {
  EnqueueOutcome,
  EnqueueRequest,
  PreparedSend,
  SendFacts,
  SendKind,
  TaskInfoRead,
} from "../native/sends/addon.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { AddonOutsideMainThread, ClientDefect } from "../thrown-errors.ts";

/** The prepared facts of one send. Throws `AddonOutsideMainThread` in a worker thread with no core. */
export async function prepareSend(core: CoreAccess, facts: SendFacts): Promise<PreparedSend> {
  switch (core.kind) {
    case "addon":
      return core.producer.prepareSend(facts);
    case "seatRelay": {
      const reply = await relayed(core.relay, { kind: "prepareSend", facts });

      return reply.kind === "prepareSend" ? reply.prepared : wrongReply("prepareSend", reply);
    }

    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    default:
      return unexpectedAccess(core);
  }
}

/** One enqueue. Throws `AddonOutsideMainThread` in a worker thread with no core. */
export async function enqueue(
  core: CoreAccess,
  request: EnqueueRequest,
  sendKind: SendKind,
  trace: TraceArgument,
): Promise<EnqueueOutcome> {
  switch (core.kind) {
    case "addon":
      return core.producer.enqueue(request, sendKind, trace);
    case "seatRelay": {
      const reply = await relayed(core.relay, { kind: "enqueue", request, sendKind, trace });

      return reply.kind === "enqueue" ? reply.outcome : wrongReply("enqueue", reply);
    }

    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    default:
      return unexpectedAccess(core);
  }
}

/** One task info read. Throws `AddonOutsideMainThread` in a worker thread with no core. */
export async function taskInfo(core: CoreAccess, taskId: string): Promise<TaskInfoRead> {
  switch (core.kind) {
    case "addon":
      return core.producer.taskInfo(taskId);
    case "seatRelay": {
      const reply = await relayed(core.relay, { kind: "taskInfo", taskId });

      return reply.kind === "taskInfo" ? reply.read : wrongReply("taskInfo", reply);
    }

    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    default:
      return unexpectedAccess(core);
  }
}

async function relayed(relay: SeatRelay, operation: SendsSeatOperation): Promise<SendsSeatReply> {
  return relay.request("sends", operation);
}

function wrongReply(asked: SendsSeatOperation["kind"], reply: SendsSeatReply): never {
  throw new ClientDefect({ subject: "seat relay", rule: `the reply to ${asked} is ${reply.kind}` });
}

function unexpectedAccess(core: never): never {
  throw new ClientDefect({ subject: "core access", rule: `a core access outside its type: ${String(core)}` });
}
