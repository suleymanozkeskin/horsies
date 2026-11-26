// The core operations of the send and read paths, through the core access of this thread: the addon's producer on a
// main thread, the relay in a seat isolate. A core operation in any other worker thread is a defect. In a seat isolate
// each operation can answer `seatRelayFull`: the relay held its bound and sent nothing.

import { threadId } from "node:worker_threads";

import type { CoreAccess, RelayAnswer, SeatRelay, SendsSeatOperation, SendsSeatReply } from "../app/core-access.ts";
import type {
  EnqueueOutcome,
  EnqueueRequest,
  PreparedSend,
  SendFacts,
  SendKind,
  TaskInfoRead,
} from "../native/sends/addon.ts";
import type { TraceArgument } from "../native/values/records.ts";
import type { SeatRelayFullOutcome } from "../seats/relay-full.ts";
import { AddonOutsideMainThread, ClientDefect } from "../thrown-errors.ts";

/** The answer to a prepare: the core's prepared facts, or the seat relay at its bound (nothing was sent). */
export type PrepareAnswer = PreparedSend | SeatRelayFullOutcome;

/** The answer to an enqueue: the core's outcome, or the seat relay at its bound (nothing was written). */
export type EnqueueAnswer = EnqueueOutcome | SeatRelayFullOutcome;

/** The answer to a task info read: the core's read, or the seat relay at its bound (nothing was read). */
export type TaskInfoAnswer = TaskInfoRead | SeatRelayFullOutcome;

/** The prepared facts of one send of `taskName`. Throws `AddonOutsideMainThread` in a worker thread with no core. */
export async function prepareSend(core: CoreAccess, taskName: string, facts: SendFacts): Promise<PrepareAnswer> {
  switch (core.kind) {
    case "addon":
      return core.producer.prepareSend(taskName, facts);
    case "seatRelay": {
      const answer = await relayed(core.relay, { kind: "prepareSend", taskName, facts });

      switch (answer.kind) {
        case "reply":
          return answer.reply.kind === "prepareSend" ? answer.reply.prepared : wrongReply("prepareSend", answer.reply);
        case "seatRelayFull":
          return answer;
        default:
          return unexpectedAccess(answer);
      }
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
): Promise<EnqueueAnswer> {
  switch (core.kind) {
    case "addon":
      return core.producer.enqueue(request, sendKind, trace);
    case "seatRelay": {
      const answer = await relayed(core.relay, { kind: "enqueue", request, sendKind, trace });

      switch (answer.kind) {
        case "reply":
          return answer.reply.kind === "enqueue" ? answer.reply.outcome : wrongReply("enqueue", answer.reply);
        case "seatRelayFull":
          return answer;
        default:
          return unexpectedAccess(answer);
      }
    }

    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    default:
      return unexpectedAccess(core);
  }
}

/** One task info read. Throws `AddonOutsideMainThread` in a worker thread with no core. */
export async function taskInfo(core: CoreAccess, taskId: string): Promise<TaskInfoAnswer> {
  switch (core.kind) {
    case "addon":
      return core.producer.taskInfo(taskId);
    case "seatRelay": {
      const answer = await relayed(core.relay, { kind: "taskInfo", taskId });

      switch (answer.kind) {
        case "reply":
          return answer.reply.kind === "taskInfo" ? answer.reply.read : wrongReply("taskInfo", answer.reply);
        case "seatRelayFull":
          return answer;
        default:
          return unexpectedAccess(answer);
      }
    }

    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    default:
      return unexpectedAccess(core);
  }
}

async function relayed(relay: SeatRelay, operation: SendsSeatOperation): Promise<RelayAnswer<SendsSeatReply>> {
  return relay.request("sends", operation);
}

function wrongReply(asked: SendsSeatOperation["kind"], reply: SendsSeatReply): never {
  throw new ClientDefect({ subject: "seat relay", rule: `the reply to ${asked} is ${reply.kind}` });
}

function unexpectedAccess(core: never): never {
  throw new ClientDefect({ subject: "core access", rule: `a core access outside its type: ${String(core)}` });
}
