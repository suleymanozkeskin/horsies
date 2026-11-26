// The relay of a seat isolate: each core operation of the isolate's app (a send, a task read) becomes one `op` message
// to the main thread of its child, and the `opReply` of that message its answer. The isolate never loads the addon.
//
// - At most `MAX_SEAT_OPS_IN_FLIGHT` operations of an isolate wait for the main thread at once. Later operations wait
//   in the isolate, in order, at most `MAX_SEAT_OPS_WAITING`. An operation past that bound is not sent: it gets the
//   transient failure of its own type at once (`bound-answers.ts`).
// - A refused answer (the core call threw a `CoreError` on the main thread) rejects with that class and its facts.
// - A reply of another area than the request's, and an answer to an operation that does not wait, are defects of the
//   package: `ClientDefect`.

import type { SeatOperationMap, SeatRelay } from "../app/core-access.ts";
import { thrownError } from "../native/call/envelope.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { boundAnswer } from "./bound-answers.ts";
import { SEAT_PROTOCOL_VERSION, type SeatAnswer, type SeatArea, type SeatRequest, type SeatToMain } from "./protocol.ts";

/**
 * The most operations of one isolate that wait for the main thread at once: the core's default task-send pool of a
 * child (`DEFAULT_TASK_SEND_POOL_CONNECTIONS`, 2). The start facts of a child do not give its own pool size.
 */
export const MAX_SEAT_OPS_IN_FLIGHT = 2;

/** The most operations that wait in one isolate: the core's bound of the work of one run (`MAX_RUN_WORK`). */
export const MAX_SEAT_OPS_WAITING = 10_000;

/** The largest request id: ids are whole numbers, and a JavaScript number holds them exactly up to this bound. */
const MAX_REQUEST_ID: number = Number.MAX_SAFE_INTEGER;

/** One operation that waits for its answer. */
type PendingOperation = { readonly request: SeatRequest; readonly answer: (answer: SeatAnswer) => void };

/** How a queue took an operation. */
type QueuePut = "queued" | "full";

/** The next operation of a queue, or none. */
type QueueTake = { readonly kind: "operation"; readonly operation: PendingOperation } | { readonly kind: "empty" };

/** A queue of at most `bound` operations, first in, first out. */
class OperationQueue {
  readonly #items: PendingOperation[] = [];
  readonly #bound: number;
  #head = 0;

  constructor(bound: number) {
    this.#bound = bound;
  }

  put(operation: PendingOperation): QueuePut {
    if (this.#items.length - this.#head >= this.#bound) {
      return "full";
    }

    this.#items.push(operation);

    return "queued";
  }

  take(): QueueTake {
    const operation = this.#items[this.#head];

    if (operation === undefined) {
      return { kind: "empty" };
    }

    this.#head += 1;

    // Drop the taken part once it is the larger part, so the array does not grow past twice the bound.
    if (this.#head * 2 >= this.#items.length) {
      this.#items.splice(0, this.#head);
      this.#head = 0;
    }

    return { kind: "operation", operation };
  }

  size(): number {
    return this.#items.length - this.#head;
  }
}

/** The relay of one seat isolate. `post` sends a message to the main thread of the child. */
export class IsolateRelay implements SeatRelay {
  readonly #post: (message: SeatToMain) => void;
  readonly #inFlight = new Map<number, PendingOperation>();
  readonly #waiting = new OperationQueue(MAX_SEAT_OPS_WAITING);
  #lastRequestId = 0;

  constructor(post: (message: SeatToMain) => void) {
    this.#post = post;
  }

  /** Run `operation` of `area` on the main thread of the child, and give its reply. */
  async request<A extends SeatArea>(area: A, operation: SeatOperationMap[A]["operation"]): Promise<SeatOperationMap[A]["reply"]> {
    // SAFETY: `operation` is an operation of `area`, so the pair is a member of the request union.
    const request = { area, operation } as SeatRequest;

    const answer = await new Promise<SeatAnswer>((resolve) => {
      this.#take({ request, answer: resolve });
    });

    return replyOf(area, answer);
  }

  /** The main thread answered the operation `requestId`. The next waiting operation goes. */
  settle(requestId: number, answer: SeatAnswer): void {
    const operation = this.#inFlight.get(requestId);

    if (operation === undefined) {
      throw new ClientDefect({ subject: "seat relay", rule: `the main thread answered the operation ${String(requestId)}, which does not wait` });
    }

    this.#inFlight.delete(requestId);
    operation.answer(answer);

    const next = this.#waiting.take();

    switch (next.kind) {
      case "operation":
        this.#send(next.operation);

        return;
      case "empty":
        return;
      default:
        unreachable(next, "IsolateRelay.settle");
    }
  }

  /** The operations that wait for the main thread, and those that wait in the isolate. */
  size(): { readonly inFlight: number; readonly waiting: number } {
    return { inFlight: this.#inFlight.size, waiting: this.#waiting.size() };
  }

  #take(operation: PendingOperation): void {
    if (this.#inFlight.size < MAX_SEAT_OPS_IN_FLIGHT) {
      this.#send(operation);

      return;
    }

    const put = this.#waiting.put(operation);

    switch (put) {
      case "queued":
        return;
      case "full":
        operation.answer(boundAnswer(operation.request, MAX_SEAT_OPS_WAITING));

        return;
      default:
        unreachable(put, "IsolateRelay.take");
    }
  }

  #send(operation: PendingOperation): void {
    if (this.#lastRequestId >= MAX_REQUEST_ID) {
      throw new ClientDefect({ subject: "seat relay", rule: "the request ids of the isolate are used up" });
    }

    this.#lastRequestId += 1;
    this.#inFlight.set(this.#lastRequestId, operation);
    this.#post({ kind: "op", protocol: SEAT_PROTOCOL_VERSION, requestId: this.#lastRequestId, request: operation.request });
  }
}

/** The reply of `area` in `answer`, or the throw of a refused call. A reply of another area is a defect. */
function replyOf<A extends SeatArea>(area: A, answer: SeatAnswer): SeatOperationMap[A]["reply"] {
  switch (answer.kind) {
    case "reply":
      if (answer.reply.area !== area) {
        throw new ClientDefect({ subject: "seat relay", rule: `the reply to an operation of ${area} is a reply of ${answer.reply.area}` });
      }

      // SAFETY: the reply is of `area`, the area of the request.
      return answer.reply.reply as SeatOperationMap[A]["reply"];
    case "refused":
      throw thrownError(answer.refusal);
    default:
      return unreachable(answer, "seat relay");
  }
}
