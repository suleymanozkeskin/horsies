// The relay of a seat isolate: each core operation of the isolate's app (a send, a task read, a facade call) becomes
// one `op` message to the main thread of its child, and the `opReply` of that message its answer. The isolate never
// loads the addon. The bounds come from the core's constants, which the main thread gives in the seat marker
// (`RelayBounds`); they hold per isolate, across its runs.
//
// - An operation that waits for I/O of the main thread (an enqueue, a task read, a workflow call, a facade call): at
//   most `opsInFlight` of them wait for the main thread at once. Later ones wait in the isolate, in order, at most
//   `opsWaiting`.
// - An operation that the main thread answers at once with no I/O (the prepare of a send) goes at once and takes no
//   place of the in-flight bound. At most `opsWaiting` prepares wait for their answer.
// - One past a bound is not sent: its answer is `seatRelayFull` at once (`lanes.ts`), and the caller gives
//   `SEAT_RELAY_FULL` through the error union of its call.
// - A refused answer (the core call threw a `CoreError` on the main thread) rejects with that class and its facts.
// - The counts change only after the post of an operation: a post that throws takes no place.
// - A reply of another area than the request's, and an answer to an operation that does not wait, are defects of the
//   package: the defect rule (the main thread of the child aborts), then the throw.

import type { RelayAnswer, SeatOperationMap, SeatRelay } from "../app/core-access.ts";
import { thrownError } from "../native/call/envelope.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type OperationLane, operationLane, relayFullAnswer } from "./lanes.ts";
import {
  type RelayBounds,
  SEAT_PROTOCOL_VERSION,
  type SeatAnswer,
  type SeatArea,
  type SeatRequest,
  type SeatToMain,
} from "./protocol.ts";
import type { SeatRelayFullOutcome } from "./relay-full.ts";

/** The largest request id: ids are whole numbers, and a JavaScript number holds them exactly up to this bound. */
const MAX_REQUEST_ID: number = Number.MAX_SAFE_INTEGER;

/** The answer of one operation: the main thread's answer, or the relay's own past a bound (nothing was sent). */
type OperationAnswer = SeatAnswer | SeatRelayFullOutcome;

/** One operation that waits for its answer. */
type PendingOperation = { readonly request: SeatRequest; readonly answer: (answer: OperationAnswer) => void };

/** One sent operation and its lane. */
type SentOperation = { readonly operation: PendingOperation; readonly lane: OperationLane };

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
  readonly #bounds: RelayBounds;
  /** Every operation sent and not answered, by its request id, with its lane. */
  readonly #sent = new Map<number, SentOperation>();
  readonly #waiting: OperationQueue;
  /** The sent operations that wait for I/O of the main thread. */
  #inFlight = 0;
  /** The sent prepares that wait for their answer. */
  #prepares = 0;
  #lastRequestId = 0;

  constructor(post: (message: SeatToMain) => void, bounds: RelayBounds) {
    this.#post = post;
    this.#bounds = bounds;
    this.#waiting = new OperationQueue(bounds.opsWaiting);
  }

  /** Run `operation` of `area` on the main thread of the child, and give its reply, or `seatRelayFull` past a bound. */
  async request<A extends SeatArea>(
    area: A,
    operation: SeatOperationMap[A]["operation"],
  ): Promise<RelayAnswer<SeatOperationMap[A]["reply"]>> {
    // SAFETY: `operation` is an operation of `area`, so the pair is a member of the request union.
    const request = { area, operation } as SeatRequest;

    const answer = await new Promise<OperationAnswer>((resolve) => {
      this.#take({ request, answer: resolve });
    });

    return relayAnswerOf(area, answer);
  }

  /** The main thread answered the operation `requestId`. The next waiting operation goes. */
  settle(requestId: number, answer: SeatAnswer): void {
    const sent = this.#sent.get(requestId);

    if (sent === undefined) {
      raiseDefect(new ClientDefect({ subject: "seat relay", rule: `the main thread answered the operation ${String(requestId)}, which does not wait` }));
    }

    this.#sent.delete(requestId);
    sent.operation.answer(answer);

    switch (sent.lane) {
      case "answeredAtOnce":
        this.#prepares -= 1;

        return;
      case "waitsForIo":
        this.#inFlight -= 1;
        this.#sendNext();

        return;
      default:
        unreachable(sent.lane, "IsolateRelay.settle");
    }
  }

  /** The operations that wait for I/O of the main thread, those that wait in the isolate, and every sent one. */
  size(): { readonly inFlight: number; readonly waiting: number; readonly sent: number } {
    return { inFlight: this.#inFlight, waiting: this.#waiting.size(), sent: this.#sent.size };
  }

  #sendNext(): void {
    const next = this.#waiting.take();

    switch (next.kind) {
      case "operation":
        this.#send(next.operation, "waitsForIo");

        return;
      case "empty":
        return;
      default:
        unreachable(next, "IsolateRelay.sendNext");
    }
  }

  #take(operation: PendingOperation): void {
    const lane = operationLane(operation.request);

    switch (lane) {
      case "answeredAtOnce":
        if (this.#prepares >= this.#bounds.opsWaiting) {
          operation.answer(relayFullAnswer(lane, this.#bounds.opsWaiting));

          return;
        }

        this.#send(operation, lane);

        return;
      case "waitsForIo":
        this.#takeBounded(operation);

        return;
      default:
        unreachable(lane, "IsolateRelay.take");
    }
  }

  /** Send an operation that waits for I/O, or queue it, or answer it with `seatRelayFull` past the bound. */
  #takeBounded(operation: PendingOperation): void {
    if (this.#inFlight < this.#bounds.opsInFlight) {
      this.#send(operation, "waitsForIo");

      return;
    }

    const put = this.#waiting.put(operation);

    switch (put) {
      case "queued":
        return;
      case "full":
        operation.answer(relayFullAnswer("waitsForIo", this.#bounds.opsWaiting));

        return;
      default:
        unreachable(put, "IsolateRelay.takeBounded");
    }
  }

  /** Post `operation`, then count it: a post that throws takes no place. */
  #send(operation: PendingOperation, lane: OperationLane): void {
    if (this.#lastRequestId >= MAX_REQUEST_ID) {
      raiseDefect(new ClientDefect({ subject: "seat relay", rule: "the request ids of the isolate are used up" }));
    }

    const requestId = this.#lastRequestId + 1;

    this.#post({ kind: "op", protocol: SEAT_PROTOCOL_VERSION, requestId, request: operation.request });
    this.#lastRequestId = requestId;
    this.#sent.set(requestId, { operation, lane });

    switch (lane) {
      case "waitsForIo":
        this.#inFlight += 1;

        return;
      case "answeredAtOnce":
        this.#prepares += 1;

        return;
      default:
        unreachable(lane, "IsolateRelay.send");
    }
  }
}

/**
 * The reply of `area` in `answer`, the relay's `seatRelayFull`, or the throw of a refused call. A reply of another area
 * is a defect.
 */
function relayAnswerOf<A extends SeatArea>(area: A, answer: OperationAnswer): RelayAnswer<SeatOperationMap[A]["reply"]> {
  switch (answer.kind) {
    case "reply":
      if (answer.reply.area !== area) {
        raiseDefect(new ClientDefect({ subject: "seat relay", rule: `the reply to an operation of ${area} is a reply of ${answer.reply.area}` }));
      }

      // SAFETY: the reply is of `area`, the area of the request.
      return { kind: "reply", reply: answer.reply.reply as SeatOperationMap[A]["reply"] };
    case "refused":
      throw thrownError(answer.refusal);
    case "seatRelayFull":
      return answer;
    default:
      return unreachable(answer, "seat relay");
  }
}
