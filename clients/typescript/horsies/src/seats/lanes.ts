// How the relay of a seat isolate sends each operation, and the answer to an operation past a bound of the relay.
//
// - An operation that waits for I/O of the main thread (an enqueue, a task read, a workflow call, a facade call) takes
//   a place of the in-flight bound and of the waiting bound (`opsWaiting`, the core's `MAX_RUN_WORK`).
// - The prepare of a send does no I/O: the main thread computes the facts of a send at once. It goes at once and takes
//   no place of the in-flight bound, but the prepares that wait for their answer have their own bound of the same
//   value (`opsWaiting`).
// - A bound that is hit is an expected failure, and a full relay is one state: the operation is not sent, so nothing is
//   read or written, and it may run again. Every operation past a bound gets the one answer `SEAT_RELAY_FULL`,
//   retryable, whose text names the bound and its value (`relay-full.ts`).

import { unreachable } from "../wire/unreachable.ts";
import type { SeatRequest } from "./protocol.ts";
import { seatRelayFull, type SeatRelayFullOutcome } from "./relay-full.ts";

/** How the relay sends one operation: at once (the main thread answers it with no I/O), or under the I/O bounds. */
export type OperationLane = "answeredAtOnce" | "waitsForIo";

/** What waits at a bound: the operations that wait for I/O, or the prepares. */
type WaitingOperations = "operations" | "prepares";

/** The lane of `request`. Pure. */
export function operationLane(request: SeatRequest): OperationLane {
  const area = request.area;

  switch (area) {
    case "sends":
      return request.operation.kind === "prepareSend" ? "answeredAtOnce" : "waitsForIo";
    case "facades":
    case "workflows":
      return "waitsForIo";
    default:
      return unreachable(area, "operationLane");
  }
}

/** The answer of an operation of `lane` past the bound `bound` of the relay: `SEAT_RELAY_FULL`. Pure. */
export function relayFullAnswer(lane: OperationLane, bound: number): SeatRelayFullOutcome {
  return Object.freeze({ kind: "seatRelayFull", error: seatRelayFull(boundMessage(waitingOf(lane), bound)) });
}

/**
 * The text of the answer past a bound: what waits, the bound and its value (the core's `MAX_RUN_WORK`), and that
 * nothing was sent. Pure.
 */
function boundMessage(waiting: WaitingOperations, bound: number): string {
  return `the relay of this seat isolate holds ${String(bound)} ${waiting} that wait for the main thread of its child, its bound (MAX_RUN_WORK = ${String(bound)}); nothing was sent; retry`;
}

function waitingOf(lane: OperationLane): WaitingOperations {
  switch (lane) {
    case "answeredAtOnce":
      return "prepares";
    case "waitsForIo":
      return "operations";
    default:
      return unreachable(lane, "waitingOf");
  }
}
