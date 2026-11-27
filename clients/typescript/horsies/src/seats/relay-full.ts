// The one failure of a call that the seat relay of a `SHARED_THREAD` seat isolate did not send: the relay holds its
// bound of waiting calls. Nothing was sent, so nothing was read or written, and the call may run again. Each relayed
// call gives it through its own error union: a send, a task info read, a workflow start, restart or handle call, a
// monitoring or health read, a ping, a cancel, a rerun. The Python and Rust clients have no seat relay.

import type { CoreCause } from "../_catalog/error_detail.ts";
import { SeatRelayCode } from "../_catalog/task_send.ts";

/**
 * `SEAT_RELAY_FULL`: the seat relay holds its bound of waiting calls; nothing was sent; retry. Only a call in a
 * `SHARED_THREAD` body gives it. `cause` is a retryable precondition of the seat relay whose text names the bound
 * and its value. A send past the bound minted no row, so `resend` does not take it: send again.
 */
export type SeatRelayFull = {
  readonly kind: "seatRelayFull";
  readonly code: typeof SeatRelayCode.SEAT_RELAY_FULL;
  readonly retryable: true;
  readonly message: string;
  readonly cause: CoreCause;
};

/** The answer of a relayed call that the seat relay did not send. */
export type SeatRelayFullOutcome = { readonly kind: "seatRelayFull"; readonly error: SeatRelayFull };

/** `SEAT_RELAY_FULL` with the text `message`, which names the bound and its value. Pure. */
export function seatRelayFull(message: string): SeatRelayFull {
  return Object.freeze({
    kind: "seatRelayFull",
    code: SeatRelayCode.SEAT_RELAY_FULL,
    retryable: true,
    message,
    cause: Object.freeze({ kind: "coreFailure", failure: "preconditionFailed", subject: "seat relay", message, retryable: true }),
  });
}
