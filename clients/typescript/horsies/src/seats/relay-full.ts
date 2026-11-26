// The one failure of a call that the seat relay of a `SHARED_THREAD` seat isolate did not send: the relay holds its
// bound of waiting calls. Nothing was sent, so nothing was read or written, and the call may run again. Each relayed
// call gives it through its own error union: a send, a task info read, a workflow start, restart or handle call, a
// monitoring or health read, a ping, a cancel, a rerun. The Python and Rust clients have no seat relay.

import type { CoreCause } from "../_catalog/error_detail.ts";

/**
 * The failure code of a call that the seat relay did not send: the core's code catalog `SeatRelayCode`
 * (`core/src/core/task/seat_relay.rs`). One constant here until the build renders the catalog into
 * `_catalog/task_send.ts`; the switch then imports it from there, with the same name and members.
 */
export const SeatRelayCode = { SEAT_RELAY_FULL: "SEAT_RELAY_FULL" } as const;

/** A member of `SeatRelayCode`. */
export type SeatRelayCode = (typeof SeatRelayCode)[keyof typeof SeatRelayCode];

/**
 * `SEAT_RELAY_FULL`: the seat relay holds its bound of waiting calls; nothing was sent; retry. Only a call in a
 * `SHARED_THREAD` body gives it. `exception` is a retryable precondition of the seat relay whose text names the bound
 * and its value. A send past the bound minted no row, so `retrySend` does not take it: send again.
 */
export type SeatRelayFull = {
  readonly code: typeof SeatRelayCode.SEAT_RELAY_FULL;
  readonly retryable: true;
  readonly message: string;
  readonly exception: CoreCause;
};

/** The answer of a relayed call that the seat relay did not send. */
export type SeatRelayFullOutcome = { readonly kind: "seatRelayFull"; readonly error: SeatRelayFull };

/** `SEAT_RELAY_FULL` with the text `message`, which names the bound and its value. Pure. */
export function seatRelayFull(message: string): SeatRelayFull {
  return Object.freeze({
    code: SeatRelayCode.SEAT_RELAY_FULL,
    retryable: true,
    message,
    exception: Object.freeze({ kind: "coreFailure", failure: "preconditionFailed", subject: "seat relay", message, retryable: true }),
  });
}
