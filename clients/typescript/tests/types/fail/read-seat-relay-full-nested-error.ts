// expect: TS2339 at 8
// The read error `seatRelayFull` holds `code`, `retryable` and `message` itself, with no nested `error`.

import type { ReadError } from "../../../horsies/src/index.ts";

/** The message of a read that the seat relay did not send. */
export function relayMessage(error: Extract<ReadError, { readonly kind: "seatRelayFull" }>): string {
  return error.error.message;
}
