// The functions of the `seats` area of the addon, their records, and the accessor.
//
// - `seatBounds` is pure: it starts no core. The main thread of a `SHARED_THREAD` child reads it; a seat isolate never
//   loads the addon, so the main thread gives the relay bounds to each isolate in its seat marker. The in-flight bound
//   of the relay is the child's task-send pool, from its start facts.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";

/** The bounds of the thread seats, from the core's constants. */
export interface SeatBounds {
  /** The most operations that wait in one isolate. */
  readonly opsWaiting: number;
  /** The longest boot of a new isolate, in milliseconds. */
  readonly replacementBootBoundMs: number;
}

/** The functions of the `seats` area. */
export interface SeatsAddon {
  seatBounds(): NativeEnvelope<SeatBounds>;
}

/** The `seats` functions of the addon. Throws as `requireAddon` does. */
export function seatsAddon(): SeatsAddon {
  // SAFETY: the addon registers this function with this signature; the declaration test of this area compares this
  // interface with the declarations that the native build writes.
  return requireAddon() as SeatsAddon;
}
