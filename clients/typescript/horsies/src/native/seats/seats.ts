// The functions of the `seats` area of the addon through the call wrapper: a throw of a call is a defect under the
// defect rule, and a thrown envelope is its error.

import { nativeValue } from "../call/envelope.ts";
import { type SeatBounds, seatsAddon } from "./addon.ts";

/** The bounds of the thread seats, from the core's constants. Pure: starts no core. */
export function seatBounds(): SeatBounds {
  return nativeValue("seatBounds", () => seatsAddon().seatBounds());
}
