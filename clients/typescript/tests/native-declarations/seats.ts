// The generated declarations of the `seats` area match the hand-kept ones: the function and its record.

import type * as Generated from "../../bridge/index.js";
import type { SeatBounds, SeatsAddon } from "../../horsies/src/native/seats/addon.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const seatBoundsMatches: SameFunction<typeof Generated.seatBounds, SeatsAddon["seatBounds"]> = true;
export const seatBoundsRecordMatches: Mutual<Generated.SeatBounds, SeatBounds> = true;
