// expect: TS2345 at 7
// hz.wire() takes a required h.* value type; zod's .optional() goes on the result.

import { h } from "../../../horsies/src/index.ts";
import { hz } from "../../../horsies/src/zod.ts";

export const note = hz.wire(h.string().optional());
