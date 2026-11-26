// expect: TS2345 at 7
// h.resultOf is a whole task input parameter; it never goes inside a zod schema.

import { h } from "../../../horsies/src/index.ts";
import { hz } from "../../../horsies/src/zod.ts";

export const upstream = hz.wire(h.resultOf(h.int()));
