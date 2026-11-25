// expect: TS2345 at 6
// An array item is always present: .optional() is for object members.

import { h } from "../../../horsies/src/index.ts";

export const tags = h.array(h.string().optional());
