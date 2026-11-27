// expect: TS2322 at 9
// A rest item of a tuple has the type of the rest item wire type.

import { h } from "../../../horsies/src/index.ts";

const row = h.tuple([h.int()], h.boolean());

// The rest item 3 is not a boolean.
export const value: h.Infer<typeof row> = [1, true, 3];
