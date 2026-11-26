// expect: TS2322 at 7
// h.resultOf is a whole task input parameter, never an object member.

import { h } from "../../../horsies/src/index.ts";

export const order = h.object({
  upstream: h.resultOf(h.int()),
});
