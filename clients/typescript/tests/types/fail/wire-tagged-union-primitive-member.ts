// expect: TS2322 at 6
// Each member of a tagged union is an object.

import { h } from "../../../horsies/src/index.ts";

export const payment = h.union("method", [h.int()]);
