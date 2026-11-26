// expect: TS2322 at 8
// h.int() is a number; h.bigint() holds a bigint.

import { h } from "../../../horsies/src/index.ts";

const count = h.int();

export const value: h.Infer<typeof count> = 7n;
