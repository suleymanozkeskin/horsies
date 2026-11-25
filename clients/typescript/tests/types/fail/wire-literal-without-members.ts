// expect: TS2555 at 6
// A literal has at least one member.

import { h } from "../../../horsies/src/index.ts";

export const level = h.literal();
