// expect: TS2345 at 6
// A default has the sent type of its wire type.

import { h } from "../../../horsies/src/index.ts";

export const retries = h.int().default("3");
