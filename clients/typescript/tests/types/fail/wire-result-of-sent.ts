// expect: TS2322 at 9
// A sender never gives an upstream outcome.

import { h } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

const upstream = h.resultOf(h.int());

export const sent: SentOf<typeof upstream> = { kind: "succeeded", value: 1 };
