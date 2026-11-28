// expect: TS2322 at 10
// A sender never gives an upstream outcome.

import { defineTask, h } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });
const upstream = h.taskOutcome(count);

export const sent: SentOf<typeof upstream> = { kind: "succeeded", value: 1 };
