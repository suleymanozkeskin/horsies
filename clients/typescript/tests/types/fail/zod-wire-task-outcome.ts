// expect: TS2345 at 9
// h.taskOutcome is a whole task input parameter; it never goes inside a zod schema.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { hz } from "../../../horsies/src/zod.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });

export const upstream = hz.wire(h.taskOutcome(count));
