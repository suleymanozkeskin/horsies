// expect: TS2322 at 9
// h.taskOutcome is a whole task input parameter, never an object member.

import { defineTask, h } from "../../../horsies/src/index.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });

export const order = h.object({
  upstream: h.taskOutcome(count),
});
