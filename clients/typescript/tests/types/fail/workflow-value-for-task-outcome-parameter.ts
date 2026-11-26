// expect: TS2322 at 12
// A value for an h.taskOutcome parameter: only an upstream node fills it.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });
const charge = defineTask({ name: "charge", input: { order: h.taskOutcome(count) }, output: h.int() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} }, (w) => {
  const charged = w.task(charge,
    { order: 7 },
    { id: "charge" });

  return { output: charged };
});
