// expect: TS2322 at 11
// A value for an h.resultOf parameter: only an upstream node fills it.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const charge = defineTask({ name: "charge", input: { order: h.resultOf(h.int()) }, output: h.int() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", params: {} }, (w) => {
  const charged = w.task(charge,
    { order: 7 },
    { id: "charge" });

  return { output: charged };
});
