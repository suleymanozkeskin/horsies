// expect: TS2322 at 13
// A node for a plain parameter: the parameter is not declared h.taskOutcome(contract).

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });
const double = defineTask({ name: "double", input: { n: h.int() }, output: h.int() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} }, (w) => {
  const counted = w.task(count, {}, { nodeId: "count" });

  return { output: w.task(double, { n: counted }, { nodeId: "double" }) };
});
