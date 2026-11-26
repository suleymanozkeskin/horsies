// expect: TS2322 at 14
// A node whose output type does not fit the output of the contract of the h.taskOutcome(contract) parameter.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });
const name = defineTask({ name: "name", input: {}, output: h.string() });
const double = defineTask({ name: "double", input: { n: h.taskOutcome(count) }, output: h.int() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} }, (w) => {
  const named = w.task(name, {}, { nodeId: "name" });

  return { output: w.task(double, { n: named }, { nodeId: "double" }) };
});
