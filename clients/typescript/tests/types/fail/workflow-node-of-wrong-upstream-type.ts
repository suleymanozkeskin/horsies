// expect: TS2322 at 13
// A node whose output type does not fit the h.resultOf(T) parameter.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const name = defineTask({ name: "name", input: {}, output: h.string() });
const double = defineTask({ name: "double", input: { n: h.resultOf(h.int()) }, output: h.int() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} }, (w) => {
  const named = w.task(name, {}, { id: "name" });

  return { output: w.task(double, { n: named }, { id: "double" }) };
});
