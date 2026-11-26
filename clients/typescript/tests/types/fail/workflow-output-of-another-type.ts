// expect: TS2322 at 11
// The output node's type is not the workflow's declared output.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const name = defineTask({ name: "name", input: {}, output: h.string() });

export const flow = defineWorkflow(
  { name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} },
  (w) => ({ output: w.task(name, {}, { nodeId: "name" }) }),
);
