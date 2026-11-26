// expect: TS2353 at 10
// The id of a node is `nodeId`; `id` is no node option.

import { defineTask, defineWorkflow, h } from "../../../horsies/src/index.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });

export const flow = defineWorkflow(
  { name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} },
  (w) => ({ output: w.task(count, {}, { id: "count" }) }),
);
