// expect: TS2322 at 14
// A check case whose param has another type than the params declare.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: { n: h.int() }, output: h.null() });

export const flow = defineWorkflow(
  {
    name: "flow",
    definitionKey: "tests.flow.v1", output: h.null(),
    params: { n: h.int() },
    checkCases: [{ n: "seven" }],
  },
  (w, { n }) => ({ output: w.task(ping, { n }, { nodeId: "ping" }) }),
);
