// expect: TS2345 at 9
// A workflow with params and no check cases.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: { n: h.int() }, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.null(), params: { n: h.int() } },
  (w, { n }) => ({ output: w.task(ping, { n }, { nodeId: "ping" }) }),
);
