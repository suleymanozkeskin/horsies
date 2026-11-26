// expect: TS2345 at 9
// A definition with no `output` field: every workflow declares the kind of its output.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", parameters: {} }, (w) => ({
  output: w.task(ping, {}, { nodeId: "ping" }),
}));
