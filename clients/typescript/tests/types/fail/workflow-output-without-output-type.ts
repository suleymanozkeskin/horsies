// expect: TS2322 at 11
// A definition with no `outputType` whose build returns an output node: a workflow with no `outputType` has no output
// node.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", parameters: {} }, (w) => ({
  output: w.task(ping, {}, { nodeId: "ping" }),
}));
