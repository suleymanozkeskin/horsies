// expect: TS2322 at 10
// The output of a build is a node.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", outputType: h.null(), parameters: {} }, (_w) => ({
  output: "ping",
}));

export const used = ping;
