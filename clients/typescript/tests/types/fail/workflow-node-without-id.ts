// expect: TS2345 at 10
// A node with no id: every node names its id.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", params: {} }, (w) => ({
  output: w.task(ping, {}, {}),
}));
