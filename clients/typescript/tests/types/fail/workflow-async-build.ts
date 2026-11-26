// expect: TS2322 at 10
// A build is sync: an async build function is refused.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.null(), params: {} },
  async (w) => ({ output: w.task(ping, {}, { id: "ping" }) }),
);
