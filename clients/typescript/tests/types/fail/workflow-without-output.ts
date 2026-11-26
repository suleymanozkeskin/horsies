// expect: TS2345 at 11
// A build that returns no output. The compiler reports a wrong return value of a block-bodied function argument at
// the function's first line, not at the return line (the accepted position).

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", outputType: h.null(), parameters: {} },
  (w) => {
    const pinged = w.task(ping, {}, { nodeId: "ping" });

    return { successPolicy: { cases: [{ required: [pinged] }] } };
  },
);
