// expect: TS2353 at 13
// The context sources of a node are `workflowContextFrom`; `ctxFrom` is no node option.

import { defineTask, defineWorkflow, h } from "../../../horsies/src/index.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });
const audit = defineTask({ name: "audit", input: {}, output: h.null(), workflowContext: true });

export const flow = defineWorkflow(
  { name: "flow", definitionKey: "tests.flow.v1", outputType: h.null(), parameters: {} },
  (w) => {
    const pinged = w.task(ping, {}, { nodeId: "ping" });
    const audited = w.task(audit, {}, { nodeId: "audit", waitsFor: [pinged], ctxFrom: [pinged] });

    return { output: audited };
  },
);
