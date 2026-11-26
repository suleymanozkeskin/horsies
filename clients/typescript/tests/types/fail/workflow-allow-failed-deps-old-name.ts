// expect: TS2353 at 12
// A node that runs after a failed dependency names `allowFailedDependencies`; `allowFailedDeps` is no node option.

import { defineTask, defineWorkflow, h } from "../../../horsies/src/index.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const flow = defineWorkflow(
  { name: "flow", definitionKey: "tests.flow.v1", outputType: h.null(), parameters: {} },
  (w) => {
    const pinged = w.task(ping, {}, { nodeId: "first" });
    const after = w.task(ping, {}, { nodeId: "second", waitsFor: [pinged], allowFailedDeps: true });

    return { output: after };
  },
);
