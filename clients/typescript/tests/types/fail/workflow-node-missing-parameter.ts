// expect: TS2741 at 11; floor: TS2345 at 11
// A node input that leaves out a required parameter.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const report = defineTask({ name: "report", input: { day: h.string(), region: h.string() }, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", outputType: h.null(), parameters: {} }, (w) => ({
  output: w.task(report,
    { day: "2026-11-26" },
    { nodeId: "report" }),
}));
