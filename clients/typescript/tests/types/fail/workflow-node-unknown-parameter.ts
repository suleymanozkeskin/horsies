// expect: TS2353 at 10
// A node input with a member that the task does not declare.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const report = defineTask({ name: "report", input: { day: h.string() }, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.null(), params: {} }, (w) => ({
  output: w.task(report, { day: "2026-11-26", extra: 1 }, { nodeId: "report" }),
}));
