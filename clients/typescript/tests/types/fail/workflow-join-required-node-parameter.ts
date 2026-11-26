// expect: TS2322 at 13
// Under an any or quorum join a node-fed parameter must be optional: its source may not have ended.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });
const tally = defineTask({ name: "tally", input: { first: h.taskOutcome(count) }, output: h.int() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} }, (w) => {
  const counted = w.task(count, {}, { nodeId: "count" });

  return { output: w.task(tally, { first: counted }, { nodeId: "tally", join: { kind: "any" } }) };
});
