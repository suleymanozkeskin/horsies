// expect: TS2741 at 18
// A check case gives every param of the build: a required `h.taskOutcome` param takes the outcome that its upstream
// node would give.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const validateOrder = defineTask({ name: "validate_order", input: {}, output: h.int(), errorCodes: ["EMPTY_ORDER"] });
const report = defineTask({ name: "report", input: { total: h.int() }, output: h.int() });

export const child = defineWorkflow(
  {
    name: "child",
    definitionKey: "tests.child.v1",
    parameters: { order: h.taskOutcome(validateOrder) },
    outputType: h.int(),
    checkCases: [{}],
  },
  (w, { order }) => ({ output: w.task(report, { total: order.kind === "succeeded" ? order.value : 0 }, { nodeId: "report" }) }),
);
