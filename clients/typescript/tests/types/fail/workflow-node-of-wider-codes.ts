// expect: TS2322 at 21
// A parameter declared `h.taskOutcome(contract)` takes a node whose codes are the contract's codes or fewer. A task
// with no `errorCodes` has any code (`string`), so its node does not fit a contract with declared codes.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const validateOrder = defineTask({ name: "validate_order", input: {}, output: h.int(), errorCodes: ["EMPTY_ORDER"] });
const openOrder = defineTask({ name: "open_order", input: {}, output: h.int() });
const charge = defineTask({ name: "charge", input: { order: h.taskOutcome(validateOrder) }, output: h.null() });

export const fulfil = defineWorkflow(
  { name: "fulfil", definitionKey: "tests.fulfil.v1", params: {}, output: h.null() },
  (w) => {
    const validated = w.task(validateOrder, {}, { nodeId: "validate" });
    const opened = w.task(openOrder, {}, { nodeId: "open" });

    w.task(charge, { order: validated }, { nodeId: "charge" });

    return { output: w.task(charge, { order: opened }, { nodeId: "charge_open" }) };
  },
);
