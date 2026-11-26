// expect: TS2322 at 19
// A node whose codes the declared contract does not list does not fit an `h.taskOutcome(contract)` parameter, even
// with the same output type.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const validateOrder = defineTask({ name: "validate_order", input: {}, output: h.int(), errorCodes: ["EMPTY_ORDER"] });
const reserveStock = defineTask({ name: "reserve_stock", input: {}, output: h.int(), errorCodes: ["OUT_OF_STOCK"] });
const charge = defineTask({ name: "charge", input: { order: h.taskOutcome(validateOrder) }, output: h.null() });

export const fulfil = defineWorkflow(
  { name: "fulfil", definitionKey: "tests.fulfil.v1", params: {}, output: h.null() },
  (w) => {
    const reserved = w.task(reserveStock, {}, { nodeId: "reserve" });

    return {
      output: w.task(charge, { order: reserved }, { nodeId: "charge" }),
    };
  },
);
