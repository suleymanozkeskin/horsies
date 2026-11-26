// expect: TS2322 at 9
// A code that the contract does not declare fails at the line that builds the error.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const validateOrder = defineTask({ name: "validate_order", input: {}, output: h.null(), errors: ["EMPTY_ORDER"] });

export const failure = validateOrder.err({ code: "INVALID_EMAIL" });
