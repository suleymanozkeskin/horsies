// expect: TS2322 at 12
// An `exceptionMapper` entry maps to a code that the contract declares.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const charge = defineTask({ name: "charge", input: {}, output: h.null(), errorCodes: ["PAYMENT_DECLINED"] });

export const implementation = app.implement(charge, async (_input, { ok }) => ok(null), {
  exceptionMapper: [[TypeError, "CARD_GONE"]],
});
