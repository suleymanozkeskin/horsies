// expect: TS2353 at 12
// The exception map of an implementation is `exceptionMapper`; `exceptions` is no option of `app.implement`.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const charge = defineTask({ name: "charge", input: {}, output: h.null(), errorCodes: ["PAYMENT_DECLINED"] });

export const implementation = app.implement(charge, async (_input, { ok }) => ok(null), {
  exceptions: [[TypeError, "PAYMENT_DECLINED"]],
});
