// expect: TS2353 at 6
// The declared codes of a task are `errorCodes`; `errors` is no field of `defineTask`.

import { defineTask, h } from "../../../horsies/src/index.ts";

export const charge = defineTask({ name: "charge", input: {}, output: h.null(), errors: ["PAYMENT_DECLINED"] });
