// expect: TS2322 at 18
// A code that the contract does not declare fails at the line of the `err` call of the body's second argument, not
// at the line of the body.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const validateOrder = defineTask({ name: "validate_order", input: { email: h.string() }, output: h.int(), errorCodes: ["INVALID_EMAIL"] });

export const implementation = app.implement(validateOrder, async ({ email }, { ok, err, signal }) => {
  if (signal.aborted) {
    return err({ code: "INVALID_EMAIL" });
  }

  if (!email.includes("@")) {
    return err({ code: "EMPTY_ORDER" });
  }

  return ok(email.length);
});
