// expect: TS2322 at 11
// A built-in name in `errorCodes` fails typecheck.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

export const validateOrder = defineTask({
  name: "validate_order",
  input: {},
  output: h.null(),
  errorCodes: ["EMPTY_ORDER", "TASK_TIMEOUT"],
});
