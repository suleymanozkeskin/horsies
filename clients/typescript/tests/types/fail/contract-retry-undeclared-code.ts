// expect: TS2820 at 12
// With `errorCodes`, a custom code in `autoRetryFor` must be declared.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

export const charge = defineTask({
  name: "charge",
  input: {},
  output: h.null(),
  errorCodes: ["PROVIDER_DOWN"],
  retryPolicy: { kind: "fixed", intervalsSeconds: [60], autoRetryFor: ["PROVIDER_GONE"] },
});
