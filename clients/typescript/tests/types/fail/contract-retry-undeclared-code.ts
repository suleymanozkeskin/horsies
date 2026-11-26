// expect: TS2820 at 12
// With `errors`, a user code in `autoRetryFor` must be declared.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

export const charge = defineTask({
  name: "charge",
  input: {},
  output: h.null(),
  errors: ["PROVIDER_DOWN"],
  retry: { kind: "fixed", intervalsSeconds: [60], autoRetryFor: ["PROVIDER_GONE"] },
});
