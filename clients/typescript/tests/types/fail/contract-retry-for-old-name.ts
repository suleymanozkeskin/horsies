// expect: TS2561 at 10
// The codes that start a retry with no call are `retryFor`; `autoRetryFor` is no field of a retry policy.

import { defineTask, h } from "../../../horsies/src/index.ts";

export const charge = defineTask({
  name: "charge",
  input: {},
  output: h.null(),
  retryPolicy: { kind: "fixed", intervalsSeconds: [60], autoRetryFor: ["TASK_TIMEOUT"] },
});
