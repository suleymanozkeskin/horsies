// expect: TS2353 at 10
// The retry policy of a task is `retryPolicy`; `retry` is no field of `defineTask`.

import { defineTask, h } from "../../../horsies/src/index.ts";

export const charge = defineTask({
  name: "charge",
  input: {},
  output: h.null(),
  retry: { kind: "fixed", intervalsSeconds: [60], retryFor: ["TASK_TIMEOUT"] },
});
