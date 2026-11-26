// expect: TS2339 at 12
// The runtime of a task that does not take the workflow context has no `workflowContext`.

import { defineTask, h, type Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

const plain = defineTask({ name: "plain", input: {}, output: h.null() });

export const implementation = app.implement(plain, (_input, runtime) => {
  // The member is in the runtime type of a task with `workflowContext: true` only.
  void runtime.workflowContext;

  return runtime.ok(null);
});
