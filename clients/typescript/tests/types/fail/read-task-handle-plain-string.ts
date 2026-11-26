// expect: TS2345 at 9
// A task handle takes a parsed `TaskId`, not a plain string.

import { defineTask, h, type Horsies } from "../../../horsies/src/index.ts";

const validateOrder = defineTask({ name: "v", input: {}, output: h.null() });

export function handleOf(app: Horsies): object {
  return app.taskHandle(validateOrder, "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b");
}
