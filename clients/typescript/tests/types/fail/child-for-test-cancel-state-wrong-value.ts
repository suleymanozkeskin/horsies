// expect: TS2322 at 14
// The cancel state of `TaskRuntime.forTest` takes the values that `runtime.cancelState()` gives: the cause is
// `"DEADLINE"` or `"OPERATOR"`.

import { type AttemptNumber, TaskRuntime } from "../../../horsies/src/child/public.ts";
import type { TaskId } from "../../../horsies/src/contract/ids.ts";

declare const taskId: TaskId;
declare const attempt: AttemptNumber;

export const runtime: TaskRuntime = TaskRuntime.forTest({
  taskId,
  attempt,
  cancelState: { kind: "requested", cause: "TIMEOUT" },
});
