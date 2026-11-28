// expect: TS2322 at 13
// The `maxRetries` of `TaskRuntime.forTest` is a `MaxRetries` (from `MaxRetries.parse`), not a plain number.

import { type AttemptNumber, TaskRuntime } from "../../../horsies/src/child/public.ts";
import type { TaskId } from "../../../horsies/src/contract/ids.ts";

declare const taskId: TaskId;
declare const attempt: AttemptNumber;

export const runtime: TaskRuntime = TaskRuntime.forTest({
  taskId,
  attempt,
  maxRetries: 3,
});
