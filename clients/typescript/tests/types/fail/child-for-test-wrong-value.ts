// expect: TS2322 at 16
// A state entry of `TaskRuntime.forTest` pairs a key with a value of the key's type.

import { type AttemptNumber, type StateKey, TaskRuntime } from "../../../horsies/src/child/public.ts";
import type { TaskId } from "../../../horsies/src/contract/ids.ts";

type Pool = { readonly query: (sql: string) => number };

declare const Db: StateKey<Pool>;
declare const taskId: TaskId;
declare const attempt: AttemptNumber;

export const runtime: TaskRuntime = TaskRuntime.forTest({
  taskId,
  attempt,
  state: [[Db, "not a pool"]],
});
