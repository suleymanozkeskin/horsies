// expect: TS2741 at 16
// A state entry of `TaskRuntime.forTest` takes only a key that `app.provide` returned.

import { type AttemptNumber, TaskRuntime } from "../../../horsies/src/child/public.ts";
import type { TaskId } from "../../../horsies/src/contract/ids.ts";

type Pool = { readonly query: (sql: string) => number };

const pool: Pool = { query: () => 1 };
declare const taskId: TaskId;
declare const attempt: AttemptNumber;

export const runtime: TaskRuntime = TaskRuntime.forTest({
  taskId,
  attempt,
  state: [[{ kind: "stateKey", name: "provide #1 openPool" }, pool]],
});
