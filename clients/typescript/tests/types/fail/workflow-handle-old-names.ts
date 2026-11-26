// expect: TS2305 at 6, TS2339 at 9, TS2339 at 11
// The nodes of a run are `wf.nodes()`, a start again is `app.resendStart`, and the join rule type is `JoinType`;
// `tasks()`, `retryStart` and `Join` are gone.

import type { BuildReturn, Horsies, StartEnqueueFailed, WorkflowHandleById } from "../../../horsies/src/index.ts";
import type { Join } from "../../../horsies/src/index.ts";

export async function reads(app: Horsies, wf: WorkflowHandleById<BuildReturn>, failed: StartEnqueueFailed<BuildReturn>): Promise<void> {
  await wf.tasks();

  await app.retryStart(failed);
}

export type Old = Join;
