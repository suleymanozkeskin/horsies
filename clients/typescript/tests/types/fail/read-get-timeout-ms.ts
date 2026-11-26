// expect: TS2353 at 10
// The wait of a read is `waitMs`; `timeoutMs` is the run time limit of a task, not a read option.

import { defineTask, h, type Horsies, type TaskId } from "../../../horsies/src/index.ts";

const report = defineTask({ name: "report", input: {}, output: h.null() });

export async function readReport(app: Horsies, taskId: TaskId): Promise<boolean> {
  const handle = app.taskHandle(report, taskId);
  const read = await handle.get({ timeoutMs: 5_000 });

  return read.ok;
}
