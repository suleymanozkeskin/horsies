// expect: TS2339 at 8, TS2339 at 10, TS2551 at 12, TS2339 at 14
// The reads of `app.monitoring` take the names of the Python and Rust clients: `listWorkerStates`,
// `getExecutionClassReport`, `getTaskDetail`, `listSchedules`, ... The short names are no members.

import type { Horsies, TaskId } from "../../../horsies/src/index.ts";

export async function reads(app: Horsies, taskId: TaskId): Promise<void> {
  await app.monitoring.workers();

  await app.monitoring.classReport();

  await app.monitoring.taskDetail(taskId);

  await app.monitoring.schedules();
}
