// expect: TS2724 at 9, TS2724 at 10, TS2724 at 11, TS2305 at 12, TS2339 at 15, TS2322 at 17, TS2339 at 19, TS2339 at 21
// The status counts read is `taskStatusCounts` with `TaskStatusCountsQuery`; the queue sort field is `queue`; the
// status of a node is `WorkflowNodeStatus`; one attempt row is `TaskAttemptDetail`; the node of a handle is
// `WorkflowNodeInfo`; the child counts of a summary are `totalNodes`, ...; the node of a context is `nodeIndex`.

import type { Horsies, SubWorkflowSummary, TaskSortField } from "../../../horsies/src/index.ts";
import type { RuntimeWorkflowContext } from "../../../horsies/src/index.ts";

import type { TaskStatsQuery } from "../../../horsies/src/index.ts";
import type { WorkflowTaskStatus } from "../../../horsies/src/index.ts";
import type { TaskAttemptInfo } from "../../../horsies/src/index.ts";
import type { WorkflowTaskInfo } from "../../../horsies/src/index.ts";

export async function reads(app: Horsies, summary: SubWorkflowSummary, context: RuntimeWorkflowContext): Promise<number> {
  await app.monitoring.taskStats();

  const field: TaskSortField = "queueName";

  const total: number = summary.totalTasks;

  return field.length + total + context.taskIndex;
}

export type Old = TaskStatsQuery | WorkflowTaskStatus | TaskAttemptInfo | WorkflowTaskInfo;
