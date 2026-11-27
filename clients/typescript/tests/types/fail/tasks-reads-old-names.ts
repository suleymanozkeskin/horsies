// expect: TS2305 at 7, TS2339 at 10, TS2339 at 12, TS2322 at 15
// The task record names its queue `queue` and its due instant `dueAt`; the rerun refusal of an idempotency key is
// `RerunIdempotencyKeyRefused`; the error category of the custom codes is `CUSTOM`.

import type { ErrorCategory, TaskInfo } from "../../../horsies/src/index.ts";
import type { TaskId } from "../../../horsies/src/index.ts";
import type { RerunCallerKeyRefused } from "../../../horsies/src/index.ts";

export function facts(info: TaskInfo): readonly [TaskId, string, Temporal.Instant] {
  const queue = info.queueName;

  return [info.taskId, queue, info.enqueuedAt];
}

export const category: ErrorCategory = "DOMAIN";

export type Old = RerunCallerKeyRefused;
