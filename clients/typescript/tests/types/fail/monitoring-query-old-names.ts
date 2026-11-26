// expect: TS2724 at 6, TS2353 at 8, TS2353 at 10, TS2353 at 12
// The monitoring and ping options: `retriedOnly` (not `retried`), `workflowName` (not `name`), `waitMs` (not
// `timeoutMs`) and `DEFAULT_PING_WAIT_MS` (not `DEFAULT_PING_TIMEOUT_MS`).

import type { PingWorkersOptions, TaskFilters, WorkflowRunsQuery } from "../../../horsies/src/index.ts";
import { DEFAULT_PING_TIMEOUT_MS } from "../../../horsies/src/index.ts";

export const filters: TaskFilters = { retried: "retriedOnly" };

export const runs: WorkflowRunsQuery = { name: "nightly" };

export const ping: PingWorkersOptions = { timeoutMs: DEFAULT_PING_TIMEOUT_MS };
