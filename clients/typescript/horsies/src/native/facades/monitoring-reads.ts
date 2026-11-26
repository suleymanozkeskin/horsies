// The monitoring read functions of the `facades` area of the addon, their arguments and outcomes, and the accessor.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";
import type { RawCatalogError } from "../values/records.ts";

/** The argument of a query that the core refuses: the window, the page of the task list, or a limit. */
export type QueryField = "window" | "page" | "limit";

/** One bound of the history window: the core's default, or an instant in epoch microseconds. */
export type WindowBoundArg = { readonly kind: "coreDefault" } | { readonly kind: "at"; readonly epochMicros: bigint };

/** The history window of a list or aggregate read. */
export interface WindowArg {
  readonly since: WindowBoundArg;
  readonly until: WindowBoundArg;
}

/** Which tasks the retried filter keeps. */
export type RetriedArg = "any" | "retriedOnly";

/** An error category, by its wire value. */
export type ErrorCategoryArg = "OPERATIONAL" | "CONTRACT" | "RETRIEVAL" | "OUTCOME" | "DOMAIN";

/** The sort field of the task list. */
export type SortFieldArg =
  | "enqueuedAt"
  | "startedAt"
  | "completedAt"
  | "failedAt"
  | "status"
  | "taskName"
  | "queueName"
  | "priority"
  | "retryCount"
  | "queueSeconds"
  | "executionSeconds";

/** The sort direction of the task list. */
export type SortDirectionArg = "ascending" | "descending";

/** The column that a breakdown groups by. */
export type GroupByArg = "worker" | "taskName" | "queue";

/** The filters of a task read. An empty list keeps every value. `statuses` are wire values of task statuses. */
export interface FiltersArg {
  readonly statuses: string[];
  readonly taskNames: string[];
  readonly queues: string[];
  readonly workers: string[];
  readonly errorCodes: string[];
  readonly errorCategories: ErrorCategoryArg[];
  readonly retried: RetriedArg;
}

/** The task stats read: every filter but the statuses. */
export interface TaskStatsArg {
  readonly window: WindowArg;
  readonly taskNames: string[];
  readonly queues: string[];
  readonly workers: string[];
  readonly errorCodes: string[];
  readonly errorCategories: ErrorCategoryArg[];
  readonly retried: RetriedArg;
}

/** The task facets read: only the coarse filters. */
export interface TaskFacetsArg {
  readonly window: WindowArg;
  readonly statuses: string[];
  readonly errorCategories: ErrorCategoryArg[];
  readonly retried: RetriedArg;
}

/** The task breakdown read. */
export interface TaskBreakdownArg {
  readonly window: WindowArg;
  readonly groupBy: GroupByArg;
  readonly filters: FiltersArg;
  readonly limit: number;
}

/** The task list read. */
export interface TaskListArg {
  readonly window: WindowArg;
  readonly filters: FiltersArg;
  readonly sortField: SortFieldArg;
  readonly sortDirection: SortDirectionArg;
  readonly offset: number;
  readonly limit: number;
}

/** The workflow name filter of the workflow runs read. */
export type RunNameArg = { readonly kind: "anyName" } | { readonly kind: "named"; readonly name: string };

/** The status filter of the workflow runs read: the stored text of a workflow status. */
export type RunStatusArg = { readonly kind: "anyStatus" } | { readonly kind: "status"; readonly status: string };

/** The workflow runs read. */
export interface WorkflowRunsArg {
  readonly name: RunNameArg;
  readonly status: RunStatusArg;
  readonly limit: number;
}

/** The outcome of one list, aggregate or workflow read. A run or node that no row holds is the text `null`. */
export type MonitoringReadOutcome =
  | { readonly kind: "json"; readonly text: string }
  | { readonly kind: "refused"; readonly field: QueryField; readonly reason: string }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** Why no table holds a task. */
export type TaskAbsenceReasonName = "PREDATES_RETAINED_HISTORY" | "WITHIN_RETAINED_HISTORY" | "NOT_CLASSIFIED";

/** The outcome of the task detail read. */
export type TaskDetailOutcome =
  | { readonly kind: "json"; readonly text: string }
  | { readonly kind: "absent"; readonly taskId: string; readonly reason: TaskAbsenceReasonName }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** A read's Promise of its envelope. */
type ReadAnswer = Promise<NativeEnvelope<MonitoringReadOutcome>>;

/** The monitoring read functions of the addon. Each takes the handle first. */
export interface MonitoringReadsAddon {
  readTaskStats(handle: NativeCoreHandle, query: TaskStatsArg): ReadAnswer;
  readTaskFacets(handle: NativeCoreHandle, query: TaskFacetsArg): ReadAnswer;
  readTaskBreakdown(handle: NativeCoreHandle, query: TaskBreakdownArg): ReadAnswer;
  readTaskList(handle: NativeCoreHandle, query: TaskListArg): ReadAnswer;
  readWorkflowNames(handle: NativeCoreHandle): ReadAnswer;
  readWorkflowRuns(handle: NativeCoreHandle, query: WorkflowRunsArg): ReadAnswer;
  readWorkflowRun(handle: NativeCoreHandle, workflowId: string): ReadAnswer;
  readWorkflowNode(handle: NativeCoreHandle, workflowId: string, taskIndex: number): ReadAnswer;
  readSchedules(handle: NativeCoreHandle): ReadAnswer;
  readTaskDetail(handle: NativeCoreHandle, taskId: string): Promise<NativeEnvelope<TaskDetailOutcome>>;
}

/** The monitoring read functions of the addon. Throws as `requireAddon` does. */
export function monitoringReadsAddon(): MonitoringReadsAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of the facades area
  // compares this interface with the declarations that the native build writes.
  return requireAddon() as MonitoringReadsAddon;
}
