// The queries of the monitoring reads, and the addon arguments that they give.
//
// - The core owns the bounds of a query (the window, the page, the limits): the read gives them to the addon, and the
//   addon answers a refused value at once as `invalidInput` (`INVALID_INPUT`) (field `window`, `page` or `limit`).
// - This module refuses what the addon cannot take: a count that is not a whole number, and text that is not
//   well-formed (a lone surrogate). Both are `invalidInput` (`INVALID_INPUT`) before any call.
// - A field that a query leaves out takes the core's default: the addon argument says `coreDefault`, and the core's
//   query keeps the value of its `new`. The package holds no copy of a default.

import type { TaskStatus } from "../_catalog/status.ts";
import type {
  CountArg,
  FiltersArg,
  RetriedArg,
  SortArg,
  TaskBreakdownArg,
  TaskFacetsArg,
  TaskListArg,
  TaskStatsArg,
  WindowArg,
  WindowBoundArg,
  WorkflowRunsArg,
} from "../native/facades/monitoring-reads.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { WorkflowStatus } from "../wire/task-error.ts";
import { queryRefused, type QueryRefused, type QueryRefusedField } from "./errors.ts";

/** Nanoseconds per microsecond: the core stores instants in microseconds. */
const NANOSECONDS_PER_MICROSECOND = 1000n;

/**
 * The history window of a read. A bound that the window leaves out takes the core's default: `until` is now, and
 * `since` is 24 hours before `until`. The core refuses a window that does not increase or that is longer than 30 days.
 */
export interface MonitoringWindow {
  readonly since?: Temporal.Instant;
  readonly until?: Temporal.Instant;
}

/** An error category, by its wire value. */
export type ErrorCategory = "OPERATIONAL" | "CONTRACT" | "RETRIEVAL" | "OUTCOME" | "CUSTOM";

/** The sort field of the task list. */
export type TaskSortField =
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

/** The order of the task list. */
export type SortDirection = "ascending" | "descending";

/** The sort of the task list. Left out: the core's default. */
export interface TaskSort {
  readonly field: TaskSortField;
  readonly direction: SortDirection;
}

/** The column that a breakdown groups by. */
export type TaskGroupBy = "worker" | "taskName" | "queue";

/** The filters of a task read. A list that the query leaves out, or an empty list, keeps every value. */
export interface TaskFilters {
  readonly window?: MonitoringWindow;
  readonly statuses?: readonly TaskStatus[];
  readonly taskNames?: readonly string[];
  readonly queues?: readonly string[];
  readonly workers?: readonly string[];
  readonly errorCodes?: readonly string[];
  readonly errorCategories?: readonly ErrorCategory[];
  /** Keep only the tasks that ran more than one attempt. Left out: every task. */
  readonly retriedOnly?: boolean;
}

/** The task stats read: every filter but the statuses (the stats count each status). */
export type TaskStatsQuery = Omit<TaskFilters, "statuses">;

/** The task facets read: only the coarse filters, so one selection never empties the option lists of the others. */
export type TaskFacetsQuery = Pick<TaskFilters, "window" | "statuses" | "errorCategories" | "retriedOnly">;

/** The task breakdown read. `limit`: the most groups, 1 to `MAX_BREAKDOWN_LIMIT`; left out, the core's default. */
export interface TaskBreakdownQuery extends TaskFilters {
  readonly groupBy: TaskGroupBy;
  readonly limit?: number;
}

/**
 * The task list read. `limit`: 1 to `MAX_TASK_PAGE_LIMIT`; `offset` 0 or more; `offset + limit` at most
 * `MAX_TASK_PAGE_REACH`. A count or the sort that the query leaves out takes the core's default.
 */
export interface TaskListQuery extends TaskFilters {
  readonly sort?: TaskSort;
  readonly offset?: number;
  readonly limit?: number;
}

/** The workflow runs read. `limit`: 1 to `MAX_WORKFLOW_RUNS_LIMIT`; left out, the core's default. */
export interface WorkflowRunsQuery {
  readonly workflowName?: string;
  readonly status?: WorkflowStatus;
  readonly limit?: number;
}

/** The addon argument of a task stats query. Pure. */
export function taskStatsArg(query: TaskStatsQuery): Result<TaskStatsArg, QueryRefused> {
  const filters = filtersArg(query);

  if (!filters.ok) {
    return filters;
  }

  const { taskNames, queues, workers, errorCodes, errorCategories, retried } = filters.value;

  return ok({ window: windowArg(query.window), taskNames, queues, workers, errorCodes, errorCategories, retried });
}

/** The addon argument of a task facets query. Pure. */
export function taskFacetsArg(query: TaskFacetsQuery): TaskFacetsArg {
  return {
    window: windowArg(query.window),
    statuses: [...(query.statuses ?? [])],
    errorCategories: [...(query.errorCategories ?? [])],
    retried: retriedArg(query.retriedOnly),
  };
}

/** The addon argument of a task breakdown query. Pure. */
export function taskBreakdownArg(query: TaskBreakdownQuery): Result<TaskBreakdownArg, QueryRefused> {
  const filters = filtersArg(query);

  if (!filters.ok) {
    return filters;
  }

  const limit = countArg("limit", query.limit);

  if (!limit.ok) {
    return limit;
  }

  return ok({ window: windowArg(query.window), groupBy: query.groupBy, filters: filters.value, limit: limit.value });
}

/** The addon argument of a task list query. Pure. */
export function taskListArg(query: TaskListQuery): Result<TaskListArg, QueryRefused> {
  const filters = filtersArg(query);

  if (!filters.ok) {
    return filters;
  }

  const offset = countArg("page", query.offset);
  const limit = countArg("page", query.limit);

  if (!offset.ok) {
    return offset;
  }

  if (!limit.ok) {
    return limit;
  }

  return ok({
    window: windowArg(query.window),
    filters: filters.value,
    sort: sortArg(query.sort),
    offset: offset.value,
    limit: limit.value,
  });
}

/** The addon argument of a workflow runs query. Pure. */
export function workflowRunsArg(query: WorkflowRunsQuery): Result<WorkflowRunsArg, QueryRefused> {
  const limit = countArg("limit", query.limit);

  if (!limit.ok) {
    return limit;
  }

  if (query.workflowName !== undefined && !query.workflowName.isWellFormed()) {
    return err(queryRefused("workflowName", "the workflow name is not well-formed text (it holds a lone surrogate)"));
  }

  return ok({
    name: query.workflowName === undefined ? { kind: "anyName" } : { kind: "named", name: query.workflowName },
    status: query.status === undefined ? { kind: "anyStatus" } : { kind: "status", status: query.status },
    limit: limit.value,
  });
}

/** The addon argument of a count: the core's default when the query leaves it out. Pure. */
function countArg(field: QueryRefusedField, value: number | undefined): Result<CountArg, QueryRefused> {
  if (value === undefined) {
    return ok({ kind: "coreDefault" });
  }

  const whole = wholeNumber(field, value);

  return whole.ok ? ok({ kind: "set", value: whole.value }) : whole;
}

/** The addon argument of a sort: the core's default when the query leaves it out. Pure. */
function sortArg(sort: TaskSort | undefined): SortArg {
  return sort === undefined ? { kind: "coreDefault" } : { kind: "set", field: sort.field, direction: sort.direction };
}

/** The addon argument of a window. Pure. */
export function windowArg(window: MonitoringWindow | undefined): WindowArg {
  return { since: windowBound(window?.since), until: windowBound(window?.until) };
}

/** The epoch microseconds of `instant`; a part below the microsecond is cut toward the past. Pure. */
export function epochMicros(instant: Temporal.Instant): bigint {
  const nanoseconds = instant.epochNanoseconds;
  const whole = nanoseconds / NANOSECONDS_PER_MICROSECOND;

  return nanoseconds < 0n && whole * NANOSECONDS_PER_MICROSECOND !== nanoseconds ? whole - 1n : whole;
}

/** The addon argument of one bound. Pure. */
function windowBound(instant: Temporal.Instant | undefined): WindowBoundArg {
  return instant === undefined ? { kind: "coreDefault" } : { kind: "at", epochMicros: epochMicros(instant) };
}

/** The addon filters of `query`, or the refusal of a text that is not well-formed. Pure. */
function filtersArg(query: TaskFilters): Result<FiltersArg, QueryRefused> {
  const lists = [
    ["taskNames", query.taskNames],
    ["queues", query.queues],
    ["workers", query.workers],
    ["errorCodes", query.errorCodes],
  ] as const;

  for (const [field, values] of lists) {
    const refused = (values ?? []).find((value) => !value.isWellFormed());

    if (refused !== undefined) {
      return err(queryRefused(field, `a value of ${field} is not well-formed text (it holds a lone surrogate)`));
    }
  }

  return ok({
    statuses: [...(query.statuses ?? [])],
    taskNames: [...(query.taskNames ?? [])],
    queues: [...(query.queues ?? [])],
    workers: [...(query.workers ?? [])],
    errorCodes: [...(query.errorCodes ?? [])],
    errorCategories: [...(query.errorCategories ?? [])],
    retried: retriedArg(query.retriedOnly),
  });
}

/** `value` when it is a whole number that a JavaScript number holds exactly, else the refusal of `field`. Pure. */
export function wholeNumber(field: QueryRefusedField, value: number): Result<number, QueryRefused> {
  return Number.isSafeInteger(value)
    ? ok(value)
    : err(
        queryRefused(field, `${field} must be a whole number from -9007199254740991 to 9007199254740991; got ${value}`),
      );
}

/** The addon argument of the retried filter: only retried tasks when `retriedOnly` is `true`, else every task. Pure. */
function retriedArg(retriedOnly: boolean | undefined): RetriedArg {
  return retriedOnly === true ? "retriedOnly" : "any";
}
