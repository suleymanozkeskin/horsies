import { apiGet, type QueryParams } from '@/lib/http';
import type {
  Breakdown,
  Facets,
  GroupBy,
  SortDir,
  StatusCount,
  TaskDetail,
  TaskFilters,
  TaskListPage,
  TaskSortBy,
} from '@/types/tasks';

/**
 * Build the query object. Array dimensions serialize as repeated keys; empty
 * dimensions are omitted so an unset filter is never sent as an empty value.
 * `extra` carries non-filter params (sort, paging, grouping).
 */
const buildParams = (
  filters: TaskFilters,
  extra: QueryParams = {}
): QueryParams => {
  const params: QueryParams = {};
  const setDimension = (key: string, values: string[] | undefined): void => {
    if (values !== undefined && values.length > 0) {
      params[key] = values;
    }
  };
  setDimension('status', filters.status);
  setDimension('task_name', filters.task_name);
  setDimension('queue', filters.queue);
  setDimension('worker', filters.worker);
  setDimension('error_code', filters.error_code);
  if (filters.retried_only === true) {
    params.retried_only = 'true';
  }
  return { ...params, ...extra };
};

/** Task counts by status (fixed order, zero-filled), scoped by every filter
 * except status — the cards ARE the status selector, so scoping by it would
 * zero out every other card. */
export const getTaskStats = (
  filters: TaskFilters = {}
): Promise<StatusCount[]> => {
  const { status: _status, ...rest } = filters;
  void _status;
  return apiGet<StatusCount[]>('/tasks/stats', buildParams(rest));
};

/** Scoped distinct filter values (with counts) for the filter comboboxes. */
export const getFacets = (
  scope: { status?: string[]; retried_only?: boolean } = {}
): Promise<Facets> =>
  apiGet<Facets>(
    '/tasks/facets',
    buildParams({
      ...(scope.status === undefined ? {} : { status: scope.status }),
      ...(scope.retried_only === undefined
        ? {}
        : { retried_only: scope.retried_only }),
    })
  );

/** Per-group status pivot (group by worker / task_name / queue) with a TOTAL row. */
export const getBreakdown = (
  groupBy: GroupBy,
  filters: TaskFilters = {}
): Promise<Breakdown> =>
  apiGet<Breakdown>('/tasks/breakdown', buildParams(filters, { group_by: groupBy }));

/** A paginated, server-sorted, server-filtered slice of tasks. */
export const listTasks = (args: {
  filters?: TaskFilters;
  sortBy: TaskSortBy;
  sortDir: SortDir;
  offset: number;
  limit: number;
}): Promise<TaskListPage> =>
  apiGet<TaskListPage>(
    '/tasks',
    buildParams(args.filters ?? {}, {
      sort_by: args.sortBy,
      sort_dir: args.sortDir,
      offset: args.offset,
      limit: args.limit,
    })
  );

/** A single task's execution detail plus its full attempt history. */
export const getTask = (taskId: string): Promise<TaskDetail> =>
  apiGet<TaskDetail>(`/tasks/${encodeURIComponent(taskId)}`);
