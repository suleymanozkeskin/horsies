// URL search-parameter contracts.
//
// Task filters, sort and pagination live in the URL so a filtered view is
// shareable and survives a reload; the workflow view keeps its selected run and
// open node the same way. Validators coerce and drop anything unrecognised
// rather than throwing, because the URL is user-editable input.

import type { GroupBy, SortDir, TaskFilters, TaskSortBy } from '@/types/tasks';

/** Flat list, or one of the grouping pivots. */
export type TaskView = 'flat' | GroupBy;

const TASK_VIEWS: ReadonlySet<string> = new Set([
  'flat',
  'worker',
  'task_name',
  'queue',
]);

const SORT_COLUMNS: ReadonlySet<string> = new Set([
  'enqueued_at',
  'started_at',
  'completed_at',
  'failed_at',
  'status',
  'task_name',
  'queue_name',
  'priority',
  'retry_count',
  'queue_s',
  'exec_s',
]);

export const PAGE_SIZES = [25, 50, 100, 200] as const;

export interface TaskSearch {
  status?: string[];
  task_name?: string[];
  queue?: string[];
  worker?: string[];
  error_code?: string[];
  retried?: boolean;
  view?: TaskView;
  sort?: TaskSortBy;
  dir?: SortDir;
  /** 1-based page number, as it reads in the URL. */
  page?: number;
  size?: number;
  /** Task id whose detail panel is open. */
  task?: string;
}

/** A merge patch. A key set to `undefined` clears that parameter, which the
 * router then drops from the URL — that is how a filter is cleared. */
export type SearchPatch<T> = { [K in keyof T]?: T[K] | undefined };

export type TaskSearchPatch = SearchPatch<TaskSearch>;
export type WorkflowSearchPatch = SearchPatch<WorkflowSearch>;

export const DEFAULT_SORT: TaskSortBy = 'enqueued_at';
export const DEFAULT_DIR: SortDir = 'desc';
export const DEFAULT_PAGE_SIZE = 50;

/** A repeatable string param arrives as a string, an array, or not at all. */
function toStringArray(value: unknown): string[] | undefined {
  if (typeof value === 'string' && value !== '') {
    return [value];
  }
  if (Array.isArray(value)) {
    const items = value.filter(
      (item): item is string => typeof item === 'string' && item !== ''
    );
    return items.length > 0 ? items : undefined;
  }
  return undefined;
}

function toPositiveInt(value: unknown): number | undefined {
  const parsed =
    typeof value === 'number'
      ? value
      : typeof value === 'string'
        ? Number.parseInt(value, 10)
        : Number.NaN;
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
}

const isTrue = (value: unknown): boolean =>
  value === true || value === 'true' || value === '1';

/** Only values that change behaviour are kept, so a default never appears in
 * the URL and every persisted view is a deliberate one. */
export function validateTaskSearch(raw: Record<string, unknown>): TaskSearch {
  const search: TaskSearch = {};
  const status = toStringArray(raw.status);
  if (status !== undefined) search.status = status;
  const taskName = toStringArray(raw.task_name);
  if (taskName !== undefined) search.task_name = taskName;
  const queue = toStringArray(raw.queue);
  if (queue !== undefined) search.queue = queue;
  const worker = toStringArray(raw.worker);
  if (worker !== undefined) search.worker = worker;
  const errorCode = toStringArray(raw.error_code);
  if (errorCode !== undefined) search.error_code = errorCode;
  if (isTrue(raw.retried)) search.retried = true;
  if (typeof raw.view === 'string' && TASK_VIEWS.has(raw.view)) {
    search.view = raw.view as TaskView;
  }
  if (typeof raw.sort === 'string' && SORT_COLUMNS.has(raw.sort)) {
    search.sort = raw.sort as TaskSortBy;
  }
  if (raw.dir === 'asc' || raw.dir === 'desc') {
    search.dir = raw.dir;
  }
  const page = toPositiveInt(raw.page);
  if (page !== undefined) search.page = page;
  const size = toPositiveInt(raw.size);
  if (size !== undefined && (PAGE_SIZES as readonly number[]).includes(size)) {
    search.size = size;
  }
  if (typeof raw.task === 'string' && raw.task !== '') {
    search.task = raw.task;
  }
  return search;
}

/** The filter dimensions the API takes, extracted from the URL state. */
export function filtersFromSearch(search: TaskSearch): TaskFilters {
  const filters: TaskFilters = {};
  if (search.status !== undefined) filters.status = search.status;
  if (search.task_name !== undefined) filters.task_name = search.task_name;
  if (search.queue !== undefined) filters.queue = search.queue;
  if (search.worker !== undefined) filters.worker = search.worker;
  if (search.error_code !== undefined) filters.error_code = search.error_code;
  if (search.retried === true) filters.retried_only = true;
  return filters;
}

/** True when any filter dimension is active (drives the "Clear filters" button). */
export const hasActiveFilters = (search: TaskSearch): boolean =>
  Object.keys(filtersFromSearch(search)).length > 0;

export interface WorkflowSearch {
  /** Selected run id. */
  run?: string;
  /** Open node's `task_index` within the selected run. */
  node?: number;
}

export function validateWorkflowSearch(
  raw: Record<string, unknown>
): WorkflowSearch {
  const search: WorkflowSearch = {};
  if (typeof raw.run === 'string' && raw.run !== '') {
    search.run = raw.run;
  }
  const node =
    typeof raw.node === 'number'
      ? raw.node
      : typeof raw.node === 'string'
        ? Number.parseInt(raw.node, 10)
        : Number.NaN;
  if (Number.isInteger(node) && node >= 0) {
    search.node = node;
  }
  return search;
}
