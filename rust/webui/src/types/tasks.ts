// Mirrors the horsies.monitoring task response models.
// LeafTask and TaskAttempt are shared with the workflow viewer (same horsies
// rows) and re-used from types/workflows.

import type { LeafTask, TaskAttempt } from '@/types/workflows';

/** horsies task lifecycle status. Open union so unseen values still render. */
export type TaskStatus =
  | 'PENDING'
  | 'CLAIMED'
  | 'RUNNING'
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELLED'
  | 'EXPIRED'
  | (string & {});

/** Error-code taxonomy family. Mirrors the backend `error_category`. */
export type ErrorCategory =
  | 'OPERATIONAL'
  | 'CONTRACT'
  | 'RETRIEVAL'
  | 'OUTCOME'
  | 'DOMAIN';

/** One leaf task row, shaped for the list view. */
export interface TaskSummary {
  id: string;
  task_name: string;
  queue_name: string;
  status: TaskStatus;
  priority: number;
  retry_count: number;
  max_retries: number;
  is_workflow_task: boolean;
  error_code: string | null;
  error_category: ErrorCategory | null;
  worker_hostname: string | null;
  worker_id: string | null;
  enqueued_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  failed_at: string | null;
  queue_s: number | null;
  exec_s: number | null;
}

/** A paginated slice of tasks plus the total matching the active filters. */
export interface TaskListPage {
  rows: TaskSummary[];
  total: number;
}

/** Count of tasks in one status, for the overview cards. */
export interface StatusCount {
  status: TaskStatus;
  count: number;
}

/** A distinct filter value and how many tasks carry it under the scope. */
export interface FacetValue {
  value: string;
  count: number;
}

/** An error-code facet, tagged with its taxonomy category. */
export interface ErrorFacet extends FacetValue {
  category: ErrorCategory;
}

/** Scoped distinct values (with counts) that drive the filter comboboxes.
 * `error_codes` is capped for the dropdown; `error_category_totals` is the
 * accurate, uncapped per-category rollup for the taxonomy summary. */
export interface Facets {
  workers: FacetValue[];
  task_names: FacetValue[];
  queues: FacetValue[];
  error_codes: ErrorFacet[];
  error_category_totals: Partial<Record<ErrorCategory, number>>;
}

/** What the breakdown pivot groups tasks by. */
export type GroupBy = 'worker' | 'task_name' | 'queue';

/** Sortable list columns. Matches the backend allowlist exactly. */
export type TaskSortBy =
  | 'enqueued_at'
  | 'started_at'
  | 'completed_at'
  | 'failed_at'
  | 'status'
  | 'task_name'
  | 'queue_name'
  | 'priority'
  | 'retry_count'
  | 'queue_s'
  | 'exec_s';

export type SortDir = 'asc' | 'desc';

/** One group's per-status counts. `group` is 'TOTAL' for the rollup row. */
export interface GroupRow {
  group: string;
  total: number;
  pending: number;
  claimed: number;
  running: number;
  completed: number;
  failed: number;
  cancelled: number;
  expired: number;
  retried: number;
}

/** Per-group status pivot plus the rollup TOTAL row. `groups` is capped;
 * `group_count` is the true number of distinct groups (for truncation UX). */
export interface Breakdown {
  group_by: GroupBy;
  groups: GroupRow[];
  total: GroupRow;
  group_count: number;
}

/** The active filter selection shared by list / facets / breakdown calls. Each
 * value dimension is multi-select: values OR within a dimension, dimensions AND
 * across. An absent/empty array means "no filter on this dimension". */
export interface TaskFilters {
  status?: string[];
  task_name?: string[];
  queue?: string[];
  worker?: string[];
  error_code?: string[];
  /** Taxonomy families. The server expands each to the codes it covers — the
   * code lists are the library's, never the client's. */
  error_category?: string[];
  retried_only?: boolean;
}

/** A single task's execution detail plus its full attempt history. */
export interface TaskDetail {
  leaf: LeafTask;
  task_name: string;
  queue_name: string;
  priority: number;
  is_workflow_task: boolean;
  error_category: ErrorCategory | null;
  attempts: TaskAttempt[];
  /** Owning run when `is_workflow_task`; null for a standalone task, and null
   * on any server that predates this field. */
  workflow_id: string | null;
  /** The row's node index within that run — the deep-link target. */
  workflow_task_index: number | null;
}
