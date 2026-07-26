// Query-key vocabulary. Every key starts with one of these roots so the event
// layer can invalidate whole surfaces by prefix without knowing the arguments.

import type {
  GroupBy,
  SortDir,
  TaskFilters,
  TaskSortBy,
} from '@/types/tasks';

export const QUERY_ROOT = {
  taskList: 'tasks',
  taskStats: 'task-stats',
  taskFacets: 'task-facets',
  taskBreakdown: 'task-breakdown',
  taskDetail: 'task',
  workflowNames: 'workflow-names',
  workflowRuns: 'workflow-runs',
  workflowRun: 'workflow-run',
  workflowNode: 'workflow-task',
  workers: 'workers',
  workerLiveness: 'worker-liveness',
  workerHistory: 'worker-history',
  schedules: 'schedules',
  meta: 'meta',
} as const;

export type QueryRoot = (typeof QUERY_ROOT)[keyof typeof QUERY_ROOT];

export const queryKeys = {
  taskStats: (filters: TaskFilters) =>
    [QUERY_ROOT.taskStats, filters] as const,
  taskFacets: (
    status: string[],
    errorCategory: string[],
    retriedOnly: boolean
  ) => [QUERY_ROOT.taskFacets, status, errorCategory, retriedOnly] as const,
  taskBreakdown: (groupBy: GroupBy, filters: TaskFilters) =>
    [QUERY_ROOT.taskBreakdown, groupBy, filters] as const,
  taskList: (
    filters: TaskFilters,
    sortBy: TaskSortBy,
    sortDir: SortDir,
    offset: number,
    limit: number
  ) => [QUERY_ROOT.taskList, filters, sortBy, sortDir, offset, limit] as const,
  taskDetail: (taskId: string | null) =>
    [QUERY_ROOT.taskDetail, taskId] as const,
  workflowNames: () => [QUERY_ROOT.workflowNames] as const,
  workflowRuns: (name: string | null, status: string | null) =>
    [QUERY_ROOT.workflowRuns, name, status] as const,
  workflowRun: (workflowId: string | null) =>
    [QUERY_ROOT.workflowRun, workflowId] as const,
  workflowNode: (workflowId: string | null, taskIndex: number | null) =>
    [QUERY_ROOT.workflowNode, workflowId, taskIndex] as const,
  workers: () => [QUERY_ROOT.workers] as const,
  workerLiveness: () => [QUERY_ROOT.workerLiveness] as const,
  workerHistory: (workerId: string | null) =>
    [QUERY_ROOT.workerHistory, workerId] as const,
  schedules: () => [QUERY_ROOT.schedules] as const,
  meta: () => [QUERY_ROOT.meta] as const,
};
