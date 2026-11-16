// Task list / stats / facets / breakdown queries.
//
// The list's freshness comes from SSE invalidation while the stream is up;
// its interval only applies in fallback mode. The aggregates (stats, facets,
// breakdown) are decoupled from the event stream entirely: each is a
// whole-table aggregation, so they refresh on their spaced timers in both
// modes, with staleTime matching the timer so navigation inside a cycle
// serves the cached value instead of refetching.

import { keepPreviousData, useQuery } from '@tanstack/react-query';

import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { queryKeys } from '@/lib/query-keys';
import {
  getBreakdown,
  getFacets,
  getTaskStats,
  listTasks,
} from '@/services/tasks-api';
import type {
  Breakdown,
  Facets,
  GroupBy,
  SortDir,
  StatusCount,
  TaskFilters,
  TaskListPage,
  TaskSortBy,
} from '@/types/tasks';

const ACTIVE_STATUSES: ReadonlySet<string> = new Set([
  'PENDING',
  'CLAIMED',
  'RUNNING',
]);

const POLL_LIST_MS = 8_000;
const POLL_STATS_MS = 10_000;
const POLL_FACETS_MS = 30_000;
const POLL_BREAKDOWN_MS = 12_000;

const pageHasActive = (page: TaskListPage | undefined): boolean =>
  page !== undefined && page.rows.some(task => ACTIVE_STATUSES.has(task.status));

export function useTaskStats(filters: TaskFilters): {
  stats: StatusCount[];
  isError: boolean;
  refetch: () => void;
} {
  const { data, isError, refetch } = useQuery({
    queryKey: queryKeys.taskStats(filters),
    queryFn: () => getTaskStats(filters),
    refetchInterval: POLL_STATS_MS,
    staleTime: POLL_STATS_MS,
    // The filters are part of the key, so every filter change starts a new
    // query. Without this the cards would blank between scopes and the page
    // below them would reflow twice.
    placeholderData: keepPreviousData,
  });
  return {
    stats: data ?? [],
    isError,
    refetch: () => {
      void refetch();
    },
  };
}

export function useFacets(scope: {
  status?: string[];
  error_category?: string[];
  retried_only?: boolean;
}): { facets: Facets | undefined; isError: boolean } {
  const status = scope.status ?? [];
  const errorCategory = scope.error_category ?? [];
  const retriedOnly = scope.retried_only ?? false;
  const { data, isError } = useQuery({
    queryKey: queryKeys.taskFacets(status, errorCategory, retriedOnly),
    queryFn: () =>
      getFacets({
        status,
        error_category: errorCategory,
        retried_only: retriedOnly,
      }),
    refetchInterval: POLL_FACETS_MS,
    staleTime: POLL_FACETS_MS,
    // Scope is part of the key: keep the taxonomy strip and the facet option
    // lists populated across a scope change instead of emptying them.
    placeholderData: keepPreviousData,
  });
  return { facets: data, isError };
}

export function useBreakdown(
  groupBy: GroupBy,
  filters: TaskFilters,
  enabled: boolean
): {
  breakdown: Breakdown | undefined;
  isLoading: boolean;
  isError: boolean;
  refetch: () => void;
} {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: queryKeys.taskBreakdown(groupBy, filters),
    enabled,
    queryFn: () => getBreakdown(groupBy, filters),
    refetchInterval: POLL_BREAKDOWN_MS,
    staleTime: POLL_BREAKDOWN_MS,
    placeholderData: keepPreviousData,
  });
  return {
    breakdown: data,
    isLoading,
    isError,
    refetch: () => {
      void refetch();
    },
  };
}

/**
 * Paginated/sorted/filtered task list. In fallback mode it polls only while a
 * visible row is still active. Keeps the previous page while fetching so the
 * table does not flash empty on pagination or sort changes.
 */
export function useTasks(args: {
  filters: TaskFilters;
  sortBy: TaskSortBy;
  sortDir: SortDir;
  offset: number;
  limit: number;
  enabled: boolean;
}): {
  page: TaskListPage | undefined;
  isLoading: boolean;
  isError: boolean;
  refetch: () => void;
} {
  const mode = useLiveMode();
  const { filters, sortBy, sortDir, offset, limit, enabled } = args;
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: queryKeys.taskList(filters, sortBy, sortDir, offset, limit),
    enabled,
    queryFn: () => listTasks({ filters, sortBy, sortDir, offset, limit }),
    refetchInterval: query =>
      pageHasActive(query.state.data)
        ? fallbackInterval(mode, POLL_LIST_MS)
        : false,
    placeholderData: keepPreviousData,
  });
  return {
    page: data,
    // `isFetching` is deliberately excluded: it is true on every background
    // poll, and folding it in would flash the whole grid each cycle.
    isLoading,
    isError,
    refetch: () => {
      void refetch();
    },
  };
}
