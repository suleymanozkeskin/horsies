import { keepPreviousData, useQuery } from '@tanstack/react-query';

import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { queryKeys } from '@/lib/query-keys';
import { getWorkflowNames, listWorkflowRuns } from '@/services/workflows-api';
import type { WorkflowRunSummary } from '@/types/workflows';

const ACTIVE_STATUSES: ReadonlySet<string> = new Set(['PENDING', 'RUNNING']);
const POLL_WHILE_ACTIVE_MS = 5_000;

const hasActiveRun = (runs: WorkflowRunSummary[] | undefined): boolean =>
  runs !== undefined && runs.some(run => ACTIVE_STATUSES.has(run.status));

/** Distinct root-workflow names for the picker filter. Fetched once. */
export function useWorkflowNames(): { names: string[] } {
  const { data } = useQuery({
    queryKey: queryKeys.workflowNames(),
    queryFn: getWorkflowNames,
    staleTime: Infinity,
  });
  return { names: data ?? [] };
}

export function useWorkflowRuns(
  name: string | null,
  status: string | null
): { runs: WorkflowRunSummary[]; isLoading: boolean; isError: boolean } {
  const mode = useLiveMode();
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.workflowRuns(name, status),
    queryFn: () =>
      listWorkflowRuns({
        ...(name === null ? {} : { name }),
        ...(status === null ? {} : { status }),
      }),
    refetchInterval: query =>
      hasActiveRun(query.state.data)
        ? fallbackInterval(mode, POLL_WHILE_ACTIVE_MS)
        : false,
    // The filters are part of the key: keep the rail populated across a filter
    // change so it does not blank and lose its scroll position.
    placeholderData: keepPreviousData,
  });
  return { runs: data ?? [], isLoading, isError };
}
