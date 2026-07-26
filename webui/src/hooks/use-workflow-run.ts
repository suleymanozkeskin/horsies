import { useQuery, useQueryClient } from '@tanstack/react-query';

import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { queryKeys } from '@/lib/query-keys';
import { getWorkflowRun } from '@/services/workflows-api';
import type { WorkflowRunDetail } from '@/types/workflows';

const ACTIVE_STATUSES: ReadonlySet<string> = new Set([
  'PENDING',
  'RUNNING',
  'READY',
  'ENQUEUED',
]);
const POLL_WHILE_ACTIVE_MS = 4_000;

/**
 * One run's DAG. `boostInterval` overrides the cadence while an action settles
 * in fallback mode; `workflowId` null disables the query.
 */
export function useWorkflowRun(
  workflowId: string | null,
  boostInterval: number | false = false
): {
  detail: WorkflowRunDetail | undefined;
  isLoading: boolean;
  isError: boolean;
  reread: () => Promise<WorkflowRunDetail | undefined>;
} {
  const mode = useLiveMode();
  const queryClient = useQueryClient();
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.workflowRun(workflowId),
    enabled: workflowId !== null,
    queryFn: () => getWorkflowRun(workflowId as string),
    refetchInterval: query => {
      if (boostInterval !== false) {
        return boostInterval;
      }
      const detail = query.state.data;
      return detail !== undefined && ACTIVE_STATUSES.has(detail.run.status)
        ? fallbackInterval(mode, POLL_WHILE_ACTIVE_MS)
        : false;
    },
  });

  const reread = async (): Promise<WorkflowRunDetail | undefined> => {
    if (workflowId === null) {
      return undefined;
    }
    return queryClient.fetchQuery({
      queryKey: queryKeys.workflowRun(workflowId),
      queryFn: () => getWorkflowRun(workflowId),
    });
  };

  return { detail: data, isLoading, isError, reread };
}
