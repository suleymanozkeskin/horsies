import { useQuery } from '@tanstack/react-query';

import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { queryKeys } from '@/lib/query-keys';
import { getWorkflowTask } from '@/services/workflows-api';
import type { WorkflowTaskDetail } from '@/types/workflows';

const ACTIVE_STATUSES: ReadonlySet<string> = new Set([
  'PENDING',
  'RUNNING',
  'READY',
  'ENQUEUED',
]);
const POLL_WHILE_ACTIVE_MS = 4_000;

/** Failure/attempt detail for one node. Disabled when no node is selected. */
export function useWorkflowTask(
  workflowId: string | null,
  taskIndex: number | null
): { detail: WorkflowTaskDetail | undefined; isLoading: boolean } {
  const mode = useLiveMode();
  const enabled = workflowId !== null && taskIndex !== null;
  const { data, isLoading } = useQuery({
    queryKey: queryKeys.workflowNode(workflowId, taskIndex),
    enabled,
    queryFn: () => getWorkflowTask(workflowId as string, taskIndex as number),
    refetchInterval: query => {
      const detail = query.state.data;
      return detail !== undefined && ACTIVE_STATUSES.has(detail.node_status)
        ? fallbackInterval(mode, POLL_WHILE_ACTIVE_MS)
        : false;
    },
  });
  return {
    detail: enabled ? data : undefined,
    isLoading: enabled && isLoading,
  };
}
