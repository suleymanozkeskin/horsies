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

/** The error code of the task of a node. `not_loaded`: the read did not
 * come yet, or was not asked. `none`: the node has no task error code. */
export type NodeErrorCode =
  | { kind: 'code'; code: string }
  | { kind: 'none' }
  | { kind: 'not_loaded' };

/** How long the error code of a node is taken from the cache with no new
 * read: a tooltip opens many times on one node. */
const ERROR_CODE_STALE_MS = 30_000;

/**
 * The error code of the task of one node, for the tooltip of a failed node.
 * It reads the node detail of the node panel (one cache entry for both), and
 * only while `enabled`.
 */
export function useNodeErrorCode(
  workflowId: string,
  taskIndex: number,
  enabled: boolean
): NodeErrorCode {
  const { data } = useQuery({
    queryKey: queryKeys.workflowNode(workflowId, taskIndex),
    enabled,
    queryFn: () => getWorkflowTask(workflowId, taskIndex),
    staleTime: ERROR_CODE_STALE_MS,
  });
  if (!enabled || data === undefined) {
    return { kind: 'not_loaded' };
  }
  const code = data.leaf?.error_code ?? null;
  return code === null ? { kind: 'none' } : { kind: 'code', code };
}
