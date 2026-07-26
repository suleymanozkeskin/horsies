import { useQuery, useQueryClient } from '@tanstack/react-query';

import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { queryKeys } from '@/lib/query-keys';
import { getTask } from '@/services/tasks-api';
import type { TaskDetail } from '@/types/tasks';

const ACTIVE_STATUSES: ReadonlySet<string> = new Set([
  'PENDING',
  'CLAIMED',
  'RUNNING',
]);
const POLL_WHILE_ACTIVE_MS = 4_000;

const isActive = (detail: TaskDetail | undefined): boolean =>
  detail !== undefined && ACTIVE_STATUSES.has(detail.leaf.status);

/**
 * Single task detail plus attempt history. `boostInterval` overrides the normal
 * cadence while an action settles in fallback mode.
 */
export function useTask(
  taskId: string | null,
  boostInterval: number | false = false
): {
  detail: TaskDetail | undefined;
  isLoading: boolean;
  isError: boolean;
  /** Refetch and resolve with the fresh row — used to verify a lost POST. */
  reread: () => Promise<TaskDetail | undefined>;
} {
  const mode = useLiveMode();
  const queryClient = useQueryClient();
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.taskDetail(taskId),
    enabled: taskId !== null,
    queryFn: () => getTask(taskId as string),
    refetchInterval: query => {
      if (boostInterval !== false) {
        return boostInterval;
      }
      return isActive(query.state.data)
        ? fallbackInterval(mode, POLL_WHILE_ACTIVE_MS)
        : false;
    },
  });

  const reread = async (): Promise<TaskDetail | undefined> => {
    if (taskId === null) {
      return undefined;
    }
    return queryClient.fetchQuery({
      queryKey: queryKeys.taskDetail(taskId),
      queryFn: () => getTask(taskId),
    });
  };

  return { detail: data, isLoading, isError, reread };
}
