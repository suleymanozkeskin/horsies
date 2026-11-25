import { useMemo } from 'react';

import { useQuery, useQueryClient } from '@tanstack/react-query';

import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { readClockOf, type ReadClock } from '@/hooks/use-draw-time';
import { assertNever } from '@/lib/exhaustive';
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
 * True while the run can still change: its status is active, or its timing
 * has an open segment or an open pause. A PAUSED run with a task that still
 * runs, or a CANCELLED run whose nodes drain, has open segments with no
 * active status.
 */
export function runIsChanging(detail: WorkflowRunDetail): boolean {
  if (ACTIVE_STATUSES.has(detail.run.status)) {
    return true;
  }
  switch (detail.timing.kind) {
    case 'unavailable':
      return false;
    case 'composed':
      return detail.timing.open_segments;
    default:
      return assertNever(detail.timing, 'run timing');
  }
}

/**
 * One run's DAG. `boostInterval` overrides the cadence while an action settles
 * in fallback mode; `workflowId` null disables the query. `readClock` is the
 * clock of the current `detail` (its `as_of` and its arrival time); it is
 * undefined exactly when `detail` is.
 */
export function useWorkflowRun(
  workflowId: string | null,
  boostInterval: number | false = false
): {
  detail: WorkflowRunDetail | undefined;
  readClock: ReadClock | undefined;
  isLoading: boolean;
  isError: boolean;
  reread: () => Promise<WorkflowRunDetail | undefined>;
} {
  const mode = useLiveMode();
  const queryClient = useQueryClient();
  const { data, dataUpdatedAt, isLoading, isError } = useQuery({
    queryKey: queryKeys.workflowRun(workflowId),
    enabled: workflowId !== null,
    queryFn: () => getWorkflowRun(workflowId as string),
    refetchInterval: query => {
      if (boostInterval !== false) {
        return boostInterval;
      }
      const detail = query.state.data;
      return detail !== undefined && runIsChanging(detail)
        ? fallbackInterval(mode, POLL_WHILE_ACTIVE_MS)
        : false;
    },
  });

  const readClock = useMemo(
    () => (data === undefined ? undefined : readClockOf(data.timing, dataUpdatedAt)),
    [data, dataUpdatedAt]
  );

  const reread = async (): Promise<WorkflowRunDetail | undefined> => {
    if (workflowId === null) {
      return undefined;
    }
    return queryClient.fetchQuery({
      queryKey: queryKeys.workflowRun(workflowId),
      queryFn: () => getWorkflowRun(workflowId),
    });
  };

  return { detail: data, readClock, isLoading, isError, reread };
}
