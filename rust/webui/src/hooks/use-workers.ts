import { useQuery } from '@tanstack/react-query';

import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { queryKeys } from '@/lib/query-keys';
import {
  getWorkerHistory,
  listSchedules,
  listWorkers,
  pingWorkers,
} from '@/services/workers-api';
import type {
  LivenessReport,
  ScheduleState,
  WorkerHistoryPoint,
  WorkerState,
} from '@/types/workers';

const POLL_WORKERS_MS = 5_000;
const POLL_HISTORY_MS = 10_000;
/** The schedule table has no NOTIFY trigger, so it polls in both modes. */
const POLL_SCHEDULES_MS = 15_000;

export function useWorkers(): {
  workers: WorkerState[];
  isLoading: boolean;
  isError: boolean;
  refetch: () => void;
} {
  const mode = useLiveMode();
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: queryKeys.workers(),
    queryFn: listWorkers,
    refetchInterval: fallbackInterval(mode, POLL_WORKERS_MS),
  });
  return {
    workers: data ?? [],
    isLoading,
    isError,
    refetch: () => {
      void refetch();
    },
  };
}

/**
 * Active liveness (DB + worker ping-pong). The ping is a round trip over
 * LISTEN/NOTIFY with a wait window, so it is never polled: it runs once on
 * first load and otherwise only when `refresh()` is called.
 */
export function useWorkerLiveness(): {
  liveness: LivenessReport | undefined;
  isFetching: boolean;
  isError: boolean;
  /** Epoch ms of the last successful ping, or 0 if never — drives the freshness
   * label so an "online" badge is not read as live truth. */
  lastPingedAt: number;
  refresh: () => void;
} {
  const { data, isFetching, isError, refetch, dataUpdatedAt } = useQuery({
    queryKey: queryKeys.workerLiveness(),
    queryFn: pingWorkers,
    staleTime: Infinity,
    refetchInterval: false,
    refetchOnWindowFocus: false,
  });
  return {
    liveness: data,
    isFetching,
    isError,
    lastPingedAt: dataUpdatedAt,
    refresh: () => {
      void refetch();
    },
  };
}

export function useSchedules(): {
  schedules: ScheduleState[];
  isLoading: boolean;
  isError: boolean;
} {
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.schedules(),
    queryFn: listSchedules,
    refetchInterval: POLL_SCHEDULES_MS,
  });
  return { schedules: data ?? [], isLoading, isError };
}

export function useWorkerHistory(workerId: string | null): {
  history: WorkerHistoryPoint[];
  isLoading: boolean;
} {
  const mode = useLiveMode();
  const { data, isLoading } = useQuery({
    queryKey: queryKeys.workerHistory(workerId),
    enabled: workerId !== null,
    queryFn: () => getWorkerHistory(workerId as string),
    refetchInterval: fallbackInterval(mode, POLL_HISTORY_MS),
  });
  return { history: data ?? [], isLoading };
}
