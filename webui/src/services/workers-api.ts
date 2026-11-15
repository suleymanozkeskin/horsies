import { apiGet } from '@/lib/http';
import type {
  ClassReport,
  LivenessReport,
  ScheduleState,
  WorkerHistoryPoint,
  WorkerState,
} from '@/types/workers';

/** Latest state snapshot per worker, including idle workers. */
export const listWorkers = (): Promise<WorkerState[]> =>
  apiGet<WorkerState[]>('/workers');

/** Active liveness: DB round-trip plus every worker that replies in the window. */
export const pingWorkers = (): Promise<LivenessReport> =>
  apiGet<LivenessReport>('/workers/ping');

/** Recurring schedule states, soonest next-run first. */
export const listSchedules = (): Promise<ScheduleState[]> =>
  apiGet<ScheduleState[]>('/workers/schedules');

/** Timeseries snapshots for one worker (newest first), for load/resource charts. */
export const getWorkerHistory = (
  workerId: string,
  limit?: number
): Promise<WorkerHistoryPoint[]> =>
  apiGet<WorkerHistoryPoint[]>(
    `/workers/${encodeURIComponent(workerId)}/history`,
    limit === undefined ? {} : { limit }
  );

/** Pending streams per (queue, class) and the budget load of each live worker. */
export const getClassReport = (): Promise<ClassReport> =>
  apiGet<ClassReport>('/workers/classes');
