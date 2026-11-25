// Worker and class report values for tests of the Workers page.

import type {
  BudgetLoad,
  ClassReport,
  DownSlot,
  ExecutionClass,
  SnapshotProfile,
  WorkerClassLoad,
  WorkerState,
} from '@/types/workers';

export const ALL_CLASSES: ExecutionClass[] = [
  'ISOLATED',
  'SHARED_ASYNC',
  'SHARED_THREAD',
];

/** The profile of a snapshot that names the profile `name`. */
export const named = (name: string): SnapshotProfile => ({ kind: 'named', name });

/** The profile of a snapshot that a worker of an earlier build wrote. */
export const NOT_RECORDED: SnapshotProfile = { kind: 'not_recorded' };

/** A live worker of the profile `default` with `processes` children and no
 * down slot. */
export function workerState(
  workerId: string,
  processes: number,
  fields: Partial<WorkerState> = {}
): WorkerState {
  return {
    worker_id: workerId,
    hostname: `${workerId}.internal`,
    pid: 4000,
    snapshot_at: '2026-11-24T09:11:00Z',
    snapshot_age_s: 7,
    stale: false,
    worker_started_at: '2026-11-24T08:00:00Z',
    uptime_s: 4260,
    processes,
    queues: ['payments', 'analytics'],
    queue_max_concurrency: { payments: 4 },
    tasks_running: 0,
    tasks_claimed: 0,
    cluster_wide_cap: null,
    memory_usage_mb: 32.8,
    memory_percent: 0.2,
    cpu_percent: 0.25,
    slot_health: 'all_serving',
    down_slots: [],
    profile: named('default'),
    ...fields,
  };
}

export function downSlot(slot: number): DownSlot {
  return {
    slot,
    cause: 'the OS ran short of a resource (EMFILE)',
    detail: 'OSError: [Errno 24] Too many open files',
    down_since: '2026-11-24T09:00:00Z',
    failed_boots: 3,
  };
}

/** One budget with `in_flight` rows; `free` is the capacity that holds no
 * row, at least 0, as the API computes it. */
export function budgetLoad(
  budget: ExecutionClass,
  capacity: number,
  serves: ExecutionClass[],
  inFlight: number
): BudgetLoad {
  return {
    budget,
    capacity,
    serves,
    in_flight: inFlight,
    free: Math.max(0, capacity - inFlight),
  };
}

export function classLoad(
  workerId: string,
  budgets: BudgetLoad[]
): WorkerClassLoad {
  return { worker_id: workerId, snapshot_at: '2026-11-24T09:11:00Z', budgets };
}

/** A class report with no pending stream and these worker budgets. */
export function classReport(workers: WorkerClassLoad[]): ClassReport {
  return {
    checked_at: '2026-11-24T09:11:08Z',
    live_window_s: 120,
    streams: [],
    workers,
  };
}
