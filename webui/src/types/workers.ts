// Mirrors the horsies.monitoring worker/schedule response models.

/** One slot that was down when the worker wrote its snapshot. */
export interface DownSlot {
  slot: number;
  /** The cause of the last failed child start, as text. */
  cause: string;
  detail: string;
  down_since: string;
  failed_boots: number;
}

/** `not_recorded`: a worker version that did not record its slot health. */
export type SlotHealthKind = 'all_serving' | 'slots_down' | 'not_recorded';

/** Latest state snapshot for one worker. */
export interface WorkerState {
  worker_id: string;
  hostname: string;
  pid: number;
  snapshot_at: string;
  snapshot_age_s: number | null;
  stale: boolean;
  worker_started_at: string;
  uptime_s: number | null;
  processes: number;
  queues: string[];
  queue_max_concurrency: Record<string, number> | null;
  tasks_running: number;
  tasks_claimed: number;
  cluster_wide_cap: number | null;
  memory_usage_mb: number | null;
  memory_percent: number | null;
  cpu_percent: number | null;
  slot_health: SlotHealthKind;
  /** Empty unless `slot_health` is `slots_down`; in slot order. */
  down_slots: DownSlot[];
}

/** One worker's reply to an active liveness ping. */
export interface WorkerPing {
  worker_id: string;
  hostname: string;
  pid: number;
  round_trip_ms: number;
}

/** DB reachability plus the set of workers that answered an active ping. */
export interface LivenessReport {
  db_latency_ms: number | null;
  db_reachable: boolean;
  workers: WorkerPing[];
}

/** One timeseries point for a worker's load/resource charts. */
export interface WorkerHistoryPoint {
  snapshot_at: string;
  tasks_running: number;
  tasks_claimed: number;
  cpu_percent: number | null;
  memory_usage_mb: number | null;
  memory_percent: number | null;
}

/** One recurring schedule's execution state. */
export interface ScheduleState {
  schedule_name: string;
  last_run_at: string | null;
  next_run_at: string | null;
  last_task_id: string | null;
  run_count: number;
  updated_at: string;
}

export type ExecutionClass = 'ISOLATED' | 'SHARED_ASYNC' | 'SHARED_THREAD';

/** One claim budget of a live worker. `free` is `capacity - in_flight`, at
 * least 0; `capacity` counts the children or seats in service. */
export interface BudgetLoad {
  budget: ExecutionClass;
  capacity: number;
  serves: ExecutionClass[];
  in_flight: number;
  free: number;
}

/** The claim budgets of one live worker. */
export interface WorkerClassLoad {
  worker_id: string;
  snapshot_at: string;
  budgets: BudgetLoad[];
}

/** Whether live workers serve a pending stream. */
export type StreamCoverage =
  | { kind: 'served'; workers: number; capacity: number }
  | { kind: 'no_capacity'; workers: number }
  | { kind: 'unserved' };

/** How long the stream's rows wait. `none_due`: every row is due later. */
export type StreamWait =
  | { kind: 'waiting'; oldest_due_at: string; wait_s: number }
  | { kind: 'none_due'; next_due_at: string };

/** The PENDING rows of one queue and one execution class. */
export interface PendingStream {
  queue_name: string;
  execution: ExecutionClass;
  coverage: StreamCoverage;
  wait: StreamWait;
}

/** Pending streams and the budget load of each live worker. */
export interface ClassReport {
  checked_at: string;
  live_window_s: number;
  streams: PendingStream[];
  workers: WorkerClassLoad[];
}
