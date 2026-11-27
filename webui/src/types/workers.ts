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

/** Whether any slot was down when the worker wrote its snapshot; the down
 * slots are in slot order. */
export type SlotHealth =
  | { kind: 'all_serving' }
  | { kind: 'slots_down'; slots: DownSlot[] };

/** The worker profile that a snapshot names. `not_recorded`: the snapshot
 * holds no profile name, because a worker of a build before schema migration
 * 1004 wrote it. */
export type SnapshotProfile =
  | { kind: 'named'; name: string }
  | { kind: 'not_recorded' };

/** Latest state snapshot for one worker. */
export interface WorkerState {
  worker_id: string;
  hostname: string;
  pid: number;
  snapshot_at: string;
  /** Seconds since `snapshot_at`, exact to the microsecond. */
  snapshot_age_seconds: number | null;
  stale: boolean;
  worker_started_at: string;
  /** Seconds since `worker_started_at`, exact to the microsecond. */
  uptime_seconds: number | null;
  child_count: number;
  queues: string[];
  queue_max_concurrency: Record<string, number>;
  tasks_running: number;
  tasks_claimed: number;
  cluster_wide_cap: number | null;
  memory_usage_mb: number | null;
  memory_percent: number | null;
  cpu_percent: number | null;
  slot_health: SlotHealth;
  /** The worker profile that the worker ran. */
  profile: SnapshotProfile;
}

/** One worker's reply to an active liveness ping. */
export interface WorkerPong {
  worker_id: string;
  hostname: string;
  pid: number;
  round_trip_ms: number;
}

/** The database part of a liveness report: the latency of a `SELECT 1`
 * round trip in milliseconds, or unreachable. */
export type DatabaseLiveness =
  | { kind: 'ping'; latency_ms: number }
  | { kind: 'unreachable' };

/** DB reachability plus the set of workers that answered an active ping. */
export interface LivenessReport {
  database: DatabaseLiveness;
  workers: WorkerPong[];
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

/** One claim budget of a live worker. `capacity` counts the children or
 * seats in service; the free capacity is `capacity - in_flight`, at least 0
 * (`freeCapacity`). */
export interface BudgetLoad {
  budget: ExecutionClass;
  capacity: number;
  serves: ExecutionClass[];
  in_flight: number;
}

/** The claim budgets of one live worker. */
export interface WorkerExecutionClassLoad {
  worker_id: string;
  snapshot_at: string;
  budgets: BudgetLoad[];
}

/** Whether live workers serve a pending stream. */
export type StreamCoverageState =
  | { kind: 'served'; workers: number; capacity: number }
  | { kind: 'no_capacity'; workers: number }
  | { kind: 'unserved' };

/** How long the stream's rows wait. `none_due`: every row is due later.
 * `wait_seconds` is in seconds, exact to the microsecond. */
export type StreamWait =
  | { kind: 'due'; oldest_due_at: string; wait_seconds: number }
  | { kind: 'none_due'; next_due_at: string };

/** The PENDING rows of one queue and one execution class. */
export interface PendingStream {
  queue: string;
  execution: ExecutionClass;
  coverage: StreamCoverageState;
  wait: StreamWait;
}

/** Pending streams and the budget load of each live worker. */
export interface ExecutionClassReport {
  checked_at: string;
  live_window_seconds: number;
  streams: PendingStream[];
  workers: WorkerExecutionClassLoad[];
}
