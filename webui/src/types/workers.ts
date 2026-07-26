// Mirrors the horsies.monitoring worker/schedule response models.

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
