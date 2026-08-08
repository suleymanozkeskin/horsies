// Mirrors the horsies.monitoring workflow response models. The DAG is
// run-derived: every workflow run persists its full node set, so these types
// describe any workflow definition and any run generically.

/** Run/node lifecycle states. Open string union — backend is source of truth. */
export type WorkflowStatus =
  | 'PENDING'
  | 'READY'
  | 'ENQUEUED'
  | 'RUNNING'
  | 'COMPLETED'
  | 'FAILED'
  | 'SKIPPED'
  | 'CANCELLED'
  | 'EXPIRED'
  | 'PAUSED'
  | (string & {});

/** One workflow run (root or subworkflow). */
export interface WorkflowRunSummary {
  id: string;
  name: string;
  definition_key: string | null;
  status: WorkflowStatus;
  created_at: string | null;
  completed_at: string | null;
  /** Wall-clock seconds: completed_at (or now) minus created_at. */
  wall_s: number | null;
}

/** A single DAG node. `task_index` is the stable key; `node_id` is a label. */
export interface WorkflowNode {
  task_index: number;
  node_id: string | null;
  task_name: string;
  node_status: WorkflowStatus;
  is_subworkflow: boolean;
  sub_workflow_id: string | null;
  allow_failed_deps: boolean;
  started_at: string | null;
  completed_at: string | null;
  /** Execution time only; null until the node actually starts running. */
  exec_s: number | null;
  /** Direct-child task count for subworkflow nodes; null for leaf nodes. */
  child_total: number | null;
  /** FAILED direct-child task count for subworkflow nodes; null for leaf nodes. */
  child_failed: number | null;
}

/** A dependency edge: `from_index` must settle before `to_index` runs. */
export interface WorkflowEdge {
  from_index: number;
  to_index: number;
}

/** A run's metadata plus its full node/edge graph. */
export interface WorkflowRunDetail {
  run: WorkflowRunSummary;
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
  /** Number of FAILED nodes in this run. */
  failed_count: number;
  /** `task_index` of every FAILED node, ascending — for failure navigation. */
  failed_indices: number[];
}

/** One immutable execution attempt of a leaf task. `error_message` holds the cause. */
export interface TaskAttempt {
  attempt: number;
  outcome: string;
  will_retry: boolean;
  error_code: string | null;
  error_message: string | null;
  failed_reason: string | null;
  worker_hostname: string | null;
  started_at: string | null;
  finished_at: string | null;
}

/** The leaf task execution backing a task node. */
export interface LeafTask {
  task_id: string;
  status: WorkflowStatus;
  error_code: string | null;
  failed_reason: string | null;
  retry_count: number;
  max_retries: number;
  enqueued_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  failed_at: string | null;
  /** Queue wait (enqueued -> started). */
  queue_s: number | null;
  /** Execution time (started -> completed). */
  exec_s: number | null;
  worker_hostname: string | null;
  /** Absolute expiry. A manual retry is refused once it has passed. */
  good_until: string | null;
}

/** Per-node failure detail: node error, leaf task, and full attempt history. */
export interface WorkflowTaskDetail {
  task_index: number;
  node_id: string | null;
  task_name: string;
  node_status: WorkflowStatus;
  is_subworkflow: boolean;
  node_error: string | null;
  leaf: LeafTask | null;
  attempts: TaskAttempt[];
}
