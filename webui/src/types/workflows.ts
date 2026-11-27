// Mirrors the horsies.monitoring workflow response models. The DAG is
// run-derived: every workflow run persists its full node set, so these types
// describe any workflow definition and any run generically.

import type { AttemptPhases, NodeTiming, RunTiming } from '@/types/timing';

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
  workflow_id: string;
  workflow_name: string;
  definition_key: string | null;
  status: WorkflowStatus;
  created_at: string | null;
  completed_at: string | null;
  /** Wall-clock seconds, exact to the microsecond: completed_at (or now)
   * minus created_at. */
  wall_seconds: number | null;
}

/** A single DAG node. `node_index` is the stable key; `node_id` is a label. */
export interface WorkflowNode {
  node_index: number;
  node_id: string | null;
  task_name: string;
  node_status: WorkflowStatus;
  is_sub_workflow: boolean;
  sub_workflow_id: string | null;
  allow_failed_dependencies: boolean;
  started_at: string | null;
  completed_at: string | null;
  /** Execution time only, in seconds, exact to the microsecond; null until
   * the node actually starts running. */
  execution_seconds: number | null;
  /** Direct-child task count for subworkflow nodes; null for leaf nodes. */
  child_total: number | null;
  /** FAILED direct-child task count for subworkflow nodes; null for leaf nodes. */
  child_failed: number | null;
  /** Phase timing of the node: its segments, endpoint and coverage label. */
  timing: NodeTiming;
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
  /** `node_index` of every FAILED node, ascending — for failure navigation. */
  failed_indices: number[];
  /** Trace id of the span that started the run (32 hex digits); null when
   * the start carried no trace. */
  trace_id: string | null;
  /** Phase timing of the run: read time, range, pauses and coverage. */
  timing: RunTiming;
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
  /** Phase record of the attempt row. */
  phases: AttemptPhases;
}

/** The leaf task execution backing a task node. */
export interface TaskRow {
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
  /** Queue wait (enqueued -> started), in seconds, exact to the microsecond. */
  queue_seconds: number | null;
  /** Execution time (started -> completed), in seconds, exact to the
   * microsecond. */
  execution_seconds: number | null;
  worker_hostname: string | null;
  /** Absolute expiry. */
  good_until: string | null;
  /** Trace id of the span that sent the task (32 hex digits); null when the
   * send carried no trace. */
  trace_id: string | null;
}

/** Per-node failure detail: node error, leaf task, and full attempt history. */
export interface WorkflowNodeDetail {
  node_index: number;
  node_id: string | null;
  task_name: string;
  node_status: WorkflowStatus;
  is_sub_workflow: boolean;
  node_error: string | null;
  task_row: TaskRow | null;
  attempts: TaskAttempt[];
}
