// Runs and nodes for the timing tests, built from the shared examples of the
// core (test/timing-examples.ts), so the tests draw the JSON the API writes.

import type {
  NodeTiming,
  RunTiming,
  SegmentPhase,
  TerminalNodeStatus,
  TimingSegment,
} from '@/types/timing';
import type { WorkflowEdge, WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

import { TIMING_EXAMPLES } from './timing-examples';

/** The item at `index` of a test list; a missing one is a defect of the
 * test. */
export function nth<T>(list: readonly T[], index: number): T {
  const value = list[index];
  if (value === undefined) {
    throw new Error(`test list has no item ${index}`);
  }
  return value;
}

const example = nth;

/** Node timings of the shared examples: a retried node with every exact
 * phase, a partial node with an open segment, an unavailable node and an
 * inconsistent node (clock order). */
export const EXACT_RETRIED_NODE: NodeTiming = example(TIMING_EXAMPLES.node_timings, 0);
export const PARTIAL_OPEN_NODE: NodeTiming = example(TIMING_EXAMPLES.node_timings, 1);
export const UNAVAILABLE_NODE: NodeTiming = example(TIMING_EXAMPLES.node_timings, 2);
export const CLOCK_ORDER_NODE: NodeTiming = example(TIMING_EXAMPLES.node_timings, 3);

/** Run timings of the shared examples: unavailable (bound exceeded) and a
 * composed run with an open segment and three pauses (resumed, ended by the
 * run, open), from 12:00:00 to 12:01:40, read at 12:01:40. */
export const UNAVAILABLE_RUN: RunTiming = example(TIMING_EXAMPLES.run_timings, 0);
export const OPEN_RUN_WITH_PAUSES: RunTiming = example(TIMING_EXAMPLES.run_timings, 1);

export function workflowNode(
  taskIndex: number,
  nodeId: string,
  nodeStatus: string,
  timing: NodeTiming
): WorkflowNode {
  return {
    node_index: taskIndex,
    node_id: nodeId,
    task_name: `tasks.${nodeId}`,
    node_status: nodeStatus,
    is_sub_workflow: false,
    sub_workflow_id: null,
    allow_failed_dependencies: false,
    started_at: null,
    completed_at: null,
    execution_seconds: null,
    child_total: null,
    child_failed: null,
    timing,
  };
}

export function workflowRun(
  status: string,
  nodes: WorkflowNode[],
  timing: RunTiming
): WorkflowRunDetail {
  return {
    run: {
      workflow_id: 'run-1',
      workflow_name: 'nightly_rollup',
      definition_key: null,
      status,
      created_at: '2026-11-22T12:00:00Z',
      completed_at: null,
      wall_seconds: null,
    },
    nodes,
    edges: [],
    failed_count: 0,
    failed_indices: [],
    trace_id: null,
    timing,
  };
}

/** The UTC time text `seconds` after 12:00:00 on the day of the examples. */
export const at = (seconds: number): string =>
  new Date(Date.UTC(2026, 10, 22, 12, 0, 0) + seconds * 1_000).toISOString();

/** An exact node timing from `[phase, start, end]` rows (seconds after
 * 12:00:00) that ended with `status` at `endAt`. Dependency wait and
 * advancing belong to the node; every other phase to attempt 1. */
export function exactTiming(
  rows: readonly [SegmentPhase['kind'], number, number][],
  status: TerminalNodeStatus,
  endAt: number
): NodeTiming {
  const segments: TimingSegment[] = rows.map(([kind, start, end]) => ({
    phase:
      kind === 'claimed'
        ? { kind, claim: { kind: 'not_recorded' } }
        : kind === 'unknown'
          ? { kind, reason: 'WORKER_LOST' }
          : { kind },
    started_at: at(start),
    end: { kind: 'closed', at: at(end) },
    source:
      kind === 'pending' || kind === 'advancing'
        ? { kind: 'node' }
        : { kind: 'attempt', generation: 1, task_id: 't', attempt: 1 },
  }));
  return { kind: 'exact', segments, endpoint: { kind: 'terminal', status, at: at(endAt) } };
}

/** A run where `b` fails and skips `d`; `e` joins `d` and `b` with a
 * tolerant join and ends last. `x` has no timing. The longest chain is
 * a → b → e: d and b end at one instant (the skip cascade), and b ran. */
export const CASCADE_NODES: WorkflowNode[] = [
  workflowNode(0, 'a', 'COMPLETED', exactTiming([['enqueued', 0, 1], ['running', 1, 3], ['advancing', 3, 3.5]], 'COMPLETED', 3.5)),
  workflowNode(1, 'b', 'FAILED', exactTiming([['pending', 0, 3.5], ['running', 3.5, 5]], 'FAILED', 5)),
  workflowNode(2, 'c', 'COMPLETED', exactTiming([['pending', 0, 3.5], ['running', 3.5, 4]], 'COMPLETED', 4)),
  workflowNode(3, 'd', 'SKIPPED', exactTiming([['pending', 0, 5]], 'SKIPPED', 5)),
  { ...workflowNode(4, 'e', 'COMPLETED', exactTiming([['pending', 0, 5], ['enqueued', 5, 6], ['running', 6, 8]], 'COMPLETED', 8)), allow_failed_dependencies: true },
  workflowNode(5, 'x', 'COMPLETED', UNAVAILABLE_NODE),
];

export const CASCADE_EDGES: WorkflowEdge[] = [
  { from_index: 0, to_index: 1 },
  { from_index: 0, to_index: 2 },
  { from_index: 1, to_index: 3 },
  { from_index: 2, to_index: 3 },
  { from_index: 3, to_index: 4 },
  { from_index: 1, to_index: 4 },
];
