// Runs and nodes for the timing tests, built from the shared examples of the
// core (test/timing-examples.ts), so the tests draw the JSON the API writes.

import type { NodeTiming, RunTiming } from '@/types/timing';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

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
    task_index: taskIndex,
    node_id: nodeId,
    task_name: `tasks.${nodeId}`,
    node_status: nodeStatus,
    is_subworkflow: false,
    sub_workflow_id: null,
    allow_failed_deps: false,
    started_at: null,
    completed_at: null,
    exec_s: null,
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
      id: 'run-1',
      name: 'nightly_rollup',
      definition_key: null,
      status,
      created_at: '2026-11-22T12:00:00Z',
      completed_at: null,
      wall_s: null,
    },
    nodes,
    edges: [],
    failed_count: 0,
    failed_indices: [],
    trace_id: null,
    timing,
  };
}
