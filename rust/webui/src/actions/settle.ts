// Settle predicates: when refetched server data proves an action landed.
//
// "Settled" is not the same as "succeeded". A 200 already means the CAS
// committed; settling is about the UI's own boost window and the copy it shows
// while residual states (draining nodes, a re-enqueued task waiting for a
// worker) resolve.

import type { ActionKind } from '@/types/actions';
import type { TaskDetail } from '@/types/tasks';
import type { WorkflowRunDetail } from '@/types/workflows';

/** Node states that still count as unfinished scheduling work under a run. */
const UNSETTLED_NODE_STATUSES: ReadonlySet<string> = new Set([
  'PENDING',
  'READY',
  'ENQUEUED',
]);

/** What the entity looked like immediately before the action was submitted. */
export interface SettleContext {
  /** Status the server reported in the action response (task actions). */
  wasStatus: string | null;
}

const taskCancelSettled = (detail: TaskDetail): boolean =>
  detail.leaf.status === 'CANCELLED';

const workflowCancelSettled = (detail: WorkflowRunDetail): boolean =>
  detail.run.status === 'CANCELLED' &&
  !detail.nodes.some(node => UNSETTLED_NODE_STATUSES.has(node.node_status));

/** Nodes still executing under a cancelled/paused run — the "draining" count. */
export const executingNodeCount = (detail: WorkflowRunDetail): number =>
  detail.nodes.filter(node => node.node_status === 'RUNNING').length;

export function isTaskActionSettled(
  action: 'task-cancel',
  detail: TaskDetail,
  _context: SettleContext
): boolean {
  switch (action) {
    case 'task-cancel':
      return taskCancelSettled(detail);
  }
}

export function isWorkflowActionSettled(
  action: 'workflow-pause' | 'workflow-resume' | 'workflow-cancel',
  detail: WorkflowRunDetail
): boolean {
  switch (action) {
    case 'workflow-pause':
      return detail.run.status === 'PAUSED';
    case 'workflow-resume':
      return detail.run.status === 'RUNNING';
    case 'workflow-cancel':
      return workflowCancelSettled(detail);
  }
}

/** Entity kind an action targets — used to pick the refetch/settle path. */
export function entityOf(action: ActionKind): 'task' | 'workflow' {
  switch (action) {
    case 'task-cancel':
      return 'task';
    case 'workflow-pause':
    case 'workflow-resume':
    case 'workflow-cancel':
      return 'workflow';
  }
}
