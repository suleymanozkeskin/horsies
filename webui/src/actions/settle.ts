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
  /** `retry_count` captured before the retry POST, or null when unknown. */
  retryCountBefore: number | null;
}

const taskCancelSettled = (detail: TaskDetail): boolean =>
  detail.leaf.status === 'CANCELLED';

/**
 * A retry is settled once the row visibly moved. The `retry_count` clause
 * closes the hole where a fast task fails again into the SAME status between
 * two refetches: the reset sets retry_count to MAX(attempt), so it always
 * changes when any prior attempt exists.
 */
const taskRetrySettled = (
  detail: TaskDetail,
  context: SettleContext
): boolean => {
  if (context.wasStatus !== null && detail.leaf.status !== context.wasStatus) {
    return true;
  }
  return (
    context.retryCountBefore !== null &&
    detail.leaf.retry_count !== context.retryCountBefore
  );
};

const workflowCancelSettled = (detail: WorkflowRunDetail): boolean =>
  detail.run.status === 'CANCELLED' &&
  !detail.nodes.some(node => UNSETTLED_NODE_STATUSES.has(node.node_status));

/** Nodes still executing under a cancelled/paused run — the "draining" count. */
export const executingNodeCount = (detail: WorkflowRunDetail): number =>
  detail.nodes.filter(node => node.node_status === 'RUNNING').length;

export function isTaskActionSettled(
  action: 'task-cancel' | 'task-retry',
  detail: TaskDetail,
  context: SettleContext
): boolean {
  switch (action) {
    case 'task-cancel':
      return taskCancelSettled(detail);
    case 'task-retry':
      return taskRetrySettled(detail, context);
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
    case 'task-retry':
      return 'task';
    case 'workflow-pause':
    case 'workflow-resume':
    case 'workflow-cancel':
      return 'workflow';
  }
}
