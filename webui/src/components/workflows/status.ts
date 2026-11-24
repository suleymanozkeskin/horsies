import type { WorkflowStatus } from '@/types/workflows';

/** Node statuses in display order: the order of the status counts of a run. */
export const NODE_STATUS_ORDER: WorkflowStatus[] = [
  'COMPLETED',
  'RUNNING',
  'FAILED',
  'PENDING',
  'READY',
  'ENQUEUED',
  'SKIPPED',
  'PAUSED',
  'CANCELLED',
  'EXPIRED',
];

/** Run-level statuses offered as run-list filters, in display order. */
export const RUN_STATUS_FILTERS: WorkflowStatus[] = [
  'RUNNING',
  'PENDING',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
  'EXPIRED',
  'PAUSED',
];

/** The number of nodes in one status. */
export interface StatusCount {
  status: WorkflowStatus;
  count: number;
}

/** The statuses of the nodes with their counts: the statuses of
 * NODE_STATUS_ORDER first, in that order, then any other status in name
 * order. A status with no node is left out. */
export function nodeStatusCounts(statuses: readonly WorkflowStatus[]): StatusCount[] {
  const counts = new Map<WorkflowStatus, number>();
  for (const status of statuses) {
    counts.set(status, (counts.get(status) ?? 0) + 1);
  }
  const known = NODE_STATUS_ORDER.filter(status => counts.has(status));
  const other = [...counts.keys()].filter(status => !NODE_STATUS_ORDER.includes(status)).sort();
  return [...known, ...other].map(status => ({ status, count: counts.get(status) ?? 0 }));
}
