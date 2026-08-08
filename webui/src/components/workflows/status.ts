import type { WorkflowStatus } from '@/types/workflows';

/** Statuses shown in the legend, in display order. */
export const LEGEND_STATUSES: WorkflowStatus[] = [
  'COMPLETED',
  'RUNNING',
  'FAILED',
  'PENDING',
  'READY',
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
