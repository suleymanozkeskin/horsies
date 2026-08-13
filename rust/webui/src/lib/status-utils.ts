// Single source for status -> color/label, shared by the task and workflow
// views. Their status sets overlap only on shared names, which map to the same
// color in both, so the union below is conflict-free. Colors are tokens
// (tokens.css); returned as `var(--token)` for inline styles.

/** CSS variable reference for a status' representative color. */
export function statusColorVar(status: string): string {
  switch (status) {
    case 'COMPLETED':
    case 'SUCCESS': // task-attempt outcome
      return 'var(--success)';
    case 'RUNNING':
      return 'var(--info)';
    case 'FAILED':
    case 'WORKER_FAILURE': // task-attempt outcome
      return 'var(--error)';
    case 'READY': // workflow only
    case 'ENQUEUED': // workflow only
      return 'var(--info-light)';
    case 'CLAIMED': // task only
    case 'PAUSED': // workflow only
      return 'var(--warning)';
    case 'EXPIRED': // task or workflow: time ran out
      return 'var(--warning-dark)';
    case 'CANCELLED':
      return 'var(--cancelled)';
    case 'PENDING':
    case 'SKIPPED': // workflow only
      return 'var(--muted-foreground)';
    default:
      return 'var(--muted-foreground)';
  }
}

/** Lowercased label for chips/legend. */
export const statusLabel = (status: string): string => status.toLowerCase();

/**
 * The task lifecycle statuses, in the order the monitoring API reports them.
 * The stats and breakdown endpoints always emit all seven, zeros included, so
 * a shorter list means "not loaded yet" — never "these statuses are absent".
 * That makes the row geometry of a status strip known before its counts are.
 */
export const TASK_STATUS_ORDER = [
  'PENDING',
  'CLAIMED',
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
  'EXPIRED',
] as const;

/** One of the seven lifecycle statuses — closed, unlike the open `TaskStatus`
 * response union, so a mapping over it can be checked for exhaustiveness. */
export type LifecycleStatus = (typeof TASK_STATUS_ORDER)[number];
