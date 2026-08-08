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
