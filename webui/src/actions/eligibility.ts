// Which action buttons are shown, and which of the shown ones are enabled.
//
// Recomputed from the freshest server data on every render — a poll that lands
// while a confirm dialog is open re-runs this and can turn Confirm into an
// explanatory line.

import type { ActionKind } from '@/types/actions';

export type ActionAvailability =
  | { shown: false }
  | { shown: true; enabled: true }
  | { shown: true; enabled: false; disabledReason: string };

const HIDDEN: ActionAvailability = { shown: false };
const ENABLED: ActionAvailability = { shown: true, enabled: true };

/** Statuses a task can still be cancelled from. Stated as an allowlist rather
 * than "not terminal" so an unknown future status hides the button instead of
 * offering an action whose semantics are unknown. */
const CANCELLABLE_TASK_STATUSES: ReadonlySet<string> = new Set([
  'PENDING',
  'CLAIMED',
  'RUNNING',
]);

/** A manual retry resets the row; only settled-without-success rows qualify. */
const RETRYABLE_TASK_STATUSES: ReadonlySet<string> = new Set([
  'FAILED',
  'EXPIRED',
  'CANCELLED',
]);

const PAUSABLE_RUN_STATUSES: ReadonlySet<string> = new Set(['RUNNING']);
const RESUMABLE_RUN_STATUSES: ReadonlySet<string> = new Set(['PAUSED']);
const CANCELLABLE_RUN_STATUSES: ReadonlySet<string> = new Set([
  'PENDING',
  'RUNNING',
  'PAUSED',
]);

export const EXPIRY_PASSED_REASON =
  'Cannot retry: task expiry (good_until) has passed.';

export interface TaskEligibilityInput {
  isWorkflowTask: boolean;
  status: string;
  /** ISO timestamp, or null when the task never expires. */
  goodUntil: string | null;
}

/** True when `good_until` is set and already in the past. An unparseable value
 * is treated as "not expired" so a bad timestamp cannot silently disable the
 * only recovery affordance; the server still refuses with TASK_EXPIRY_PASSED. */
function expiryPassed(goodUntil: string | null, nowMs: number): boolean {
  if (goodUntil === null) {
    return false;
  }
  const parsed = Date.parse(goodUntil);
  return !Number.isNaN(parsed) && parsed <= nowMs;
}

export function taskCancelAvailability(
  task: TaskEligibilityInput
): ActionAvailability {
  if (task.isWorkflowTask) {
    return HIDDEN;
  }
  return CANCELLABLE_TASK_STATUSES.has(task.status) ? ENABLED : HIDDEN;
}

export function taskRetryAvailability(
  task: TaskEligibilityInput,
  nowMs: number
): ActionAvailability {
  if (task.isWorkflowTask) {
    return HIDDEN;
  }
  if (!RETRYABLE_TASK_STATUSES.has(task.status)) {
    return HIDDEN;
  }
  return expiryPassed(task.goodUntil, nowMs)
    ? { shown: true, enabled: false, disabledReason: EXPIRY_PASSED_REASON }
    : ENABLED;
}

/** Workflow-level actions. There is no restart primitive, so no restart action
 * is ever offered. */
export function workflowActionAvailability(
  action: ActionKind,
  runStatus: string
): ActionAvailability {
  switch (action) {
    case 'workflow-pause':
      return PAUSABLE_RUN_STATUSES.has(runStatus) ? ENABLED : HIDDEN;
    case 'workflow-resume':
      return RESUMABLE_RUN_STATUSES.has(runStatus) ? ENABLED : HIDDEN;
    case 'workflow-cancel':
      return CANCELLABLE_RUN_STATUSES.has(runStatus) ? ENABLED : HIDDEN;
    case 'task-cancel':
    case 'task-retry':
      return HIDDEN;
  }
}
