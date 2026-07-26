// Action request/response shapes for the mutating endpoints.

import type { TaskStatus } from '@/types/tasks';
import type { WorkflowStatus } from '@/types/workflows';

/** The five actions the UI can invoke. Workflow restart does not exist. */
export type ActionKind =
  | 'task-cancel'
  | 'task-retry'
  | 'workflow-pause'
  | 'workflow-resume'
  | 'workflow-cancel';

/** Entity an action targets. Keys the one-in-flight-per-entity registry. */
export type EntityKind = 'task' | 'workflow';

export interface EntityRef {
  kind: EntityKind;
  id: string;
}

export type ActionOutcome = 'cancelled' | 'retried' | 'paused' | 'resumed';

/** The only warning the server emits: resume committed, recovery pass failed. */
export type ActionWarning = 'post_resume_recovery_failed';

/** 200 envelope shared by every action endpoint. */
export interface ActionResponse {
  outcome: ActionOutcome;
  /** Task actions only: the status the row held before the CAS. */
  was_status?: TaskStatus;
  /** Retry only: the attempt number the next run will record. */
  next_attempt_number?: number;
  warning?: ActionWarning | null;
}

/** Server-side codes surfaced as 409. `SCHEMA_INCOMPATIBLE` is not a state
 * conflict: the stored schema does not match this build, so actions are
 * force-disabled server-side regardless of policy. */
export type ConflictCode =
  | 'TASK_NOT_CANCELLABLE'
  | 'TASK_NOT_RETRYABLE'
  | 'TASK_EXPIRY_PASSED'
  | 'STATE_CONFLICT'
  | 'SCHEMA_INCOMPATIBLE';

export const SCHEMA_INCOMPATIBLE_CODE = 'SCHEMA_INCOMPATIBLE';

/** 409 body. `current_status` is the freshly re-read server status. */
export interface ConflictBody {
  code: ConflictCode;
  current_status: TaskStatus | WorkflowStatus | null;
}
