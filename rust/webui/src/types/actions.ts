// Action request/response shapes for the mutating endpoints.

import type { TaskStatus } from '@/types/tasks';
import type { WorkflowStatus } from '@/types/workflows';

/** The four actions the UI can invoke. Workflow restart does not exist. */
export type ActionKind =
  | 'task-cancel'
  | 'workflow-pause'
  | 'workflow-resume'
  | 'workflow-cancel';

/** Entity an action targets. Keys the one-in-flight-per-entity registry. */
export type EntityKind = 'task' | 'workflow';

export interface EntityRef {
  kind: EntityKind;
  id: string;
}

export type ActionOutcome = 'cancelled' | 'paused' | 'resumed';

/** The only warning the server emits: resume committed, recovery pass failed. */
export type ActionWarning = 'post_resume_recovery_failed';

/** 200 envelope shared by every action endpoint. */
export interface ActionResponse {
  outcome: ActionOutcome;
  /** Task actions only: the status the row held before the CAS. */
  was_status?: TaskStatus;
  warning?: ActionWarning | null;
}

/** Server-side codes surfaced as 409. Schema compatibility codes are not state
 * conflicts. Actions stay disabled until the probe confirms a matching schema. */
export type ConflictCode =
  | 'TASK_NOT_CANCELLABLE'
  | 'STATE_CONFLICT'
  | 'SCHEMA_INCOMPATIBLE'
  | 'SCHEMA_UNKNOWN';

export const SCHEMA_INCOMPATIBLE_CODE = 'SCHEMA_INCOMPATIBLE';
export const SCHEMA_UNKNOWN_CODE = 'SCHEMA_UNKNOWN';

/** 409 body. `current_status` is the freshly re-read server status. */
export interface ConflictBody {
  code: ConflictCode;
  current_status: TaskStatus | WorkflowStatus | null;
}
