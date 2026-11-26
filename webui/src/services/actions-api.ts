import { apiPost } from '@/lib/http';
import type { ActionKind, ActionResponse } from '@/types/actions';

/** The task statuses that a task cancel acts on: the `policy` of the cancel
 * body. Only `INCLUDE_RUNNING` touches a RUNNING row, because the process
 * keeps executing (§ confirm copy). */
export type CancelPolicy = 'PENDING_OR_CLAIMED_ONLY' | 'INCLUDE_RUNNING';

/** Mark a non-terminal task CANCELLED with `policy`. */
export const cancelTask = (
  taskId: string,
  policy: CancelPolicy
): Promise<ActionResponse> =>
  apiPost<ActionResponse>(`/tasks/${encodeURIComponent(taskId)}/cancel`, {
    policy,
  });

export const pauseWorkflow = (workflowId: string): Promise<ActionResponse> =>
  apiPost<ActionResponse>(`/workflows/${encodeURIComponent(workflowId)}/pause`);

export const resumeWorkflow = (workflowId: string): Promise<ActionResponse> =>
  apiPost<ActionResponse>(`/workflows/${encodeURIComponent(workflowId)}/resume`);

export const cancelWorkflow = (workflowId: string): Promise<ActionResponse> =>
  apiPost<ActionResponse>(`/workflows/${encodeURIComponent(workflowId)}/cancel`);

/** Extra input an action needs beyond the entity id. */
export interface ActionArgs {
  /** Task cancel only: acknowledge that a RUNNING process keeps executing. */
  includeRunning: boolean;
}

/** Dispatch one action against one entity id. */
export function invokeAction(
  action: ActionKind,
  entityId: string,
  args: ActionArgs
): Promise<ActionResponse> {
  switch (action) {
    case 'task-cancel':
      return cancelTask(
        entityId,
        args.includeRunning ? 'INCLUDE_RUNNING' : 'PENDING_OR_CLAIMED_ONLY'
      );
    case 'workflow-pause':
      return pauseWorkflow(entityId);
    case 'workflow-resume':
      return resumeWorkflow(entityId);
    case 'workflow-cancel':
      return cancelWorkflow(entityId);
  }
}
