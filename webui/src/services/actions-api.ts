import { apiPost } from '@/lib/http';
import type { ActionKind, ActionResponse } from '@/types/actions';

/** Mark a non-terminal task CANCELLED. `includeRunning` is required to touch a
 * RUNNING row, because the process keeps executing (§ confirm copy). */
export const cancelTask = (
  taskId: string,
  includeRunning: boolean
): Promise<ActionResponse> =>
  apiPost<ActionResponse>(`/tasks/${encodeURIComponent(taskId)}/cancel`, {
    include_running: includeRunning,
  });

/** Reset a FAILED/EXPIRED/CANCELLED task row and re-enqueue it. */
export const retryTask = (taskId: string): Promise<ActionResponse> =>
  apiPost<ActionResponse>(`/tasks/${encodeURIComponent(taskId)}/retry`);

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
      return cancelTask(entityId, args.includeRunning);
    case 'task-retry':
      return retryTask(entityId);
    case 'workflow-pause':
      return pauseWorkflow(entityId);
    case 'workflow-resume':
      return resumeWorkflow(entityId);
    case 'workflow-cancel':
      return cancelWorkflow(entityId);
  }
}
