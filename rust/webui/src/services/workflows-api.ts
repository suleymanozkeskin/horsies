import { apiGet } from '@/lib/http';
import type {
  WorkflowRunDetail,
  WorkflowRunSummary,
  WorkflowTaskDetail,
} from '@/types/workflows';

/** Distinct root-workflow names, for the run-picker filter. */
export const getWorkflowNames = (): Promise<string[]> =>
  apiGet<string[]>('/workflows/names');

/** Recent root workflow runs, newest first, optionally filtered by name/status. */
export const listWorkflowRuns = (
  params: { name?: string; status?: string; limit?: number } = {}
): Promise<WorkflowRunSummary[]> =>
  apiGet<WorkflowRunSummary[]>('/workflows', {
    ...(params.name === undefined ? {} : { name: params.name }),
    ...(params.status === undefined ? {} : { status: params.status }),
    ...(params.limit === undefined ? {} : { limit: params.limit }),
  });

/** A single run's DAG. Pass a node's `sub_workflow_id` to drill into a subworkflow. */
export const getWorkflowRun = (
  workflowId: string
): Promise<WorkflowRunDetail> =>
  apiGet<WorkflowRunDetail>(`/workflows/${encodeURIComponent(workflowId)}`);

/** Per-node failure detail: node error, leaf task, and attempt history. */
export const getWorkflowTask = (
  workflowId: string,
  taskIndex: number
): Promise<WorkflowTaskDetail> =>
  apiGet<WorkflowTaskDetail>(
    `/workflows/${encodeURIComponent(workflowId)}/tasks/${taskIndex}`
  );
