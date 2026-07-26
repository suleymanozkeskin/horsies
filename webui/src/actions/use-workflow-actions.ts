// Workflow run detail + its action lifecycle, wired together.

import { useEffect, useState } from 'react';

import type { ActionAvailability } from '@/actions/eligibility';
import { workflowActionAvailability } from '@/actions/eligibility';
import {
  entityOf,
  executingNodeCount,
  isWorkflowActionSettled,
  type SettleContext,
} from '@/actions/settle';
import {
  useEntityAction,
  type EntityActionControls,
} from '@/actions/use-entity-action';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import type { ActionKind } from '@/types/actions';
import type { WorkflowRunDetail } from '@/types/workflows';

type WorkflowAction = 'workflow-pause' | 'workflow-resume' | 'workflow-cancel';

const runSnapshot = (detail: WorkflowRunDetail | undefined): SettleContext => ({
  wasStatus: detail?.run.status ?? null,
  retryCountBefore: null,
});

const hasSettled = (
  action: ActionKind,
  detail: WorkflowRunDetail
): boolean =>
  entityOf(action) === 'workflow' &&
  isWorkflowActionSettled(action as WorkflowAction, detail);

export interface WorkflowActionsView {
  detail: WorkflowRunDetail | undefined;
  isLoading: boolean;
  isError: boolean;
  controls: EntityActionControls;
  availability: Record<WorkflowAction, ActionAvailability>;
}

/** `workflowId` null means no run is selected: the detail query stays disabled
 * and no action can be started, because every affordance needs run data. */
export function useWorkflowActions(
  workflowId: string | null
): WorkflowActionsView {
  const [boost, setBoost] = useState<number | false>(false);
  const { detail, isLoading, isError, reread } = useWorkflowRun(
    workflowId,
    boost
  );

  const controls = useEntityAction<WorkflowRunDetail>({
    entity: { kind: 'workflow', id: workflowId ?? '' },
    detail,
    snapshot: runSnapshot,
    hasSettled,
    reread,
    successContext: current => ({
      drainingNodes: current === undefined ? 0 : executingNodeCount(current),
      workerHostname: null,
    }),
  });

  useEffect(() => {
    setBoost(controls.boostInterval);
  }, [controls.boostInterval]);

  const status = detail?.run.status ?? null;
  const availabilityFor = (action: WorkflowAction): ActionAvailability =>
    status === null ? { shown: false } : workflowActionAvailability(action, status);

  return {
    detail,
    isLoading,
    isError,
    controls,
    availability: {
      'workflow-pause': availabilityFor('workflow-pause'),
      'workflow-resume': availabilityFor('workflow-resume'),
      'workflow-cancel': availabilityFor('workflow-cancel'),
    },
  };
}
