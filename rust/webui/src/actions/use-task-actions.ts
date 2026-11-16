// Task detail + its action lifecycle, wired together.
//
// The settle boost feeds back into the detail query, so it is held in state
// here: the query must be created before the action hook can read its data, and
// a one-render lag on a polling cadence is immaterial.

import { useEffect, useState } from 'react';

import type { ActionAvailability } from '@/actions/eligibility';
import { taskCancelAvailability } from '@/actions/eligibility';
import {
  entityOf,
  isTaskActionSettled,
  type SettleContext,
} from '@/actions/settle';
import {
  useEntityAction,
  type EntityActionControls,
} from '@/actions/use-entity-action';
import { useTask } from '@/hooks/use-task';
import type { ActionKind } from '@/types/actions';
import type { TaskDetail } from '@/types/tasks';

const taskSnapshot = (detail: TaskDetail | undefined): SettleContext => ({
  wasStatus: detail?.leaf.status ?? null,
});

const hasSettled = (
  action: ActionKind,
  detail: TaskDetail,
  context: SettleContext
): boolean =>
  entityOf(action) === 'task' &&
  isTaskActionSettled(action as 'task-cancel', detail, context);

export interface TaskActionsView {
  detail: TaskDetail | undefined;
  isLoading: boolean;
  isError: boolean;
  controls: EntityActionControls;
  cancel: ActionAvailability;
}

export function useTaskActions(
  taskId: string,
  onGone?: () => void
): TaskActionsView {
  const [boost, setBoost] = useState<number | false>(false);
  const { detail, isLoading, isError, reread } = useTask(taskId, boost);

  const controls = useEntityAction<TaskDetail>({
    entity: { kind: 'task', id: taskId },
    detail,
    snapshot: taskSnapshot,
    hasSettled,
    reread,
    successContext: current => ({
      drainingNodes: 0,
      workerHostname: current?.leaf.worker_hostname ?? null,
    }),
    ...(onGone === undefined ? {} : { onGone }),
  });

  useEffect(() => {
    setBoost(controls.boostInterval);
  }, [controls.boostInterval]);

  const eligibilityInput =
    detail === undefined
      ? null
      : {
          isWorkflowTask: detail.is_workflow_task,
          status: detail.leaf.status,
        };

  return {
    detail,
    isLoading,
    isError,
    controls,
    cancel:
      eligibilityInput === null
        ? { shown: false }
        : taskCancelAvailability(eligibilityInput),
  };
}
