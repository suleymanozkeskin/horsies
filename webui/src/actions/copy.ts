// Every user-visible string the action lifecycle produces.
//
// Centralised because the wording is part of the contract: each message states
// what horsies actually does, including the parts that are eventually
// consistent (a cancelled task's process keeps running; cancelled workflow
// nodes drain).

import type { ActionKind, ActionResponse } from '@/types/actions';

export interface ConfirmCopy {
  title: string;
  body: string;
  confirmLabel: string;
  dismissLabel: string;
  /** When set, the confirm button stays disabled until this box is ticked. */
  acknowledgement?: string;
}

export interface TaskConfirmContext {
  status: string;
  queueName: string;
  workerHostname: string | null;
  /** Attempt number this run will record — a client-side estimate; the server
   * response is authoritative. */
  nextAttempt: number;
}

const DISMISS = 'Keep';

const RUNNING_ACKNOWLEDGEMENT = 'I understand the process keeps running';

export function taskCancelConfirm(context: TaskConfirmContext): ConfirmCopy {
  if (context.status === 'RUNNING') {
    const host = context.workerHostname ?? 'its worker';
    return {
      title: 'Cancel running task?',
      body:
        'Horsies does not kill running processes. The task’s code KEEPS ' +
        `EXECUTING on ${host} until it finishes and its side effects still ` +
        'happen, but its result will be discarded and the task row is marked ' +
        'CANCELLED now. No attempt record will be written for this run.',
      confirmLabel: 'Cancel task',
      dismissLabel: DISMISS,
      acknowledgement: RUNNING_ACKNOWLEDGEMENT,
    };
  }
  return {
    title: 'Cancel task?',
    body:
      'The task will be marked CANCELLED and will not run. Waiting result ' +
      'handles receive TASK_CANCELLED.',
    confirmLabel: 'Cancel task',
    dismissLabel: DISMISS,
  };
}

export function taskRetryConfirm(context: TaskConfirmContext): ConfirmCopy {
  return {
    title: 'Retry task?',
    body:
      `The task row is reset and re-enqueued on queue '${context.queueName}' ` +
      'with the same payload. Attempt history is preserved; this run will be ' +
      `attempt ${context.nextAttempt}.`,
    confirmLabel: 'Retry task',
    dismissLabel: DISMISS,
  };
}

export const WORKFLOW_CONFIRM: Record<
  'workflow-pause' | 'workflow-resume' | 'workflow-cancel',
  ConfirmCopy
> = {
  'workflow-pause': {
    title: 'Pause workflow?',
    body:
      'New nodes stop being scheduled and claimed-but-not-started work is ' +
      'returned to READY. Nodes already executing will finish.',
    confirmLabel: 'Pause workflow',
    dismissLabel: DISMISS,
  },
  'workflow-resume': {
    title: 'Resume workflow?',
    body: 'Paused nodes are re-enqueued immediately.',
    confirmLabel: 'Resume workflow',
    dismissLabel: DISMISS,
  },
  'workflow-cancel': {
    title: 'Cancel workflow?',
    body:
      'The workflow and all its sub-workflows are cancelled. Pending nodes ' +
      'are skipped. Nodes already executing keep running to completion ' +
      '(draining) but their results will not advance the workflow.',
    confirmLabel: 'Cancel workflow',
    dismissLabel: DISMISS,
  },
};

/** Short verb used in failure copy: "Cancel failed: …". */
export const ACTION_LABEL: Record<ActionKind, string> = {
  'task-cancel': 'Cancel',
  'task-retry': 'Retry',
  'workflow-pause': 'Pause',
  'workflow-resume': 'Resume',
  'workflow-cancel': 'Cancel',
};

/** Entity noun used in 404 copy. */
export const ENTITY_LABEL = {
  task: 'Task',
  workflow: 'Workflow run',
} as const;

export interface SuccessContext {
  /** Nodes currently executing under a cancelled run, for the draining count. */
  drainingNodes: number;
  workerHostname: string | null;
}

/** §13.5 success toast for one action, given the server's response envelope. */
export function successMessage(
  action: ActionKind,
  response: ActionResponse,
  context: SuccessContext
): string {
  switch (action) {
    case 'task-cancel': {
      if (response.was_status !== 'RUNNING') {
        return 'Task cancelled.';
      }
      const host = context.workerHostname ?? 'its worker';
      return (
        'Task cancelled. The running process on ' +
        `${host} will keep executing until it finishes.`
      );
    }
    case 'task-retry':
      return `Task re-enqueued (attempt ${response.next_attempt_number ?? '?'}).`;
    case 'workflow-pause':
      return 'Workflow paused. Executing nodes will finish.';
    case 'workflow-resume':
      return response.warning === 'post_resume_recovery_failed'
        ? 'Resume applied, but a post-resume recovery step failed; check worker logs.'
        : 'Workflow resumed.';
    case 'workflow-cancel':
      return `Workflow cancelled. ${context.drainingNodes} executing node(s) draining.`;
  }
}

/** 409: the CAS was lost to a concurrent change; the panel refetches. */
export function conflictMessage(
  action: ActionKind,
  currentStatus: string | null
): string {
  const verb = ACTION_LABEL[action].toLowerCase();
  return currentStatus === null
    ? `Cannot ${verb}: the state changed before the request arrived.`
    : `Cannot ${verb}: the state is now ${currentStatus.toLowerCase()}.`;
}

/** 404: retention can remove rows out from under an open panel. */
export function goneMessage(entity: keyof typeof ENTITY_LABEL): string {
  return `${ENTITY_LABEL[entity]} no longer exists (retention may have removed it).`;
}

/** 503: the broker or database could not be reached. */
export function unavailableMessage(
  action: ActionKind,
  detail: string | null
): string {
  const reason = detail === null ? 'the request could not be completed' : detail;
  return `${ACTION_LABEL[action]} failed: ${reason}. The broker may be unreachable.`;
}

export const FORBIDDEN_MESSAGE = 'Not authorized to perform actions.';

/** 409 SCHEMA_INCOMPATIBLE — not a state conflict; actions stay off until the
 * stored schema matches this build. */
export const SCHEMA_INCOMPATIBLE_MESSAGE =
  'Actions are disabled: the database schema does not match this UI version.';

/** 400 TASK_IS_WORKFLOW_TASK — the row is managed by its workflow. */
export const WORKFLOW_MANAGED_MESSAGE =
  'This task is managed by its workflow; task-level actions do not apply.';

/** The POST produced no response and the entity does not show the effect. */
export function unverifiedMessage(action: ActionKind): string {
  return `${ACTION_LABEL[action]} could not be confirmed. Nothing appears to have changed.`;
}

/** The POST produced no response but the re-read entity shows the effect, so
 * the action landed and the response was lost in transit. */
export function verifiedMessage(action: ActionKind): string {
  return `${ACTION_LABEL[action]} applied.`;
}

/** §13.6 residual states — rendered, never inferred as failure. */
export const RESIDUAL_COPY = {
  draining: {
    badge: 'draining',
    tooltip:
      'This node was executing when the workflow was cancelled. Horsies ' +
      'never kills running task processes; the node finishes but will not ' +
      'advance the workflow. If its worker crashed, it may remain here ' +
      'indefinitely.',
  },
  finishing: {
    badge: 'finishing',
    tooltip: 'Pause lets already-executing work finish.',
  },
} as const;

/** §13.2 note shown instead of actions on a workflow-bound task row. The run
 * reference that follows is a link when the server supplies `workflow_id`. */
export const MANAGED_BY_WORKFLOW_PREFIX = 'Managed by workflow';
