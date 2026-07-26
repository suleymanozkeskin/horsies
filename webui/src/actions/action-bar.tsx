// Action affordances for the two detail surfaces.
//
// Actions live in detail panels only. Table and run-list rows carry neither the
// full eligibility data (`good_until` is on the leaf, not the row) nor enough
// context for a destructive click to be safe.

import { Link } from '@tanstack/react-router';
import { Ban, Pause, Play, RotateCcw } from 'lucide-react';

import { useCapability } from '@/actions/capability';
import { ActionConfirmDialog } from '@/actions/confirm-dialog';
import {
  MANAGED_BY_WORKFLOW_PREFIX,
  taskCancelConfirm,
  taskRetryConfirm,
  WORKFLOW_CONFIRM,
  type ConfirmCopy,
} from '@/actions/copy';
import type { ActionAvailability } from '@/actions/eligibility';
import type { TaskActionsView } from '@/actions/use-task-actions';
import type { WorkflowActionsView } from '@/actions/use-workflow-actions';
import { Button } from '@/components/ui/button';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import type { ActionKind } from '@/types/actions';
import type { TaskDetail } from '@/types/tasks';

interface ActionButtonProps {
  action: ActionKind;
  label: string;
  icon: typeof Ban;
  availability: ActionAvailability;
  /** True while ANY action for this entity is in flight. */
  busy: boolean;
  destructive?: boolean;
  onSelect: (action: ActionKind) => void;
}

function ActionButton({
  action,
  label,
  icon: Icon,
  availability,
  busy,
  destructive = false,
  onSelect,
}: ActionButtonProps) {
  if (!availability.shown) {
    return null;
  }
  const disabled = busy || !availability.enabled;
  const button = (
    <Button
      variant={destructive ? 'destructive' : 'outline'}
      size="sm"
      className="h-8"
      disabled={disabled}
      onClick={() => onSelect(action)}
    >
      <Icon className="size-3.5" />
      {label}
    </Button>
  );
  if (availability.enabled) {
    return button;
  }
  return (
    <TooltipProvider>
      <Tooltip>
        {/* A disabled button emits no pointer events, so the trigger wraps it. */}
        <TooltipTrigger asChild>
          <span tabIndex={0}>{button}</span>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs">
          {availability.disabledReason}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/**
 * A workflow-bound row carries no task-level actions: retrying it would orphan
 * or hang the node, and the workflow owns its lifecycle. The note links to the
 * owning run — or degrades to a plain sentence when the server does not supply
 * the reference, which is also what an older backend returns.
 */
function ManagedByWorkflowNote({ detail }: { detail: TaskDetail }) {
  const runId = detail.workflow_id;
  if (runId === null) {
    return (
      <p className="text-xs text-muted-foreground">
        {MANAGED_BY_WORKFLOW_PREFIX}.
      </p>
    );
  }
  const nodeIndex = detail.workflow_task_index;
  return (
    <p className="text-xs text-muted-foreground">
      {MANAGED_BY_WORKFLOW_PREFIX}{' '}
      <Link
        to="/workflows"
        search={{
          run: runId,
          ...(nodeIndex === null ? {} : { node: nodeIndex }),
        }}
        className="font-mono text-primary underline-offset-4 hover:underline"
        title={runId}
      >
        {runId.slice(0, 8)}
      </Link>
      .
    </p>
  );
}

export function TaskActionBar({ view }: { view: TaskActionsView }) {
  const { canAct } = useCapability();
  const { detail, controls, cancel, retry, nextAttempt } = view;

  if (detail === undefined) {
    return null;
  }
  if (detail.is_workflow_task) {
    return <ManagedByWorkflowNote detail={detail} />;
  }
  if (!canAct) {
    return null;
  }

  const pending = controls.pending;
  const dialogOpen =
    controls.state.phase === 'confirming' || controls.state.phase === 'submitting';
  const confirmContext = {
    status: detail.leaf.status,
    queueName: detail.queue_name,
    workerHostname: detail.leaf.worker_hostname,
    nextAttempt,
  };
  const copy: ConfirmCopy | null =
    pending === 'task-cancel'
      ? taskCancelConfirm(confirmContext)
      : pending === 'task-retry'
        ? taskRetryConfirm(confirmContext)
        : null;
  const availability =
    pending === 'task-cancel' ? cancel : pending === 'task-retry' ? retry : null;

  return (
    <div className="flex flex-wrap gap-2">
      <ActionButton
        action="task-cancel"
        label="Cancel"
        icon={Ban}
        availability={cancel}
        busy={controls.busy}
        destructive
        onSelect={controls.begin}
      />
      <ActionButton
        action="task-retry"
        label="Retry"
        icon={RotateCcw}
        availability={retry}
        busy={controls.busy}
        onSelect={controls.begin}
      />
      {copy !== null && availability !== null && (
        <ActionConfirmDialog
          open={dialogOpen}
          copy={copy}
          currentStatus={detail.leaf.status}
          availability={availability}
          submitting={controls.state.phase === 'submitting'}
          onConfirm={() =>
            controls.confirm({
              includeRunning: detail.leaf.status === 'RUNNING',
            })
          }
          onDismiss={controls.dismiss}
        />
      )}
    </div>
  );
}

export function WorkflowActionBar({ view }: { view: WorkflowActionsView }) {
  const { canAct } = useCapability();
  const { detail, controls, availability } = view;

  if (!canAct || detail === undefined) {
    return null;
  }

  const pending = controls.pending;
  const dialogOpen =
    controls.state.phase === 'confirming' || controls.state.phase === 'submitting';
  const copy =
    pending === 'workflow-pause' ||
    pending === 'workflow-resume' ||
    pending === 'workflow-cancel'
      ? WORKFLOW_CONFIRM[pending]
      : null;
  const pendingAvailability =
    pending === 'workflow-pause' ||
    pending === 'workflow-resume' ||
    pending === 'workflow-cancel'
      ? availability[pending]
      : null;

  return (
    <div className="flex flex-wrap items-center gap-2">
      <ActionButton
        action="workflow-pause"
        label="Pause"
        icon={Pause}
        availability={availability['workflow-pause']}
        busy={controls.busy}
        onSelect={controls.begin}
      />
      <ActionButton
        action="workflow-resume"
        label="Resume"
        icon={Play}
        availability={availability['workflow-resume']}
        busy={controls.busy}
        onSelect={controls.begin}
      />
      <ActionButton
        action="workflow-cancel"
        label="Cancel"
        icon={Ban}
        availability={availability['workflow-cancel']}
        busy={controls.busy}
        destructive
        onSelect={controls.begin}
      />
      {copy !== null && pendingAvailability !== null && (
        <ActionConfirmDialog
          open={dialogOpen}
          copy={copy}
          currentStatus={detail.run.status}
          availability={pendingAvailability}
          submitting={controls.state.phase === 'submitting'}
          onConfirm={() => controls.confirm({ includeRunning: false })}
          onDismiss={controls.dismiss}
        />
      )}
    </div>
  );
}
