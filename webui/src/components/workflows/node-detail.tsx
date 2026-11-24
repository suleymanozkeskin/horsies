import { Layers } from 'lucide-react';

import { RESIDUAL_COPY } from '@/actions/copy';
import { AttemptTiming } from '@/components/monitoring/attempt-timing';
import { StatusChip } from '@/components/ui/status-chip';
import { useWorkflowTask } from '@/hooks/use-workflow-task';
import type { WorkflowNode } from '@/types/workflows';

import { NodeTimingSection } from './node-timing';
import { residualState } from './residual';

interface NodeDetailContentProps {
  workflowId: string;
  /** Status of the run this node belongs to — decides the residual badge. */
  runStatus: string;
  node: WorkflowNode;
  onOpenSubworkflow: (subWorkflowId: string, label: string) => void;
}

/** Detail body for one node: status and task, the one node timing summary,
 * the node error, and the attempts. Chrome-less — the surrounding panel owns
 * the header/close/resize and the inline padding. */
export function NodeDetailContent({
  workflowId,
  runStatus,
  node,
  onOpenSubworkflow,
}: NodeDetailContentProps) {
  const label = node.node_id ?? node.task_name ?? `#${node.task_index}`;
  const drillId = node.is_subworkflow ? node.sub_workflow_id : null;
  const { detail, isLoading } = useWorkflowTask(workflowId, node.task_index);

  const leaf = detail?.leaf ?? null;
  const nodeError = detail?.node_error;
  const residual = residualState(runStatus, node.node_status, leaf?.status ?? null);

  return (
    <div className="flex flex-col gap-4 py-4">
      <div className="flex flex-col gap-1.5">
        <span className="inline-flex flex-wrap items-center gap-2">
          <StatusChip status={node.node_status} />
          {residual !== null && (
            <span
              className="rounded-full border border-border px-2 py-0.5 text-10 uppercase text-muted-foreground"
              title={RESIDUAL_COPY[residual].tooltip}
            >
              {RESIDUAL_COPY[residual].badge}
            </span>
          )}
          {node.allow_failed_deps && (
            <span
              className="rounded-full border border-dashed border-border px-2 py-0.5 text-10 text-muted-foreground"
              title="Tolerant join: the node runs even when a dependency failed."
            >
              tolerant join
            </span>
          )}
          <span className="min-w-0 truncate font-mono text-xs text-muted-foreground" title="Task">
            {node.task_name}
          </span>
        </span>
        {residual !== null && (
          <p className="text-xs text-muted-foreground">{RESIDUAL_COPY[residual].tooltip}</p>
        )}
      </div>

      <NodeTimingSection workflowId={workflowId} taskIndex={node.task_index} />

      {drillId && (
        <button
          type="button"
          onClick={() => onOpenSubworkflow(drillId, label)}
          className="inline-flex items-center justify-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary-hover"
        >
          <Layers className="size-4" />
          Open subworkflow
        </button>
      )}

      {nodeError && (
        <div>
          <span className="text-xs uppercase tracking-wide text-muted-foreground">
            Node error
          </span>
          <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-glass-inset p-2 font-mono text-11 leading-snug">
            {nodeError}
          </pre>
        </div>
      )}

      {detail && !detail.is_subworkflow && (
        <AttemptTiming attempts={detail.attempts} leaf={leaf} isLoading={isLoading} />
      )}
    </div>
  );
}
