import { Layers } from 'lucide-react';

import { RESIDUAL_COPY } from '@/actions/copy';
import { AttemptTiming } from '@/components/monitoring/attempt-timing';
import { Hint } from '@/components/ui/hint';
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
 * the header/close/resize and the inline padding. A node error that an
 * attempt shows as its message is not shown twice, and a skipped node, which
 * never has an attempt, has no attempts section. */
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
  const repeatsAttemptMessage =
    detail?.attempts.some(
      attempt => (attempt.error_message ?? attempt.failed_reason) === nodeError
    ) ?? false;
  const skippedWithNoAttempt =
    node.node_status === 'SKIPPED' && (detail?.attempts.length ?? 0) === 0;

  return (
    <div className="flex flex-col gap-4 py-4">
      <div className="flex flex-col gap-1.5">
        <span className="inline-flex flex-wrap items-center gap-2">
          <StatusChip status={node.node_status} />
          {residual !== null && (
            <span className="rounded-full border border-hairline-strong px-2 py-0.5 text-10 uppercase text-muted-foreground">
              {RESIDUAL_COPY[residual].badge}
            </span>
          )}
          {node.allow_failed_deps && (
            <Hint text="Tolerant join: the node runs even when a dependency failed. The graph draws it with a dashed border.">
              <span className="rounded-full border border-dashed border-hairline-strong px-2 py-0.5 text-10 text-muted-foreground">
                tolerant join
              </span>
            </Hint>
          )}
          <Hint text={`The task of the node: ${node.task_name}.`} className="min-w-0">
            <span className="block min-w-0 break-all font-mono text-xs text-muted-foreground">
              {node.task_name}
            </span>
          </Hint>
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

      {nodeError && !repeatsAttemptMessage && (
        <div>
          <span className="text-xs uppercase tracking-wide text-muted-foreground">
            Node error
          </span>
          <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-glass-inset p-2 font-mono text-11 leading-snug">
            {nodeError}
          </pre>
        </div>
      )}

      {detail && !detail.is_subworkflow && !skippedWithNoAttempt && (
        <AttemptTiming attempts={detail.attempts} leaf={leaf} isLoading={isLoading} />
      )}
    </div>
  );
}
