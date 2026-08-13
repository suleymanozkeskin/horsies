import type { ReactNode } from 'react';

import { Layers } from 'lucide-react';

import { RESIDUAL_COPY } from '@/actions/copy';
import { AttemptCard } from '@/components/monitoring/detail';
import { StatusChip } from '@/components/ui/status-chip';
import { useWorkflowTask } from '@/hooks/use-workflow-task';
import { formatDuration } from '@/lib/format-duration';
import type { WorkflowNode } from '@/types/workflows';

import { residualState } from './residual';

interface NodeDetailContentProps {
  workflowId: string;
  /** Status of the run this node belongs to — decides the residual badge. */
  runStatus: string;
  node: WorkflowNode;
  onOpenSubworkflow: (subWorkflowId: string, label: string) => void;
}

function Row({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs uppercase tracking-wide text-muted-foreground">
        {label}
      </span>
      <span className="break-words text-sm">{value}</span>
    </div>
  );
}

/** Detail body for one node: fields, failure, and attempt history. Chrome-less —
 * the surrounding panel owns the header/close/resize. */
export function NodeDetailContent({
  workflowId,
  runStatus,
  node,
  onOpenSubworkflow,
}: NodeDetailContentProps) {
  const label = node.node_id ?? node.task_name ?? `#${node.task_index}`;
  const drillId = node.is_subworkflow ? node.sub_workflow_id : null;
  const { detail, isLoading } = useWorkflowTask(workflowId, node.task_index);

  const leaf = detail?.leaf;
  const attempts = detail?.attempts ?? [];
  const nodeError = detail?.node_error;
  const execS = leaf?.exec_s ?? node.exec_s;
  const queueS = leaf?.queue_s ?? null;
  const residual = residualState(
    runStatus,
    node.node_status,
    leaf?.status ?? null
  );

  return (
    <div className="flex flex-col gap-4 p-4">
      <Row
        label="Status"
        value={
          <span className="inline-flex items-center gap-2">
            <StatusChip status={node.node_status} />
            {residual !== null && (
              <span
                className="rounded-full border border-border px-2 py-0.5 text-10 uppercase text-muted-foreground"
                title={RESIDUAL_COPY[residual].tooltip}
              >
                {RESIDUAL_COPY[residual].badge}
              </span>
            )}
          </span>
        }
      />
      {residual !== null && (
        <p className="text-xs text-muted-foreground">
          {RESIDUAL_COPY[residual].tooltip}
        </p>
      )}
      <Row
        label="Task"
        value={<span className="font-mono text-xs">{node.task_name}</span>}
      />
      <div className="flex gap-8">
        <Row label="Execution" value={formatDuration(execS)} />
        {queueS !== null && (
          <Row
            label="Queued"
            value={
              <span title="Time waiting in the queue after dispatch">
                {formatDuration(queueS)}
              </span>
            }
          />
        )}
      </div>
      <Row label="Tolerant join" value={node.allow_failed_deps ? 'yes' : 'no'} />

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
          <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap break-words rounded bg-glass-field p-2 font-mono text-11 leading-snug">
            {nodeError}
          </pre>
        </div>
      )}

      {leaf && (leaf.max_retries > 0 || leaf.retry_count > 0) && (
        <Row
          label="Attempts"
          value={`${leaf.retry_count + 1} of ${leaf.max_retries + 1}`}
        />
      )}

      {detail && !detail.is_subworkflow && (
        <div className="flex flex-col gap-2">
          <span className="text-xs uppercase tracking-wide text-muted-foreground">
            Attempt history{attempts.length ? ` (${attempts.length})` : ''}
          </span>
          {isLoading && attempts.length === 0 ? (
            <p className="text-xs text-muted-foreground">Loading…</p>
          ) : attempts.length === 0 ? (
            <p className="text-xs text-muted-foreground">
              No attempts recorded yet.
            </p>
          ) : (
            attempts.map(attempt => (
              <AttemptCard key={attempt.attempt} attempt={attempt} />
            ))
          )}
        </div>
      )}
    </div>
  );
}
