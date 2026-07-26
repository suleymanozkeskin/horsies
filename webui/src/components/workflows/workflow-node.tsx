import { memo } from 'react';

import { Handle, type Node, type NodeProps, Position } from '@xyflow/react';
import { Layers } from 'lucide-react';

import { RESIDUAL_COPY } from '@/actions/copy';
import { formatDuration } from '@/lib/format-duration';
import { statusColorVar } from '@/lib/status-utils';
import type { WorkflowStatus } from '@/types/workflows';

import { NODE_H, NODE_W } from './layout';
import type { ResidualState } from './residual';

export interface WorkflowNodeData extends Record<string, unknown> {
  label: string;
  taskName: string;
  status: WorkflowStatus;
  isSubworkflow: boolean;
  /** Tolerant join — runs even if dependencies failed. Rendered dashed. */
  allowFailedDeps: boolean;
  /** Execution time only; null while queued. */
  execS: number | null;
  /** True when this node has a drillable subworkflow run. */
  drillable: boolean;
  /** Direct-child task count for subworkflow nodes; null for leaf nodes. */
  childTotal: number | null;
  /** FAILED direct-child task count for subworkflow nodes; null for leaf nodes. */
  childFailed: number | null;
  /** Faded because a search or status filter excludes it. */
  dimmed: boolean;
  /** Executing under a cancelled/paused run — informational, never a failure. */
  residual: ResidualState;
}

export type WorkflowFlowNode = Node<WorkflowNodeData, 'workflow'>;

function WorkflowNodeCardImpl({ data, selected }: NodeProps<WorkflowFlowNode>) {
  const color = statusColorVar(data.status);
  // Null while queued — keep it null (not the shared '—') so the badge below
  // stays hidden rather than rendering a dash.
  const duration = data.execS === null ? null : formatDuration(data.execS);
  const residual = data.residual === null ? null : RESIDUAL_COPY[data.residual];

  return (
    <div
      className="relative flex flex-col justify-center overflow-hidden rounded-xl border bg-card px-3 py-2 text-card-foreground shadow-xs transition-colors"
      title={data.drillable ? 'Double-click to open subworkflow' : undefined}
      style={{
        width: NODE_W,
        height: NODE_H,
        borderColor: selected ? 'var(--ring)' : 'var(--border)',
        borderWidth: selected ? 2 : 1,
        borderStyle: data.allowFailedDeps ? 'dashed' : 'solid',
        background: `color-mix(in oklab, ${color} 8%, var(--card))`,
        cursor: data.drillable ? 'pointer' : 'default',
        opacity: data.dimmed ? 0.35 : 1,
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!h-1.5 !w-1.5 !border-0 !bg-border"
      />
      <span
        className="absolute left-0 top-0 h-full w-1 rounded-l-xl"
        style={{ background: color }}
        aria-hidden
      />
      <div className="flex items-center gap-1.5 pl-1.5">
        {data.isSubworkflow && (
          <Layers className="size-3.5 shrink-0 text-muted-foreground" aria-hidden />
        )}
        <span
          className="min-w-0 flex-1 truncate text-13 font-medium"
          title={data.label}
        >
          {data.label}
        </span>
        {data.childTotal !== null && (
          <span
            className="shrink-0 rounded px-1 font-mono text-9 font-medium"
            style={{
              color:
                data.childFailed !== null && data.childFailed > 0
                  ? 'var(--error)'
                  : 'var(--muted-foreground)',
            }}
            title={`${data.childFailed ?? 0} of ${data.childTotal} child tasks failed`}
          >
            {data.childFailed ?? 0}/{data.childTotal}
          </span>
        )}
        <span
          className="size-2 shrink-0 rounded-full"
          style={{ background: color }}
          aria-hidden
        />
      </div>
      <div className="flex items-center gap-1.5 pl-1.5">
        {residual === null ? (
          <span
            className="truncate font-mono text-10 text-muted-foreground"
            title={data.taskName}
          >
            {data.taskName}
          </span>
        ) : (
          <span
            className="shrink-0 rounded border border-border px-1 text-9 uppercase text-muted-foreground"
            title={residual.tooltip}
          >
            {residual.badge}
          </span>
        )}
        {duration && (
          <span className="ml-auto shrink-0 font-mono text-10 text-muted-foreground">
            {duration}
          </span>
        )}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-1.5 !w-1.5 !border-0 !bg-border"
      />
    </div>
  );
}

export const WorkflowNodeCard = memo(WorkflowNodeCardImpl);
