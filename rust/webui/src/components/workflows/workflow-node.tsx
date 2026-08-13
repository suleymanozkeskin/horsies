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

/** The child-rollup badge's text, tooltip, and whether it reports failures. */
export interface ChildRollupBadge {
  text: string;
  title: string;
  failed: boolean;
}

/**
 * Describe a subworkflow node's child rollup.
 *
 * A bare `k/N` reads as "k of N done" — a fully successful child run rendered
 * `0/3`, which is the opposite of what it means. So a healthy rollup shows the
 * child count alone and only a failing one spends the fraction, with the word
 * that fixes its meaning. Returns null for a leaf node, which has no rollup.
 */
export function childRollupBadge(
  childTotal: number | null,
  childFailed: number | null
): ChildRollupBadge | null {
  if (childTotal === null) {
    return null;
  }
  const failedCount = childFailed ?? 0;
  const title = `${childTotal} child ${childTotal === 1 ? 'node' : 'nodes'}, ${failedCount} failed`;
  return failedCount > 0
    ? { text: `${failedCount}/${childTotal} failed`, title, failed: true }
    : { text: String(childTotal), title, failed: false };
}

function WorkflowNodeCardImpl({ data, selected }: NodeProps<WorkflowFlowNode>) {
  const color = statusColorVar(data.status);
  // Null while queued — keep it null (not the shared '—') so the badge below
  // stays hidden rather than rendering a dash.
  const duration = data.execS === null ? null : formatDuration(data.execS);
  const residual = data.residual === null ? null : RESIDUAL_COPY[data.residual];
  const rollup = childRollupBadge(data.childTotal, data.childFailed);

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
        {rollup !== null && (
          <span
            className="shrink-0 rounded px-1 font-mono text-9 font-medium"
            style={{
              color: rollup.failed ? 'var(--error)' : 'var(--muted-foreground)',
            }}
            title={rollup.title}
          >
            {rollup.text}
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
