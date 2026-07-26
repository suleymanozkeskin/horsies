import { useMemo } from 'react';

import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { formatElapsed } from '@/lib/format-duration';
import { statusColorVar } from '@/lib/status-utils';
import type { WorkflowNode } from '@/types/workflows';

interface WorkflowTimelineProps {
  workflowId: string;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  /** Statuses to keep visible; rows of other statuses are faded (empty = all). */
  statusFilter?: ReadonlySet<string>;
}

/** Stable empty default so an absent statusFilter prop keeps a constant reference. */
const NO_STATUS_FILTER: ReadonlySet<string> = new Set();

/** One node's placement on the shared time axis (percentages of the run span). */
interface TimelineRow {
  node: WorkflowNode;
  leftPct: number;
  widthPct: number;
}

const nodeLabel = (node: WorkflowNode): string =>
  node.node_id ?? node.task_name ?? `#${node.task_index}`;

const parseMs = (iso: string | null): number | null => {
  if (iso === null) {
    return null;
  }
  const ms = Date.parse(iso);
  return Number.isNaN(ms) ? null : ms;
};

/**
 * Execution-time Gantt for one run: each node's exec span (started -> completed,
 * or -> now while running) on a shared axis from the run's start. Reveals
 * serialization and stragglers the DAG cannot. Queued nodes (no start) are
 * listed without a bar. Queue spans are intentionally absent — `enqueued_at`
 * lives only on the per-node detail, not the run payload.
 */
export function WorkflowTimeline({
  workflowId,
  selectedIndex,
  onSelectNode,
  statusFilter = NO_STATUS_FILTER,
}: WorkflowTimelineProps) {
  const { detail, isLoading, isError } = useWorkflowRun(workflowId);
  const statusFiltering = statusFilter.size > 0;

  const { rows, spanS } = useMemo(() => {
    if (!detail) {
      return { rows: [] as TimelineRow[], spanS: 0 };
    }
    const start = parseMs(detail.run.created_at);
    if (start === null) {
      return { rows: [] as TimelineRow[], spanS: 0 };
    }
    const now = Date.now();
    const end = parseMs(detail.run.completed_at) ?? now;
    const span = Math.max(end - start, 1);

    const ordered = [...detail.nodes].sort((a, b) => {
      const startA = parseMs(a.started_at);
      const startB = parseMs(b.started_at);
      if (startA === null && startB === null) {
        return a.task_index - b.task_index;
      }
      if (startA === null) {
        return 1;
      }
      if (startB === null) {
        return -1;
      }
      return startA - startB;
    });

    const built: TimelineRow[] = ordered.map(node => {
      const started = parseMs(node.started_at);
      if (started === null) {
        return { node, leftPct: 0, widthPct: 0 };
      }
      // A node with no completion only counts up while it is genuinely RUNNING;
      // a SKIPPED-on-cancel node keeps `completed_at` null and must not grow.
      const finished =
        parseMs(node.completed_at) ??
        (node.node_status === 'RUNNING' ? now : started);
      return {
        node,
        leftPct: ((started - start) / span) * 100,
        widthPct: Math.max(((finished - started) / span) * 100, 0.5),
      };
    });

    return { rows: built, spanS: Math.round(span / 1000) };
  }, [detail]);

  if (isError) {
    return (
      <div
        className="flex h-full items-center justify-center text-sm"
        style={{ color: 'var(--error)' }}
        role="alert"
      >
        Could not load this workflow run.
      </div>
    );
  }

  if (isLoading && !detail) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Loading workflow…
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        No timing data for this run yet.
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b border-border px-3 py-1.5 text-xs text-muted-foreground">
        <span>start</span>
        <span className="tabular-nums">{formatElapsed(spanS)}</span>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <ul>
          {rows.map(({ node, leftPct, widthPct }) => {
            const selected = node.task_index === selectedIndex;
            const color = statusColorVar(node.node_status);
            const dimmed = statusFiltering && !statusFilter.has(node.node_status);
            return (
              <li key={node.task_index}>
                <button
                  type="button"
                  onClick={() => onSelectNode(node)}
                  style={{ opacity: dimmed ? 0.35 : 1 }}
                  className={
                    selected
                      ? 'flex w-full items-center gap-2 border-b border-border bg-accent-surface px-3 py-1 text-left'
                      : 'flex w-full items-center gap-2 border-b border-border px-3 py-1 text-left hover:bg-glass-surface-strong'
                  }
                >
                  <span
                    className="w-40 shrink-0 truncate text-xs font-medium"
                    title={nodeLabel(node)}
                  >
                    {nodeLabel(node)}
                  </span>
                  <span className="relative h-3 min-w-0 flex-1">
                    {widthPct > 0 && (
                      <span
                        className="absolute top-0 h-3 rounded-sm"
                        style={{
                          left: `${leftPct}%`,
                          width: `${widthPct}%`,
                          background: color,
                        }}
                      />
                    )}
                  </span>
                  <span className="w-12 shrink-0 text-right font-mono text-10 text-muted-foreground">
                    {node.exec_s !== null ? formatElapsed(node.exec_s) : '—'}
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}
