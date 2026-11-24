import { Info } from 'lucide-react';

import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusColorVar, statusLabel } from '@/lib/status-utils';
import type { WorkflowRunDetail, WorkflowStatus } from '@/types/workflows';

import { TimingLegend, type LegendMark } from './legend';
import { phaseLabel } from './phase';
import { PhaseBar } from './phase-bar';
import { criticalPath, runPhaseTotals, type CriticalPath } from './run-timing';
import { nodeStatusCounts } from './status';
import { runAxis, type PhaseShare } from './timeline-geometry';
import { unavailableReasonText } from './timing-text';

/** The view under the summary: it decides the marks of the legend. */
export type RunView = 'graph' | 'timeline';

const nodeLabelOf = (detail: WorkflowRunDetail, taskIndex: number): string => {
  const node = detail.nodes.find(candidate => candidate.task_index === taskIndex);
  return node === undefined ? `#${taskIndex}` : (node.node_id ?? node.task_name);
};

/** The critical path in words: its chain of nodes, its length, its phases. */
export function criticalPathText(detail: WorkflowRunDetail, path: CriticalPath): string {
  switch (path.kind) {
    case 'path':
      return (
        `${path.taskIndices.map(index => nodeLabelOf(detail, index)).join(' → ')}. ` +
        `${formatMilliseconds(path.lengthMs)} without dependency wait: ` +
        path.phases.map(share => `${phaseLabel(share.key)} ${formatMilliseconds(share.ms)}`).join(', ') +
        '.'
      );
    case 'no_timing':
      return 'No node has timing.';
    default:
      return assertNever(path, 'critical path');
  }
}

function legendMarks(view: RunView, detail: WorkflowRunDetail): LegendMark[] {
  switch (view) {
    case 'timeline': {
      const paused =
        detail.timing.kind === 'composed' && detail.timing.pauses.length > 0 ? (['paused'] as const) : [];
      return [...paused, 'end', 'critical_row'];
    }
    case 'graph':
      return [
        'critical_edge',
        ...(detail.nodes.some(node => node.allow_failed_deps) ? (['tolerant_join'] as const) : []),
      ];
    default:
      return assertNever(view, 'run view');
  }
}

function Stat({ label, value, title }: { label: string; value: string; title: string }) {
  return (
    <span className="inline-flex items-baseline gap-1.5" title={title}>
      <span className="text-10 uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="font-mono text-sm font-semibold tabular-nums">{value}</span>
    </span>
  );
}

/** The status counts of the run; each toggles a filter that fades the nodes
 * of the other statuses. */
function StatusFilter({
  detail,
  active,
  onToggle,
}: {
  detail: WorkflowRunDetail;
  active: ReadonlySet<string>;
  onToggle: (status: WorkflowStatus) => void;
}) {
  const filtering = active.size > 0;
  return (
    <span className="inline-flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
      {nodeStatusCounts(detail.nodes.map(node => node.node_status)).map(({ status, count }) => {
        const on = active.has(status);
        return (
          <button
            key={status}
            type="button"
            onClick={() => onToggle(status)}
            aria-pressed={on}
            title={on ? `Showing ${statusLabel(status)} nodes. Click to show all.` : `Show only ${statusLabel(status)} nodes`}
            className="inline-flex items-center gap-1.5 rounded px-1 text-muted-foreground hover:text-foreground"
            style={{ opacity: filtering && !on ? 0.45 : 1 }}
          >
            <span className="size-2 rounded-full" style={{ background: statusColorVar(status) }} aria-hidden />
            <span className="font-mono text-foreground tabular-nums">{count}</span>
            {statusLabel(status)}
          </button>
        );
      })}
    </span>
  );
}

function CriticalPathStat({ detail, path }: { detail: WorkflowRunDetail; path: CriticalPath }) {
  switch (path.kind) {
    case 'no_timing':
      return null;
    case 'path': {
      const text = criticalPathText(detail, path);
      return (
        <span className="inline-flex items-center gap-1.5">
          <Stat
            label="critical path"
            value={formatMilliseconds(path.lengthMs)}
            title="The time of the critical path without dependency wait"
          />
          <span className="text-xs text-muted-foreground">
            {path.taskIndices.length} {path.taskIndices.length === 1 ? 'node' : 'nodes'}
          </span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  aria-label={`Critical path: ${text}`}
                  className="rounded p-0.5 text-muted-foreground hover:text-foreground"
                >
                  <Info className="size-3.5" aria-hidden />
                </button>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="max-w-sm text-xs">
                <p>
                  The critical path is the chain of nodes whose end times decided the end of the
                  run. It ends at the node that ended last; each node on it was released by the
                  dependency that ended last. A delay on this chain delays the run.
                </p>
                <p className="mt-1.5 font-mono">{text}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </span>
      );
    }
    default:
      return assertNever(path, 'critical path');
  }
}

function TimeBreakdown({ shares, nodeCount }: { shares: readonly PhaseShare[]; nodeCount: number }) {
  if (shares.length === 0) {
    return null;
  }
  const text = shares.map(share => `${phaseLabel(share.key)} ${formatMilliseconds(share.ms)}`).join(', ');
  return (
    <div className="flex min-w-48 flex-1 items-center gap-2">
      <span
        className="shrink-0 text-10 uppercase tracking-wide text-muted-foreground"
        title={`The time of each phase summed over the ${nodeCount} nodes of the run`}
      >
        where the time went
      </span>
      <PhaseBar
        parts={shares.map(share => ({
          key: share.key,
          ms: share.ms,
          title: `${phaseLabel(share.key)} ${formatMilliseconds(share.ms)} (${Math.round(share.fraction * 100)}% of the node time)`,
        }))}
        label={`Where the time went, summed over ${nodeCount} nodes: ${text}`}
        size="regular"
      />
    </div>
  );
}

/**
 * The run summary over the graph and the timeline: the wall time of the run,
 * its critical path, one bar of where the node time went by phase, the one
 * legend (each phase with its total time, then the marks of the view), and
 * the status counts that filter the nodes.
 */
export function RunTimingSummary({
  workflowId,
  view,
  statusFilter,
  onToggleStatus,
}: {
  workflowId: string;
  view: RunView;
  statusFilter: ReadonlySet<string>;
  onToggleStatus: (status: WorkflowStatus) => void;
}) {
  const { detail, readClock } = useWorkflowRun(workflowId);
  if (detail === undefined || readClock === undefined) {
    return null;
  }
  return (
    <RunTimingSummaryView
      detail={detail}
      readClock={readClock}
      view={view}
      statusFilter={statusFilter}
      onToggleStatus={onToggleStatus}
    />
  );
}

export function RunTimingSummaryView({
  detail,
  readClock,
  view,
  statusFilter,
  onToggleStatus,
}: {
  detail: WorkflowRunDetail;
  readClock: ReadClock;
  view: RunView;
  statusFilter: ReadonlySet<string>;
  onToggleStatus: (status: WorkflowStatus) => void;
}) {
  const drawAtMs = useDrawTime(readClock, readClock.open);
  const timing = detail.timing;
  switch (timing.kind) {
    case 'unavailable':
      return (
        <section aria-label="Run summary" className="flex flex-wrap items-center gap-x-5 gap-y-1 border-b border-hairline pb-3">
          <span className="text-xs text-muted-foreground">
            Timing unavailable. {unavailableReasonText(timing.reason)}
          </span>
          <span className="ml-auto">
            <StatusFilter detail={detail} active={statusFilter} onToggle={onToggleStatus} />
          </span>
        </section>
      );
    case 'composed': {
      const shares = runPhaseTotals(detail.nodes, drawAtMs);
      const path = criticalPath(detail.nodes, detail.edges, drawAtMs);
      return (
        <section aria-label="Run summary" className="flex flex-col gap-1.5 border-b border-hairline pb-3">
          <div className="flex flex-wrap items-center gap-x-5 gap-y-1">
            <Stat
              label="wall"
              value={formatMilliseconds(runAxis(timing, drawAtMs).spanMs)}
              title="Wall time of the run: from its start to its last recorded time"
            />
            <CriticalPathStat detail={detail} path={path} />
            <TimeBreakdown shares={shares} nodeCount={detail.nodes.length} />
            <StatusFilter detail={detail} active={statusFilter} onToggle={onToggleStatus} />
          </div>
          <TimingLegend shares={shares} marks={legendMarks(view, detail)} />
        </section>
      );
    }
    default:
      return assertNever(timing, 'run timing');
  }
}
