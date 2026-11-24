import { TriangleAlert } from 'lucide-react';

import { Hint } from '@/components/ui/hint';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusColorVar, statusLabel } from '@/lib/status-utils';
import type { WorkflowNode, WorkflowRunDetail, WorkflowStatus } from '@/types/workflows';

import { LONG_ENGINE_STEP_MS, phaseLabel } from './phase';
import {
  criticalPath,
  offPathLongSteps,
  pausedTime,
  type CriticalPath,
  type OffPathLongStep,
  type PausedTime,
} from './run-timing';
import { nodeStatusCounts } from './status';
import { phaseFigure, TimeFigures, type TimeFigure } from './time-figures';
import { runAxis } from './timeline-geometry';
import { compositionText, durationText, unavailableReasonText } from './timing-text';

const nodeLabelOf = (detail: WorkflowRunDetail, taskIndex: number): string => {
  const node = detail.nodes.find(candidate => candidate.task_index === taskIndex);
  return node === undefined ? `#${taskIndex}` : (node.node_id ?? node.task_name);
};

/** The critical path in words: what it is, its chain of nodes, and what its
 * times are. */
export function criticalPathText(detail: WorkflowRunDetail, path: CriticalPath): string {
  switch (path.kind) {
    case 'path':
      return (
        'The critical path is the chain of nodes whose end times decided the end of the run: ' +
        `${path.taskIndices.map(index => nodeLabelOf(detail, index)).join(' → ')}. ` +
        'Each node of the chain starts when the node before it ends. ' +
        'These times are the phases of the chain without its dependency wait: ' +
        `${compositionText(path.phases)}. ` +
        'They add up to the duration of a run that had no pause.'
      );
    case 'no_timing':
      return 'No node has timing.';
    default:
      return assertNever(path, 'critical path');
  }
}

/** The most nodes that the hint of a long step names. */
const NAMED_NODES_LIMIT = 8;

const labelOf = (node: WorkflowNode): string => node.node_id ?? node.task_name;

/** The long steps of one phase off the critical path in words: the nodes
 * with their times, then what the long time tells. */
export function offPathStepText(step: OffPathLongStep): string {
  const named = step.nodes
    .slice(0, NAMED_NODES_LIMIT)
    .map(entry => `${labelOf(entry.node)} ${formatMilliseconds(entry.longestMs)}`)
    .join(', ');
  const more = step.nodes.length - NAMED_NODES_LIMIT;
  return (
    `${phaseLabel(step.key)} of ${formatMilliseconds(LONG_ENGINE_STEP_MS)} or more in nodes that are not on the critical path: ` +
    `${named}${more > 0 ? `, and ${more} more` : ''}. ` +
    `These nodes did not decide the end of the run. ${step.tells}`
  );
}

/** Where a long step off the path is: its one node by name, or the count of
 * its nodes with the longest time. */
function offPathStepWhere(step: OffPathLongStep): { time: string; where: string } {
  const [longest] = step.nodes;
  if (longest === undefined) {
    throw new Error('a long step off the path has a node');
  }
  const time = formatMilliseconds(longest.longestMs);
  return step.nodes.length === 1
    ? { time, where: `in ${labelOf(longest.node)}` }
    : { time: `up to ${time}`, where: `in ${step.nodes.length} nodes` };
}

/** The long engine steps of the nodes that are not on the critical path.
 * The split of the duration does not hold them, and they are the same signal
 * about the queue and the workers. Nothing when no such step exists. */
function OffPathSteps({ steps }: { steps: readonly OffPathLongStep[] }) {
  if (steps.length === 0) {
    return null;
  }
  return (
    <span data-off-path className="inline-flex flex-wrap items-baseline gap-x-3 gap-y-0.5">
      <span className="text-xs text-muted-foreground">other nodes</span>
      {steps.map(step => {
        const { time, where } = offPathStepWhere(step);
        return (
          <Hint
            key={step.key}
            text={offPathStepText(step)}
            className="inline-flex items-baseline gap-1.5 whitespace-nowrap text-xs"
          >
            <span className="text-muted-foreground">{phaseLabel(step.key)}</span>
            <span
              className="inline-flex items-center gap-1 font-mono font-semibold tabular-nums"
              style={{ color: 'var(--marked-ink)' }}
            >
              <TriangleAlert className="size-3 shrink-0" aria-hidden />
              {time}
            </span>
            <span className="text-muted-foreground">{where}</span>
          </Hint>
        );
      })}
    </span>
  );
}

/** The figure of the pause time of a run: a planned wait. */
function pausedFigures(paused: PausedTime): TimeFigure[] {
  switch (paused.kind) {
    case 'not_paused':
      return [];
    case 'paused':
      return [
        {
          id: 'paused',
          label: 'paused',
          ms: paused.ms,
          open: paused.open,
          weight: { kind: 'plain' },
          meaning:
            'The time the run was paused. A pause stops new tasks of the run; tasks that run go on. A node that waits keeps its dependency wait during the pause.',
        },
      ];
    default:
      return assertNever(paused, 'paused time');
  }
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
          <Hint
            key={status}
            text={
              on
                ? `The view shows the ${statusLabel(status)} nodes. Click to show all nodes.`
                : `${count} ${count === 1 ? 'node is' : 'nodes are'} ${statusLabel(status)}. Click to fade the other nodes.`
            }
          >
            <button
              type="button"
              onClick={() => onToggle(status)}
              aria-pressed={on}
              className="inline-flex items-center gap-1.5 rounded px-1 text-muted-foreground hover:text-foreground"
              style={{ opacity: filtering && !on ? 0.45 : 1 }}
            >
              <span className="size-2 rounded-full" style={{ background: statusColorVar(status) }} aria-hidden />
              <span className="font-mono text-foreground tabular-nums">{count}</span>
              {statusLabel(status)}
            </button>
          </Hint>
        );
      })}
    </span>
  );
}

/** The split of the run duration: the phases of the critical path, and the
 * pause time of the run. A run in which no node has timing shows its pause
 * time alone. */
function DurationSplit({
  detail,
  path,
  paused,
}: {
  detail: WorkflowRunDetail;
  path: CriticalPath;
  paused: PausedTime;
}) {
  switch (path.kind) {
    case 'no_timing': {
      const figures = pausedFigures(paused);
      return figures.length === 0 ? null : (
        <TimeFigures figures={figures} layout="inline" label="Pause time of the run" />
      );
    }
    case 'path':
      return (
        <span className="inline-flex flex-wrap items-baseline gap-x-3 gap-y-0.5">
          <Hint text={criticalPathText(detail, path)}>
            <span className="text-xs text-muted-foreground underline decoration-dotted underline-offset-2">
              critical path
            </span>
          </Hint>
          <TimeFigures
            figures={[...path.phases.map(phaseFigure), ...pausedFigures(paused)]}
            layout="inline"
            label={`Time of the critical path by phase: ${compositionText(path.phases)}`}
          />
        </span>
      );
    default:
      return assertNever(path, 'critical path');
  }
}

/**
 * The run summary over the graph and the timeline, in one line of facts: the
 * duration of the run, its split into the phases of the critical path (which
 * add up to the duration of a run with no pause), the pause time, the long
 * engine steps of the nodes off the path, and the status counts that filter
 * the nodes.
 */
export function RunTimingSummary({
  workflowId,
  statusFilter,
  onToggleStatus,
}: {
  workflowId: string;
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
      statusFilter={statusFilter}
      onToggleStatus={onToggleStatus}
    />
  );
}

export function RunTimingSummaryView({
  detail,
  readClock,
  statusFilter,
  onToggleStatus,
}: {
  detail: WorkflowRunDetail;
  readClock: ReadClock;
  statusFilter: ReadonlySet<string>;
  onToggleStatus: (status: WorkflowStatus) => void;
}) {
  const drawAtMs = useDrawTime(readClock, readClock.open);
  const timing = detail.timing;
  switch (timing.kind) {
    case 'unavailable':
      return (
        <section aria-label="Run summary" className="flex flex-wrap items-center gap-x-5 gap-y-1 border-b border-border px-3 py-2">
          <span className="text-xs text-muted-foreground">
            Timing unavailable. {unavailableReasonText(timing.reason)}
          </span>
          <span className="ml-auto">
            <StatusFilter detail={detail} active={statusFilter} onToggle={onToggleStatus} />
          </span>
        </section>
      );
    case 'composed': {
      const path = criticalPath(detail.nodes, detail.edges, drawAtMs);
      return (
        <section
          aria-label="Run summary"
          className="flex flex-wrap items-baseline gap-x-5 gap-y-1 border-b border-border px-3 py-2"
        >
          <Hint
            text="The duration of the run: from its start to its last recorded time."
            className="inline-flex items-baseline gap-1.5"
          >
            <span className="text-xs text-muted-foreground">Duration</span>
            <span data-run-duration className="font-mono text-sm font-semibold tabular-nums">
              {durationText(runAxis(timing, drawAtMs).spanMs, timing.open_segments)}
            </span>
          </Hint>
          <DurationSplit detail={detail} path={path} paused={pausedTime(timing, drawAtMs)} />
          <OffPathSteps steps={offPathLongSteps(detail.nodes, path, drawAtMs)} />
          <span className="ml-auto">
            <StatusFilter detail={detail} active={statusFilter} onToggle={onToggleStatus} />
          </span>
        </section>
      );
    }
    default:
      return assertNever(timing, 'run timing');
  }
}
