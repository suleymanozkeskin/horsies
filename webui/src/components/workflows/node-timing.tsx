import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { clockText, dateText, zoneText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type { NodeEndpoint, NodeTiming, TimingSegment } from '@/types/timing';

import { phaseLabel } from './phase';
import { PhaseBar, type PhaseBarPart } from './phase-bar';
import { PhaseStrip } from './phase-strip';
import {
  criticalMembership,
  criticalPath,
  nodeTimeParts,
  type CriticalMembership,
  type NodeTimePart,
} from './run-timing';
import { nodeTimingIsOpen, nodeWallTime, segmentSpan } from './timeline-geometry';
import { nodeCoverageText, segmentPhaseText, segmentSourceText, wallTimeText } from './timing-text';

/** One part of the time of a node in words: `dependency wait 5.2s`,
 * `task 2.4s`, `3 attempts 32.1s`. */
export function nodeTimePartText(part: NodeTimePart): string {
  switch (part.kind) {
    case 'phase':
      return `${phaseLabel(part.key)} ${formatMilliseconds(part.ms)}`;
    case 'task': {
      const name =
        part.tasks > 1 ? `${part.tasks} tasks` : part.attempts > 1 ? `${part.attempts} attempts` : 'task';
      return `${name} ${formatMilliseconds(part.ms)}`;
    }
    default:
      return assertNever(part, 'node time part');
  }
}

/** The end of a node: its time of day, with the day and the zone in its
 * tooltip (the attempts below show the date line). */
function endLabel(endpoint: NodeEndpoint): { text: string; title: string } {
  switch (endpoint.kind) {
    case 'terminal': {
      const atMs = instantMs(endpoint.at);
      return {
        text: `${statusLabel(endpoint.status)} at ${clockText(atMs)}`,
        title: `${dateText(atMs)} ${clockText(atMs)} ${zoneText(atMs)}`,
      };
    }
    case 'not_terminal':
      return { text: 'not ended', title: 'The node has no terminal status yet.' };
    default:
      return assertNever(endpoint, 'node endpoint');
  }
}

function segmentParts(segments: readonly TimingSegment[], drawAtMs: number): PhaseBarPart[] {
  return segments.map(segment => {
    const span = segmentSpan(segment, drawAtMs);
    const ms = span.endMs - span.startMs;
    return {
      key: segment.phase.kind,
      ms,
      title:
        `${segmentPhaseText(segment)} ${formatMilliseconds(ms)} · ${segmentSourceText(segment.source)} ` +
        `(${clockText(span.startMs)} → ${segment.end.kind === 'open' ? 'open' : clockText(span.endMs)})`,
    };
  });
}

/** The timing summary of a node of a run, read from the run query, which the
 * graph and the timeline share. Nothing while the run is not loaded. */
export function NodeTimingSection({
  workflowId,
  taskIndex,
}: {
  workflowId: string;
  taskIndex: number;
}) {
  const { detail, readClock } = useWorkflowRun(workflowId);
  const node = detail?.nodes.find(candidate => candidate.task_index === taskIndex);
  if (detail === undefined || node === undefined || readClock === undefined) {
    return null;
  }
  const membership = criticalMembership(
    criticalPath(detail.nodes, detail.edges, readClock.asOfMs),
    taskIndex
  );
  return <NodeTimingView timing={node.timing} readClock={readClock} membership={membership} />;
}

/**
 * The one timing summary of a node: its wall time and end, a bar of its
 * segments in time order (dependency wait as a thin track), and its time
 * split into its own phases and its task. The attempts below hold the task
 * detail. An open segment grows on the one-second clock.
 */
export function NodeTimingView({
  timing,
  readClock,
  membership,
}: {
  timing: NodeTiming;
  readClock: ReadClock;
  membership: CriticalMembership;
}) {
  const drawAtMs = useDrawTime(readClock, readClock.open && nodeTimingIsOpen(timing));
  return (
    <section aria-label="Node timing" className="flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs uppercase tracking-wide text-muted-foreground">Node timing</span>
        <CriticalBadge membership={membership} />
      </div>
      <TimingBody timing={timing} drawAtMs={drawAtMs} />
    </section>
  );
}

function CriticalBadge({ membership }: { membership: CriticalMembership }) {
  switch (membership) {
    case 'on_path':
      return (
        <span
          className="rounded-full border px-2 py-px text-10 font-medium"
          style={{ borderColor: 'var(--critical-path)' }}
          title="On the critical path: the chain of nodes whose end times decided the end of the run. A delay here delays the run."
        >
          critical path
        </span>
      );
    case 'off_path':
      return null;
    default:
      return assertNever(membership, 'critical membership');
  }
}

function TimingBody({ timing, drawAtMs }: { timing: NodeTiming; drawAtMs: number }) {
  switch (timing.kind) {
    case 'unavailable':
    case 'inconsistent':
      return (
        <>
          <PhaseStrip timing={timing} drawAtMs={drawAtMs} />
          <p className="text-xs text-muted-foreground">{nodeCoverageText(timing)}</p>
        </>
      );
    case 'exact':
    case 'partial': {
      const wall = nodeWallTime(timing.segments, timing.endpoint, drawAtMs);
      const parts = nodeTimeParts(timing.segments, drawAtMs);
      const end = endLabel(timing.endpoint);
      return (
        <>
          <div className="flex flex-wrap items-baseline gap-x-3">
            <span className="font-mono text-lg font-semibold tabular-nums" title="Wall time of the node">
              {wallTimeText(wall)}
            </span>
            <span className="text-xs text-muted-foreground" title={end.title}>
              {end.text}
            </span>
          </div>
          {timing.segments.length > 0 && (
            <PhaseBar
              parts={segmentParts(timing.segments, drawAtMs)}
              label={`Node segments: ${parts.map(nodeTimePartText).join(', ')}`}
              size="regular"
            />
          )}
          <p className="text-xs text-muted-foreground">{parts.map(nodeTimePartText).join(' · ')}</p>
          {timing.kind === 'partial' && (
            <p className="text-xs text-muted-foreground">{nodeCoverageText(timing)}</p>
          )}
        </>
      );
    }
    default:
      return assertNever(timing, 'node timing');
  }
}
