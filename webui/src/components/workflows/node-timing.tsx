import { Hint } from '@/components/ui/hint';
import { NoteLine, type NoteView } from '@/components/ui/note-line';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { clockText, dateText, zoneText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type { NodeEndpoint, NodeTiming } from '@/types/timing';

import { phaseLabel } from './phase';
import {
  criticalMembership,
  criticalPath,
  dependencyWait,
  lastDependency,
  nodeParts,
  nodeTime,
  type CriticalMembership,
  type LastDependency,
  type NodePart,
  type NodeTime,
} from './run-timing';
import { phaseFigure, TimeFigures, type TimeFigure } from './time-figures';
import { nodeTimingIsOpen } from './timeline-geometry';
import { durationText, nodeCoverageText } from './timing-text';

/** The name of the task part of a node: `task`, `3 attempts`, `2 tasks`. */
function taskPartLabel(part: Extract<NodePart, { kind: 'task' }>): string {
  return part.tasks > 1 ? `${part.tasks} tasks` : part.attempts > 1 ? `${part.attempts} attempts` : 'task';
}

/** The figure of one part of a node: a phase of the node itself, or its
 * task. */
export function nodePartFigure(part: NodePart): TimeFigure {
  switch (part.kind) {
    case 'phase':
      return phaseFigure(part.total);
    case 'task':
      return {
        id: 'task',
        label: taskPartLabel(part),
        ms: part.ms,
        open: part.open,
        weight: { kind: 'main' },
        meaning:
          'The time of the task of the node: each attempt with its queue wait and slot wait, and each retry wait. The attempts below split it.',
      };
    default:
      return assertNever(part, 'node part');
  }
}

/** The parts of a node in words: `dependency wait 5.2s, 3 attempts 32.1s,
 * advancing 1ms`. */
export function nodePartsText(parts: readonly NodePart[]): string {
  return parts
    .map(nodePartFigure)
    .map(figure => `${figure.label} ${formatMilliseconds(figure.ms)}`)
    .join(', ');
}

/** The first line of a node: its time without dependency wait as a number,
 * or what the node did in place of a run, in words. */
export type NodeHeadline = { kind: 'time'; text: string } | { kind: 'words'; text: string };

export function nodeHeadline(time: NodeTime, endpoint: NodeEndpoint): NodeHeadline {
  switch (time.kind) {
    case 'measured':
      return { kind: 'time', text: durationText(time.ms, time.open) };
    case 'not_started':
      switch (endpoint.kind) {
        case 'terminal':
          return { kind: 'words', text: 'Did not run' };
        case 'not_terminal':
          return { kind: 'words', text: 'Not started' };
        default:
          return assertNever(endpoint, 'node endpoint');
      }
    case 'no_timing':
      return { kind: 'words', text: 'No timing' };
    default:
      return assertNever(time, 'node time');
  }
}

/** The end of a terminal node: its status and time of day, with the day and
 * the zone in its hint (the attempts below show the date line). A node that
 * is not terminal has no end line: its time is a time so far. */
export type NodeEndLine = { kind: 'ended'; text: string; hint: string } | { kind: 'not_ended' };

export function nodeEndLine(endpoint: NodeEndpoint): NodeEndLine {
  switch (endpoint.kind) {
    case 'terminal': {
      const atMs = instantMs(endpoint.at);
      return {
        kind: 'ended',
        text: `${statusLabel(endpoint.status)} at ${clockText(atMs)}`,
        hint: `The end of the node: ${dateText(atMs)} ${clockText(atMs)} ${zoneText(atMs)}.`,
      };
    }
    case 'not_terminal':
      return { kind: 'not_ended' };
    default:
      return assertNever(endpoint, 'node endpoint');
  }
}

/** When the dependency wait of a node ended: at the end of its dependency
 * that ended last. Null for a node with no dependency wait, and for a wait
 * with no end yet. */
export function waitedForText(timing: NodeTiming, drawAtMs: number, last: LastDependency): string | null {
  const wait = dependencyWait(timing, drawAtMs);
  switch (wait.kind) {
    case 'none':
      return null;
    case 'waited':
      break;
    default:
      return assertNever(wait, 'dependency wait');
  }
  switch (last.kind) {
    case 'none':
      return null;
    case 'node':
      return wait.open
        ? null
        : `The dependency wait ended when ${last.node.node_id ?? last.node.task_name} ended.`;
    default:
      return assertNever(last, 'last dependency');
  }
}

/** The notes of the parts of the node itself: each long engine step with
 * what it tells. A long step of an attempt has its note on its attempt. */
export function nodePartNotes(parts: readonly NodePart[]): NoteView[] {
  return parts.flatMap((part): NoteView[] => {
    switch (part.kind) {
      case 'task':
        return [];
      case 'phase': {
        const weight = phaseFigure(part.total).weight;
        switch (weight.kind) {
          case 'marked': {
            const label = phaseLabel(part.total.key);
            return [
              {
                tone: 'warning',
                text: `${label.charAt(0).toUpperCase()}${label.slice(1)} ${formatMilliseconds(part.total.ms)}. ${weight.tells}`,
              },
            ];
          }
          case 'main':
          case 'plain':
          case 'quiet':
            return [];
          default:
            return assertNever(weight, 'phase weight');
        }
      }
      default:
        return assertNever(part, 'node part');
    }
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
  return (
    <NodeTimingView
      timing={node.timing}
      readClock={readClock}
      membership={membership}
      waitedFor={lastDependency(detail.nodes, detail.edges, taskIndex, readClock.asOfMs)}
    />
  );
}

/**
 * The one timing summary of a node: its time without dependency wait and its
 * end, then its wall time as named numbers: its dependency wait, its task (or
 * its child run) and its own engine steps. The attempts below split the
 * task. A time with no end yet grows on the one-second clock.
 */
export function NodeTimingView({
  timing,
  readClock,
  membership,
  waitedFor,
}: {
  timing: NodeTiming;
  readClock: ReadClock;
  membership: CriticalMembership;
  waitedFor: LastDependency;
}) {
  const drawAtMs = useDrawTime(readClock, readClock.open && nodeTimingIsOpen(timing));
  return (
    <section aria-label="Node timing" className="flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs uppercase tracking-wide text-muted-foreground">Node timing</span>
        <CriticalBadge membership={membership} />
      </div>
      <TimingBody timing={timing} drawAtMs={drawAtMs} waitedFor={waitedFor} />
    </section>
  );
}

function CriticalBadge({ membership }: { membership: CriticalMembership }) {
  switch (membership) {
    case 'on_path':
      return (
        <Hint text="This node is on the critical path: the chain of nodes whose end times decided the end of the run. A delay of this node delays the run.">
          <span
            className="rounded-full border px-2 py-px text-10 font-medium"
            style={{ borderColor: 'var(--critical-path)' }}
          >
            critical path
          </span>
        </Hint>
      );
    case 'off_path':
      return null;
    default:
      return assertNever(membership, 'critical membership');
  }
}

function TimingBody({
  timing,
  drawAtMs,
  waitedFor,
}: {
  timing: NodeTiming;
  drawAtMs: number;
  waitedFor: LastDependency;
}) {
  switch (timing.kind) {
    case 'unavailable':
      return <NoteLine view={{ tone: 'info', text: sentence(nodeCoverageText(timing)) }} />;
    case 'inconsistent':
      return <NoteLine view={{ tone: 'warning', text: sentence(nodeCoverageText(timing)) }} />;
    case 'exact':
    case 'partial': {
      const parts = nodeParts(timing.segments, drawAtMs);
      const waited = waitedForText(timing, drawAtMs, waitedFor);
      return (
        <>
          <div className="flex flex-wrap items-baseline gap-x-3">
            <Headline headline={nodeHeadline(nodeTime(timing, drawAtMs), timing.endpoint)} />
            <EndLine end={nodeEndLine(timing.endpoint)} />
          </div>
          {parts.length > 0 && (
            <TimeFigures
              figures={parts.map(nodePartFigure)}
              layout="stacked"
              label={`Time of the node: ${nodePartsText(parts)}`}
            />
          )}
          {waited !== null && <p className="text-xs text-muted-foreground">{waited}</p>}
          {nodePartNotes(parts).map((view, index) => (
            <NoteLine key={index} view={view} />
          ))}
          {timing.kind === 'partial' && (
            <NoteLine view={{ tone: 'info', text: sentence(nodeCoverageText(timing)) }} />
          )}
        </>
      );
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

function Headline({ headline }: { headline: NodeHeadline }) {
  switch (headline.kind) {
    case 'time':
      return (
        <Hint text="The time of the node without its dependency wait: from the time it could start to its end.">
          <span data-node-time className="font-mono text-lg font-semibold tabular-nums">
            {headline.text}
          </span>
        </Hint>
      );
    case 'words':
      return (
        <span data-node-time className="text-sm font-semibold">
          {headline.text}
        </span>
      );
    default:
      return assertNever(headline, 'node headline');
  }
}

function EndLine({ end }: { end: NodeEndLine }) {
  switch (end.kind) {
    case 'ended':
      return (
        <Hint text={end.hint}>
          <span className="text-xs text-muted-foreground">{end.text}</span>
        </Hint>
      );
    case 'not_ended':
      return null;
    default:
      return assertNever(end, 'node end line');
  }
}

/** A coverage text as a sentence: first letter upper case, one closing
 * period. */
function sentence(text: string): string {
  const head = `${text.charAt(0).toUpperCase()}${text.slice(1)}`;
  return head.endsWith('.') ? head : `${head}.`;
}
