import { Hint } from '@/components/ui/hint';
import { NoteLine } from '@/components/ui/note-line';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { clockText, dateText, zoneText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type { NodeEndpoint, NodeTiming } from '@/types/timing';

import { PhaseSwatch } from './mark-tip';
import { EndDiamond } from './node-tooltip';
import { phaseLabel, phaseMeaning, phaseTrack, type TimingKind } from './phase';
import {
  dependencyWait,
  lastDependency,
  nodeParts,
  nodeTime,
  type ChainMembership,
  type LastDependency,
  type NodePart,
  type NodeTime,
} from './run-timing';
import { nodeTimingIsOpen } from './timeline-geometry';
import { durationText, nodeCoverageText } from './timing-text';

/** The name of the task part of a node: `task`, `3 attempts`, `2 tasks`. */
function taskPartLabel(part: Extract<NodePart, { kind: 'task' }>): string {
  return part.tasks > 1 ? `${part.tasks} tasks` : part.attempts > 1 ? `${part.attempts} attempts` : 'task';
}

/** One part of the time of a node: its kind (none for the task, which holds
 * many kinds), its label, its time, and what it is. */
export interface NodePartView {
  id: string;
  kind: { kind: 'phase'; phase: TimingKind } | { kind: 'task' };
  label: string;
  ms: number;
  open: boolean;
  meaning: string;
}

/** The view of one part of a node: a phase of the node itself, or its
 * task. */
export function nodePartView(part: NodePart): NodePartView {
  switch (part.kind) {
    case 'phase':
      return {
        id: part.total.key,
        kind: { kind: 'phase', phase: part.total.key },
        label: phaseLabel(part.total.key),
        ms: part.total.ms,
        open: part.total.open,
        meaning: phaseMeaning(part.total.key),
      };
    case 'task':
      return {
        id: 'task',
        kind: { kind: 'task' },
        label: taskPartLabel(part),
        ms: part.ms,
        open: part.open,
        meaning: 'each attempt with its queue wait and slot wait, and each retry wait; the attempts below split it',
      };
    default:
      return assertNever(part, 'node part');
  }
}

/** The parts of a node in words: `dependency wait 5.20s, 3 attempts 32.1s,
 * advancing 1.00ms`. */
export function nodePartsText(parts: readonly NodePart[]): string {
  return parts
    .map(nodePartView)
    .map(view => `${view.label} ${durationText(view.ms, false)}`)
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
          return { kind: 'words', text: 'did not run' };
        case 'not_terminal':
          return { kind: 'words', text: 'not started' };
        default:
          return assertNever(endpoint, 'node endpoint');
      }
    case 'no_timing':
      return { kind: 'words', text: 'no timing' };
    default:
      return assertNever(time, 'node time');
  }
}

/** The end of a terminal node: its status and time of day, with the day and
 * the zone in its hint. A node that is not terminal has no end line: its
 * time is a time so far. */
export type NodeEndLine =
  | { kind: 'ended'; status: string; text: string; hint: string }
  | { kind: 'not_ended' };

export function nodeEndLine(endpoint: NodeEndpoint): NodeEndLine {
  switch (endpoint.kind) {
    case 'terminal': {
      const atMs = instantMs(endpoint.at);
      return {
        kind: 'ended',
        status: endpoint.status,
        text: `${statusLabel(endpoint.status)} at ${clockText(atMs)}`,
        hint: `end of the node: ${dateText(atMs)} ${clockText(atMs)} ${zoneText(atMs)}`,
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
      return wait.open ? null : `dependency wait ended with ${last.node.node_id ?? last.node.task_name}`;
    default:
      return assertNever(last, 'last dependency');
  }
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
  return (
    <NodeTimingView
      timing={node.timing}
      readClock={readClock}
      waitedFor={lastDependency(detail.nodes, detail.edges, taskIndex, readClock.asOfMs)}
    />
  );
}

/**
 * The timing summary of a node: its time without dependency wait and its
 * end, then each part of its time: its dependency wait, its task (or its
 * child run) and its own engine steps. The attempts below split the task. A
 * time with no end yet grows on the one-second clock.
 */
export function NodeTimingView({
  timing,
  readClock,
  waitedFor,
}: {
  timing: NodeTiming;
  readClock: ReadClock;
  waitedFor: LastDependency;
}) {
  const drawAtMs = useDrawTime(readClock, readClock.open && nodeTimingIsOpen(timing));
  return (
    <section aria-label="Node timing" className="flex flex-col gap-1.5">
      <TimingBody timing={timing} drawAtMs={drawAtMs} waitedFor={waitedFor} />
    </section>
  );
}

/** The mark of a node on the longest chain of its run; nothing off it. */
export function ChainBadge({ membership }: { membership: ChainMembership }) {
  switch (membership) {
    case 'on_chain':
      return (
        <Hint text="on the longest chain: the chain of nodes whose end times decided the end of the run">
          <span
            className="rounded-full border px-2 py-px text-10 font-medium"
            style={{ borderColor: 'var(--longest-chain)' }}
          >
            longest chain
          </span>
        </Hint>
      );
    case 'off_chain':
      return null;
    default:
      return assertNever(membership, 'chain membership');
  }
}

function PartList({ parts }: { parts: readonly NodePart[] }) {
  return (
    <ul aria-label={`Time of the node: ${nodePartsText(parts)}`} className="m-0 flex list-none flex-wrap gap-x-3 gap-y-1 p-0 text-11">
      {parts.map(nodePartView).map(view => (
        <li key={view.id} data-node-part={view.id}>
          <Hint text={view.meaning} className="inline-flex items-center gap-[5px] whitespace-nowrap">
            {view.kind.kind === 'phase' && (
              <PhaseSwatch kind={view.kind.phase} thin={phaseTrack(view.kind.phase) === 'thin'} />
            )}
            <span>{view.label}</span>
            <span className="font-mono text-muted-foreground tabular-nums">{durationText(view.ms, view.open)}</span>
          </Hint>
        </li>
      ))}
    </ul>
  );
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
          <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
            <span className="text-xs text-muted-foreground">duration</span>
            <Headline headline={nodeHeadline(nodeTime(timing, drawAtMs), timing.endpoint)} />
            <EndLine end={nodeEndLine(timing.endpoint)} />
          </div>
          {parts.length > 0 && <PartList parts={parts} />}
          {waited !== null && <p className="m-0 text-11 text-muted-foreground">{waited}</p>}
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
        <Hint text="the time of the node without its dependency wait: from the time it could start to its end">
          <span data-node-time className="font-mono text-lg leading-tight font-semibold tabular-nums">
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
        <Hint text={end.hint} className="inline-flex items-center gap-1.5 self-center">
          <EndDiamond status={end.status} />
          <span className="font-mono text-11 text-muted-foreground">{end.text}</span>
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
