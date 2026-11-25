import { memo } from 'react';

import { Handle, type Node, type NodeProps, Position } from '@xyflow/react';
import { Layers, TriangleAlert } from 'lucide-react';

import { RESIDUAL_COPY } from '@/actions/copy';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { assertNever } from '@/lib/exhaustive';
import { formatDuration, formatMilliseconds } from '@/lib/format-duration';
import { statusColorVar } from '@/lib/status-utils';
import type { NodeTiming } from '@/types/timing';
import type { WorkflowStatus } from '@/types/workflows';

import { NODE_H, NODE_W } from './layout';
import type { ResidualState } from './residual';
import { nodeFacts, nodeTime, type NodeFact } from './run-timing';
import { nodeTimingIsOpen } from './timeline-geometry';

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
  /** Phase timing of the node: its time and its facts on the card. */
  timing: NodeTiming;
  /** Clock of the run read that holds `timing`. */
  readClock: ReadClock;
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

/** The duration a card shows: the time of the node without its dependency
 * wait when its timing has it, else the execution time of a node with no
 * phase timing (a run from before the lifecycle record), else none. */
export type CardDuration =
  | { kind: 'node_time'; ms: number }
  | { kind: 'execution'; seconds: number }
  | { kind: 'none' };

export function cardDuration(
  timing: NodeTiming,
  execS: number | null,
  drawAtMs: number
): CardDuration {
  const time = nodeTime(timing, drawAtMs);
  switch (time.kind) {
    case 'measured':
      return { kind: 'node_time', ms: time.ms };
    case 'not_started':
      return { kind: 'none' };
    case 'no_timing':
      return execS === null ? { kind: 'none' } : { kind: 'execution', seconds: execS };
    default:
      return assertNever(time, 'node time');
  }
}

/** The text of a card duration, or null for none. */
function cardDurationText(duration: CardDuration): string | null {
  switch (duration.kind) {
    case 'node_time':
      return formatMilliseconds(duration.ms);
    case 'execution':
      return formatDuration(duration.seconds);
    case 'none':
      return null;
    default:
      return assertNever(duration, 'card duration');
  }
}

/** One fact on a card, in words. `warning`: the record of the node
 * contradicts itself, and the reader must see it. */
export interface CardFact {
  tone: 'warning' | 'plain';
  text: string;
}

function nodeFactText(fact: NodeFact): CardFact {
  switch (fact.kind) {
    case 'attempts':
      return { tone: 'plain', text: `${fact.count} attempts` };
    case 'tasks':
      return { tone: 'plain', text: `${fact.count} tasks` };
    default:
      return assertNever(fact, 'node fact');
  }
}

/**
 * The facts a card states in words: what the normal case does not have. A
 * node that ran once has none, so its card stays quiet.
 */
export function cardFacts(timing: NodeTiming): CardFact[] {
  switch (timing.kind) {
    case 'unavailable':
      return [{ tone: 'plain', text: 'timing unavailable' }];
    case 'inconsistent':
      return [{ tone: 'warning', text: 'timing inconsistent' }];
    case 'exact':
      return nodeFacts(timing.segments).map(nodeFactText);
    case 'partial':
      return [
        ...nodeFacts(timing.segments).map(nodeFactText),
        { tone: 'plain', text: 'partial timing' },
      ];
    default:
      return assertNever(timing, 'node timing');
  }
}

function FactsLine({ facts }: { facts: readonly CardFact[] }) {
  if (facts.length === 0) {
    return null;
  }
  const warned = facts.some(fact => fact.tone === 'warning');
  return (
    <div data-card-facts className="flex min-w-0 items-center gap-1 pl-1.5 text-10 leading-tight">
      {warned && (
        <TriangleAlert className="size-3 shrink-0" style={{ color: 'var(--warning-dark)' }} aria-hidden />
      )}
      <span className="min-w-0 truncate">
        {facts.map((fact, index) => (
          <span key={fact.text}>
            {index > 0 && <span className="text-muted-foreground"> · </span>}
            <FactText fact={fact} />
          </span>
        ))}
      </span>
    </div>
  );
}

function FactText({ fact }: { fact: CardFact }) {
  switch (fact.tone) {
    case 'warning':
      return (
        <span className="font-semibold" style={{ color: 'var(--warning-dark)' }}>
          {fact.text}
        </span>
      );
    case 'plain':
      return <span className="text-muted-foreground">{fact.text}</span>;
    default:
      return assertNever(fact.tone, 'card fact tone');
  }
}

/** One node of the graph. The card holds text and the status color; the
 * tooltip of the graph says what each of its marks is. */
function WorkflowNodeCardImpl({ data, selected }: NodeProps<WorkflowFlowNode>) {
  const color = statusColorVar(data.status);
  // Only a card with an open segment follows the one-second clock.
  const drawAtMs = useDrawTime(
    data.readClock,
    data.readClock.open && nodeTimingIsOpen(data.timing)
  );
  const duration = cardDurationText(cardDuration(data.timing, data.execS, drawAtMs));
  const residual = data.residual === null ? null : RESIDUAL_COPY[data.residual];
  const rollup = childRollupBadge(data.childTotal, data.childFailed);

  // The surface draws the edge of a plain card. A border shows only for the
  // selection ring and for the dashed edge of a tolerant join.
  return (
    <div
      className="content-surface relative flex flex-col justify-center gap-0.5 overflow-hidden rounded-xl px-3 py-1.5 text-card-foreground transition-colors"
      style={{
        width: NODE_W,
        height: NODE_H,
        borderColor: selected ? 'var(--ring)' : 'var(--border)',
        borderWidth: selected ? 2 : data.allowFailedDeps ? 1 : 0,
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
        <span className="min-w-0 flex-1 truncate text-13 font-medium">{data.label}</span>
        {rollup !== null && (
          <span
            className="shrink-0 rounded px-1 font-mono text-9 font-medium"
            style={{
              color: rollup.failed ? 'var(--error)' : 'var(--muted-foreground)',
            }}
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
          <span className="truncate font-mono text-10 text-muted-foreground">{data.taskName}</span>
        ) : (
          <span className="shrink-0 rounded border border-hairline-strong px-1 text-9 uppercase text-muted-foreground">
            {residual.badge}
          </span>
        )}
        {duration !== null && (
          <span className="ml-auto shrink-0 font-mono text-10 text-muted-foreground">{duration}</span>
        )}
      </div>
      <FactsLine facts={cardFacts(data.timing)} />
      <Handle
        type="source"
        position={Position.Right}
        className="!h-1.5 !w-1.5 !border-0 !bg-border"
      />
    </div>
  );
}

export const WorkflowNodeCard = memo(WorkflowNodeCardImpl);
