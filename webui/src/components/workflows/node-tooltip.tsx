import { useNodeErrorCode, type NodeErrorCode } from '@/hooks/use-workflow-task';
import { clockText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusColorVar, statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type { NodeEndpoint, NodeTiming, PauseInterval, TimingSegment } from '@/types/timing';
import type { WorkflowNode } from '@/types/workflows';

import { PhaseSwatch, TipHead, TipNote, TipPhaseList, TipTimes } from './mark-tip';
import { OutcomeCap } from './outcome-sign';
import { phaseLabel, phaseMeaning, phaseTrack, segmentKind, type TimingKind } from './phase';
import {
  nodeRunCount,
  nodeTime,
  runCountText,
  timedSegments,
  type ChainMembership,
} from './run-timing';
import { pauseSpan, phaseComposition, segmentInterval, segmentSpan } from './timeline-geometry';
import {
  durationText,
  nodeCoverageText,
  nodeTimeText,
  segmentDetailText,
  segmentSourceText,
} from './timing-text';

/** What of a node the pointer or the keyboard cursor is on. The tooltip shows
 * that mark: what it is, its duration, and its start → end. */
export type NodeFocus =
  /** The node as a whole: a graph card, the name or the time of a row, or
   * the track of a row off its marks. */
  | { kind: 'node' }
  | { kind: 'segment'; segmentIndex: number }
  /** The diamond at the end of the node. */
  | { kind: 'endpoint' }
  /** The bar of one kind in the log cell of a row. */
  | { kind: 'lane'; phase: TimingKind };

/** A pause of the run under the pointer. */
export type PauseFocus = { kind: 'in_pause'; pause: PauseInterval } | { kind: 'not_in_pause' };

export const nodeLabel = (node: WorkflowNode): string =>
  node.node_id ?? node.task_name ?? `#${node.task_index}`;

/** The end of a node in words: `completed at 09:54:36.528`, `not ended`. */
export function nodeEndText(endpoint: NodeEndpoint): string {
  switch (endpoint.kind) {
    case 'terminal':
      return `${statusLabel(endpoint.status)} at ${clockText(instantMs(endpoint.at))}`;
    case 'not_terminal':
      return 'not ended';
    default:
      return assertNever(endpoint, 'node endpoint');
  }
}

/** The end of a node with the error code of its task, when the node has
 * one: `failed at 09:54:36.528, error code PSP_UNAVAILABLE`. */
export function nodeEndWithCodeText(endpoint: NodeEndpoint, failure: NodeErrorCode): string {
  const end = nodeEndText(endpoint);
  switch (failure.kind) {
    case 'code':
      return `${end}, error code ${failure.code}`;
    case 'none':
    case 'not_loaded':
      return end;
    default:
      return assertNever(failure, 'node error code');
  }
}

/** Start → end of a time span; an open one ends `open`. */
export const spanClockText = (startMs: number, endMs: number, open: boolean): string =>
  `${clockText(startMs)} → ${open ? 'open' : clockText(endMs)}`;

/** The tooltip of one segment of a node: its kind, its node and source, its
 * time, its start → end, what it is, and what it holds beyond its kind. */
export function SegmentTip({
  owner,
  segment,
  drawAtMs,
}: {
  owner: string;
  segment: TimingSegment;
  drawAtMs: number;
}) {
  const span = segmentSpan(segment, drawAtMs);
  const open = segment.end.kind === 'open';
  const kind = segmentKind(segment.phase);
  const detail = segmentDetailText(segment);
  return (
    <div data-focus="segment" className="flex flex-col gap-0.5">
      <TipHead
        mark={<PhaseSwatch kind={kind} thin={phaseTrack(kind) === 'thin'} />}
        name={phaseLabel(kind)}
        context={`${owner} · ${segmentSourceText(segment.source)}`}
        value={durationText(span.endMs - span.startMs, open)}
      />
      <TipTimes text={spanClockText(span.startMs, span.endMs, open)} />
      <TipNote text={phaseMeaning(kind)} />
      {detail !== null && <TipNote text={detail} />}
    </div>
  );
}

/** The diamond of a terminal node. */
export function EndDiamond({ status }: { status: string }) {
  return (
    <span
      className="mx-0.5 inline-block size-2.5 shrink-0 rotate-45 border border-card"
      style={{ background: statusColorVar(status) }}
      aria-hidden
    />
  );
}

/** The tooltip of the end marker: `end of <node>`, the status with the error
 * code of a failed task, and the time with milliseconds. */
function EndpointTip({ node, failure }: { node: WorkflowNode; failure: NodeErrorCode }) {
  const timing = node.timing;
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      switch (timing.endpoint.kind) {
        case 'terminal':
          return (
            <div data-focus="endpoint" className="flex flex-col gap-0.5">
              <TipHead
                mark={<EndDiamond status={timing.endpoint.status} />}
                name={`end of ${nodeLabel(node)}`}
                context={statusLabel(timing.endpoint.status)}
              />
              <TipTimes text={clockText(instantMs(timing.endpoint.at))} />
              <EndCode failure={failure} />
            </div>
          );
        case 'not_terminal':
          return null;
        default:
          return assertNever(timing.endpoint, 'node endpoint');
      }
    case 'unavailable':
    case 'inconsistent':
      return null;
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The error code of the task of a failed node, on a line of its own. */
function EndCode({ failure }: { failure: NodeErrorCode }) {
  switch (failure.kind) {
    case 'code':
      return <TipNote text={`error code ${failure.code}`} />;
    case 'none':
    case 'not_loaded':
      return null;
    default:
      return assertNever(failure, 'node error code');
  }
}

/** The time of one kind of a node, from its segments: the total, the
 * interval count and the longest interval, or the span of the one
 * interval. */
function LaneTip({ node, phase, drawAtMs }: { node: WorkflowNode; phase: TimingKind; drawAtMs: number }) {
  const intervals = timedSegments(node.timing)
    .map(segment => segmentInterval(segment, drawAtMs))
    .filter(interval => interval.key === phase);
  const [first] = intervals;
  if (first === undefined) {
    return null;
  }
  const lengths = intervals.map(interval => Math.max(interval.endMs - interval.startMs, 0));
  const total = lengths.reduce((sum, ms) => sum + ms, 0);
  return (
    <div data-focus="lane" className="flex flex-col gap-0.5">
      <TipHead
        mark={<PhaseSwatch kind={phase} />}
        name={phaseLabel(phase)}
        context={nodeLabel(node)}
        value={durationText(total, intervals.some(interval => interval.open))}
      />
      <TipTimes
        text={
          intervals.length > 1
            ? `${intervals.length} intervals · longest ${formatMilliseconds(Math.max(...lengths))}`
            : spanClockText(first.startMs, first.endMs, first.open)
        }
      />
      <TipNote text={phaseMeaning(phase)} />
    </div>
  );
}

/** The pause of the run at the cursor: its time and its start → end. */
export function PauseLine({ pause, drawAtMs }: { pause: PauseFocus; drawAtMs: number }) {
  switch (pause.kind) {
    case 'not_in_pause':
      return null;
    case 'in_pause': {
      const span = pauseSpan(pause.pause, drawAtMs);
      const open = pause.pause.end.kind === 'open';
      return (
        <div data-focus="pause" className="mt-0.5 flex flex-col gap-0.5 border-t border-hairline pt-1">
          <TipHead name="run paused" value={durationText(span.endMs - span.startMs, open)} />
          <TipTimes text={spanClockText(span.startMs, span.endMs, open)} />
        </div>
      );
    }
    default:
      return assertNever(pause, 'pause focus');
  }
}

/** What a node is beyond its times: that it is a sub-workflow, or its task
 * or attempt count; and its place on the longest chain. */
export function nodeCountText(node: WorkflowNode, membership: ChainMembership): string {
  const count = node.is_subworkflow ? 'sub-workflow' : runCountText(nodeRunCount(timedSegments(node.timing)));
  switch (membership) {
    case 'on_chain':
      return `${count} · on the longest chain`;
    case 'off_chain':
      return count;
    default:
      return assertNever(membership, 'chain membership');
  }
}

/** The times of the node by kind, or its coverage text when it has none. */
function NodeTimes({ node, drawAtMs }: { node: WorkflowNode; drawAtMs: number }) {
  const timing = node.timing;
  switch (timing.kind) {
    case 'unavailable':
    case 'inconsistent':
      return <TipNote text={nodeCoverageText(timing)} />;
    case 'exact':
    case 'partial': {
      const totals = phaseComposition(timing.segments, drawAtMs);
      return (
        <>
          {totals.length > 0 && <TipPhaseList items={totals.map(total => ({ kind: total.key, ms: total.ms }))} />}
          {timing.kind === 'partial' && <TipNote text={nodeCoverageText(timing)} />}
        </>
      );
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

function endLine(timing: NodeTiming, failure: NodeErrorCode): string | null {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return nodeEndWithCodeText(timing.endpoint, failure);
    case 'unavailable':
    case 'inconsistent':
      return null;
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The tooltip of the node as a whole: its sign, name, status and time
 * without dependency wait; the time of each kind; its counts, its end, and
 * the facts of the view. */
function NodeSummaryTip({
  node,
  drawAtMs,
  membership,
  facts,
  failure,
}: Omit<NodeTooltipProps, 'focus' | 'pause'> & { failure: NodeErrorCode }) {
  const time = nodeTime(node.timing, drawAtMs);
  const end = endLine(node.timing, failure);
  return (
    <>
      <TipHead
        mark={<OutcomeCap status={node.node_status} sizePx={12} />}
        name={nodeLabel(node)}
        context={statusLabel(node.node_status)}
        value={nodeTimeText(time)}
      />
      <NodeTimes node={node} drawAtMs={drawAtMs} />
      <TipNote text={nodeCountText(node, membership)} />
      {end !== null && <TipNote text={`end: ${end}`} />}
      {facts.map(fact => (
        <TipNote key={fact} text={fact} />
      ))}
    </>
  );
}

/**
 * What a node is, in one box, for the graph card and for the timeline row:
 * the mark under the cursor (a segment, the end marker, a bar of the log
 * cell), or the node as a whole, with the pause of the run at the cursor.
 * `facts`: the facts that the view adds to the node as a whole, one line
 * each.
 */
export interface NodeTooltipProps {
  node: WorkflowNode;
  drawAtMs: number;
  membership: ChainMembership;
  focus: NodeFocus;
  pause: PauseFocus;
  facts: readonly string[];
}

/** The tooltip of a node of the run `workflowId`. A failed node reads the
 * error code of its task for its end; the other nodes read nothing. */
export function NodeTooltip({ workflowId, ...props }: NodeTooltipProps & { workflowId: string }) {
  return props.node.node_status === 'FAILED' ? (
    <FailedNodeTooltip workflowId={workflowId} {...props} />
  ) : (
    <NodeTooltipBody {...props} failure={{ kind: 'none' }} />
  );
}

function FailedNodeTooltip({ workflowId, ...props }: NodeTooltipProps & { workflowId: string }) {
  const failure = useNodeErrorCode(workflowId, props.node.task_index, true);
  return <NodeTooltipBody {...props} failure={failure} />;
}

function FocusTip({
  focus,
  failure,
  ...props
}: Omit<NodeTooltipProps, 'pause'> & { failure: NodeErrorCode }) {
  switch (focus.kind) {
    case 'node':
      return <NodeSummaryTip {...props} failure={failure} />;
    case 'segment': {
      const segment = timedSegments(props.node.timing)[focus.segmentIndex];
      // A refetch can remove the segment under the cursor.
      return segment === undefined ? null : (
        <SegmentTip owner={nodeLabel(props.node)} segment={segment} drawAtMs={props.drawAtMs} />
      );
    }
    case 'endpoint':
      return <EndpointTip node={props.node} failure={failure} />;
    case 'lane':
      return <LaneTip node={props.node} phase={focus.phase} drawAtMs={props.drawAtMs} />;
    default:
      return assertNever(focus, 'node focus');
  }
}

export function NodeTooltipBody({ pause, ...props }: NodeTooltipProps & { failure: NodeErrorCode }) {
  return (
    <>
      <FocusTip {...props} />
      <PauseLine pause={pause} drawAtMs={props.drawAtMs} />
    </>
  );
}
