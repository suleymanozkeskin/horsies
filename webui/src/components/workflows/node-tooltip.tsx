import type { ReactNode } from 'react';

import { TriangleAlert } from 'lucide-react';
import { createPortal } from 'react-dom';

import { useNodeErrorCode, type NodeErrorCode } from '@/hooks/use-workflow-task';
import { clockText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { statusColorVar, statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type { NodeEndpoint, NodeTiming, PauseInterval, TimingSegment } from '@/types/timing';
import type { WorkflowNode } from '@/types/workflows';

import { phaseFill, phaseWeight } from './phase';
import { nodeTime, timedSegments, type CriticalMembership } from './run-timing';
import { phaseFigure, TimeFigures } from './time-figures';
import { pauseSpan, phaseComposition, segmentSpan } from './timeline-geometry';
import {
  durationText,
  nodeCoverageText,
  nodeTimeText,
  segmentPhaseText,
  segmentSourceText,
} from './timing-text';

/** Offset of a floating tooltip from the pointer, and its widest box. */
const TOOLTIP_OFFSET_PX = 12;
const TOOLTIP_MAX_WIDTH_PX = 320;
/** Below this distance to the bottom of the window the tooltip opens above
 * the pointer. */
const TOOLTIP_FLIP_HEIGHT_PX = 240;

/** What of a node the pointer or the keyboard cursor is on. The tooltip names
 * it first, so each mark explains itself. */
export type NodeFocus =
  /** The node as a whole: a graph card, or the track of a row. */
  | { kind: 'node' }
  | { kind: 'segment'; segmentIndex: number }
  /** The marker of the terminal status of the node. */
  | { kind: 'endpoint' }
  /** The line under a row of the critical path. */
  | { kind: 'critical_line' };

/** A pause of the run under the pointer. */
export type PauseFocus = { kind: 'in_pause'; pause: PauseInterval } | { kind: 'not_in_pause' };

const nodeLabel = (node: WorkflowNode): string =>
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

const CRITICAL_PATH_TEXT =
  'On the critical path: the chain of nodes whose end times decided the end of the run. A delay of this node delays the run.';

/** A box that follows the pointer. It stays in the window: it moves left at
 * the right edge and opens above the pointer near the bottom. It is a child
 * of the document body: a view inside a blurred panel is the containing
 * block of its fixed children, which would shift the box by the offset of
 * the panel. */
export function FloatingTooltip({
  id,
  xPx,
  yPx,
  children,
}: {
  id: string;
  xPx: number;
  yPx: number;
  children: ReactNode;
}) {
  const maxLeft = window.innerWidth - TOOLTIP_MAX_WIDTH_PX - TOOLTIP_OFFSET_PX;
  const left = Math.max(Math.min(xPx + TOOLTIP_OFFSET_PX, maxLeft), 0);
  const above = yPx > window.innerHeight - TOOLTIP_FLIP_HEIGHT_PX;
  return createPortal(
    <div
      id={id}
      role="tooltip"
      className="pointer-events-none fixed z-50 flex flex-col gap-1.5 rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md"
      style={{
        left,
        maxWidth: TOOLTIP_MAX_WIDTH_PX,
        ...(above
          ? { bottom: window.innerHeight - yPx + TOOLTIP_OFFSET_PX }
          : { top: yPx + TOOLTIP_OFFSET_PX }),
      }}
    >
      {children}
    </div>,
    document.body
  );
}

/** The segment under the cursor: its phase and time, what a long time tells,
 * then its source and its time span. */
function SegmentFocus({ segment, drawAtMs }: { segment: TimingSegment; drawAtMs: number }) {
  const span = segmentSpan(segment, drawAtMs);
  const ms = span.endMs - span.startMs;
  const open = segment.end.kind === 'open';
  const weight = phaseWeight(segment.phase.kind, ms);
  return (
    <div data-focus="segment" className="flex flex-col gap-0.5 border-l-2 border-border pl-2">
      <div className="flex items-center gap-1.5 font-medium">
        <span
          className="inline-block h-2.5 w-3 shrink-0 rounded-sm"
          style={{ background: phaseFill(segment.phase.kind) }}
          aria-hidden
        />
        {segmentPhaseText(segment)}
        <span className="ml-auto pl-3 font-mono">{durationText(ms, open)}</span>
      </div>
      <span className="flex flex-wrap gap-x-2 font-mono text-10 text-muted-foreground">
        <span className="whitespace-nowrap">
          {clockText(span.startMs)} → {open ? 'open' : clockText(span.endMs)}
        </span>
        <span className="whitespace-nowrap">{segmentSourceText(segment.source)}</span>
      </span>
      {weight.kind === 'marked' && (
        <span className="flex items-start gap-1.5">
          <TriangleAlert className="mt-px size-3 shrink-0" style={{ color: 'var(--marked-ink)' }} aria-hidden />
          <span>{weight.tells}</span>
        </span>
      )}
    </div>
  );
}

function FocusBlock({
  focus,
  timing,
  drawAtMs,
  failure,
}: {
  focus: NodeFocus;
  timing: NodeTiming;
  drawAtMs: number;
  failure: NodeErrorCode;
}) {
  switch (focus.kind) {
    case 'node':
      return null;
    case 'segment': {
      const segment = timedSegments(timing)[focus.segmentIndex];
      // A refetch can remove the segment under the cursor.
      return segment === undefined ? null : <SegmentFocus segment={segment} drawAtMs={drawAtMs} />;
    }
    case 'endpoint':
      switch (timing.kind) {
        case 'exact':
        case 'partial':
          return (
            <p data-focus="endpoint" className="border-l-2 border-border pl-2">
              End of the node: {nodeEndWithCodeText(timing.endpoint, failure)}. The marker is a
              point in time, in the color of the status.
            </p>
          );
        case 'unavailable':
        case 'inconsistent':
          return null;
        default:
          return assertNever(timing, 'node timing');
      }
    case 'critical_line':
      return (
        <p data-focus="critical_line" className="border-l-2 border-border pl-2">
          {CRITICAL_PATH_TEXT} The line runs from the end of its dependency wait to its end.
        </p>
      );
    default:
      return assertNever(focus, 'node focus');
  }
}

function PauseLine({ pause, drawAtMs }: { pause: PauseFocus; drawAtMs: number }) {
  switch (pause.kind) {
    case 'not_in_pause':
      return null;
    case 'in_pause': {
      const span = pauseSpan(pause.pause, drawAtMs);
      const open = pause.pause.end.kind === 'open';
      return (
        <p data-focus="pause" className="border-l-2 border-border pl-2">
          The run was paused here: {clockText(span.startMs)} → {open ? 'open' : clockText(span.endMs)} (
          {durationText(span.endMs - span.startMs, open)}).
        </p>
      );
    }
    default:
      return assertNever(pause, 'pause focus');
  }
}

/** The times of the node by phase, or its coverage text when it has none. */
function NodeTimes({ node, drawAtMs }: { node: WorkflowNode; drawAtMs: number }) {
  const timing = node.timing;
  switch (timing.kind) {
    case 'unavailable':
    case 'inconsistent':
      return <p>{nodeCoverageText(timing)}</p>;
    case 'exact':
    case 'partial': {
      const totals = phaseComposition(timing.segments, drawAtMs);
      return (
        <>
          {totals.length > 0 && (
            <TimeFigures
              figures={totals.map(phaseFigure)}
              layout="stacked"
              label={`Time of ${nodeLabel(node)} by phase`}
            />
          )}
          {timing.kind === 'partial' && <p className="text-muted-foreground">{nodeCoverageText(timing)}</p>}
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

function membershipLine(membership: CriticalMembership): string | null {
  switch (membership) {
    case 'on_path':
      return CRITICAL_PATH_TEXT;
    case 'off_path':
      return null;
    default:
      return assertNever(membership, 'critical membership');
  }
}

/**
 * What a node is, in one box, for the graph card and for the timeline row:
 * its status and its time without dependency wait, the mark under the cursor
 * (a segment, the end marker, the critical path line, a pause), each phase of
 * the node as a number, and the facts of the view (`facts`). A phase that is
 * too short to draw on the timeline is still a number here.
 */
export interface NodeTooltipProps {
  node: WorkflowNode;
  drawAtMs: number;
  membership: CriticalMembership;
  focus: NodeFocus;
  pause: PauseFocus;
  /** Facts of the node that the view adds, one line each. */
  facts: readonly string[];
}

/** The tooltip of a node of the run `workflowId`. A failed node reads the
 * error code of its task for its end line; the other nodes read nothing. */
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

export function NodeTooltipBody({
  node,
  drawAtMs,
  membership,
  focus,
  pause,
  facts,
  failure,
}: NodeTooltipProps & { failure: NodeErrorCode }) {
  const end = endLine(node.timing, failure);
  const critical = membershipLine(membership);
  return (
    <>
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
        <span className="inline-flex max-w-full items-baseline gap-2">
          <span
            className="size-2 shrink-0 self-center rounded-full"
            style={{ background: statusColorVar(node.node_status) }}
            aria-hidden
          />
          <span className="min-w-0 break-all font-semibold">{nodeLabel(node)}</span>
        </span>
        <span className="text-muted-foreground">{statusLabel(node.node_status)}</span>
        <span className="ml-auto whitespace-nowrap pl-3">
          <span className="text-muted-foreground">node time </span>
          <span className="font-mono font-semibold">{nodeTimeText(nodeTime(node.timing, drawAtMs))}</span>
        </span>
      </div>
      <FocusBlock focus={focus} timing={node.timing} drawAtMs={drawAtMs} failure={failure} />
      <PauseLine pause={pause} drawAtMs={drawAtMs} />
      <NodeTimes node={node} drawAtMs={drawAtMs} />
      {(end !== null || critical !== null || facts.length > 0) && (
        <ul className="flex flex-col gap-0.5 text-muted-foreground">
          {end !== null && focus.kind !== 'endpoint' && <li>End of the node: {end}.</li>}
          {critical !== null && focus.kind !== 'critical_line' && <li>{critical}</li>}
          {facts.map(fact => (
            <li key={fact}>{fact}</li>
          ))}
        </ul>
      )}
    </>
  );
}
