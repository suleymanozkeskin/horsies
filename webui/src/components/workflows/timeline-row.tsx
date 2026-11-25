// The rows of the run table. A node row: its name with its counts, a linear
// cell (the dependency wait as a line, the phases, the diamond at its end), a
// log cell (one thin bar for each kind), and its time without dependency
// wait. The run row: the phases along the longest chain.

import type { KeyboardEvent, PointerEvent } from 'react';

import { assertNever } from '@/lib/exhaustive';
import { statusColorVar } from '@/lib/status-utils';
import type { NodeTiming, TimingSegment } from '@/types/timing';
import type { WorkflowNode } from '@/types/workflows';

import { nodeLabel } from './node-tooltip';
import { phaseFill, phaseTrack, segmentKind, type TimingKind } from './phase';
import { nodeRunCount, timedSegments, type ChainMembership } from './run-timing';
import type { TimelineFrame } from './timeline-frame';
import { boxOf, endpointMark, segmentSpan, type EndpointMark, type PhaseTotal, type TimeAxis } from './timeline-geometry';
import { LANE_BAR_PX, LANE_LEFT_PX, LANE_PITCH_PX, LANE_RIGHT_PX, LINEAR_GAP_PX, lanesHeightPx, TABLE_COLUMNS } from './timeline-layout';
import { logPct, type LogDomain } from './time-scale';
import type { KeyLight } from './timing-key';

/** A segment narrower than this is not drawn: at the scale of the run axis
 * it is less than a pixel. Its time is a bar of the log cell. */
const MIN_DRAWN_SEGMENT_PX = 1;
/** The opacity of a mark of another kind while a key entry lights one kind. */
const DIMMED_OPACITY = 0.22;

/** The width of the linear cell in pixels, once the layout measured it. */
export type TrackWidth = { kind: 'measured'; px: number } | { kind: 'unmeasured' };

/** Whether a segment of `widthPct` of the linear cell is drawn: a segment
 * below one pixel is not. Before the cell has a width, every segment is
 * drawn. */
export type SegmentDrawing = 'drawn' | 'not_drawn';

export function segmentDrawing(widthPct: number, track: TrackWidth): SegmentDrawing {
  switch (track.kind) {
    case 'unmeasured':
      return 'drawn';
    case 'measured':
      return (widthPct / 100) * track.px >= MIN_DRAWN_SEGMENT_PX ? 'drawn' : 'not_drawn';
    default:
      return assertNever(track, 'track width');
  }
}

/** The opacity of a mark of `kind` under the light of the key. */
export function kindOpacity(light: KeyLight, kind: TimingKind): number {
  switch (light.kind) {
    case 'all':
      return 1;
    case 'phase':
      return light.phase === kind ? 1 : DIMMED_OPACITY;
    default:
      return assertNever(light, 'key light');
  }
}

/** The look of a segment: dependency wait as a thin line, every other kind
 * at full height (taller in the run row). */
type SegmentSize = 'row' | 'run';

function fullFillClass(size: SegmentSize): string {
  switch (size) {
    case 'row':
      return 'absolute inset-x-0 top-1/2 h-2.5 -translate-y-1/2';
    case 'run':
      return 'absolute inset-x-0 top-1/2 h-3.5 -translate-y-1/2';
    default:
      return assertNever(size, 'segment size');
  }
}

function fillClass(kind: TimingKind, size: SegmentSize): string {
  const track = phaseTrack(kind);
  switch (track) {
    case 'thin':
      return 'absolute inset-x-0 top-1/2 h-0.5 -translate-y-1/2 rounded-[1px]';
    case 'full':
      return fullFillClass(size);
    default:
      return assertNever(track, 'phase track');
  }
}

/** One segment at its time on the axis. The box is always in the row, so
 * the keyboard cursor finds its place; its fill is drawn only when it is
 * wide enough to see. `separated`: the first segment of a node in the run
 * row, with a line of the window color before it. */
function SegmentMark({
  segment,
  axis,
  drawAtMs,
  trackWidth,
  light,
  size,
  separated,
  data,
}: {
  segment: TimingSegment;
  axis: TimeAxis;
  drawAtMs: number;
  trackWidth: TrackWidth;
  light: KeyLight;
  size: SegmentSize;
  separated: boolean;
  data: Record<`data-${string}`, string | number>;
}) {
  const box = boxOf(segmentSpan(segment, drawAtMs), axis);
  const kind = segmentKind(segment.phase);
  const drawing = segmentDrawing(box.widthPct, trackWidth);
  return (
    <span
      {...data}
      data-phase={kind}
      data-end={segment.end.kind}
      data-drawn={drawing === 'drawn' ? 'true' : 'false'}
      className={drawing === 'drawn' ? 'absolute inset-y-0 z-[2]' : 'pointer-events-none absolute inset-y-0'}
      style={{ left: `${box.leftPct}%`, width: `${box.widthPct}%` }}
    >
      {drawing === 'drawn' && (
        <span
          className={fillClass(kind, size)}
          style={{
            background: phaseFill(kind),
            opacity: kindOpacity(light, kind),
            ...(separated ? { boxShadow: '-1px 0 0 var(--background)' } : {}),
          }}
        />
      )}
    </span>
  );
}

/** The terminal status of a node: the diamond at its time, in the color of
 * the status. It is a point in time, not a duration. */
export function EndpointMarker({ mark }: { mark: EndpointMark }) {
  switch (mark.kind) {
    case 'terminal':
      return (
        <span
          data-endpoint={mark.status}
          className="absolute top-1/2 z-[3] size-2.5 -translate-x-1/2 -translate-y-1/2 rotate-45 border border-card"
          style={{ left: `${mark.pct}%`, background: statusColorVar(mark.status) }}
        />
      );
    case 'not_terminal':
      return null;
    default:
      return assertNever(mark, 'endpoint mark');
  }
}

/** A row with no segments: the label says why, and the tooltip of the row
 * gives the reason. */
function HatchedRow({ text, fill }: { text: string; fill: string }) {
  return (
    <span
      data-hatched-row
      className="absolute inset-x-0 top-1/2 flex h-4 -translate-y-1/2 items-center rounded-sm px-1.5 text-10 text-muted-foreground"
      style={{ background: fill }}
    >
      {text}
    </span>
  );
}

interface CellContext {
  drawAtMs: number;
  trackWidth: TrackWidth;
  light: KeyLight;
}

function NodeSegments({ timing, axis, context }: { timing: NodeTiming; axis: TimeAxis; context: CellContext }) {
  switch (timing.kind) {
    case 'unavailable':
      return <HatchedRow text="Timing unavailable" fill="var(--timing-unavailable-hatch)" />;
    case 'inconsistent':
      return <HatchedRow text="Timing inconsistent" fill="var(--timing-inconsistent-hatch)" />;
    case 'exact':
    case 'partial':
      return (
        <>
          {timing.segments.map((segment, segmentIndex) => (
            <SegmentMark
              key={segmentIndex}
              segment={segment}
              axis={axis}
              drawAtMs={context.drawAtMs}
              trackWidth={context.trackWidth}
              light={context.light}
              size="row"
              separated={false}
              data={{ 'data-segment-index': segmentIndex }}
            />
          ))}
          <EndpointMarker mark={endpointMark(timing.endpoint, axis)} />
        </>
      );
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The linear cell of a node row. */
function NodeLinearCell({ node, frame, context }: { node: WorkflowNode; frame: TimelineFrame; context: CellContext }) {
  return (
    <span data-row-track className="relative block h-full" style={{ marginRight: LINEAR_GAP_PX }}>
      {frame.kind === 'composed' ? (
        <NodeSegments timing={node.timing} axis={frame.axis} context={context} />
      ) : (
        <HatchedRow text="Timing unavailable" fill="var(--timing-unavailable-hatch)" />
      )}
    </span>
  );
}

/** The log cell: one thin bar for each kind of `totals`, in its lane. */
export function LanesCell({
  totals,
  laneKinds,
  domain,
  light,
}: {
  totals: readonly PhaseTotal[];
  laneKinds: readonly TimingKind[];
  domain: LogDomain;
  light: KeyLight;
}) {
  return (
    <span
      data-row-lanes
      className="relative block"
      style={{ height: lanesHeightPx(laneKinds.length), marginLeft: LANE_LEFT_PX, marginRight: LANE_RIGHT_PX }}
    >
      {totals.map(total => {
        const lane = laneKinds.indexOf(total.key);
        return lane < 0 ? null : (
          <span
            key={total.key}
            data-lane={total.key}
            className="absolute left-0 z-[2] min-w-0.5 rounded-r-[2px] transition-opacity motion-reduce:transition-none"
            style={{
              top: lane * LANE_PITCH_PX,
              height: LANE_BAR_PX,
              width: `${logPct(total.ms, domain)}%`,
              background: phaseFill(total.key),
              opacity: kindOpacity(light, total.key),
            }}
          />
        );
      })}
    </span>
  );
}

/** The counts of a node beside its name: `sub` for a sub-workflow, `×3` for
 * three attempts, `2 tasks` for a replaced task. */
function NameTags({ node }: { node: WorkflowNode }) {
  const count = nodeRunCount(timedSegments(node.timing));
  const tag = 'shrink-0 rounded-full bg-glass-inset px-1.25 py-px font-mono text-10 leading-tight text-muted-foreground';
  return (
    <>
      {node.is_subworkflow && <span className={tag}>sub</span>}
      {count.kind === 'attempts' && count.count > 1 && <span className={tag}>×{count.count}</span>}
      {count.kind === 'tasks' && <span className={tag}>{count.count} tasks</span>}
    </>
  );
}

export interface NodeRowProps {
  node: WorkflowNode;
  index: number;
  rowCount: number;
  topPx: number;
  rowPx: number;
  frame: TimelineFrame;
  context: CellContext;
  totals: readonly PhaseTotal[];
  laneKinds: readonly TimingKind[];
  domain: LogDomain;
  timeText: string;
  accessibleName: string;
  membership: ChainMembership;
  selected: boolean;
  dimmed: boolean;
  lit: boolean;
  /** The shared tooltip shows this row. */
  described: boolean;
  tooltipId: string;
  onSelect: () => void;
  onKeyDown: (event: KeyboardEvent<HTMLButtonElement>) => void;
  onPointerMove: (event: PointerEvent<HTMLButtonElement>) => void;
  onLeave: () => void;
}

function rowClass(selected: boolean, lit: boolean): string {
  const base =
    'grid h-full w-full items-center border-b border-hairline px-5 text-left focus-visible:outline-2 focus-visible:outline-ring';
  if (selected) {
    return `${base} bg-accent-surface`;
  }
  return lit ? `${base} bg-surface-hover` : `${base} hover:bg-surface-hover`;
}

/** One node: its name, its linear cell, its log cell and its time. Its
 * accessible name holds its timing; the pointer on a mark shows the tooltip
 * of the mark. */
export function NodeRow(props: NodeRowProps) {
  const { node, frame, context } = props;
  return (
    <li
      aria-setsize={props.rowCount}
      aria-posinset={props.index + 1}
      className="absolute inset-x-0"
      style={{ top: props.topPx, height: props.rowPx }}
    >
      <button
        type="button"
        data-row-index={props.index}
        aria-label={props.accessibleName}
        aria-describedby={props.described ? props.tooltipId : undefined}
        onClick={props.onSelect}
        onKeyDown={props.onKeyDown}
        onPointerMove={props.onPointerMove}
        onPointerLeave={props.onLeave}
        onBlur={props.onLeave}
        style={{ gridTemplateColumns: TABLE_COLUMNS, opacity: props.dimmed ? 0.35 : 1 }}
        className={rowClass(props.selected, props.lit)}
      >
        <span className="flex min-w-0 items-center gap-1.5 pr-2.5 text-xs">
          <span className="truncate" title={nodeLabel(node)}>
            {nodeLabel(node)}
          </span>
          <NameTags node={node} />
        </span>
        <NodeLinearCell node={node} frame={frame} context={context} />
        <LanesCell totals={props.totals} laneKinds={props.laneKinds} domain={props.domain} light={context.light} />
        <span className="text-right font-mono text-11 whitespace-nowrap tabular-nums">{props.timeText}</span>
      </button>
    </li>
  );
}

/** One segment of the run row: a segment of a node of the longest chain. */
export interface ChainSegment {
  taskIndex: number;
  segmentIndex: number;
  segment: TimingSegment;
}

/** The segments of the run row that start a node: the first drawn segment
 * of each node after the first, which a line of the window color separates
 * from the node before it. */
function separatedSegments(
  segments: readonly ChainSegment[],
  axis: TimeAxis,
  context: CellContext
): ReadonlySet<ChainSegment> {
  const [firstSegment] = segments;
  const seen = new Set<number>(firstSegment === undefined ? [] : [firstSegment.taskIndex]);
  const separated = new Set<ChainSegment>();
  for (const chain of segments) {
    if (chain.segment.phase.kind === 'pending' || seen.has(chain.taskIndex)) {
      continue;
    }
    const box = boxOf(segmentSpan(chain.segment, context.drawAtMs), axis);
    if (segmentDrawing(box.widthPct, context.trackWidth) === 'drawn') {
      seen.add(chain.taskIndex);
      separated.add(chain);
    }
  }
  return separated;
}

export interface RunRowProps {
  runPx: number;
  axis: TimeAxis;
  segments: readonly ChainSegment[];
  context: CellContext;
  totals: readonly PhaseTotal[];
  laneKinds: readonly TimingKind[];
  domain: LogDomain;
  timeText: string;
  accessibleName: string;
  described: boolean;
  tooltipId: string;
  onPointerMove: (event: PointerEvent<HTMLDivElement>) => void;
  /** The keyboard focus is on the row, whose box is `box`. */
  onFocus: (box: DOMRect) => void;
  onLeave: () => void;
}

/** The first row: the run, as the phases along its longest chain, with the
 * time of each kind in its log cell and the duration of the run. */
export function RunRow(props: RunRowProps) {
  const separated = separatedSegments(props.segments, props.axis, props.context);
  return (
    <div
      role="group"
      tabIndex={0}
      data-run-row
      aria-label={props.accessibleName}
      aria-describedby={props.described ? props.tooltipId : undefined}
      onPointerMove={props.onPointerMove}
      onPointerLeave={props.onLeave}
      onFocus={event => props.onFocus(event.currentTarget.getBoundingClientRect())}
      onBlur={props.onLeave}
      onKeyDown={event => {
        if (event.key === 'Escape') {
          props.onLeave();
        }
      }}
      className="grid items-center border-b border-hairline bg-glass-inset px-5 focus-visible:outline-2 focus-visible:outline-ring"
      style={{ gridTemplateColumns: TABLE_COLUMNS, height: props.runPx }}
    >
      <span className="pr-2.5 text-xs font-semibold">run</span>
      <span data-row-track className="relative block h-full" style={{ marginRight: LINEAR_GAP_PX }}>
        {props.segments.map(chain =>
          chain.segment.phase.kind === 'pending' ? null : (
            <SegmentMark
              key={`${chain.taskIndex}:${chain.segmentIndex}`}
              segment={chain.segment}
              axis={props.axis}
              drawAtMs={props.context.drawAtMs}
              trackWidth={props.context.trackWidth}
              light={props.context.light}
              size="run"
              separated={separated.has(chain)}
              data={{ 'data-run-segment': chain.segmentIndex, 'data-task-index': chain.taskIndex }}
            />
          )
        )}
      </span>
      <LanesCell totals={props.totals} laneKinds={props.laneKinds} domain={props.domain} light={props.context.light} />
      <span className="text-right font-mono text-xs font-semibold whitespace-nowrap tabular-nums">{props.timeText}</span>
    </div>
  );
}
