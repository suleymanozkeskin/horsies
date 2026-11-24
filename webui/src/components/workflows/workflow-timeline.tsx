import {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type RefObject,
} from 'react';

import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { assertNever } from '@/lib/exhaustive';
import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import { statusColorVar, statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type {
  NodeTiming,
  RunCoverage,
  TimingSegment,
  UnavailableReason,
} from '@/types/timing';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

import { phaseFill } from './phase';
import { phaseStripText } from './phase-strip';
import {
  axisTicks,
  boxOf,
  endpointMark,
  nodeWallTime,
  pauseSpan,
  runAxis,
  segmentSpan,
  type AxisBox,
  type EndpointMark,
  type AxisTick,
  type TimeAxis,
} from './timeline-geometry';
import {
  rowWindow,
  scrollTopForRow,
  TIMELINE_ROW_HEIGHT_PX,
  type ListViewport,
  type RowWindow,
} from './timeline-window';
import {
  conflictText,
  endpointText,
  runCoverageText,
  segmentDurationText,
  segmentEndText,
  segmentPhaseText,
  segmentSourceText,
  unavailableReasonText,
  wallTimeText,
} from './timing-text';

interface WorkflowTimelineProps {
  workflowId: string;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  /** Statuses to keep visible; rows of other statuses are faded (empty = all). */
  statusFilter?: ReadonlySet<string>;
}

/** Stable empty default so an absent statusFilter prop keeps a constant reference. */
const NO_STATUS_FILTER: ReadonlySet<string> = new Set();

/** Labels on the time axis, the start and the end included. */
const AXIS_TICK_COUNT = 5;
/** A segment shorter than one pixel still shows; its times do not change. */
const MIN_SEGMENT_WIDTH_PX = 1;
/** Offset of the shared tooltip from the pointer, and its widest box. */
const TOOLTIP_OFFSET_PX = 12;
const TOOLTIP_MAX_WIDTH_PX = 288;

const nodeLabel = (node: WorkflowNode): string =>
  node.node_id ?? node.task_name ?? `#${node.task_index}`;

/** What the rows are drawn against: the axis of a composed run, or the
 * reason that every node of the run is unavailable. */
type TimelineFrame =
  | {
      kind: 'composed';
      axis: TimeAxis;
      ticks: AxisTick[];
      pauses: AxisBox[];
      coverage: RunCoverage;
    }
  | { kind: 'unavailable'; reason: UnavailableReason; capturedSinceMs: number };

function timelineFrame(detail: WorkflowRunDetail, drawAtMs: number): TimelineFrame {
  const timing = detail.timing;
  switch (timing.kind) {
    case 'composed': {
      const axis = runAxis(timing, drawAtMs);
      return {
        kind: 'composed',
        axis,
        ticks: axisTicks(axis, AXIS_TICK_COUNT),
        pauses: timing.pauses.map(pause => boxOf(pauseSpan(pause, drawAtMs), axis)),
        coverage: timing.coverage,
      };
    }
    case 'unavailable':
      return {
        kind: 'unavailable',
        reason: timing.reason,
        capturedSinceMs: instantMs(timing.captured_since),
      };
    default:
      return assertNever(timing, 'run timing');
  }
}

/** The right-column text of a row: the wall time of the node. */
function wallText(timing: NodeTiming, drawAtMs: number): string {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return wallTimeText(nodeWallTime(timing.segments, timing.endpoint, drawAtMs));
    case 'unavailable':
    case 'inconsistent':
      return '—';
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The accessible name of a row: node, status, coverage, phase times, wall
 * time and endpoint. */
function rowAccessibleName(
  node: WorkflowNode,
  frame: TimelineFrame,
  drawAtMs: number
): string {
  const head = `${nodeLabel(node)}, ${statusLabel(node.node_status)}`;
  switch (frame.kind) {
    case 'unavailable':
      return `${head}, timing unavailable: ${unavailableReasonText(frame.reason)}`;
    case 'composed':
      break;
    default:
      return assertNever(frame, 'timeline frame');
  }
  const timing = node.timing;
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return (
        `${head}, ${phaseStripText(timing, drawAtMs)}, ` +
        `wall ${wallText(timing, drawAtMs)}, ${endpointText(timing.endpoint)}`
      );
    case 'unavailable':
    case 'inconsistent':
      return `${head}, ${phaseStripText(timing, drawAtMs)}`;
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The segment under the pointer or the keyboard cursor, where the shared
 * tooltip shows it. Keyed by task index, which a refetch keeps. */
type Inspection =
  | { kind: 'none' }
  | { kind: 'segment'; taskIndex: number; segmentIndex: number; xPx: number; yPx: number };

/** A row to focus once it is rendered (a keyboard move can scroll to it). */
type PendingFocus = { kind: 'none' } | { kind: 'row'; index: number };

const segmentsOf = (timing: NodeTiming): readonly TimingSegment[] => {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return timing.segments;
    case 'unavailable':
    case 'inconsistent':
      return [];
    default:
      return assertNever(timing, 'node timing');
  }
};

/**
 * Phase timeline of one run: one row per node in task-index order on one run
 * axis. Each row stacks the phase segments of the node in time order (a retry
 * repeats its segments), marks the terminal status as an endpoint, and shows
 * the wall time of the node on the right. Pauses are bands over every row. A
 * node whose timing is unavailable or inconsistent shows a hatched row with
 * its label. Open segments grow on a one-second local clock.
 */
export function WorkflowTimeline({
  workflowId,
  selectedIndex,
  onSelectNode,
  statusFilter = NO_STATUS_FILTER,
}: WorkflowTimelineProps) {
  const { detail, readClock, isError } = useWorkflowRun(workflowId);

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

  if (detail === undefined || readClock === undefined) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Loading workflow…
      </div>
    );
  }

  if (detail.nodes.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        This run has no nodes.
      </div>
    );
  }

  return (
    <TimelineView
      detail={detail}
      readClock={readClock}
      selectedIndex={selectedIndex}
      onSelectNode={onSelectNode}
      statusFilter={statusFilter}
    />
  );
}

export interface TimelineViewProps {
  detail: WorkflowRunDetail;
  readClock: ReadClock;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  statusFilter: ReadonlySet<string>;
}

/** The timeline of one loaded run. */
export function TimelineView({
  detail,
  readClock,
  selectedIndex,
  onSelectNode,
  statusFilter,
}: TimelineViewProps) {
  const drawAtMs = useDrawTime(readClock, readClock.open);
  const nodes = useMemo(
    () => [...detail.nodes].sort((a, b) => a.task_index - b.task_index),
    [detail.nodes]
  );
  const frame = timelineFrame(detail, drawAtMs);
  const tooltipId = useId();
  const [inspection, setInspection] = useState<Inspection>({ kind: 'none' });
  const hide = useCallback(() => setInspection({ kind: 'none' }), []);

  return (
    <div className="flex h-full flex-col" onPointerLeave={hide}>
      <RunTimingBanner frame={frame} />
      <TimelineHeader frame={frame} />
      <TimelineRows
        nodes={nodes}
        frame={frame}
        drawAtMs={drawAtMs}
        selectedIndex={selectedIndex}
        onSelectNode={onSelectNode}
        statusFilter={statusFilter}
        inspection={inspection}
        onInspect={setInspection}
        tooltipId={tooltipId}
      />
      <SegmentTooltip
        id={tooltipId}
        inspection={inspection}
        nodes={nodes}
        drawAtMs={drawAtMs}
      />
    </div>
  );
}

function RunTimingBanner({ frame }: { frame: TimelineFrame }) {
  switch (frame.kind) {
    case 'unavailable':
      return (
        <p className="border-b border-border px-3 py-1.5 text-xs text-muted-foreground">
          Timing unavailable. {unavailableReasonText(frame.reason)} The lifecycle
          record starts at {formatInstant(frame.capturedSinceMs)}.
        </p>
      );
    case 'composed': {
      const text = runCoverageText(frame.coverage);
      return text === null ? null : (
        <p
          className="border-b border-border px-3 py-1.5 text-xs"
          style={{
            color:
              frame.coverage.kind === 'inconsistent'
                ? 'var(--error)'
                : 'var(--muted-foreground)',
          }}
        >
          {text}
        </p>
      );
    }
    default:
      return assertNever(frame, 'timeline frame');
  }
}

function TimelineHeader({ frame }: { frame: TimelineFrame }) {
  return (
    <div className="flex items-center gap-2 border-b border-border px-3 py-1.5 text-xs text-muted-foreground">
      <span className="w-40 shrink-0">node</span>
      <span className="relative h-4 min-w-0 flex-1" aria-hidden>
        {frame.kind === 'composed' &&
          frame.ticks.map((tick, index) => (
            <span
              key={tick.pct}
              className="absolute top-0 whitespace-nowrap tabular-nums"
              style={{
                left: `${tick.pct}%`,
                transform:
                  index === 0
                    ? 'none'
                    : index === frame.ticks.length - 1
                      ? 'translateX(-100%)'
                      : 'translateX(-50%)',
              }}
            >
              {formatMilliseconds(tick.offsetMs)}
            </span>
          ))}
      </span>
      <span className="w-16 shrink-0 text-right">wall</span>
    </div>
  );
}

const viewportOf = (element: HTMLElement): ListViewport =>
  element.clientHeight > 0
    ? { kind: 'measured', scrollTopPx: element.scrollTop, heightPx: element.clientHeight }
    : { kind: 'unmeasured' };

/** The controls of the windowed row list. */
interface RowWindowControls {
  scrollRef: RefObject<HTMLDivElement | null>;
  rowRange: RowWindow;
  /** Read the scroll state of the container and update `rowRange`. */
  measure: () => void;
  /** Scroll row `index` into view and focus it once it is rendered. */
  focusRow: (index: number) => void;
}

/**
 * The windowing of the row list: the scroll container, the rows to render,
 * and a keyboard move to a row, which scrolls the row into view and focuses
 * it once it is rendered.
 */
function useRowWindow(rowCount: number): RowWindowControls {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [rowRange, setRowRange] = useState<RowWindow>(() =>
    rowWindow({ kind: 'unmeasured' }, rowCount)
  );
  const [pendingFocus, setPendingFocus] = useState<PendingFocus>({ kind: 'none' });

  const measure = useCallback(() => {
    const element = scrollRef.current;
    const next = rowWindow(
      element === null ? { kind: 'unmeasured' } : viewportOf(element),
      rowCount
    );
    setRowRange(previous =>
      previous.start === next.start && previous.end === next.end ? previous : next
    );
  }, [rowCount]);

  useLayoutEffect(() => {
    measure();
    const element = scrollRef.current;
    if (element === null) {
      return undefined;
    }
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [measure]);

  useEffect(() => {
    if (pendingFocus.kind === 'none') {
      return;
    }
    const button = scrollRef.current?.querySelector<HTMLButtonElement>(
      `[data-row-index="${pendingFocus.index}"]`
    );
    if (button) {
      button.focus();
      setPendingFocus({ kind: 'none' });
    }
  }, [pendingFocus, rowRange]);

  const focusRow = (index: number): void => {
    const target = Math.min(Math.max(index, 0), rowCount - 1);
    const element = scrollRef.current;
    if (element !== null && element.clientHeight > 0) {
      element.scrollTop = scrollTopForRow(target, element.scrollTop, element.clientHeight);
    }
    measure();
    setPendingFocus({ kind: 'row', index: target });
  };

  return { scrollRef, rowRange, measure, focusRow };
}

/** The inspection after an arrow key on a row: the next (`step` 1) or the
 * previous (`step` -1) segment of the row, placed under the segment. */
function keyboardInspection(
  inspection: Inspection,
  node: WorkflowNode,
  row: HTMLElement,
  step: 1 | -1
): Inspection {
  const count = segmentsOf(node.timing).length;
  if (count === 0) {
    return inspection;
  }
  const current =
    inspection.kind === 'segment' && inspection.taskIndex === node.task_index
      ? inspection.segmentIndex
      : step > 0
        ? -1
        : count;
  const segmentIndex = Math.min(Math.max(current + step, 0), count - 1);
  const box = row
    .querySelector(`[data-segment-index="${segmentIndex}"]`)
    ?.getBoundingClientRect();
  return {
    kind: 'segment',
    taskIndex: node.task_index,
    segmentIndex,
    xPx: box === undefined ? 0 : box.left + box.width / 2,
    yPx: box === undefined ? 0 : box.bottom,
  };
}

/** What a key does on a focused row. */
export type RowKeyAction =
  | { kind: 'focus_row'; index: number }
  | { kind: 'step_segment'; step: 1 | -1 }
  | { kind: 'hide_tooltip' }
  | { kind: 'none' };

/** The action of `key` on row `index` of `rowCount` rows. A row move past
 * the first or the last row stays on it. */
export function rowKeyAction(key: string, index: number, rowCount: number): RowKeyAction {
  const last = Math.max(rowCount - 1, 0);
  switch (key) {
    case 'ArrowDown':
      return { kind: 'focus_row', index: Math.min(index + 1, last) };
    case 'ArrowUp':
      return { kind: 'focus_row', index: Math.max(index - 1, 0) };
    case 'Home':
      return { kind: 'focus_row', index: 0 };
    case 'End':
      return { kind: 'focus_row', index: last };
    case 'ArrowRight':
      return { kind: 'step_segment', step: 1 };
    case 'ArrowLeft':
      return { kind: 'step_segment', step: -1 };
    case 'Escape':
      return { kind: 'hide_tooltip' };
    default:
      return { kind: 'none' };
  }
}

interface TimelineRowsProps {
  nodes: readonly WorkflowNode[];
  frame: TimelineFrame;
  drawAtMs: number;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  statusFilter: ReadonlySet<string>;
  inspection: Inspection;
  onInspect: (inspection: Inspection) => void;
  tooltipId: string;
}

/** The rows, windowed: only the rows in view and a few around them render.
 * Up and down arrows (and Home, End) move between rows; left and right
 * arrows step through the segments of a row in the shared tooltip. */
function TimelineRows(props: TimelineRowsProps) {
  const { nodes, frame, drawAtMs, statusFilter, inspection, onInspect } = props;
  const rowCount = nodes.length;
  const { scrollRef, rowRange, measure, focusRow } = useRowWindow(rowCount);
  const hintId = useId();

  const onRowKeyDown = (
    event: KeyboardEvent<HTMLButtonElement>,
    node: WorkflowNode,
    index: number
  ): void => {
    const action = rowKeyAction(event.key, index, rowCount);
    switch (action.kind) {
      case 'focus_row':
        event.preventDefault();
        focusRow(action.index);
        return;
      case 'step_segment':
        event.preventDefault();
        onInspect(keyboardInspection(inspection, node, event.currentTarget, action.step));
        return;
      case 'hide_tooltip':
        onInspect({ kind: 'none' });
        return;
      case 'none':
        return;
      default:
        assertNever(action, 'row key action');
    }
  };

  return (
    <div ref={scrollRef} onScroll={measure} className="relative min-h-0 flex-1 overflow-y-auto">
      <p id={hintId} className="sr-only">
        Up and down arrows move between rows. Left and right arrows step through
        the phase segments of a row.
      </p>
      <div className="relative" style={{ height: rowCount * TIMELINE_ROW_HEIGHT_PX }}>
        <ul aria-label="Nodes of the run" aria-describedby={hintId}>
          {nodes.slice(rowRange.start, rowRange.end).map((node, offset) => (
            <TimelineRow
              key={node.task_index}
              node={node}
              index={rowRange.start + offset}
              rowCount={rowCount}
              frame={frame}
              drawAtMs={drawAtMs}
              selected={node.task_index === props.selectedIndex}
              dimmed={statusFilter.size > 0 && !statusFilter.has(node.node_status)}
              described={
                inspection.kind === 'segment' && inspection.taskIndex === node.task_index
              }
              tooltipId={props.tooltipId}
              onSelect={() => props.onSelectNode(node)}
              onKeyDown={event => onRowKeyDown(event, node, rowRange.start + offset)}
              onInspect={onInspect}
            />
          ))}
        </ul>
        <TrackOverlay frame={frame} />
      </div>
    </div>
  );
}

interface TimelineRowProps {
  node: WorkflowNode;
  index: number;
  rowCount: number;
  frame: TimelineFrame;
  drawAtMs: number;
  selected: boolean;
  dimmed: boolean;
  /** The shared tooltip shows a segment of this row. */
  described: boolean;
  tooltipId: string;
  onSelect: () => void;
  onKeyDown: (event: KeyboardEvent<HTMLButtonElement>) => void;
  onInspect: (inspection: Inspection) => void;
}

/** One node: label, track, wall time. Its accessible name holds its timing. */
function TimelineRow({
  node,
  index,
  rowCount,
  frame,
  drawAtMs,
  selected,
  dimmed,
  described,
  tooltipId,
  onSelect,
  onKeyDown,
  onInspect,
}: TimelineRowProps) {
  return (
    <li
      aria-setsize={rowCount}
      aria-posinset={index + 1}
      className="absolute inset-x-0"
      style={{ top: index * TIMELINE_ROW_HEIGHT_PX, height: TIMELINE_ROW_HEIGHT_PX }}
    >
      <button
        type="button"
        data-row-index={index}
        aria-label={rowAccessibleName(node, frame, drawAtMs)}
        aria-describedby={described ? tooltipId : undefined}
        onClick={onSelect}
        onKeyDown={onKeyDown}
        onBlur={() => onInspect({ kind: 'none' })}
        style={{ opacity: dimmed ? 0.35 : 1 }}
        className={
          selected
            ? 'flex h-full w-full items-center gap-2 border-b border-border bg-accent-surface px-3 text-left focus-visible:outline-2 focus-visible:outline-ring'
            : 'flex h-full w-full items-center gap-2 border-b border-border px-3 text-left hover:bg-glass-surface-strong focus-visible:outline-2 focus-visible:outline-ring'
        }
      >
        <span className="w-40 shrink-0 truncate text-xs font-medium" title={nodeLabel(node)}>
          {nodeLabel(node)}
        </span>
        <span className="relative h-3 min-w-0 flex-1">
          <RowTrack node={node} frame={frame} drawAtMs={drawAtMs} onInspect={onInspect} />
        </span>
        <span className="w-16 shrink-0 text-right font-mono text-10 text-muted-foreground">
          {frame.kind === 'composed' ? wallText(node.timing, drawAtMs) : '—'}
        </span>
      </button>
    </li>
  );
}

/** Pause bands and tick lines over every row, in the track column. */
function TrackOverlay({ frame }: { frame: TimelineFrame }) {
  switch (frame.kind) {
    case 'unavailable':
      return null;
    case 'composed':
      return (
        <div className="pointer-events-none absolute inset-0 flex gap-2 px-3" aria-hidden>
          <span className="w-40 shrink-0" />
          <span className="relative min-w-0 flex-1">
            {frame.ticks.map(tick => (
              <span
                key={tick.pct}
                className="absolute inset-y-0 w-px bg-border"
                style={{ left: `${tick.pct}%`, opacity: 0.35 }}
              />
            ))}
            {frame.pauses.map((band, index) => (
              <span
                key={index}
                data-pause-band
                className="absolute inset-y-0"
                style={{
                  left: `${band.leftPct}%`,
                  width: `${band.widthPct}%`,
                  minWidth: MIN_SEGMENT_WIDTH_PX,
                  background: 'var(--pause-band)',
                }}
              />
            ))}
          </span>
          <span className="w-16 shrink-0" />
        </div>
      );
    default:
      return assertNever(frame, 'timeline frame');
  }
}

function HatchedRow({ text, title, fill }: { text: string; title: string; fill: string }) {
  return (
    <span
      className="absolute inset-0 flex items-center rounded-sm px-1.5 text-9 uppercase tracking-wide text-muted-foreground"
      style={{ background: fill }}
      title={title}
    >
      {text}
    </span>
  );
}

/** One row's track: the segments and the endpoint marker, or a hatched
 * placeholder with its label. */
function RowTrack({
  node,
  frame,
  drawAtMs,
  onInspect,
}: {
  node: WorkflowNode;
  frame: TimelineFrame;
  drawAtMs: number;
  onInspect: (inspection: Inspection) => void;
}) {
  switch (frame.kind) {
    case 'unavailable':
      return (
        <HatchedRow
          text="Timing unavailable"
          title={unavailableReasonText(frame.reason)}
          fill="var(--timing-unavailable-hatch)"
        />
      );
    case 'composed':
      return (
        <NodeTrack
          taskIndex={node.task_index}
          timing={node.timing}
          axis={frame.axis}
          drawAtMs={drawAtMs}
          onInspect={onInspect}
        />
      );
    default:
      return assertNever(frame, 'timeline frame');
  }
}

function NodeTrack({
  taskIndex,
  timing,
  axis,
  drawAtMs,
  onInspect,
}: {
  taskIndex: number;
  timing: NodeTiming;
  axis: TimeAxis;
  drawAtMs: number;
  onInspect: (inspection: Inspection) => void;
}) {
  switch (timing.kind) {
    case 'unavailable':
      return (
        <HatchedRow
          text="Timing unavailable"
          title={unavailableReasonText(timing.reason)}
          fill="var(--timing-unavailable-hatch)"
        />
      );
    case 'inconsistent':
      return (
        <HatchedRow
          text="Timing inconsistent"
          title={conflictText(timing.conflict)}
          fill="var(--timing-inconsistent-hatch)"
        />
      );
    case 'exact':
    case 'partial': {
      const mark = endpointMark(timing.endpoint, axis);
      return (
        <>
          {timing.segments.map((segment, segmentIndex) => {
            const box = boxOf(segmentSpan(segment, drawAtMs), axis);
            return (
              <span
                key={segmentIndex}
                data-segment-index={segmentIndex}
                data-phase={segment.phase.kind}
                data-end={segment.end.kind}
                className="absolute inset-y-0"
                style={{
                  left: `${box.leftPct}%`,
                  width: `${box.widthPct}%`,
                  minWidth: MIN_SEGMENT_WIDTH_PX,
                  background: phaseFill(segment.phase.kind),
                }}
                onPointerMove={event =>
                  onInspect({
                    kind: 'segment',
                    taskIndex,
                    segmentIndex,
                    xPx: event.clientX,
                    yPx: event.clientY,
                  })
                }
                onPointerLeave={() => onInspect({ kind: 'none' })}
              />
            );
          })}
          <EndpointMarker mark={mark} />
        </>
      );
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The terminal status of a node: a marker at its time, not a duration. */
function EndpointMarker({ mark }: { mark: EndpointMark }) {
  switch (mark.kind) {
    case 'terminal':
      return (
        <span
          data-endpoint={mark.status}
          className="absolute top-1/2 size-2.5 -translate-x-1/2 -translate-y-1/2 rotate-45 border border-card"
          style={{ left: `${mark.pct}%`, background: statusColorVar(mark.status) }}
          title={`${statusLabel(mark.status)} at ${formatInstant(mark.atMs)}`}
        />
      );
    case 'not_terminal':
      return null;
    default:
      return assertNever(mark, 'endpoint mark');
  }
}

/** The shared tooltip: phase, source, start, end, duration of one segment. */
function SegmentTooltip({
  id,
  inspection,
  nodes,
  drawAtMs,
}: {
  id: string;
  inspection: Inspection;
  nodes: readonly WorkflowNode[];
  drawAtMs: number;
}) {
  if (inspection.kind === 'none') {
    return null;
  }
  const node = nodes.find(candidate => candidate.task_index === inspection.taskIndex);
  const segment =
    node === undefined ? undefined : segmentsOf(node.timing)[inspection.segmentIndex];
  if (segment === undefined) {
    // A refetch removed the segment under the cursor.
    return null;
  }
  const span = segmentSpan(segment, drawAtMs);
  const maxLeft = window.innerWidth - TOOLTIP_MAX_WIDTH_PX - TOOLTIP_OFFSET_PX;
  const left = Math.max(Math.min(inspection.xPx + TOOLTIP_OFFSET_PX, maxLeft), 0);
  const top = inspection.yPx + TOOLTIP_OFFSET_PX;
  return (
    <div
      id={id}
      role="tooltip"
      className="pointer-events-none fixed z-50 rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md"
      style={{ left, top, maxWidth: TOOLTIP_MAX_WIDTH_PX }}
    >
      <div className="flex items-center gap-1.5 font-medium">
        <span
          className="inline-block h-2 w-3 shrink-0 rounded-sm"
          style={{ background: phaseFill(segment.phase.kind) }}
          aria-hidden
        />
        {segmentPhaseText(segment.phase)}
      </div>
      <dl className="mt-1 grid grid-cols-[auto_1fr] gap-x-2 gap-y-0.5 text-muted-foreground">
        <dt>source</dt>
        <dd className="text-popover-foreground">{segmentSourceText(segment.source)}</dd>
        <dt>start</dt>
        <dd className="font-mono text-popover-foreground">{formatInstant(span.startMs)}</dd>
        <dt>end</dt>
        <dd className="font-mono text-popover-foreground">{segmentEndText(segment.end)}</dd>
        <dt>duration</dt>
        <dd className="font-mono text-popover-foreground">
          {segmentDurationText(segment.end, span.endMs - span.startMs)}
        </dd>
      </dl>
    </div>
  );
}
