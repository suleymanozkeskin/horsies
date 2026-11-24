import {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type PointerEvent,
  type RefObject,
} from 'react';

import { Hint } from '@/components/ui/hint';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { assertNever } from '@/lib/exhaustive';
import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import { statusColorVar, statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type {
  NodeTiming,
  PauseInterval,
  RunCoverage,
  TimingSegment,
  UnavailableReason,
} from '@/types/timing';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

import { TimingLegend, type LegendMark } from './legend';
import {
  FloatingTooltip,
  nodeEndText,
  NodeTooltip,
  type NodeFocus,
  type PauseFocus,
} from './node-tooltip';
import {
  PHASE_ORDER,
  phaseFill,
  phaseInk,
  phaseTrack,
  phaseWeight,
  type PhaseKey,
} from './phase';
import {
  activeSpan,
  criticalMembership,
  criticalPath,
  nodeTime,
  type CriticalMembership,
} from './run-timing';
import {
  axisTicks,
  boxOf,
  endpointMark,
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
  nodeTimeText,
  nodeTimingText,
  runCoverageText,
  unavailableReasonText,
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
/** A segment narrower than this is not drawn: at the scale of the run axis
 * it is less than a pixel. Its time is a number in the tooltip of its row. */
const MIN_DRAWN_SEGMENT_PX = 1;
/** A marked segment (a long engine step, a gap of the record) keeps this
 * width, so a long step on a long run axis stays visible. */
const MARKED_SEGMENT_MIN_PX = 3;
/** A pause band keeps one pixel on a long run axis. */
const MIN_PAUSE_BAND_PX = 1;
/** Estimated width of one character of a segment label (9px type), and the
 * padding around the label: a label shows only when it fits its segment. */
const LABEL_CHAR_WIDTH_PX = 5.6;
const LABEL_PADDING_PX = 6;

const nodeLabel = (node: WorkflowNode): string =>
  node.node_id ?? node.task_name ?? `#${node.task_index}`;

/** The width of the track column in pixels, once the layout measured it. */
export type TrackWidth = { kind: 'measured'; px: number } | { kind: 'unmeasured' };

/** The inline label of a segment: its duration, when the phase is drawn at
 * full height and the text fits the width of the segment. */
export type SegmentLabel = { kind: 'label'; text: string } | { kind: 'no_label' };

export function segmentLabel(
  key: PhaseKey,
  durationMs: number,
  widthPct: number,
  track: TrackWidth
): SegmentLabel {
  switch (track.kind) {
    case 'unmeasured':
      return { kind: 'no_label' };
    case 'measured':
      break;
    default:
      return assertNever(track, 'track width');
  }
  const drawing = phaseTrack(key);
  switch (drawing) {
    case 'thin':
      return { kind: 'no_label' };
    case 'full': {
      const text = formatMilliseconds(durationMs);
      const neededPx = text.length * LABEL_CHAR_WIDTH_PX + LABEL_PADDING_PX;
      return (widthPct / 100) * track.px >= neededPx ? { kind: 'label', text } : { kind: 'no_label' };
    }
    default:
      return assertNever(drawing, 'phase track');
  }
}

/** Whether a segment is drawn on the run axis, and its smallest width. */
export type SegmentDrawing = { kind: 'drawn'; minWidthPx: number } | { kind: 'not_drawn' };

/**
 * The drawing of a segment of `durationMs` that covers `widthPct` of the
 * track. The axis is linear: a segment is as wide as its time. A segment
 * below one pixel is not drawn; a marked one keeps a small width. Before the
 * track has a width, every segment is drawn at its own width.
 */
export function segmentDrawing(
  key: PhaseKey,
  durationMs: number,
  widthPct: number,
  track: TrackWidth
): SegmentDrawing {
  const weight = phaseWeight(key, durationMs);
  switch (weight.kind) {
    case 'marked':
      return { kind: 'drawn', minWidthPx: MARKED_SEGMENT_MIN_PX };
    case 'main':
    case 'plain':
    case 'quiet':
      break;
    default:
      return assertNever(weight, 'phase weight');
  }
  switch (track.kind) {
    case 'unmeasured':
      return { kind: 'drawn', minWidthPx: 0 };
    case 'measured':
      return (widthPct / 100) * track.px >= MIN_DRAWN_SEGMENT_PX
        ? { kind: 'drawn', minWidthPx: 0 }
        : { kind: 'not_drawn' };
    default:
      return assertNever(track, 'track width');
  }
}

/** The phases that the timeline draws at the width of its track, in
 * lifecycle order: the fills that its key explains. A phase whose every
 * segment is below one pixel is not in the key. */
export function drawnPhases(
  nodes: readonly WorkflowNode[],
  axis: TimeAxis,
  drawAtMs: number,
  track: TrackWidth
): PhaseKey[] {
  const drawn = new Set<PhaseKey>();
  for (const node of nodes) {
    for (const segment of segmentsOf(node.timing)) {
      const key = segment.phase.kind;
      if (drawn.has(key)) {
        continue;
      }
      const span = segmentSpan(segment, drawAtMs);
      const drawing = segmentDrawing(key, span.endMs - span.startMs, boxOf(span, axis).widthPct, track);
      if (drawing.kind === 'drawn') {
        drawn.add(key);
      }
    }
  }
  return PHASE_ORDER.filter(key => drawn.has(key));
}

/** What the rows are drawn against: the axis of a composed run, or the
 * reason that every node of the run is unavailable. */
/** One pause of the run with its box on the axis. */
interface PauseBand {
  pause: PauseInterval;
  box: AxisBox;
}

type TimelineFrame =
  | {
      kind: 'composed';
      axis: TimeAxis;
      ticks: AxisTick[];
      pauses: PauseBand[];
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
        pauses: timing.pauses.map(pause => ({ pause, box: boxOf(pauseSpan(pause, drawAtMs), axis) })),
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

/** The right-column text of a row: the time of the node without its
 * dependency wait. */
const timeText = (timing: NodeTiming, drawAtMs: number): string =>
  nodeTimeText(nodeTime(timing, drawAtMs));

/** The accessible name of a row: node, status, coverage, phase times, node
 * time and end. */
function rowAccessibleName(
  node: WorkflowNode,
  frame: TimelineFrame,
  drawAtMs: number,
  membership: CriticalMembership
): string {
  const head = `${nodeLabel(node)}, ${statusLabel(node.node_status)}${criticalSuffix(membership)}`;
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
        `${head}, ${nodeTimingText(timing, drawAtMs)}, ` +
        `node time ${timeText(timing, drawAtMs)}, ${nodeEndText(timing.endpoint)}`
      );
    case 'unavailable':
    case 'inconsistent':
      return `${head}, ${nodeTimingText(timing, drawAtMs)}`;
    default:
      return assertNever(timing, 'node timing');
  }
}

function criticalSuffix(membership: CriticalMembership): string {
  switch (membership) {
    case 'on_path':
      return ', on the critical path';
    case 'off_path':
      return '';
    default:
      return assertNever(membership, 'critical membership');
  }
}

/** What the pointer or the keyboard cursor is on, where the shared tooltip
 * shows it: a mark of a row (a segment, the end marker, the critical path
 * line, the track) and the pause of the run at that time. Keyed by task
 * index, which a refetch keeps. */
type Inspection =
  | { kind: 'none' }
  | {
      kind: 'row';
      taskIndex: number;
      focus: NodeFocus;
      pause: PauseFocus;
      xPx: number;
      yPx: number;
    };

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
 * Phase timeline of one run: one row per node in task-index order on one
 * linear run axis. The axis shows when each node ran: order, overlap and the
 * waits that are long at the scale of the run. Each row stacks the phase
 * segments of the node in time order (a retry repeats its segments), marks
 * the terminal status as an endpoint, and shows the time of the node without
 * its dependency wait on the right. A segment below one pixel is not drawn:
 * the tooltip of the row gives each phase as a number. Pauses are bands over
 * every row. A node whose timing is unavailable or inconsistent shows a
 * hatched row with its label. Open segments grow on a one-second local clock.
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
  const path = useMemo(
    () => criticalPath(detail.nodes, detail.edges, drawAtMs),
    [detail.nodes, detail.edges, drawAtMs]
  );
  const tooltipId = useId();
  const [inspection, setInspection] = useState<Inspection>({ kind: 'none' });
  const hide = useCallback(() => setInspection({ kind: 'none' }), []);
  const { trackRef, trackWidth } = useTrackWidth();

  return (
    <div className="flex h-full flex-col" onPointerLeave={hide}>
      <RunTimingBanner frame={frame} />
      <TimelineKey
        nodes={nodes}
        frame={frame}
        drawAtMs={drawAtMs}
        trackWidth={trackWidth}
        hasCriticalPath={path.kind === 'path'}
      />
      <TimelineHeader frame={frame} trackRef={trackRef} />
      <TimelineRows
        nodes={nodes}
        frame={frame}
        drawAtMs={drawAtMs}
        trackWidth={trackWidth}
        membershipOf={taskIndex => criticalMembership(path, taskIndex)}
        selectedIndex={selectedIndex}
        onSelectNode={onSelectNode}
        statusFilter={statusFilter}
        inspection={inspection}
        onInspect={setInspection}
        tooltipId={tooltipId}
      />
      <RowTooltip
        id={tooltipId}
        workflowId={detail.run.id}
        inspection={inspection}
        nodes={nodes}
        drawAtMs={drawAtMs}
        membershipOf={taskIndex => criticalMembership(path, taskIndex)}
      />
    </div>
  );
}

/** The key to the drawing of the timeline: the fill of each phase that it
 * draws at this scale, then its marks. */
function TimelineKey({
  nodes,
  frame,
  drawAtMs,
  trackWidth,
  hasCriticalPath,
}: {
  nodes: readonly WorkflowNode[];
  frame: TimelineFrame;
  drawAtMs: number;
  trackWidth: TrackWidth;
  hasCriticalPath: boolean;
}) {
  const axis = frame.kind === 'composed' ? frame.axis : null;
  const startMs = axis === null ? 0 : axis.startMs;
  const spanMs = axis === null ? 0 : axis.spanMs;
  const phases = useMemo(
    () => (spanMs > 0 ? drawnPhases(nodes, { startMs, spanMs }, drawAtMs, trackWidth) : []),
    [nodes, startMs, spanMs, drawAtMs, trackWidth]
  );
  const hasEnd = useMemo(
    () =>
      nodes.some(
        node =>
          (node.timing.kind === 'exact' || node.timing.kind === 'partial') &&
          node.timing.endpoint.kind === 'terminal'
      ),
    [nodes]
  );
  switch (frame.kind) {
    case 'unavailable':
      return null;
    case 'composed': {
      const marks: LegendMark[] = [
        ...(frame.pauses.length > 0 ? (['paused'] as const) : []),
        ...(hasEnd ? (['end'] as const) : []),
        ...(hasCriticalPath ? (['critical_row'] as const) : []),
      ];
      return (
        <div className="border-b border-border px-3 py-1.5">
          <TimingLegend phases={phases} marks={marks} />
        </div>
      );
    }
    default:
      return assertNever(frame, 'timeline frame');
  }
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

/** The measured width of the track column, from the header track. */
function useTrackWidth(): { trackRef: RefObject<HTMLSpanElement | null>; trackWidth: TrackWidth } {
  const trackRef = useRef<HTMLSpanElement>(null);
  const [trackWidth, setTrackWidth] = useState<TrackWidth>({ kind: 'unmeasured' });
  useLayoutEffect(() => {
    const element = trackRef.current;
    if (element === null) {
      return undefined;
    }
    const measure = (): void => {
      const px = element.getBoundingClientRect().width;
      setTrackWidth(previous =>
        px <= 0
          ? { kind: 'unmeasured' }
          : previous.kind === 'measured' && previous.px === px
            ? previous
            : { kind: 'measured', px }
      );
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return { trackRef, trackWidth };
}

function TimelineHeader({
  frame,
  trackRef,
}: {
  frame: TimelineFrame;
  trackRef: RefObject<HTMLSpanElement | null>;
}) {
  return (
    <div className="flex items-center gap-2 border-b border-border px-3 py-1.5 text-xs text-muted-foreground">
      <span className="w-40 shrink-0">node</span>
      <span ref={trackRef} className="relative h-4 min-w-0 flex-1" aria-hidden>
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
      <Hint
        text="The time of each node without its dependency wait: from the time it could start to its end."
        className="w-16 shrink-0 whitespace-nowrap text-right"
      >
        node time
      </Hint>
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

/** The segment that the inspection is on in the row of `taskIndex`. */
type SegmentCursor = { kind: 'on_segment'; index: number } | { kind: 'off_segments' };

function segmentCursor(inspection: Inspection, taskIndex: number): SegmentCursor {
  switch (inspection.kind) {
    case 'none':
      return { kind: 'off_segments' };
    case 'row':
      return inspection.taskIndex === taskIndex && inspection.focus.kind === 'segment'
        ? { kind: 'on_segment', index: inspection.focus.segmentIndex }
        : { kind: 'off_segments' };
    default:
      return assertNever(inspection, 'inspection');
  }
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
  const cursor = segmentCursor(inspection, node.task_index);
  const current = cursor.kind === 'on_segment' ? cursor.index : step > 0 ? -1 : count;
  const segmentIndex = Math.min(Math.max(current + step, 0), count - 1);
  const box = row
    .querySelector(`[data-segment-index="${segmentIndex}"]`)
    ?.getBoundingClientRect();
  return {
    kind: 'row',
    taskIndex: node.task_index,
    focus: { kind: 'segment', segmentIndex },
    pause: { kind: 'not_in_pause' },
    xPx: box === undefined ? 0 : box.left + box.width / 2,
    yPx: box === undefined ? 0 : box.bottom,
  };
}

/** The mark of a row under the pointer, from the element the pointer is on:
 * the end marker, the critical path line, a segment, else the track. */
function focusUnder(target: EventTarget): NodeFocus {
  if (!(target instanceof Element)) {
    return { kind: 'node' };
  }
  if (target.closest('[data-endpoint]') !== null) {
    return { kind: 'endpoint' };
  }
  if (target.closest('[data-critical-line]') !== null) {
    return { kind: 'critical_line' };
  }
  const segment = target.closest('[data-segment-index]');
  const index = segment === null ? Number.NaN : Number(segment.getAttribute('data-segment-index'));
  return Number.isInteger(index) ? { kind: 'segment', segmentIndex: index } : { kind: 'node' };
}

/** The pause of the run at the pointer: the pause whose band holds the
 * position `pct` of the track. */
function pauseAt(frame: TimelineFrame, pct: number): PauseFocus {
  switch (frame.kind) {
    case 'unavailable':
      return { kind: 'not_in_pause' };
    case 'composed': {
      const band = frame.pauses.find(
        candidate => pct >= candidate.box.leftPct && pct <= candidate.box.leftPct + candidate.box.widthPct
      );
      return band === undefined ? { kind: 'not_in_pause' } : { kind: 'in_pause', pause: band.pause };
    }
    default:
      return assertNever(frame, 'timeline frame');
  }
}

/** The inspection of a pointer on the track of a row. */
function pointerInspection(
  event: PointerEvent<HTMLElement>,
  taskIndex: number,
  frame: TimelineFrame
): Inspection {
  const track = event.currentTarget.getBoundingClientRect();
  const pct = track.width > 0 ? ((event.clientX - track.left) / track.width) * 100 : -1;
  return {
    kind: 'row',
    taskIndex,
    focus: focusUnder(event.target),
    pause: pauseAt(frame, pct),
    xPx: event.clientX,
    yPx: event.clientY,
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
  trackWidth: TrackWidth;
  membershipOf: (taskIndex: number) => CriticalMembership;
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
              trackWidth={props.trackWidth}
              membership={props.membershipOf(node.task_index)}
              selected={node.task_index === props.selectedIndex}
              dimmed={statusFilter.size > 0 && !statusFilter.has(node.node_status)}
              described={inspection.kind === 'row' && inspection.taskIndex === node.task_index}
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
  trackWidth: TrackWidth;
  membership: CriticalMembership;
  selected: boolean;
  dimmed: boolean;
  /** The shared tooltip shows this row. */
  described: boolean;
  tooltipId: string;
  onSelect: () => void;
  onKeyDown: (event: KeyboardEvent<HTMLButtonElement>) => void;
  onInspect: (inspection: Inspection) => void;
}

/** One node: label, track, node time. Its accessible name holds its timing.
 * The pointer on the track shows the tooltip of the row. */
function TimelineRow({
  node,
  index,
  rowCount,
  frame,
  drawAtMs,
  trackWidth,
  membership,
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
        aria-label={rowAccessibleName(node, frame, drawAtMs, membership)}
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
        <span
          data-row-track
          className="relative h-4 min-w-0 flex-1"
          onPointerMove={event => onInspect(pointerInspection(event, node.task_index, frame))}
          onPointerLeave={() => onInspect({ kind: 'none' })}
        >
          <RowTrack
            node={node}
            frame={frame}
            drawAtMs={drawAtMs}
            trackWidth={trackWidth}
            membership={membership}
          />
        </span>
        <span className="w-16 shrink-0 text-right font-mono text-10 text-muted-foreground">
          {frame.kind === 'composed' ? timeText(node.timing, drawAtMs) : '—'}
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
                  left: `${band.box.leftPct}%`,
                  width: `${band.box.widthPct}%`,
                  minWidth: MIN_PAUSE_BAND_PX,
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

/** A row with no segments: the label says why, and the tooltip of the row
 * gives the reason. */
function HatchedRow({ text, fill }: { text: string; fill: string }) {
  return (
    <span
      data-hatched-row
      className="absolute inset-0 flex items-center rounded-sm px-1.5 text-10 text-muted-foreground"
      style={{ background: fill }}
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
  trackWidth,
  membership,
}: {
  node: WorkflowNode;
  frame: TimelineFrame;
  drawAtMs: number;
  trackWidth: TrackWidth;
  membership: CriticalMembership;
}) {
  switch (frame.kind) {
    case 'unavailable':
      return <HatchedRow text="Timing unavailable" fill="var(--timing-unavailable-hatch)" />;
    case 'composed':
      return (
        <NodeTrack
          timing={node.timing}
          axis={frame.axis}
          drawAtMs={drawAtMs}
          trackWidth={trackWidth}
          membership={membership}
        />
      );
    default:
      return assertNever(frame, 'timeline frame');
  }
}

function NodeTrack({
  timing,
  axis,
  drawAtMs,
  trackWidth,
  membership,
}: {
  timing: NodeTiming;
  axis: TimeAxis;
  drawAtMs: number;
  trackWidth: TrackWidth;
  membership: CriticalMembership;
}) {
  switch (timing.kind) {
    case 'unavailable':
      return <HatchedRow text="Timing unavailable" fill="var(--timing-unavailable-hatch)" />;
    case 'inconsistent':
      return <HatchedRow text="Timing inconsistent" fill="var(--timing-inconsistent-hatch)" />;
    case 'exact':
    case 'partial': {
      const mark = endpointMark(timing.endpoint, axis);
      return (
        <>
          {timing.segments.map((segment, segmentIndex) => (
            <SegmentBox
              key={segmentIndex}
              segment={segment}
              segmentIndex={segmentIndex}
              axis={axis}
              drawAtMs={drawAtMs}
              trackWidth={trackWidth}
            />
          ))}
          <CriticalLine timing={timing} axis={axis} drawAtMs={drawAtMs} membership={membership} />
          <EndpointMarker mark={mark} />
        </>
      );
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

/** One segment at its time on the axis. The box is always in the row, so the
 * keyboard cursor finds its place; its fill is drawn only when the segment
 * is wide enough to see. */
function SegmentBox({
  segment,
  segmentIndex,
  axis,
  drawAtMs,
  trackWidth,
}: {
  segment: TimingSegment;
  segmentIndex: number;
  axis: TimeAxis;
  drawAtMs: number;
  trackWidth: TrackWidth;
}) {
  const span = segmentSpan(segment, drawAtMs);
  const box = boxOf(span, axis);
  const durationMs = span.endMs - span.startMs;
  const phase = segment.phase.kind;
  const drawing = segmentDrawing(phase, durationMs, box.widthPct, trackWidth);
  switch (drawing.kind) {
    case 'not_drawn':
      return (
        <span
          data-segment-index={segmentIndex}
          data-phase={phase}
          data-end={segment.end.kind}
          data-drawn="false"
          className="pointer-events-none absolute inset-y-0"
          style={{ left: `${box.leftPct}%`, width: `${box.widthPct}%` }}
        />
      );
    case 'drawn':
      return (
        <span
          data-segment-index={segmentIndex}
          data-phase={phase}
          data-end={segment.end.kind}
          data-drawn="true"
          className="absolute inset-y-0"
          style={{
            left: `${box.leftPct}%`,
            width: `${box.widthPct}%`,
            minWidth: drawing.minWidthPx,
          }}
        >
          <SegmentFill
            phase={phase}
            label={segmentLabel(phase, durationMs, box.widthPct, trackWidth)}
          />
        </span>
      );
    default:
      return assertNever(drawing, 'segment drawing');
  }
}

/** The fill of one segment: dependency wait as a thin track, every other
 * phase at full height, with its duration when the label fits. */
function SegmentFill({ phase, label }: { phase: PhaseKey; label: SegmentLabel }) {
  const drawing = phaseTrack(phase);
  switch (drawing) {
    case 'thin':
      return (
        <span
          className="absolute inset-x-0 top-1/2 h-1 -translate-y-1/2 rounded-full"
          style={{ background: phaseFill(phase) }}
        />
      );
    case 'full':
      return (
        <span
          className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-xs"
          style={{ background: phaseFill(phase) }}
        >
          <SegmentLabelText phase={phase} label={label} />
        </span>
      );
    default:
      return assertNever(drawing, 'phase track');
  }
}

function SegmentLabelText({ phase, label }: { phase: PhaseKey; label: SegmentLabel }) {
  switch (label.kind) {
    case 'no_label':
      return null;
    case 'label':
      return (
        <span
          data-segment-label
          className="whitespace-nowrap px-0.5 text-9 font-semibold leading-none tabular-nums"
          style={{ color: phaseInk(phase) }}
          aria-hidden
        >
          {label.text}
        </span>
      );
    default:
      return assertNever(label, 'segment label');
  }
}

/** The line under the active span of a node on the critical path: from the
 * end of its dependency wait to its end. The lines of the path form a chain
 * of steps across the rows. The line has a taller box for the pointer, and
 * the tooltip of the row says what it is. */
function CriticalLine({
  timing,
  axis,
  drawAtMs,
  membership,
}: {
  timing: NodeTiming;
  axis: TimeAxis;
  drawAtMs: number;
  membership: CriticalMembership;
}) {
  switch (membership) {
    case 'off_path':
      return null;
    case 'on_path':
      break;
    default:
      return assertNever(membership, 'critical membership');
  }
  const span = activeSpan(timing, drawAtMs);
  switch (span.kind) {
    case 'no_active_time':
      return null;
    case 'span': {
      const box = boxOf(span, axis);
      return (
        <span
          data-critical-line
          className="absolute -bottom-1.5 flex h-2 items-center"
          style={{ left: `${box.leftPct}%`, width: `${box.widthPct}%`, minWidth: MIN_DRAWN_SEGMENT_PX }}
        >
          <span
            className="h-0.5 w-full rounded-full"
            style={{ background: 'var(--critical-path)' }}
          />
        </span>
      );
    }
    default:
      return assertNever(span, 'active span');
  }
}

/** The terminal status of a node: a marker at its time, not a duration. The
 * tooltip of the row names the status and the time. */
function EndpointMarker({ mark }: { mark: EndpointMark }) {
  switch (mark.kind) {
    case 'terminal':
      return (
        <span
          data-endpoint={mark.status}
          className="absolute top-1/2 size-2.5 -translate-x-1/2 -translate-y-1/2 rotate-45 border border-card"
          style={{ left: `${mark.pct}%`, background: statusColorVar(mark.status) }}
        />
      );
    case 'not_terminal':
      return null;
    default:
      return assertNever(mark, 'endpoint mark');
  }
}

/** The shared tooltip of the row under the pointer or the keyboard cursor:
 * the node, the mark under the cursor, and each phase of the node as a
 * number. */
function RowTooltip({
  id,
  workflowId,
  inspection,
  nodes,
  drawAtMs,
  membershipOf,
}: {
  id: string;
  workflowId: string;
  inspection: Inspection;
  nodes: readonly WorkflowNode[];
  drawAtMs: number;
  membershipOf: (taskIndex: number) => CriticalMembership;
}) {
  switch (inspection.kind) {
    case 'none':
      return null;
    case 'row': {
      const node = nodes.find(candidate => candidate.task_index === inspection.taskIndex);
      // A refetch can remove the node under the cursor.
      return node === undefined ? null : (
        <FloatingTooltip id={id} xPx={inspection.xPx} yPx={inspection.yPx}>
          <NodeTooltip
            workflowId={workflowId}
            node={node}
            drawAtMs={drawAtMs}
            membership={membershipOf(node.task_index)}
            focus={inspection.focus}
            pause={inspection.pause}
            facts={[]}
          />
        </FloatingTooltip>
      );
    }
    default:
      return assertNever(inspection, 'inspection');
  }
}
