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
import { formatInstant } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

import { ShownTipBox, TipHead, TipNote, useMarkTips, type MarkTips } from './mark-tip';
import { nodeEndText, nodeLabel } from './node-tooltip';
import { TIMING_KIND_ORDER, type TimingKind } from './phase';
import {
  chainMembership,
  longestChain,
  nodeTime,
  timedSegments,
  type ChainMembership,
  type LongestChain,
} from './run-timing';
import { chainNodesText, shownLight, useReportLight } from './run-summary';
import { ChainLinks, chainLinksOf } from './timeline-chain';
import { timelineFrame, type TimelineFrame } from './timeline-frame';
import { phaseComposition, type PhaseTotal } from './timeline-geometry';
import {
  keyboardInspection,
  litRows,
  rowInspection,
  rowKeyAction,
  runInspection,
  type Inspection,
} from './timeline-inspection';
import {
  HEADER_PX,
  LANE_LEFT_PX,
  LANE_RIGHT_PX,
  LANES_RIGHT_PX,
  LANES_WIDTH_PX,
  LINEAR_GAP_PX,
  LINEAR_LEFT_PX,
  LINEAR_RIGHT_PX,
  TABLE_COLUMNS,
  TABLE_MIN_WIDTH_PX,
  tableRows,
  type TableRows,
} from './timeline-layout';
import { NodeRow, RunRow, type ChainSegment, type TrackWidth } from './timeline-row';
import { RunTableTooltip } from './timeline-tooltip';
import { rowWindow, scrollTopForRow, type ListViewport, type RowWindow } from './timeline-window';
import { logDomain, type LogDomain } from './time-scale';
import { LinearAxisLabels, LinearGridLines, LogAxisLabels, LogLines } from './timing-axes';
import { keyEntryId, TimingKey, type KeyEntry, type KeyLight, type KeyMark, type KeyPhase } from './timing-key';
import { durationText, nodeTimeText, nodeTimingText, runCoverageText, unavailableReasonText } from './timing-text';

interface WorkflowTimelineProps {
  workflowId: string;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  /** Statuses to keep visible; rows of other statuses are faded (empty = all). */
  statusFilter?: ReadonlySet<string>;
  /** The kind that a key entry lights, in this view and in the run summary. */
  isolation: KeyLight;
  onIsolate: (light: KeyLight) => void;
}

/** A tooltip for the keyboard focus on the run row opens under the row, at
 * most this far right of its left edge. */
const FOCUS_ANCHOR_MAX_PX = 120;

/** Stable empty default so an absent statusFilter prop keeps a constant reference. */
const NO_STATUS_FILTER: ReadonlySet<string> = new Set();

/**
 * Phase timeline of one run on two scales: one row per node in task-index
 * order, after the run row. The linear cell shows when each node ran: order,
 * overlap, and the waits that are long at the scale of the run; the log cell
 * shows the size of each kind of its time, one thin bar for each. Pauses are
 * bands over every row, and the longest chain links its nodes. A node whose
 * timing is unavailable or inconsistent shows a hatched row with its label.
 * Open segments grow on a one-second local clock.
 */
export function WorkflowTimeline({
  workflowId,
  selectedIndex,
  onSelectNode,
  statusFilter = NO_STATUS_FILTER,
  isolation,
  onIsolate,
}: WorkflowTimelineProps) {
  const { detail, readClock, isError } = useWorkflowRun(workflowId);

  if (isError) {
    return (
      <div className="flex h-full items-center justify-center text-sm" style={{ color: 'var(--error)' }} role="alert">
        Could not load this workflow run.
      </div>
    );
  }

  if (detail === undefined || readClock === undefined) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">Loading workflow…</div>
    );
  }

  if (detail.nodes.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">This run has no nodes.</div>
    );
  }

  return (
    <TimelineView
      detail={detail}
      readClock={readClock}
      selectedIndex={selectedIndex}
      onSelectNode={onSelectNode}
      statusFilter={statusFilter}
      isolation={isolation}
      onIsolate={onIsolate}
    />
  );
}

export interface TimelineViewProps {
  detail: WorkflowRunDetail;
  readClock: ReadClock;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  statusFilter: ReadonlySet<string>;
  isolation: KeyLight;
  onIsolate: (light: KeyLight) => void;
}

/** The totals of the kinds of a node without its dependency wait: the bars
 * of its log cell. */
const ownTotals = (node: WorkflowNode, drawAtMs: number): PhaseTotal[] =>
  phaseComposition(timedSegments(node.timing), drawAtMs).filter(total => total.key !== 'pending');

/** Everything the table draws for one draw time. */
interface TableModel {
  nodes: WorkflowNode[];
  frame: TimelineFrame;
  chain: LongestChain;
  rows: TableRows;
  domain: LogDomain;
}

/** The longest total of a kind in the log cells: of a node, or of the
 * longest chain. A loop, not a spread: a run holds up to 10,000 nodes. */
function longestTotalMs(nodes: readonly WorkflowNode[], chain: LongestChain, drawAtMs: number): number {
  let longestMs = 0;
  for (const node of nodes) {
    for (const total of ownTotals(node, drawAtMs)) {
      longestMs = Math.max(longestMs, total.ms);
    }
  }
  for (const total of chain.kind === 'chain' ? chain.phases : []) {
    longestMs = Math.max(longestMs, total.ms);
  }
  return longestMs;
}

function tableModel(detail: WorkflowRunDetail, drawAtMs: number): TableModel {
  const nodes = [...detail.nodes].sort((a, b) => a.node_index - b.node_index);
  const chain = longestChain(detail.nodes, detail.edges, drawAtMs);
  return {
    nodes,
    frame: timelineFrame(detail, drawAtMs),
    chain,
    rows: tableRows(nodes, drawAtMs),
    domain: logDomain(longestTotalMs(nodes, chain, drawAtMs)),
  };
}

/** The timeline of one loaded run. */
export function TimelineView({
  detail,
  readClock,
  selectedIndex,
  onSelectNode,
  statusFilter,
  isolation,
  onIsolate,
}: TimelineViewProps) {
  const drawAtMs = useDrawTime(readClock, readClock.open);
  const model = useMemo(() => tableModel(detail, drawAtMs), [detail, drawAtMs]);
  const tooltipId = useId();
  const [inspection, setInspection] = useState<Inspection>({ kind: 'none' });
  const hide = useCallback(() => setInspection({ kind: 'none' }), []);
  const { trackRef, trackWidth } = useTrackWidth();
  const tips = useMarkTips<KeyLight>();
  useReportLight(shownLight(tips), onIsolate);
  return (
    <div className="flex h-full flex-col" onPointerLeave={hide}>
      <RunTimingBanner frame={model.frame} />
      <TimelineKey detail={detail} model={model} drawAtMs={drawAtMs} isolation={isolation} tips={tips} />
      <TableBody
        detail={detail}
        model={model}
        drawAtMs={drawAtMs}
        trackRef={trackRef}
        trackWidth={trackWidth}
        selectedIndex={selectedIndex}
        onSelectNode={onSelectNode}
        statusFilter={statusFilter}
        isolation={isolation}
        inspection={inspection}
        onInspect={setInspection}
        tooltipId={tooltipId}
        tips={tips}
      />
      <RunTableTooltip
        id={tooltipId}
        detail={detail}
        inspection={inspection}
        nodes={model.nodes}
        chain={model.chain}
        frame={model.frame}
        drawAtMs={drawAtMs}
      />
      <ShownTipBox tips={tips} />
    </div>
  );
}

/** The key entries of the table that the run summary does not hold: the
 * kinds off the longest chain, dependency wait, and the marks. */
export function tableKeyPhases(model: TableModel, nodes: readonly WorkflowNode[], drawAtMs: number): KeyPhase[] {
  const onChain = new Set<TimingKind>(model.chain.kind === 'chain' ? model.chain.phases.map(total => total.key) : []);
  const waits = nodes.some(node =>
    phaseComposition(timedSegments(node.timing), drawAtMs).some(total => total.key === 'pending')
  );
  const kinds = TIMING_KIND_ORDER.filter(
    kind => (kind === 'pending' && waits) || (model.rows.laneKinds.includes(kind) && !onChain.has(kind))
  );
  return kinds.map(kind => ({ phase: kind, total: { kind: 'no_total' } }));
}

function tableKeyMarks(detail: WorkflowRunDetail, model: TableModel): KeyMark[] {
  const frame = model.frame;
  const ends = model.nodes.some(
    node => (node.timing.kind === 'exact' || node.timing.kind === 'partial') && node.timing.endpoint.kind === 'terminal'
  );
  return [
    ...(ends ? [{ kind: 'end' } as const] : []),
    ...(model.chain.kind === 'chain'
      ? [{ kind: 'chain', nodes: chainNodesText(detail, model.chain.taskIndices) } as const]
      : []),
    ...(frame.kind === 'composed' && frame.pauses.length > 0 ? [{ kind: 'paused' } as const] : []),
    { kind: 'pattern', pattern: 'wait' },
    { kind: 'pattern', pattern: 'work' },
  ];
}

/** What a key entry of the table lights: the marks of its kind. */
function keyEntryLight(entry: KeyEntry): KeyLight {
  switch (entry.kind) {
    case 'phase':
      return { kind: 'phase', phase: entry.entry.phase };
    case 'mark':
      return { kind: 'all' };
    default:
      return assertNever(entry, 'key entry');
  }
}

/** The key of the drawing of the table: the kinds off the longest chain
 * (the run summary holds the kinds of the chain with their totals),
 * dependency wait, the end marker, the links of the longest chain, the
 * pause band, and the two patterns. */
function TimelineKey({
  detail,
  model,
  drawAtMs,
  isolation,
  tips,
}: {
  detail: WorkflowRunDetail;
  model: TableModel;
  drawAtMs: number;
  isolation: KeyLight;
  tips: MarkTips<KeyLight>;
}) {
  const phases = useMemo(() => tableKeyPhases(model, model.nodes, drawAtMs), [model, drawAtMs]);
  const marks = useMemo(() => tableKeyMarks(detail, model), [detail, model]);
  switch (model.frame.kind) {
    case 'unavailable':
      return null;
    case 'composed':
      return (
        <div className="border-b border-hairline px-5 py-2">
          <TimingKey
            phases={phases}
            marks={marks}
            light={isolation}
            bind={(entry, content) => tips.bind({ id: keyEntryId(entry), content, highlight: keyEntryLight(entry) })}
          />
        </div>
      );
    default:
      return assertNever(model.frame, 'timeline frame');
  }
}

function RunTimingBanner({ frame }: { frame: TimelineFrame }) {
  switch (frame.kind) {
    case 'unavailable':
      return (
        <p className="border-b border-hairline px-5 py-1.5 text-xs text-muted-foreground">
          Timing unavailable. {unavailableReasonText(frame.reason)} The lifecycle record starts at{' '}
          {formatInstant(frame.capturedSinceMs)}.
        </p>
      );
    case 'composed': {
      const text = runCoverageText(frame.coverage);
      return text === null ? null : (
        <p
          className="border-b border-hairline px-5 py-1.5 text-xs"
          style={{ color: frame.coverage.kind === 'inconsistent' ? 'var(--error)' : 'var(--muted-foreground)' }}
        >
          {text}
        </p>
      );
    }
    default:
      return assertNever(frame, 'timeline frame');
  }
}

/** The measured width of the linear cell, from the axis of the header. */
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

const HEADER_TIPS = {
  linear: { name: 'time', context: 'linear', note: 'elapsed time from the start of the run' },
  log: {
    name: 'duration',
    context: 'log scale',
    note: 'one bar for each phase of the node; each labeled line is 10 times the line before it, up to 10s; then 1m, 10m, 1h',
  },
  time: { name: 'duration', context: 'without dependency wait', note: 'the time of a node after its dependency wait' },
} as const;

function HeaderTitle({
  tips,
  which,
  children,
}: {
  tips: MarkTips<KeyLight>;
  which: keyof typeof HEADER_TIPS;
  children: string;
}) {
  const text = HEADER_TIPS[which];
  return (
    <span
      tabIndex={0}
      className="cursor-default rounded-[3px] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
      {...tips.bind({
        id: `header:${which}`,
        content: (
          <>
            <TipHead name={text.name} context={text.context} />
            <TipNote text={text.note} />
          </>
        ),
        highlight: { kind: 'all' },
      })}
    >
      {children}
    </span>
  );
}

function TableHeader({
  model,
  trackRef,
  tips,
}: {
  model: TableModel;
  trackRef: RefObject<HTMLSpanElement | null>;
  tips: MarkTips<KeyLight>;
}) {
  const frame = model.frame;
  return (
    <div
      className="sticky top-0 z-20 grid items-stretch border-y border-hairline bg-background px-5 text-11 leading-none text-muted-foreground"
      style={{ gridTemplateColumns: TABLE_COLUMNS, height: HEADER_PX }}
    >
      <span className="pt-[9px]">node</span>
      <span className="flex min-w-0 flex-col">
        <span className="h-[22px] flex-none pt-[9px]">
          <HeaderTitle tips={tips} which="linear">
            timeline
          </HeaderTitle>
        </span>
        <span ref={trackRef} className="relative flex-1 font-mono text-10" style={{ marginRight: LINEAR_GAP_PX }} aria-hidden>
          {frame.kind === 'composed' && <LinearAxisLabels spanMs={frame.axis.spanMs} placement="over_rows" />}
        </span>
      </span>
      <span className="flex min-w-0 flex-col">
        <span className="h-[22px] flex-none pt-[9px]" style={{ paddingLeft: LANE_LEFT_PX }}>
          <HeaderTitle tips={tips} which="log">
            phases · log scale
          </HeaderTitle>
        </span>
        <span className="relative flex-1" style={{ marginLeft: LANE_LEFT_PX, marginRight: LANE_RIGHT_PX }}>
          <LogAxisLabels domain={model.domain} />
        </span>
      </span>
      <span className="pt-[9px] text-right">
        <HeaderTitle tips={tips} which="time">
          duration
        </HeaderTitle>
      </span>
    </div>
  );
}

/** The accessible name of a row: node, status, coverage, phase times, time
 * and end. */
function rowAccessibleName(node: WorkflowNode, frame: TimelineFrame, drawAtMs: number, membership: ChainMembership): string {
  const head = `${nodeLabel(node)}, ${statusLabel(node.node_status)}${chainSuffix(membership)}`;
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
        `duration ${nodeTimeText(nodeTime(timing, drawAtMs))}, ${nodeEndText(timing.endpoint)}`
      );
    case 'unavailable':
    case 'inconsistent':
      return `${head}, ${nodeTimingText(timing, drawAtMs)}`;
    default:
      return assertNever(timing, 'node timing');
  }
}

function chainSuffix(membership: ChainMembership): string {
  switch (membership) {
    case 'on_chain':
      return ', on the longest chain';
    case 'off_chain':
      return '';
    default:
      return assertNever(membership, 'chain membership');
  }
}

/** The segments of the run row: the segments of each node of the longest
 * chain, in chain order. */
function chainSegments(nodes: readonly WorkflowNode[], chain: LongestChain): ChainSegment[] {
  switch (chain.kind) {
    case 'no_timing':
      return [];
    case 'chain':
      return chain.taskIndices.flatMap(taskIndex => {
        const node = nodes.find(candidate => candidate.node_index === taskIndex);
        const segments = node === undefined ? [] : timedSegments(node.timing);
        return segments.map((segment, segmentIndex) => ({ taskIndex, segmentIndex, segment }));
      });
    default:
      return assertNever(chain, 'longest chain');
  }
}

const viewportOf = (element: HTMLElement, runPx: number): ListViewport =>
  element.clientHeight > 0
    ? { kind: 'measured', scrollTopPx: element.scrollTop - runPx, heightPx: element.clientHeight - HEADER_PX }
    : { kind: 'unmeasured' };

/** The pending keyboard move to a row: focus it once it renders. */
type PendingFocus = { kind: 'none' } | { kind: 'row'; index: number };

/** The windowing of the rows: the scroll container, the rows to render, and
 * a keyboard move to a row, which scrolls the row into view under the sticky
 * header and focuses it once it renders. */
function useRowWindow(rowCount: number, rowPx: number, runPx: number) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [rowRange, setRowRange] = useState<RowWindow>(() => rowWindow({ kind: 'unmeasured' }, rowCount, rowPx));
  const [pendingFocus, setPendingFocus] = useState<PendingFocus>({ kind: 'none' });

  const measure = useCallback(() => {
    const element = scrollRef.current;
    const next = rowWindow(element === null ? { kind: 'unmeasured' } : viewportOf(element, runPx), rowCount, rowPx);
    setRowRange(previous => (previous.start === next.start && previous.end === next.end ? previous : next));
  }, [rowCount, rowPx, runPx]);

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
    const button = scrollRef.current?.querySelector<HTMLButtonElement>(`[data-row-index="${pendingFocus.index}"]`);
    if (button) {
      button.focus();
      setPendingFocus({ kind: 'none' });
    }
  }, [pendingFocus, rowRange]);

  const focusRow = (index: number): void => {
    const target = Math.min(Math.max(index, 0), rowCount - 1);
    const element = scrollRef.current;
    if (element !== null && element.clientHeight > 0) {
      element.scrollTop =
        runPx + scrollTopForRow(target, element.scrollTop - runPx, element.clientHeight - HEADER_PX, rowPx);
    }
    measure();
    setPendingFocus({ kind: 'row', index: target });
  };

  return { scrollRef, rowRange, measure, focusRow };
}

interface TableBodyProps {
  detail: WorkflowRunDetail;
  model: TableModel;
  drawAtMs: number;
  trackRef: RefObject<HTMLSpanElement | null>;
  trackWidth: TrackWidth;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  statusFilter: ReadonlySet<string>;
  isolation: KeyLight;
  inspection: Inspection;
  onInspect: (inspection: Inspection) => void;
  tooltipId: string;
  tips: MarkTips<KeyLight>;
}

type CellContext = { drawAtMs: number; trackWidth: TrackWidth; light: KeyLight };

/** The node rows in view. Up and down arrows (and Home, End) move between
 * rows; left and right arrows step through the segments of a row in the
 * shared tooltip. */
function NodeRows({
  props,
  rowRange,
  focusRow,
  context,
  hintId,
}: {
  props: TableBodyProps;
  rowRange: RowWindow;
  focusRow: (index: number) => void;
  context: CellContext;
  hintId: string;
}) {
  const { model, drawAtMs, inspection, onInspect } = props;
  const { nodes, frame, rows } = model;
  const lit = litRows(inspection);
  const onRowKeyDown = (event: KeyboardEvent<HTMLButtonElement>, node: WorkflowNode, index: number): void => {
    const action = rowKeyAction(event.key, index, nodes.length);
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
    <ul aria-label="Nodes of the run" aria-describedby={hintId} className="relative m-0 list-none p-0" style={{ height: nodes.length * rows.rowPx }}>
      {nodes.slice(rowRange.start, rowRange.end).map((node, offset) => {
        const index = rowRange.start + offset;
        const membership = chainMembership(model.chain, node.node_index);
        return (
          <NodeRow
            key={node.node_index}
            node={node}
            index={index}
            rowCount={nodes.length}
            topPx={index * rows.rowPx}
            rowPx={rows.rowPx}
            frame={frame}
            context={context}
            totals={ownTotals(node, drawAtMs)}
            laneKinds={rows.laneKinds}
            domain={model.domain}
            timeText={frame.kind === 'composed' ? nodeTimeText(nodeTime(node.timing, drawAtMs)) : '—'}
            accessibleName={rowAccessibleName(node, frame, drawAtMs, membership)}
            membership={membership}
            selected={node.node_index === props.selectedIndex}
            dimmed={props.statusFilter.size > 0 && !props.statusFilter.has(node.node_status)}
            lit={lit.has(node.node_index)}
            described={inspection.kind === 'row' && inspection.taskIndex === node.node_index}
            tooltipId={props.tooltipId}
            onSelect={() => props.onSelectNode(node)}
            onKeyDown={event => onRowKeyDown(event, node, index)}
            onPointerMove={event => onInspect(rowInspection(event, node.node_index, frame))}
            onLeave={() => onInspect({ kind: 'none' })}
          />
        );
      })}
    </ul>
  );
}

/** The scroll container of the table: its sticky header, the run row, the
 * windowed node rows, and the lines, bands and links over them. */
function TableBody(props: TableBodyProps) {
  const { model, drawAtMs } = props;
  const { nodes, frame, rows } = model;
  const { scrollRef, rowRange, measure, focusRow } = useRowWindow(nodes.length, rows.rowPx, rows.runPx);
  const hintId = useId();
  const links = useMemo(() => chainLinksOf(model.chain, nodes, drawAtMs), [model.chain, nodes, drawAtMs]);
  const context: CellContext = { drawAtMs, trackWidth: props.trackWidth, light: props.isolation };
  return (
    <div ref={scrollRef} onScroll={measure} className="relative min-h-0 flex-1 overflow-auto">
      <div style={{ minWidth: TABLE_MIN_WIDTH_PX }}>
        <TableHeader model={model} trackRef={props.trackRef} tips={props.tips} />
        <div className="relative isolate">
          <p id={hintId} className="sr-only">
            Up and down arrows move between rows. Left and right arrows step through the phase segments of a row.
          </p>
          <RunRowOf {...props} context={context} />
          <NodeRows props={props} rowRange={rowRange} focusRow={focusRow} context={context} hintId={hintId} />
          <BodyOverlay model={model} />
          <ChainLinks
            links={links}
            frame={frame}
            rowPx={rows.rowPx}
            runPx={frame.kind === 'composed' ? rows.runPx : 0}
            onInspect={props.onInspect}
          />
        </div>
      </div>
    </div>
  );
}

/** The run row of a composed run; a run with no timing has none. */
function RunRowOf({
  detail,
  model,
  context,
  inspection,
  onInspect,
  tooltipId,
}: TableBodyProps & { context: CellContext }) {
  const frame = model.frame;
  switch (frame.kind) {
    case 'unavailable':
      return null;
    case 'composed': {
      const chain = model.chain;
      const time = durationText(frame.axis.spanMs, false);
      const chainText = chain.kind === 'chain' ? `, longest chain ${chainNodesText(detail, chain.taskIndices)}` : '';
      return (
        <RunRow
          runPx={model.rows.runPx}
          axis={frame.axis}
          segments={chainSegments(model.nodes, chain)}
          context={context}
          totals={chain.kind === 'chain' ? chain.phases : []}
          laneKinds={model.rows.laneKinds}
          domain={model.domain}
          timeText={time}
          accessibleName={`run, ${statusLabel(detail.run.status)}, duration ${time}${chainText}`}
          described={inspection.kind === 'run'}
          tooltipId={tooltipId}
          onPointerMove={event => onInspect(runInspection(event, frame))}
          onFocus={box =>
            onInspect({
              kind: 'run',
              focus: { kind: 'run' },
              pause: { kind: 'not_in_pause' },
              xPx: box.left + Math.min(box.width / 2, FOCUS_ANCHOR_MAX_PX),
              yPx: box.bottom,
            })
          }
          onLeave={() => onInspect({ kind: 'none' })}
        />
      );
    }
    default:
      return assertNever(frame, 'timeline frame');
  }
}

/** The lines of both axes and the pause bands, over the run row and the node
 * rows. */
function BodyOverlay({ model }: { model: TableModel }) {
  const frame = model.frame;
  switch (frame.kind) {
    case 'unavailable':
      return null;
    case 'composed':
      return (
        <div className="pointer-events-none absolute inset-0 z-[1]" aria-hidden>
          <div className="absolute inset-y-0" style={{ left: LINEAR_LEFT_PX, right: LINEAR_RIGHT_PX }}>
            <LinearGridLines spanMs={frame.axis.spanMs} />
            {frame.pauses.map((band, index) => (
              <span
                key={index}
                data-pause-band
                className="absolute inset-y-0 min-w-px"
                style={{ left: `${band.box.leftPct}%`, width: `${band.box.widthPct}%`, background: 'var(--pause-band)' }}
              />
            ))}
          </div>
          <div className="absolute inset-y-0" style={{ right: LANES_RIGHT_PX, width: LANES_WIDTH_PX }}>
            <LogLines domain={model.domain} tone="grid" />
          </div>
        </div>
      );
    default:
      return assertNever(frame, 'timeline frame');
  }
}

