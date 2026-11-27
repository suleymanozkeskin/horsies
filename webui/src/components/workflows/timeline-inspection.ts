// What the pointer or the keyboard cursor is on in the run table, where the
// shared tooltip shows it: a mark of a node row, a mark of the run row, or a
// link of the longest chain. Keyed by task index, which a refetch keeps.

import type { PointerEvent } from 'react';

import { assertNever } from '@/lib/exhaustive';
import type { NodeTiming, TimingSegment } from '@/types/timing';
import type { WorkflowNode } from '@/types/workflows';

import type { NodeFocus, PauseFocus } from './node-tooltip';
import { TIMING_KIND_ORDER, type TimingKind } from './phase';
import type { TimelineFrame } from './timeline-frame';

/** What of the run row the cursor is on: the row as a whole, a segment of a
 * node of the longest chain, or a bar of its log cell. */
export type RunFocus =
  | { kind: 'run' }
  | { kind: 'segment'; taskIndex: number; segmentIndex: number }
  | { kind: 'lane'; phase: TimingKind };

export type Inspection =
  | { kind: 'none' }
  | { kind: 'row'; taskIndex: number; focus: NodeFocus; pause: PauseFocus; xPx: number; yPx: number }
  | { kind: 'run'; focus: RunFocus; pause: PauseFocus; xPx: number; yPx: number }
  | { kind: 'link'; fromIndex: number; toIndex: number; xPx: number; yPx: number };

/** The kind that a `data-lane` attribute names, read at the DOM boundary. */
function laneKindOf(element: Element | null): TimingKind | undefined {
  const text = element?.getAttribute('data-lane') ?? null;
  return TIMING_KIND_ORDER.find(kind => kind === text);
}

const indexOf = (element: Element | null, attribute: string): number =>
  element === null ? Number.NaN : Number(element.getAttribute(attribute));

/** The mark of a node row under the pointer: the end marker, a bar of the
 * log cell, a segment, else the node as a whole. */
export function nodeFocusUnder(target: EventTarget): NodeFocus {
  if (!(target instanceof Element)) {
    return { kind: 'node' };
  }
  if (target.closest('[data-endpoint]') !== null) {
    return { kind: 'endpoint' };
  }
  const lane = laneKindOf(target.closest('[data-lane]'));
  if (lane !== undefined) {
    return { kind: 'lane', phase: lane };
  }
  const segmentIndex = indexOf(target.closest('[data-segment-index]'), 'data-segment-index');
  return Number.isInteger(segmentIndex) ? { kind: 'segment', segmentIndex } : { kind: 'node' };
}

/** The mark of the run row under the pointer. */
export function runFocusUnder(target: EventTarget): RunFocus {
  if (!(target instanceof Element)) {
    return { kind: 'run' };
  }
  const lane = laneKindOf(target.closest('[data-lane]'));
  if (lane !== undefined) {
    return { kind: 'lane', phase: lane };
  }
  const segment = target.closest('[data-run-segment]');
  const taskIndex = indexOf(segment, 'data-task-index');
  const segmentIndex = indexOf(segment, 'data-run-segment');
  return Number.isInteger(taskIndex) && Number.isInteger(segmentIndex)
    ? { kind: 'segment', taskIndex, segmentIndex }
    : { kind: 'run' };
}

/** The pause of the run at the pointer: the pause whose band holds the
 * position `pct` of the linear cell. */
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

/** The pause at the pointer when the pointer is on a linear cell. */
function pauseUnder(event: PointerEvent<HTMLElement>, frame: TimelineFrame): PauseFocus {
  const track = event.target instanceof Element ? event.target.closest('[data-row-track]') : null;
  if (track === null) {
    return { kind: 'not_in_pause' };
  }
  const box = track.getBoundingClientRect();
  return box.width > 0 ? pauseAt(frame, ((event.clientX - box.left) / box.width) * 100) : { kind: 'not_in_pause' };
}

/** The inspection of a pointer on a node row. */
export function rowInspection(event: PointerEvent<HTMLElement>, taskIndex: number, frame: TimelineFrame): Inspection {
  return {
    kind: 'row',
    taskIndex,
    focus: nodeFocusUnder(event.target),
    pause: pauseUnder(event, frame),
    xPx: event.clientX,
    yPx: event.clientY,
  };
}

/** The inspection of a pointer on the run row. */
export function runInspection(event: PointerEvent<HTMLElement>, frame: TimelineFrame): Inspection {
  return {
    kind: 'run',
    focus: runFocusUnder(event.target),
    pause: pauseUnder(event, frame),
    xPx: event.clientX,
    yPx: event.clientY,
  };
}

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

/** The segment that the inspection is on in the row of `taskIndex`. */
type SegmentCursor = { kind: 'on_segment'; index: number } | { kind: 'off_segments' };

function segmentCursor(inspection: Inspection, taskIndex: number): SegmentCursor {
  switch (inspection.kind) {
    case 'none':
    case 'run':
    case 'link':
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
export function keyboardInspection(
  inspection: Inspection,
  node: WorkflowNode,
  row: HTMLElement,
  step: 1 | -1
): Inspection {
  const count = segmentsOf(node.timing).length;
  if (count === 0) {
    return inspection;
  }
  const cursor = segmentCursor(inspection, node.node_index);
  const current = cursor.kind === 'on_segment' ? cursor.index : step > 0 ? -1 : count;
  const segmentIndex = Math.min(Math.max(current + step, 0), count - 1);
  const box = row.querySelector(`[data-segment-index="${segmentIndex}"]`)?.getBoundingClientRect();
  return {
    kind: 'row',
    taskIndex: node.node_index,
    focus: { kind: 'segment', segmentIndex },
    pause: { kind: 'not_in_pause' },
    xPx: box === undefined ? 0 : box.left + box.width / 2,
    yPx: box === undefined ? 0 : box.bottom,
  };
}

/** The node rows that the inspection lights: the node of a segment of the
 * run row, or the two nodes of a link of the longest chain. */
export function litRows(inspection: Inspection): ReadonlySet<number> {
  switch (inspection.kind) {
    case 'none':
    case 'row':
      return new Set();
    case 'run':
      return inspection.focus.kind === 'segment' ? new Set([inspection.focus.taskIndex]) : new Set();
    case 'link':
      return new Set([inspection.fromIndex, inspection.toIndex]);
    default:
      return assertNever(inspection, 'inspection');
  }
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
