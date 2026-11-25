// Pure geometry of the phase timeline: times to positions on one run axis.
// No React, no DOM. Every function takes the draw time (`drawAtMs`, see
// hooks/use-draw-time.ts), the time to which an open segment or pause runs.

import { assertNever } from '@/lib/exhaustive';
import { instantMs } from '@/lib/time-text';
import type {
  ComposedRunTiming,
  NodeEndpoint,
  NodeTiming,
  PauseInterval,
  TerminalNodeStatus,
  TimingSegment,
} from '@/types/timing';

import { segmentKind, TIMING_KIND_ORDER, type TimingKind } from './phase';

/** The smallest axis span: a run whose range is one instant still divides. */
const MIN_AXIS_SPAN_MS = 1;

/** The run axis: its start and its length, both in epoch milliseconds. */
export interface TimeAxis {
  startMs: number;
  spanMs: number;
}

/** A closed interval of time in epoch milliseconds. */
export interface TimeSpan {
  startMs: number;
  endMs: number;
}

/** A position on the axis, in percent of the axis length. */
export interface AxisBox {
  leftPct: number;
  widthPct: number;
}

const clampPct = (value: number): number => Math.min(Math.max(value, 0), 100);

/**
 * The axis of a composed run: from `range_started_at` to `range_ended_at`,
 * or to the draw time when an open segment or pause has grown past it.
 */
export function runAxis(timing: ComposedRunTiming, drawAtMs: number): TimeAxis {
  const startMs = instantMs(timing.range_started_at);
  const rangeEndMs = instantMs(timing.range_ended_at);
  const endMs = timing.open_segments ? Math.max(rangeEndMs, drawAtMs) : rangeEndMs;
  return { startMs, spanMs: Math.max(endMs - startMs, MIN_AXIS_SPAN_MS) };
}

/** The time span of a segment; an open segment ends at the draw time. */
export function segmentSpan(segment: TimingSegment, drawAtMs: number): TimeSpan {
  const startMs = instantMs(segment.started_at);
  switch (segment.end.kind) {
    case 'ended':
      return { startMs, endMs: instantMs(segment.end.at) };
    case 'open':
      return { startMs, endMs: Math.max(drawAtMs, startMs) };
    default:
      return assertNever(segment.end, 'segment end');
  }
}

/** The time span of a pause; an open pause ends at the draw time. */
export function pauseSpan(pause: PauseInterval, drawAtMs: number): TimeSpan {
  const startMs = instantMs(pause.started_at);
  switch (pause.end.kind) {
    case 'resumed':
    case 'run_ended':
      return { startMs, endMs: instantMs(pause.end.at) };
    case 'open':
      return { startMs, endMs: Math.max(drawAtMs, startMs) };
    default:
      return assertNever(pause.end, 'pause end');
  }
}

/**
 * The box of a span on the axis. The box is clipped to the axis; the span
 * keeps its times, so a tooltip still shows them. A zero-length span has
 * zero width: the component gives it a minimum visible width in pixels.
 */
export function boxOf(span: TimeSpan, axis: TimeAxis): AxisBox {
  const leftPct = clampPct(((span.startMs - axis.startMs) / axis.spanMs) * 100);
  const rightPct = clampPct(((span.endMs - axis.startMs) / axis.spanMs) * 100);
  return { leftPct, widthPct: Math.max(rightPct - leftPct, 0) };
}

/** The position of one instant on the axis, clipped to the axis. */
export const pointPct = (epochMs: number, axis: TimeAxis): number =>
  clampPct(((epochMs - axis.startMs) / axis.spanMs) * 100);

/** The position of the endpoint marker, or `not_terminal` for no marker. */
export type EndpointMark =
  | { kind: 'terminal'; status: TerminalNodeStatus; pct: number; atMs: number }
  | { kind: 'not_terminal' };

export function endpointMark(endpoint: NodeEndpoint, axis: TimeAxis): EndpointMark {
  switch (endpoint.kind) {
    case 'terminal': {
      const atMs = instantMs(endpoint.at);
      return { kind: 'terminal', status: endpoint.status, pct: pointPct(atMs, axis), atMs };
    }
    case 'not_terminal':
      return { kind: 'not_terminal' };
    default:
      return assertNever(endpoint, 'node endpoint');
  }
}

/** One interval of a phase, for a total: its kind, its time span, and
 * whether it has no end yet. */
export interface PhaseInterval {
  key: TimingKind;
  startMs: number;
  endMs: number;
  open: boolean;
}

/** The time of one kind over a set of intervals. */
export interface PhaseTotal {
  key: TimingKind;
  /** The time of the kind, summed over its intervals. */
  ms: number;
  /** The time of its longest interval. */
  longestMs: number;
  /** The number of its intervals. */
  count: number;
  /** True when an interval of the kind has no end yet. */
  open: boolean;
}

/**
 * The time of each kind in the intervals, in the order of the keys. An
 * interval whose times are in the wrong order counts as no time. A kind with
 * no time is left out; with no time at all the list is empty.
 */
export function phaseTotals(intervals: readonly PhaseInterval[]): PhaseTotal[] {
  const totals = new Map<TimingKind, PhaseTotal>();
  for (const interval of intervals) {
    const ms = Math.max(interval.endMs - interval.startMs, 0);
    const total = totals.get(interval.key) ?? {
      key: interval.key,
      ms: 0,
      longestMs: 0,
      count: 0,
      open: false,
    };
    totals.set(interval.key, {
      key: interval.key,
      ms: total.ms + ms,
      longestMs: Math.max(total.longestMs, ms),
      count: total.count + 1,
      open: total.open || interval.open,
    });
  }
  return TIMING_KIND_ORDER.flatMap(key => {
    const total = totals.get(key);
    return total !== undefined && total.ms > 0 ? [total] : [];
  });
}

/** One segment as an interval; an open segment ends at the draw time. */
export function segmentInterval(segment: TimingSegment, drawAtMs: number): PhaseInterval {
  const span = segmentSpan(segment, drawAtMs);
  return {
    key: segmentKind(segment.phase),
    startMs: span.startMs,
    endMs: span.endMs,
    open: segment.end.kind === 'open',
  };
}

/**
 * The time of each kind in the segments, summed over retries and
 * generations, in the order of the keys.
 */
export function phaseComposition(
  segments: readonly TimingSegment[],
  drawAtMs: number
): PhaseTotal[] {
  return phaseTotals(segments.map(segment => segmentInterval(segment, drawAtMs)));
}

/** True when a segment of the node has no end yet. */
export function nodeTimingIsOpen(timing: NodeTiming): boolean {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return timing.segments.some(segment => segment.end.kind === 'open');
    case 'unavailable':
    case 'inconsistent':
      return false;
    default:
      return assertNever(timing, 'node timing');
  }
}
