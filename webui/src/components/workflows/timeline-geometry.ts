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

import { PHASE_ORDER, type PhaseKey } from './phase';

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

/** One tick of the axis: its position and its offset from the axis start. */
export interface AxisTick {
  pct: number;
  offsetMs: number;
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

/** `count` evenly spaced ticks from the axis start to its end. */
export function axisTicks(axis: TimeAxis, count: number): AxisTick[] {
  const intervals = Math.max(count - 1, 1);
  return Array.from({ length: intervals + 1 }, (_, index) => ({
    pct: (index / intervals) * 100,
    offsetMs: (axis.spanMs * index) / intervals,
  }));
}

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

/** The wall time of a node: from its first segment start to the latest of
 * its segment ends and its endpoint. A task that runs on after a workflow
 * cancel ends after the endpoint, and the wall time includes it. */
export type NodeWallTime =
  | { kind: 'measured'; ms: number }
  | { kind: 'no_segments' };

export function nodeWallTime(
  segments: readonly TimingSegment[],
  endpoint: NodeEndpoint,
  drawAtMs: number
): NodeWallTime {
  if (segments.length === 0) {
    return { kind: 'no_segments' };
  }
  let firstMs = Number.POSITIVE_INFINITY;
  let lastMs = Number.NEGATIVE_INFINITY;
  for (const segment of segments) {
    const span = segmentSpan(segment, drawAtMs);
    firstMs = Math.min(firstMs, span.startMs);
    lastMs = Math.max(lastMs, span.endMs);
  }
  switch (endpoint.kind) {
    case 'terminal':
      lastMs = Math.max(lastMs, instantMs(endpoint.at));
      break;
    case 'not_terminal':
      break;
    default:
      return assertNever(endpoint, 'node endpoint');
  }
  return { kind: 'measured', ms: Math.max(lastMs - firstMs, 0) };
}

/** The time of one phase in a node, and its part of the node total. */
export interface PhaseShare {
  key: PhaseKey;
  ms: number;
  fraction: number;
}

/**
 * The time of each phase in the segments, summed over retries and
 * generations, in lifecycle order. A phase with no time is left out; with
 * no time at all the list is empty.
 */
export function phaseComposition(
  segments: readonly TimingSegment[],
  drawAtMs: number
): PhaseShare[] {
  const totals = new Map<PhaseKey, number>();
  let totalMs = 0;
  for (const segment of segments) {
    const span = segmentSpan(segment, drawAtMs);
    const ms = Math.max(span.endMs - span.startMs, 0);
    totals.set(segment.phase.kind, (totals.get(segment.phase.kind) ?? 0) + ms);
    totalMs += ms;
  }
  if (totalMs === 0) {
    return [];
  }
  return PHASE_ORDER.flatMap(key => {
    const ms = totals.get(key) ?? 0;
    return ms > 0 ? [{ key, ms, fraction: ms / totalMs }] : [];
  });
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
