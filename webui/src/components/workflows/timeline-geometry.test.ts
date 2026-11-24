import { describe, expect, it } from 'vitest';

import {
  axisTicks,
  boxOf,
  endpointMark,
  nodeTimingIsOpen,
  pauseSpan,
  phaseComposition,
  phaseTotals,
  runAxis,
  segmentInterval,
  segmentSpan,
  type TimeAxis,
} from '@/components/workflows/timeline-geometry';
import { instantMs } from '@/lib/time-text';
import {
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  nth,
  OPEN_RUN_WITH_PAUSES,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
} from '@/test/workflow-fixtures';
import type {
  ComposedRunTiming,
  NodeTiming,
  TimingSegment,
} from '@/types/timing';

const at = (seconds: number): number => instantMs('2026-11-22T12:00:00Z') + seconds * 1000;

/** The run axis of the examples: 12:00:00 plus 100 s. */
const AXIS: TimeAxis = { startMs: at(0), spanMs: 100_000 };

function composed(timing: typeof OPEN_RUN_WITH_PAUSES): ComposedRunTiming {
  if (timing.kind !== 'composed') {
    throw new Error('the example run is composed');
  }
  return timing;
}

type TimedNode = Extract<NodeTiming, { kind: 'exact' | 'partial' }>;

/** The example node with its segments; an example without them is a defect
 * of the test. */
function timed(timing: NodeTiming): TimedNode {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return timing;
    case 'unavailable':
    case 'inconsistent':
      throw new Error('the example node has segments');
  }
}

const segmentsOf = (timing: NodeTiming): TimingSegment[] => timed(timing).segments;

const closedRun = (startedAt: string, endedAt: string): ComposedRunTiming => ({
  ...composed(OPEN_RUN_WITH_PAUSES),
  range_started_at: startedAt,
  range_ended_at: endedAt,
  open_segments: false,
  pauses: [],
});

describe('runAxis', () => {
  it('spans the range of a closed run and ignores the draw time', () => {
    const run = closedRun('2026-11-22T12:00:00Z', '2026-11-22T12:00:50Z');

    expect(runAxis(run, at(500))).toEqual({ startMs: at(0), spanMs: 50_000 });
  });

  it('grows past the range of an open run to the draw time', () => {
    const run = composed(OPEN_RUN_WITH_PAUSES);

    expect(runAxis(run, at(100))).toEqual({ startMs: at(0), spanMs: 100_000 });
    expect(runAxis(run, at(130))).toEqual({ startMs: at(0), spanMs: 130_000 });
  });

  it('keeps a one-instant range divisible', () => {
    const run = closedRun('2026-11-22T12:00:00Z', '2026-11-22T12:00:00Z');

    expect(runAxis(run, at(0)).spanMs).toBe(1);
  });
});

describe('segment and pause spans', () => {
  const segments = segmentsOf(PARTIAL_OPEN_NODE);

  it('ends an ended segment at its end time', () => {
    expect(segmentSpan(nth(segments, 0), at(100))).toEqual({ startMs: at(0), endMs: at(5) });
  });

  it('ends an open segment at the draw time', () => {
    const open = nth(segments, 1);

    expect(segmentSpan(open, at(100))).toEqual({ startMs: at(5), endMs: at(100) });
    expect(segmentSpan(open, at(102))).toEqual({ startMs: at(5), endMs: at(102) });
  });

  it('gives an open segment no negative length when the draw time is before it', () => {
    expect(segmentSpan(nth(segments, 1), at(1))).toEqual({ startMs: at(5), endMs: at(5) });
  });

  it('ends a resumed pause, a pause the run ended, and an open pause', () => {
    const pauses = composed(OPEN_RUN_WITH_PAUSES).pauses;

    expect(pauseSpan(nth(pauses, 0), at(100))).toEqual({ startMs: at(1), endMs: at(2) });
    expect(pauseSpan(nth(pauses, 1), at(100))).toEqual({ startMs: at(3), endMs: at(4) });
    expect(pauseSpan(nth(pauses, 2), at(100))).toEqual({ startMs: at(5), endMs: at(100) });
  });
});

describe('boxOf', () => {
  it('places a span at its absolute position on the axis', () => {
    expect(boxOf({ startMs: at(2), endMs: at(3) }, AXIS)).toEqual({ leftPct: 2, widthPct: 1 });
  });

  it('gives a zero-length span zero width', () => {
    expect(boxOf({ startMs: at(40), endMs: at(40) }, AXIS)).toEqual({ leftPct: 40, widthPct: 0 });
  });

  it('clips a span that runs past either end of the axis', () => {
    expect(boxOf({ startMs: at(-10), endMs: at(10) }, AXIS)).toEqual({ leftPct: 0, widthPct: 10 });
    expect(boxOf({ startMs: at(90), endMs: at(150) }, AXIS)).toEqual({ leftPct: 90, widthPct: 10 });
  });
});

describe('axisTicks', () => {
  it('spaces the ticks evenly from the start to the end', () => {
    expect(axisTicks(AXIS, 5)).toEqual([
      { pct: 0, offsetMs: 0 },
      { pct: 25, offsetMs: 25_000 },
      { pct: 50, offsetMs: 50_000 },
      { pct: 75, offsetMs: 75_000 },
      { pct: 100, offsetMs: 100_000 },
    ]);
  });
});

describe('endpointMark', () => {
  it('marks a terminal node at its completion time', () => {
    expect(endpointMark(timed(EXACT_RETRIED_NODE).endpoint, AXIS)).toEqual({
      kind: 'terminal',
      status: 'COMPLETED',
      pct: 12,
      atMs: at(12),
    });
  });

  it('has no marker for a node that is not terminal', () => {
    expect(endpointMark({ kind: 'not_terminal' }, AXIS)).toEqual({ kind: 'not_terminal' });
  });
});

describe('phaseTotals', () => {
  it('sums each phase, keeps its longest interval, and says when one is open', () => {
    const totals = phaseTotals([
      { key: 'enqueued', startMs: at(0), endMs: at(0.1), open: false },
      { key: 'running', startMs: at(0.1), endMs: at(3.1), open: false },
      { key: 'enqueued', startMs: at(10), endMs: at(10.2), open: false },
      { key: 'running', startMs: at(10.2), endMs: at(15), open: true },
    ]);

    // Two queue waits of 100 ms and 200 ms: 300 ms in all, and no long step.
    expect(totals).toEqual([
      { key: 'enqueued', ms: 300, longestMs: 200, open: false },
      { key: 'running', ms: 7_800, longestMs: 4_800, open: true },
    ]);
  });

  it('counts an interval whose times are in the wrong order as no time', () => {
    expect(phaseTotals([{ key: 'claimed', startMs: at(5), endMs: at(4), open: false }])).toEqual([]);
  });

  it('is empty with no interval', () => {
    expect(phaseTotals([])).toEqual([]);
  });
});

describe('segmentInterval', () => {
  it('runs an open segment to the draw time and says that it is open', () => {
    const segments = segmentsOf(PARTIAL_OPEN_NODE);

    expect(segmentInterval(nth(segments, 0), at(100))).toEqual({
      key: 'unknown',
      startMs: at(0),
      endMs: at(5),
      open: false,
    });
    expect(segmentInterval(nth(segments, 1), at(100))).toEqual({
      key: 'unknown',
      startMs: at(5),
      endMs: at(100),
      open: true,
    });
  });
});

describe('phaseComposition', () => {
  it('sums each phase over retries, every claim kind into claimed, in lifecycle order', () => {
    const totals = phaseComposition(segmentsOf(EXACT_RETRIED_NODE), at(100));

    // claimed: earlier claims 1 s + the recorded claim 2 s + the second
    // attempt's claim 1 s; its longest interval is the recorded claim.
    expect(totals.map(total => [total.key, total.ms, total.longestMs])).toEqual([
      ['pending', 1_000, 1_000],
      ['ready', 1_000, 1_000],
      ['enqueued', 1_000, 1_000],
      ['retry_wait', 1_000, 1_000],
      ['claimed', 4_000, 2_000],
      ['running', 1_000, 1_000],
      ['finalizing', 1_000, 1_000],
      ['advancing', 1_000, 1_000],
    ]);
    expect(totals.every(total => !total.open)).toBe(true);
  });

  it('keeps unknown time as its own phase and counts an open segment to the draw time', () => {
    const totals = phaseComposition(segmentsOf(PARTIAL_OPEN_NODE), at(100));

    expect(totals).toEqual([{ key: 'unknown', ms: 100_000, longestMs: 95_000, open: true }]);
  });

  it('is empty when no segment has length', () => {
    const zero: TimingSegment = {
      phase: { kind: 'ready' },
      started_at: '2026-11-22T12:00:04Z',
      end: { kind: 'ended', at: '2026-11-22T12:00:04Z' },
      source: { kind: 'node' },
    };

    expect(phaseComposition([zero], at(100))).toEqual([]);
    expect(phaseComposition([], at(100))).toEqual([]);
  });
});

describe('nodeTimingIsOpen', () => {
  it('is true only for a node with an open segment', () => {
    expect(nodeTimingIsOpen(PARTIAL_OPEN_NODE)).toBe(true);
    expect(nodeTimingIsOpen(EXACT_RETRIED_NODE)).toBe(false);
    expect(nodeTimingIsOpen(UNAVAILABLE_NODE)).toBe(false);
    expect(nodeTimingIsOpen(CLOCK_ORDER_NODE)).toBe(false);
  });
});
