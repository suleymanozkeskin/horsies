import { describe, expect, it } from 'vitest';

import {
  compositionText,
  durationText,
  nodeTimeText,
  nodeTimingText,
  placementText,
  segmentDetailText,
  segmentPhaseText,
} from '@/components/workflows/timing-text';
import { instantMs } from '@/lib/time-text';
import {
  at,
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  nth,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
} from '@/test/workflow-fixtures';
import type { TimingSegment } from '@/types/timing';

const READ_MS = instantMs('2026-11-22T12:01:40Z');
const ms = (seconds: number): number => instantMs(at(seconds));

const claimed = (startAt: number, placedAt: number, endAt: number | null): TimingSegment => ({
  phase: {
    kind: 'claimed',
    claim: { kind: 'recorded', placement: { kind: 'placed', placed_at: at(placedAt) } },
  },
  started_at: at(startAt),
  end: endAt === null ? { kind: 'open' } : { kind: 'ended', at: at(endAt) },
  source: { kind: 'attempt', generation: 1, task_id: 't', attempt: 1 },
});

describe('placementText', () => {
  it('says when a child took the task, at any length of the slot wait: no limit judges it', () => {
    expect(placementText(ms(0), ms(0.002))).toBe('a child took the task after 2.00ms');
    expect(placementText(ms(0), ms(0.79))).toBe('a child took the task after 790ms');
  });
});

describe('segmentPhaseText and segmentDetailText', () => {
  it('adds the placement to every slot wait that ended', () => {
    expect(segmentPhaseText(claimed(0, 0.001, 0.002))).toBe('slot wait · a child took the task after 1.00ms');
    expect(segmentPhaseText(claimed(0, 0.79, 0.8))).toBe('slot wait · a child took the task after 790ms');
  });

  it('names earlier claims as a kind of their own, with their count', () => {
    if (EXACT_RETRIED_NODE.kind !== 'exact') {
      throw new Error('the example node is exact');
    }
    expect(segmentPhaseText(nth(EXACT_RETRIED_NODE.segments, 3))).toBe('earlier claims · 1 earlier claim');
    // The claim of an attempt with no row has no detail.
    expect(segmentPhaseText(nth(EXACT_RETRIED_NODE.segments, 8))).toBe('slot wait');
    expect(segmentDetailText(nth(EXACT_RETRIED_NODE.segments, 8))).toBeNull();
  });

  it('adds no placement to a slot wait with no end yet', () => {
    expect(segmentPhaseText(claimed(0, 0.79, null))).toBe('slot wait');
  });

  it('names a lost worker by its kind, and the reason of another unknown time', () => {
    if (PARTIAL_OPEN_NODE.kind !== 'partial') {
      throw new Error('the example node is partial');
    }

    expect(segmentPhaseText(nth(PARTIAL_OPEN_NODE.segments, 1))).toBe('worker lost');
    expect(segmentPhaseText(nth(PARTIAL_OPEN_NODE.segments, 0))).toBe('unknown · history row removed by retention');
  });
});

describe('durationText and nodeTimeText', () => {
  it('writes a time with the duration rule, and a time with no end yet as a time so far', () => {
    expect(durationText(4_200, false)).toBe('4.20s');
    expect(durationText(4_200, true)).toBe('4.20s so far');
  });

  it('writes the node time, or a dash for a node with none', () => {
    expect(nodeTimeText({ kind: 'measured', ms: 32_100, open: false })).toBe('32.1s');
    expect(nodeTimeText({ kind: 'not_started' })).toBe('—');
    expect(nodeTimeText({ kind: 'no_timing' })).toBe('—');
  });
});

describe('compositionText and nodeTimingText', () => {
  it('writes each phase time, or that no time was measured', () => {
    expect(
      compositionText([
        { key: 'enqueued', ms: 1_000, longestMs: 1_000, count: 1, open: false },
        { key: 'running', ms: 4_200, longestMs: 4_200, count: 1, open: false },
      ])
    ).toBe('queue wait 1.00s, running 4.20s');
    expect(compositionText([])).toBe('no measured time');
  });

  it('puts the coverage and each phase time in the accessible name of a node', () => {
    expect(nodeTimingText(EXACT_RETRIED_NODE, READ_MS)).toBe(
      'exact timing; dependency wait 1.00s, ready 1.00s, queue wait 1.00s, earlier claims 1.00s, ' +
        'slot wait 3.00s, running 1.00s, finalizing 1.00s, advancing 1.00s, retry wait 1.00s'
    );
  });

  it('names the reason of an unavailable node and the conflict of an inconsistent node', () => {
    expect(nodeTimingText(UNAVAILABLE_NODE, READ_MS)).toBe(
      'timing unavailable: The run began before the lifecycle record.'
    );
    expect(nodeTimingText(CLOCK_ORDER_NODE, READ_MS)).toMatch(/^timing inconsistent: Clock order: claim/);
  });
});
