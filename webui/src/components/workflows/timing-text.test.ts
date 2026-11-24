import { describe, expect, it } from 'vitest';

import { LONG_ENGINE_STEP_MS } from '@/components/workflows/phase';
import {
  compositionText,
  durationText,
  nodeTimeText,
  nodeTimingText,
  placementText,
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
  it('says nothing for a slot wait in its normal range: the placement is noise there', () => {
    // With a free child, the child takes the task in the millisecond of the claim.
    expect(placementText(ms(0), ms(0), ms(0.002))).toBeNull();
    expect(placementText(ms(0), ms(0.1), ms(0) + LONG_ENGINE_STEP_MS - 1)).toBeNull();
  });

  it('says when a child took the task of a long slot wait', () => {
    expect(placementText(ms(0), ms(0.79), ms(0.8))).toBe('a child took the task after 790ms');
  });

});

describe('segmentPhaseText', () => {
  it('names a normal slot wait with no placement text', () => {
    expect(segmentPhaseText(claimed(0, 0, 0.002))).toBe('slot wait');
  });

  it('adds the placement to a long slot wait, and the count to earlier claims', () => {
    expect(segmentPhaseText(claimed(0, 0.79, 0.8))).toBe('slot wait · a child took the task after 790ms');
    if (EXACT_RETRIED_NODE.kind !== 'exact') {
      throw new Error('the example node is exact');
    }
    expect(segmentPhaseText(nth(EXACT_RETRIED_NODE.segments, 3))).toBe('slot wait · 1 earlier claim');
    // The claim of an attempt with no row has no detail.
    expect(segmentPhaseText(nth(EXACT_RETRIED_NODE.segments, 8))).toBe('slot wait');
  });

  it('adds no placement to a slot wait with no end yet', () => {
    expect(segmentPhaseText(claimed(0, 0.79, null))).toBe('slot wait');
  });

  it('names the reason of an unknown segment', () => {
    if (PARTIAL_OPEN_NODE.kind !== 'partial') {
      throw new Error('the example node is partial');
    }

    expect(segmentPhaseText(nth(PARTIAL_OPEN_NODE.segments, 1))).toBe('unknown · worker lost');
  });
});

describe('durationText and nodeTimeText', () => {
  it('writes a time with no end yet as a time so far', () => {
    expect(durationText(4_200, false)).toBe('4.2s');
    expect(durationText(4_200, true)).toBe('4.2s so far');
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
        { key: 'enqueued', ms: 1_000, longestMs: 1_000, open: false },
        { key: 'running', ms: 4_200, longestMs: 4_200, open: false },
      ])
    ).toBe('queue wait 1.0s, running 4.2s');
    expect(compositionText([])).toBe('no measured time');
  });

  it('puts the coverage and each phase time in the accessible name of a node', () => {
    expect(nodeTimingText(EXACT_RETRIED_NODE, READ_MS)).toBe(
      'exact timing; dependency wait 1.0s, ready 1.0s, queue wait 1.0s, retry wait 1.0s, ' +
        'slot wait 4.0s, running 1.0s, finalizing 1.0s, advancing 1.0s'
    );
  });

  it('names the reason of an unavailable node and the conflict of an inconsistent node', () => {
    expect(nodeTimingText(UNAVAILABLE_NODE, READ_MS)).toBe(
      'timing unavailable: The run began before the lifecycle record.'
    );
    expect(nodeTimingText(CLOCK_ORDER_NODE, READ_MS)).toMatch(/^timing inconsistent: Clock order: claim/);
  });
});
