import { describe, expect, it } from 'vitest';

import * as phase from '@/components/workflows/phase';
import {
  phaseFill,
  phaseLabel,
  phaseMeaning,
  phaseTrack,
  segmentKind,
  TIMING_KIND_ORDER,
} from '@/components/workflows/phase';

describe('the kinds of the timing views', () => {
  it('lists each kind once, in the order of the prototype: the retry wait last', () => {
    expect(TIMING_KIND_ORDER).toEqual([
      'pending',
      'ready',
      'enqueued',
      'earlier_claims',
      'claimed',
      'running',
      'finalizing',
      'advancing',
      'worker_lost',
      'unknown',
      'retry_wait',
    ]);
    expect(new Set(TIMING_KIND_ORDER).size).toBe(TIMING_KIND_ORDER.length);
  });

  it('names each kind by the time it is, with a short meaning', () => {
    expect(TIMING_KIND_ORDER.map(phaseLabel)).toEqual([
      'dependency wait',
      'ready',
      'queue wait',
      'earlier claims',
      'slot wait',
      'running',
      'finalizing',
      'advancing',
      'worker lost',
      'unknown',
      'retry wait',
    ]);
    for (const kind of TIMING_KIND_ORDER) {
      // A short phrase, never a sentence.
      expect(phaseMeaning(kind)).not.toMatch(/\.$/);
      expect(phaseMeaning(kind).length).toBeLessThan(60);
    }
    expect(phaseMeaning('retry_wait')).toBe('record of a failed attempt → due time of the next');
  });

  it('fills every kind from a theme token; a part keeps the fill of its phase', () => {
    for (const kind of TIMING_KIND_ORDER) {
      expect(phaseFill(kind)).toMatch(/^var\(--phase-[a-z-]+\)$/);
    }
    expect(phaseFill('earlier_claims')).toBe(phaseFill('claimed'));
    expect(phaseFill('worker_lost')).toBe('var(--phase-unknown-hatch)');
  });

  it('draws only dependency wait as a thin line', () => {
    expect(TIMING_KIND_ORDER.filter(kind => phaseTrack(kind) === 'thin')).toEqual(['pending']);
  });

  it('gives earlier claims and a lost worker a kind of their own', () => {
    expect(segmentKind({ kind: 'claimed', claim: { kind: 'earlier', count: 2 } })).toBe('earlier_claims');
    expect(segmentKind({ kind: 'claimed', claim: { kind: 'not_recorded' } })).toBe('claimed');
    expect(segmentKind({ kind: 'unknown', reason: 'WORKER_LOST' })).toBe('worker_lost');
    expect(segmentKind({ kind: 'unknown', reason: 'CLAIMS_NOT_COUNTED' })).toBe('unknown');
    expect(segmentKind({ kind: 'retry_wait' })).toBe('retry_wait');
  });

  it('has no limit and no weight that judges a time: the log bars show size', () => {
    expect(Object.keys(phase)).not.toContain('LONG_ENGINE_STEP_MS');
    expect(Object.keys(phase)).not.toContain('phaseWeight');
  });
});
