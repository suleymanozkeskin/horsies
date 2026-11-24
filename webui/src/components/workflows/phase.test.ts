import { describe, expect, it } from 'vitest';

import {
  LONG_ENGINE_STEP_MS,
  PHASE_ORDER,
  phaseFill,
  phaseInk,
  phaseLabel,
  phaseMeaning,
  phaseTrack,
  phaseWeight,
} from '@/components/workflows/phase';

describe('the phase list', () => {
  it('holds the nine phases of the API once each, in lifecycle order', () => {
    expect(PHASE_ORDER).toEqual([
      'pending',
      'ready',
      'enqueued',
      'retry_wait',
      'claimed',
      'running',
      'finalizing',
      'advancing',
      'unknown',
    ]);
    expect(new Set(PHASE_ORDER).size).toBe(PHASE_ORDER.length);
  });

  it('gives every phase a label, a meaning and a fill of its own', () => {
    const fills = PHASE_ORDER.map(phaseFill);
    const labels = PHASE_ORDER.map(phaseLabel);

    for (const key of PHASE_ORDER) {
      expect(phaseLabel(key)).not.toBe('');
      expect(phaseMeaning(key)).not.toBe('');
    }
    expect(new Set(fills).size).toBe(PHASE_ORDER.length);
    expect(new Set(labels).size).toBe(PHASE_ORDER.length);
  });

  it('fills every phase from a theme token, and unknown with a hatch', () => {
    for (const key of PHASE_ORDER) {
      expect(phaseFill(key)).toMatch(/^var\(--phase-[a-z-]+\)$/);
      expect(phaseInk(key)).toMatch(/^var\(--[a-z-]+\)$/);
    }
    expect(phaseFill('unknown')).toBe('var(--phase-unknown-hatch)');
  });

  it('names each phase by the time it is, not by the record state', () => {
    expect(phaseLabel('pending')).toBe('dependency wait');
    expect(phaseLabel('enqueued')).toBe('queue wait');
    expect(phaseLabel('claimed')).toBe('slot wait');
    expect(phaseLabel('retry_wait')).toBe('retry wait');
  });

  it('draws only dependency wait as a thin track', () => {
    expect(PHASE_ORDER.filter(key => phaseTrack(key) === 'thin')).toEqual(['pending']);
  });
});

describe('the weight of a phase time', () => {
  it('makes the task code the main time and a planned wait a plain time, at any length', () => {
    expect(phaseWeight('running', 2)).toEqual({ kind: 'main' });
    expect(phaseWeight('running', 3_600_000)).toEqual({ kind: 'main' });
    expect(phaseWeight('pending', 140_000)).toEqual({ kind: 'plain' });
    expect(phaseWeight('retry_wait', 72_000)).toEqual({ kind: 'plain' });
  });

  it('keeps an engine step in its normal range quiet', () => {
    // With a local database an engine step is a few milliseconds.
    for (const key of ['ready', 'enqueued', 'claimed', 'finalizing', 'advancing'] as const) {
      expect(phaseWeight(key, 8)).toEqual({ kind: 'quiet' });
      expect(phaseWeight(key, LONG_ENGINE_STEP_MS - 1)).toEqual({ kind: 'quiet' });
    }
  });

  it('marks an engine step at the long step limit, and says what its time tells', () => {
    for (const key of ['ready', 'enqueued', 'claimed', 'finalizing', 'advancing', 'unknown'] as const) {
      const weight = phaseWeight(key, LONG_ENGINE_STEP_MS);

      expect(weight.kind).toBe('marked');
      expect(weight.kind === 'marked' ? weight.tells : '').not.toBe('');
    }
    const queue = phaseWeight('enqueued', 7_200);
    expect(queue.kind === 'marked' ? queue.tells : '').toContain('concurrency limit');
    const slot = phaseWeight('claimed', 800);
    expect(slot.kind === 'marked' ? slot.tells : '').toContain('no child was free');
  });

  it('keeps a short gap of the record quiet', () => {
    expect(phaseWeight('unknown', 3)).toEqual({ kind: 'quiet' });
  });
});
