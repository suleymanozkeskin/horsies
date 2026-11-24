import { describe, expect, it } from 'vitest';

import {
  PHASE_ORDER,
  phaseFill,
  phaseInk,
  phaseLabel,
  phaseMeaning,
  phaseTrack,
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
