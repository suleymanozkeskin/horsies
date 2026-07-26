import { describe, expect, it } from 'vitest';

import { residualState } from '@/components/workflows/residual';

describe('residual node states', () => {
  it('marks a still-executing node under a cancelled run as draining', () => {
    expect(residualState('CANCELLED', 'RUNNING', null)).toBe('draining');
  });

  it('marks a still-executing node under a paused run as finishing', () => {
    expect(residualState('PAUSED', 'RUNNING', 'RUNNING')).toBe('finishing');
    expect(residualState('PAUSED', 'RUNNING', 'CLAIMED')).toBe('finishing');
  });

  it('does not claim "finishing" without the backing task status', () => {
    // The run payload carries no leaf status; the node detail supplies it.
    expect(residualState('PAUSED', 'RUNNING', null)).toBeNull();
  });

  it('does not mark a paused node whose task already left the worker', () => {
    expect(residualState('PAUSED', 'RUNNING', 'PENDING')).toBeNull();
  });

  it.each(['PENDING', 'READY', 'ENQUEUED', 'SKIPPED', 'COMPLETED', 'FAILED'])(
    'never marks a %s node',
    nodeStatus => {
      expect(residualState('CANCELLED', nodeStatus, 'RUNNING')).toBeNull();
      expect(residualState('PAUSED', nodeStatus, 'RUNNING')).toBeNull();
    }
  );

  it('never marks nodes under a healthy run', () => {
    expect(residualState('RUNNING', 'RUNNING', 'RUNNING')).toBeNull();
    expect(residualState('COMPLETED', 'RUNNING', 'RUNNING')).toBeNull();
  });
});
