import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { attemptPhaseTimes } from '@/components/monitoring/attempt-phases';
import { AttemptCard } from '@/components/monitoring/detail';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import { nth } from '@/test/workflow-fixtures';
import type { AttemptPhases } from '@/types/timing';
import type { TaskAttempt } from '@/types/workflows';

/** An attempt of the shared examples: started at 12:00:04, recorded at
 * 12:00:09. */
function attempt(phases: AttemptPhases): TaskAttempt {
  return {
    attempt: 1,
    outcome: 'SUCCESS',
    will_retry: false,
    error_code: null,
    error_message: null,
    failed_reason: null,
    worker_hostname: 'worker-a',
    started_at: '2026-11-22T12:00:04Z',
    finished_at: '2026-11-22T12:00:09Z',
    phases,
  };
}

const NOT_CAPTURED = nth(TIMING_EXAMPLES.attempt_phases, 0);
const PLACED_TWO_CLAIMS = nth(TIMING_EXAMPLES.attempt_phases, 1);
const RECOVERED = nth(TIMING_EXAMPLES.attempt_phases, 2);
const ONE_CLAIM_NOT_OBSERVED = nth(TIMING_EXAMPLES.attempt_phases, 3);
const CANCELLED = nth(TIMING_EXAMPLES.attempt_phases, 4);

const labelsOf = (phases: AttemptPhases): string[] => {
  const times = attemptPhaseTimes(attempt(phases));
  switch (times.kind) {
    case 'captured':
      return times.boundaries.map(boundary => boundary.label);
    case 'not_captured':
      return [];
  }
};

describe('attemptPhaseTimes', () => {
  it('has no times for an attempt from before the lifecycle record', () => {
    expect(attemptPhaseTimes(attempt(NOT_CAPTURED))).toEqual({ kind: 'not_captured' });
  });

  it('lists every recorded time of a placed attempt with earlier claims', () => {
    expect(labelsOf(PLACED_TWO_CLAIMS)).toEqual([
      'due',
      'first claim',
      'claimed',
      'placed',
      'started',
      'finalizing',
      'recorded',
    ]);
  });

  it('lists the last sign of life of a recovered attempt and no placement', () => {
    expect(labelsOf(RECOVERED)).toEqual(['due', 'claimed', 'started', 'last seen', 'recorded']);
  });

  it('leaves out the first claim of a single claim and an unobserved placement', () => {
    expect(labelsOf(ONE_CLAIM_NOT_OBSERVED)).toEqual(['due', 'claimed', 'started', 'recorded']);
  });

  it('lists the finalizing marker of a cancelled attempt and no placement or last sign of life', () => {
    expect(labelsOf(CANCELLED)).toEqual(['due', 'claimed', 'started', 'finalizing', 'recorded']);
  });

  it('leaves out a start and a record time that the attempt row does not hold', () => {
    const times = attemptPhaseTimes({
      ...attempt(ONE_CLAIM_NOT_OBSERVED),
      started_at: null,
      finished_at: null,
    });

    expect(times.kind === 'captured' && times.boundaries.map(boundary => boundary.label)).toEqual([
      'due',
      'claimed',
    ]);
  });

  it('states the claim count and the writer', () => {
    const placed = attemptPhaseTimes(attempt(PLACED_TWO_CLAIMS));
    const recovered = attemptPhaseTimes(attempt(RECOVERED));

    expect(placed.kind === 'captured' && [placed.claims, placed.recordedBy]).toEqual([
      '2 claims',
      'recorded by the worker',
    ]);
    expect(recovered.kind === 'captured' && recovered.claims).toBe('claims not counted');
    const cancelled = attemptPhaseTimes(attempt(CANCELLED));
    expect(cancelled.kind === 'captured' && [cancelled.claims, cancelled.recordedBy]).toEqual([
      '1 claim',
      'recorded by the cancel; an operator cancelled the task while it ran',
    ]);
  });
});

describe('AttemptCard phase times', () => {
  it('shows each time with the time since the one before it', () => {
    render(<AttemptCard attempt={attempt(PLACED_TWO_CLAIMS)} />);

    const times = screen.getByRole('group', { name: 'Phase times' });

    // Due 12:00:00, first claim 12:00:01, claimed 12:00:02, placed 12:00:03.
    expect(times.textContent).toContain('first claim');
    expect(times.textContent).toContain('+1.0s');
    expect(times.textContent).toContain('2 claims · recorded by the worker');
  });

  it('says that an attempt from before the lifecycle record has no phase times', () => {
    render(<AttemptCard attempt={attempt(NOT_CAPTURED)} />);

    expect(screen.getByText(/Phase times not captured/)).not.toBeNull();
  });
});
