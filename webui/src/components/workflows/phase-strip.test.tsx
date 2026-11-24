import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { NODE_H } from '@/components/workflows/layout';
import { PhaseStrip } from '@/components/workflows/phase-strip';
import { cardDuration } from '@/components/workflows/workflow-node';
import { instantMs } from '@/lib/time-text';
import {
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
} from '@/test/workflow-fixtures';

const READ_MS = instantMs('2026-11-22T12:01:40Z');

describe('PhaseStrip', () => {
  it('draws one share per phase, sized by its part of the node time', () => {
    render(<PhaseStrip timing={EXACT_RETRIED_NODE} drawAtMs={READ_MS} />);

    const strip = screen.getByRole('img');
    const shares = [...strip.querySelectorAll<HTMLElement>('[data-phase]')];

    // 11 s in all; claimed holds 4 s of it, every other phase 1 s.
    expect(shares.map(share => share.getAttribute('data-phase'))).toEqual([
      'pending',
      'ready',
      'enqueued',
      'retry_wait',
      'claimed',
      'running',
      'finalizing',
      'advancing',
    ]);
    const claimed = shares.find(share => share.getAttribute('data-phase') === 'claimed');
    expect(Number.parseFloat(claimed?.style.width ?? '')).toBeCloseTo((4 / 11) * 100, 3);
  });

  it('draws unknown time as its own share, filled with the hatch of phase.ts', () => {
    render(<PhaseStrip timing={PARTIAL_OPEN_NODE} drawAtMs={READ_MS} />);

    const shares = [...screen.getByRole('img').querySelectorAll<HTMLElement>('[data-phase]')];

    expect(shares.map(share => share.getAttribute('data-phase'))).toEqual(['unknown']);
    expect(shares[0]?.style.width).toBe('100%');
  });

  it('hatches an unavailable node and names its reason', () => {
    render(<PhaseStrip timing={UNAVAILABLE_NODE} drawAtMs={READ_MS} />);
    const unavailable = screen.getByRole('img');

    expect(unavailable.getAttribute('data-coverage')).toBe('unavailable');
    expect(unavailable.getAttribute('aria-label')).toBe(
      'timing unavailable: The run began before the lifecycle record.'
    );
    expect(unavailable.querySelector('[data-phase]')).toBeNull();
  });

  it('names an inconsistent node by its conflict', () => {
    render(<PhaseStrip timing={CLOCK_ORDER_NODE} drawAtMs={READ_MS} />);
    const strip = screen.getByRole('img');

    expect(strip.getAttribute('data-coverage')).toBe('inconsistent');
    expect(strip.getAttribute('aria-label')).toMatch(/^timing inconsistent: Clock order: claim/);
  });

  it('puts the coverage and each phase time in its accessible name', () => {
    render(<PhaseStrip timing={EXACT_RETRIED_NODE} drawAtMs={READ_MS} />);

    expect(screen.getByRole('img').getAttribute('aria-label')).toBe(
      'exact timing; dependency wait 1.0s, ready 1.0s, queue wait 1.0s, retry wait 1.0s, ' +
        'slot wait 4.0s, running 1.0s, finalizing 1.0s, advancing 1.0s'
    );
  });
});

describe('cardDuration', () => {
  it('shows the wall time of a node with segments', () => {
    expect(cardDuration(EXACT_RETRIED_NODE, 1, READ_MS)).toEqual({ kind: 'wall', ms: 12_000 });
  });

  it('shows the execution time of a node with no phase timing', () => {
    expect(cardDuration(UNAVAILABLE_NODE, 42, READ_MS)).toEqual({
      kind: 'execution',
      seconds: 42,
    });
    expect(cardDuration(CLOCK_ORDER_NODE, 7, READ_MS)).toEqual({ kind: 'execution', seconds: 7 });
  });

  it('shows nothing for a node with neither', () => {
    expect(cardDuration(UNAVAILABLE_NODE, null, READ_MS)).toEqual({ kind: 'none' });
    expect(
      cardDuration(
        { kind: 'exact', segments: [], endpoint: { kind: 'not_terminal' } },
        null,
        READ_MS
      )
    ).toEqual({ kind: 'none' });
  });

  it('fits the phase strip: the card is 60 px high', () => {
    expect(NODE_H).toBe(60);
  });
});
