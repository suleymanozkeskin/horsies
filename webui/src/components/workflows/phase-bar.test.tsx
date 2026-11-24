import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { PhaseBar } from '@/components/workflows/phase-bar';

describe('PhaseBar', () => {
  it('sizes each part by its time, with a minimum width for a tiny part', () => {
    render(
      <PhaseBar
        parts={[
          { key: 'enqueued', ms: 3, title: 'queue wait 3ms' },
          { key: 'running', ms: 4_500, title: 'running 4.5s' },
        ]}
        label="attempt"
        size="regular"
      />
    );

    const parts = [...screen.getByRole('img', { name: 'attempt' }).querySelectorAll<HTMLElement>('[data-phase]')];
    expect(parts.map(part => part.style.flexGrow)).toEqual(['3', '4500']);
    expect(parts.map(part => part.style.minWidth)).toEqual(['3px', '3px']);
    expect(parts.map(part => part.getAttribute('title'))).toEqual(['queue wait 3ms', 'running 4.5s']);
  });

  it('draws dependency wait as a thin track and the other phases at full height', () => {
    render(
      <PhaseBar
        parts={[
          { key: 'pending', ms: 5_000, title: 'dependency wait' },
          { key: 'running', ms: 1_000, title: 'running' },
        ]}
        label="node"
        size="slim"
      />
    );

    const [pending, running] = [...screen.getByRole('img').querySelectorAll<HTMLElement>('[data-phase]')];
    expect(pending?.firstElementChild?.className).toContain('h-1/3');
    expect(running?.firstElementChild?.className).toContain('inset-0');
    expect(running?.firstElementChild?.getAttribute('style')).toContain('var(--phase-running-fill)');
  });

  it('gives a negative time (a clock step) no width beyond the minimum', () => {
    render(<PhaseBar parts={[{ key: 'claimed', ms: -40, title: 'slot wait' }]} label="step" size="regular" />);

    expect(screen.getByRole('img').querySelector<HTMLElement>('[data-phase]')?.style.flexGrow).toBe('0');
  });
});
