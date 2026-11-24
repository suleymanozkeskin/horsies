import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { TimingLegend } from '@/components/workflows/legend';
import { phaseMeaning } from '@/components/workflows/phase';

describe('TimingLegend', () => {
  it('lists each phase of the run with its total time, then the marks of the view', () => {
    render(
      <TimingLegend
        shares={[
          { key: 'pending', ms: 59_000, fraction: 0.8 },
          { key: 'enqueued', ms: 18, fraction: 0.01 },
          { key: 'running', ms: 14_600, fraction: 0.19 },
        ]}
        marks={['paused', 'end', 'critical_row']}
      />
    );

    const items = screen.getAllByRole('listitem');
    expect(items.map(item => item.textContent?.split(':')[0])).toEqual([
      'dependency wait59.0s',
      'queue wait18ms',
      'running14.6s',
      'paused',
      'end',
      'critical path',
    ]);
  });

  it('states the meaning of each item in its tooltip and to a screen reader', () => {
    render(<TimingLegend shares={[{ key: 'claimed', ms: 22, fraction: 1 }]} marks={['tolerant_join']} />);

    const slot = screen.getByText('slot wait').closest('li');
    expect(slot?.getAttribute('title')).toBe(phaseMeaning('claimed'));
    expect(slot?.textContent).toContain(phaseMeaning('claimed'));
    const tolerant = screen.getByText('tolerant join').closest('li');
    expect(tolerant?.getAttribute('title')).toContain('runs even when a dependency failed');
  });

  it('wraps instead of clipping', () => {
    render(<TimingLegend shares={[]} marks={['end']} />);

    expect(screen.getByRole('list', { name: 'Legend' }).className).toContain('flex-wrap');
  });
});
