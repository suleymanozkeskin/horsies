import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { markText, TimingLegend, type LegendMark } from '@/components/workflows/legend';

const MARKS: LegendMark[] = ['chain_edge', 'tolerant_join'];

describe('TimingLegend', () => {
  it('lists the marks of the graph: the longest chain, then the tolerant join', () => {
    render(<TimingLegend marks={MARKS} />);

    expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual([
      'longest chain',
      'tolerant join',
    ]);
  });

  it('states the meaning of each item as its description, for the hover and for a screen reader', () => {
    render(<TimingLegend marks={MARKS} />);

    const described = (text: string): string | null | undefined =>
      screen.getByText(text).closest('[aria-description]')?.getAttribute('aria-description');
    expect(described('longest chain')).toContain('decided the end of the run');
    expect(described('tolerant join')).toContain('runs even when a dependency failed');
  });

  it('never says critical path', () => {
    for (const mark of MARKS) {
      expect(markText(mark).label).not.toMatch(/critical/i);
      expect(markText(mark).meaning).not.toMatch(/critical/i);
    }
  });

  it('wraps instead of clipping, and shows nothing with no mark', () => {
    const { container, rerender } = render(<TimingLegend marks={['chain_edge']} />);

    expect(screen.getByRole('list', { name: 'Legend' }).className).toContain('flex-wrap');

    rerender(<TimingLegend marks={[]} />);
    expect(container.querySelector('ul')).toBeNull();
  });
});
