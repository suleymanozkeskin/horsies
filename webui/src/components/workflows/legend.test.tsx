import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { markText, TimingLegend, type LegendMark } from '@/components/workflows/legend';
import { phaseMeaning } from '@/components/workflows/phase';

const MARKS: LegendMark[] = ['paused', 'end', 'critical_row', 'critical_edge', 'tolerant_join'];

describe('TimingLegend', () => {
  it('lists the fill of each phase that the view draws, then the marks of the view', () => {
    render(<TimingLegend phases={['pending', 'enqueued', 'running']} marks={['paused', 'end', 'critical_row']} />);

    expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual([
      'dependency wait',
      'queue wait',
      'running',
      'run paused',
      'end of a node',
      'critical path',
    ]);
  });

  it('holds no time: the key explains the drawing, and the summary holds the numbers', () => {
    render(<TimingLegend phases={['enqueued', 'running']} marks={[]} />);

    expect(screen.getByRole('list', { name: 'Legend' }).textContent).toBe('queue waitrunning');
  });

  it('draws the swatch of dependency wait as the thin track of the timeline', () => {
    render(<TimingLegend phases={['pending', 'running']} marks={[]} />);

    const swatch = (phase: string): Element | null | undefined =>
      document.querySelector(`[data-legend-phase="${phase}"]`)?.querySelector('[aria-hidden]');
    expect(swatch('pending')?.className).toContain('h-1 ');
    expect(swatch('running')?.className).toContain('h-2.5');
  });

  it('states the meaning of each item as its description, for the hover and for a screen reader', () => {
    render(<TimingLegend phases={['claimed']} marks={['tolerant_join', 'end']} />);

    const described = (text: string): string | null | undefined =>
      screen.getByText(text).closest('[aria-description]')?.getAttribute('aria-description');
    expect(described('slot wait')).toBe(phaseMeaning('claimed'));
    expect(described('tolerant join')).toContain('runs even when a dependency failed');
    expect(described('end of a node')).toContain('terminal status');
  });

  it('gives every mark a label and a meaning', () => {
    for (const mark of MARKS) {
      expect(markText(mark).label).not.toBe('');
      expect(markText(mark).meaning.length).toBeGreaterThan(20);
    }
  });

  it('wraps instead of clipping, and shows nothing with no item', () => {
    const { container, rerender } = render(<TimingLegend phases={[]} marks={['end']} />);

    expect(screen.getByRole('list', { name: 'Legend' }).className).toContain('flex-wrap');

    rerender(<TimingLegend phases={[]} marks={[]} />);
    expect(container.querySelector('ul')).toBeNull();
  });
});
