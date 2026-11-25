import type { ReactElement } from 'react';

import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import {
  ChartLegend,
  type LegendSeries,
} from '@/components/monitoring/chart-legend';
import {
  ALL_SERIES,
  type SeriesSelection,
} from '@/components/monitoring/series-selection';

type Key = 'running' | 'cpu';

const SERIES: readonly LegendSeries<Key>[] = [
  { key: 'running', label: 'running', color: 'var(--info)', mark: 'area' },
  { key: 'cpu', label: 'CPU', color: 'var(--foreground)', mark: 'line' },
];

const onlyRunning: SeriesSelection<Key> = { kind: 'only', series: 'running' };

/** The legend of a chart named `Load`, with the given selection. */
const legend = (selection: SeriesSelection<Key>): ReactElement => (
  <ChartLegend
    chart="Load"
    series={SERIES}
    selection={selection}
    onClick={() => {}}
  />
);

const item = (label: string): HTMLElement =>
  screen.getByRole('button', { name: label });

/** The one icon of a legend item. */
const iconOf = (label: string): SVGSVGElement => {
  const icon = item(label).querySelector('svg');
  if (icon === null) {
    throw new Error(`legend item ${label}: no icon`);
  }
  return icon;
};

describe('ChartLegend', () => {
  it('draws an outlined block for an area and a line with a hollow dot for a line', () => {
    render(legend(ALL_SERIES));
    const block = iconOf('running').querySelector('rect');
    const line = iconOf('CPU').querySelector('path');
    const dot = iconOf('CPU').querySelector('circle');

    expect(block?.getAttribute('fill')).toBe('var(--info)');
    expect(block?.getAttribute('stroke')).toBe('var(--info)');
    expect(line?.getAttribute('stroke')).toBe('var(--foreground)');
    expect(dot?.getAttribute('stroke')).toBe('var(--foreground)');
    expect(dot?.getAttribute('fill')).toBe('none');
  });

  it('dims and strikes through the item of a series that is not drawn', () => {
    render(legend(onlyRunning));

    expect(item('running').getAttribute('aria-pressed')).toBe('true');
    expect(item('running').className).not.toContain('line-through');
    expect(iconOf('running').classList.contains('opacity-50')).toBe(false);
    expect(item('CPU').getAttribute('aria-pressed')).toBe('false');
    expect(item('CPU').className).toContain('line-through');
    expect(iconOf('CPU').classList.contains('opacity-50')).toBe(true);
  });

  it('says on each item what a click on it does', () => {
    const { rerender } = render(legend(ALL_SERIES));

    expect(item('running').title).toBe('Show only running');
    expect(item('CPU').title).toBe('Show only CPU');

    rerender(legend(onlyRunning));

    expect(item('running').title).toBe('Show every series');
    expect(item('CPU').title).toBe('Show only CPU');
  });
});
