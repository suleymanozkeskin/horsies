import { useState } from 'react';

import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import {
  SeriesLegend,
  type LegendSeries,
} from '@/components/monitoring/series-legend';
import {
  ALL_SERIES,
  isAlone,
  isSeriesShown,
  selectionAfterClick,
  type SeriesSelection,
} from '@/components/monitoring/series-selection';

type Key = 'running' | 'claimed';

const onlyRunning: SeriesSelection<Key> = { kind: 'only', series: 'running' };
const onlyClaimed: SeriesSelection<Key> = { kind: 'only', series: 'claimed' };

describe('series selection', () => {
  it('draws every series before a click', () => {
    expect(isSeriesShown<Key>(ALL_SERIES, 'running')).toBe(true);
    expect(isSeriesShown<Key>(ALL_SERIES, 'claimed')).toBe(true);
    expect(isAlone<Key>(ALL_SERIES, 'running')).toBe(false);
  });

  it('draws one series alone after a click on it', () => {
    const selection = selectionAfterClick<Key>(ALL_SERIES, 'running');

    expect(selection).toEqual(onlyRunning);
    expect(isSeriesShown(selection, 'running')).toBe(true);
    expect(isSeriesShown(selection, 'claimed')).toBe(false);
    expect(isAlone(selection, 'running')).toBe(true);
    expect(isAlone(selection, 'claimed')).toBe(false);
  });

  it('draws every series again after a click on the series that is alone', () => {
    expect(selectionAfterClick(onlyRunning, 'running')).toEqual(ALL_SERIES);
  });

  it('moves to the other series after a click on a series that is not drawn', () => {
    expect(selectionAfterClick(onlyRunning, 'claimed')).toEqual(onlyClaimed);
  });

  it('never gives a selection that draws no series', () => {
    const clicks: Key[] = ['running', 'claimed', 'claimed', 'running', 'running'];
    let selection: SeriesSelection<Key> = ALL_SERIES;

    for (const clicked of clicks) {
      selection = selectionAfterClick(selection, clicked);

      expect(
        isSeriesShown(selection, 'running') || isSeriesShown(selection, 'claimed')
      ).toBe(true);
    }
  });
});

const SERIES: readonly LegendSeries<Key>[] = [
  { key: 'running', label: 'running', color: 'var(--info)', mark: 'area' },
  { key: 'claimed', label: 'claimed', color: 'var(--warning)', mark: 'line' },
];

/** The legend with the state that a chart keeps for it. */
function LegendUnderTest() {
  const [selection, setSelection] = useState<SeriesSelection<Key>>(ALL_SERIES);
  return (
    <SeriesLegend
      chart="Load"
      series={SERIES}
      selection={selection}
      onClick={series =>
        setSelection(current => selectionAfterClick(current, series))
      }
    />
  );
}

const item = (label: string): HTMLElement =>
  screen.getByRole('button', { name: label });

describe('SeriesLegend', () => {
  it('names the chart of the group and shows each series as drawn', () => {
    render(<LegendUnderTest />);

    expect(screen.getByRole('group', { name: 'Series of Load' })).toBeTruthy();
    expect(item('running').getAttribute('aria-pressed')).toBe('true');
    expect(item('claimed').getAttribute('aria-pressed')).toBe('true');
    expect(item('running').title).toBe('Show only running');
    expect(item('running').className).not.toContain('line-through');
  });

  it('shows on each item which series is drawn after a click', () => {
    render(<LegendUnderTest />);

    fireEvent.click(item('running'));

    expect(item('running').getAttribute('aria-pressed')).toBe('true');
    expect(item('claimed').getAttribute('aria-pressed')).toBe('false');
    expect(item('claimed').className).toContain('line-through');
    expect(item('running').className).not.toContain('line-through');
    expect(item('running').title).toBe('Show every series');
    expect(item('claimed').title).toBe('Show only claimed');
  });

  it('empties the key of a series that is not drawn', () => {
    render(<LegendUnderTest />);
    const runningKey = item('running').querySelector('span');
    const claimedKey = item('claimed').querySelector('span');

    fireEvent.click(item('claimed'));

    // An area key keeps a border and loses its fill; a line key goes to the
    // color of the track.
    expect(runningKey?.style.background).toBe('transparent');
    expect(claimedKey?.style.background).toBe('var(--warning)');

    fireEvent.click(item('running'));

    expect(runningKey?.style.background).toBe('var(--info)');
    expect(claimedKey?.style.background).toBe('var(--muted)');
  });

  it('draws every series again after a second click on the same item', () => {
    render(<LegendUnderTest />);

    fireEvent.click(item('claimed'));
    fireEvent.click(item('claimed'));

    expect(item('running').getAttribute('aria-pressed')).toBe('true');
    expect(item('claimed').getAttribute('aria-pressed')).toBe('true');
  });

  it('is a button, so the keyboard reaches it and Enter and Space click it', () => {
    render(<LegendUnderTest />);

    for (const button of [item('running'), item('claimed')]) {
      expect(button.tagName).toBe('BUTTON');
      expect(button.getAttribute('type')).toBe('button');
      expect(button.hasAttribute('disabled')).toBe(false);
      expect(button.tabIndex).toBe(0);
    }
  });
});
