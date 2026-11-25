import { describe, expect, it } from 'vitest';

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
