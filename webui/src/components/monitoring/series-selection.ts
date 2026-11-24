// Which series of a chart are drawn. Pure.

import { assertNever } from '@/lib/exhaustive';

/**
 * The series of one chart that are drawn: all of them, or one alone. A
 * selection with no series cannot be written, so a chart always draws at
 * least one.
 */
export type SeriesSelection<Key extends string> =
  | { readonly kind: 'all' }
  | { readonly kind: 'only'; readonly series: Key };

/** Every series is drawn: the state of a chart before a click. */
export const ALL_SERIES = { kind: 'all' } as const;

/** Whether `series` is drawn. */
export function isSeriesShown<Key extends string>(
  selection: SeriesSelection<Key>,
  series: Key
): boolean {
  switch (selection.kind) {
    case 'all':
      return true;
    case 'only':
      return selection.series === series;
    default:
      return assertNever(selection, 'series selection');
  }
}

/**
 * The selection after a click on the legend item of `clicked`: that series
 * alone, or every series again when it was already alone.
 */
export function selectionAfterClick<Key extends string>(
  selection: SeriesSelection<Key>,
  clicked: Key
): SeriesSelection<Key> {
  return isAlone(selection, clicked)
    ? ALL_SERIES
    : { kind: 'only', series: clicked };
}

/** Whether `series` is the one series that is drawn. */
export function isAlone<Key extends string>(
  selection: SeriesSelection<Key>,
  series: Key
): boolean {
  switch (selection.kind) {
    case 'all':
      return false;
    case 'only':
      return selection.series === series;
    default:
      return assertNever(selection, 'series selection');
  }
}
