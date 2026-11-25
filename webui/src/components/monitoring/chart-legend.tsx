import type { ReactNode } from 'react';

import { assertNever } from '@/lib/exhaustive';
import { cn } from '@/lib/utils';

import {
  isAlone,
  isSeriesShown,
  type SeriesSelection,
} from './series-selection';

/** The mark of a series in its chart. The legend icon has the same form. */
export type SeriesMark = 'area' | 'line';

/** One series of a chart, as its legend names it. */
export interface LegendSeries<Key extends string> {
  readonly key: Key;
  readonly label: string;
  /** A color token reference, for example `var(--info)`. */
  readonly color: string;
  readonly mark: SeriesMark;
}

/** The box of a legend icon, in CSS pixels. */
const ICON_WIDTH = 22;
const ICON_HEIGHT = 12;

/**
 * The shape of a legend icon in its box, in the series color.
 *
 * - area: a rounded block with a light fill and an outline.
 * - line: a short line with a hollow dot. The line stops at the dot and the
 *   dot has no fill, so the surface under the item, the hover wash included,
 *   shows through the dot.
 */
function SeriesIconShape({
  mark,
  color,
}: {
  mark: SeriesMark;
  color: string;
}): ReactNode {
  switch (mark) {
    case 'area':
      return (
        <rect
          x={1}
          y={1.5}
          width={20}
          height={9}
          rx={2.5}
          fill={color}
          fillOpacity={0.22}
          stroke={color}
          strokeWidth={1.6}
        />
      );
    case 'line':
      return (
        <>
          <path d="M1 6H8M14 6H21" stroke={color} strokeWidth={1.8} />
          <circle
            cx={11}
            cy={6}
            r={3}
            fill="none"
            stroke={color}
            strokeWidth={1.8}
          />
        </>
      );
    default:
      return assertNever(mark, 'series mark');
  }
}

/** The icon of a series in its legend item. The icon of a series that is not
 * drawn keeps its form and its color, at half strength. */
function SeriesIcon({
  mark,
  color,
  shown,
}: {
  mark: SeriesMark;
  color: string;
  shown: boolean;
}): ReactNode {
  return (
    <svg
      width={ICON_WIDTH}
      height={ICON_HEIGHT}
      viewBox={`0 0 ${ICON_WIDTH} ${ICON_HEIGHT}`}
      className={cn('shrink-0', !shown && 'opacity-50')}
      aria-hidden
    >
      <SeriesIconShape mark={mark} color={color} />
    </svg>
  );
}

/**
 * The legend of a chart, centered under it. Each item is a toggle button
 * with the icon of the series mark and the series name.
 *
 * - A click passes the series of the item to `onClick`. The owner of the
 *   selection computes the next selection.
 * - `aria-pressed` is true while the series is drawn. The item of a series
 *   that is not drawn has a dimmed icon and struck-through text. Its text
 *   keeps the muted text color, so it stays readable.
 * - The `title` of an item says what a click on it does.
 */
export function ChartLegend<Key extends string>({
  chart,
  series,
  selection,
  onClick,
}: {
  /** The name of the chart, for the accessible name of the group. */
  chart: string;
  series: readonly LegendSeries<Key>[];
  selection: SeriesSelection<Key>;
  onClick: (series: Key) => void;
}): ReactNode {
  return (
    <div
      role="group"
      aria-label={`Series of ${chart}`}
      className="flex flex-wrap items-center justify-center gap-x-1.5 gap-y-1"
    >
      {series.map(entry => {
        const shown = isSeriesShown(selection, entry.key);
        return (
          <button
            key={entry.key}
            type="button"
            aria-pressed={shown}
            onClick={() => onClick(entry.key)}
            title={
              isAlone(selection, entry.key)
                ? 'Show every series'
                : `Show only ${entry.label}`
            }
            className={cn(
              'inline-flex cursor-pointer items-center gap-1.5 rounded-full px-2.5 py-1 text-xs transition-colors hover:bg-surface-hover focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring',
              shown ? 'text-foreground' : 'text-muted-foreground line-through'
            )}
          >
            <SeriesIcon mark={entry.mark} color={entry.color} shown={shown} />
            {entry.label}
          </button>
        );
      })}
    </div>
  );
}
