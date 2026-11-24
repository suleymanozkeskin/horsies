import { assertNever } from '@/lib/exhaustive';
import { cn } from '@/lib/utils';

import {
  isAlone,
  isSeriesShown,
  type SeriesSelection,
} from './series-selection';

/** The mark of a series in its chart. The legend key has the same form. */
export type SeriesMark = 'area' | 'line';

/** One series of a chart, as its legend names it. */
export interface LegendSeries<Key extends string> {
  readonly key: Key;
  readonly label: string;
  /** A color token reference, for example `var(--info)`. */
  readonly color: string;
  readonly mark: SeriesMark;
}

/** The legend key of a series: a block for an area, a stroke for a line. A
 * series that is not drawn has an empty key. */
function SeriesKey({
  mark,
  color,
  shown,
}: {
  mark: SeriesMark;
  color: string;
  shown: boolean;
}) {
  switch (mark) {
    case 'area':
      return (
        <span
          className="size-2.5 shrink-0 rounded-xs border"
          style={{
            borderColor: shown ? color : 'var(--muted-foreground)',
            background: shown ? color : 'transparent',
          }}
          aria-hidden
        />
      );
    case 'line':
      return (
        <span
          className="h-0.5 w-3.5 shrink-0 rounded-full"
          style={{ background: shown ? color : 'var(--muted)' }}
          aria-hidden
        />
      );
    default:
      return assertNever(mark, 'series mark');
  }
}

/**
 * The legend of a chart. Each item is a button: a click draws that series
 * alone, and a click on a series that is alone draws every series again.
 * The item shows whether its series is drawn: a filled key and plain text,
 * or an empty key and struck-through text. `aria-pressed` gives the same
 * state to a screen reader.
 */
export function SeriesLegend<Key extends string>({
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
}) {
  return (
    <div
      role="group"
      aria-label={`Series of ${chart}`}
      className="flex flex-wrap items-center gap-1"
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
              'inline-flex items-center gap-1.5 rounded px-1.5 py-0.5 text-11 transition-colors hover:bg-accent-surface focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring',
              shown ? 'text-foreground' : 'text-muted-foreground line-through'
            )}
          >
            <SeriesKey mark={entry.mark} color={entry.color} shown={shown} />
            {entry.label}
          </button>
        );
      })}
    </div>
  );
}
