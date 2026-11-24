import { TriangleAlert } from 'lucide-react';

import { Hint } from '@/components/ui/hint';
import { assertNever } from '@/lib/exhaustive';

import { phaseLabel, phaseMeaning, phaseWeight, type PhaseWeight } from './phase';
import type { PhaseTotal } from './timeline-geometry';
import { durationText } from './timing-text';

/** One named time of a timing view: its label, its time, how loud it is, and
 * what it is. */
export interface TimeFigure {
  /** Stable key in its list. */
  id: string;
  label: string;
  ms: number;
  /** True when the time has no end yet: it shows as a time so far. */
  open: boolean;
  weight: PhaseWeight;
  /** What the time is, for its hint. */
  meaning: string;
}

/** The figure of one phase total. Its weight comes from its longest
 * interval. */
export function phaseFigure(total: PhaseTotal): TimeFigure {
  return {
    id: total.key,
    label: phaseLabel(total.key),
    ms: total.ms,
    open: total.open,
    weight: phaseWeight(total.key, total.longestMs),
    meaning: phaseMeaning(total.key),
  };
}

/** The hint of a figure: what the time is, then what a marked time tells. */
export function figureHint(figure: TimeFigure): string {
  switch (figure.weight.kind) {
    case 'main':
    case 'plain':
    case 'quiet':
      return figure.meaning;
    case 'marked':
      return `${figure.meaning} ${figure.weight.tells}`;
    default:
      return assertNever(figure.weight, 'phase weight');
  }
}

/** How the figures are laid out. `stacked`: the label over the time; the
 * figures have one smallest width, so the same phases line up down a list of
 * cards. `inline`: label and time side by side. Both wrap; no label is cut. */
export type FiguresLayout = 'stacked' | 'inline';

function FigureTime({ figure }: { figure: TimeFigure }) {
  const text = durationText(figure.ms, figure.open);
  switch (figure.weight.kind) {
    case 'main':
      return <span className="font-mono text-xs font-semibold tabular-nums text-foreground">{text}</span>;
    case 'plain':
      return <span className="font-mono text-xs tabular-nums text-foreground">{text}</span>;
    case 'quiet':
      return <span className="font-mono text-xs tabular-nums text-muted-foreground">{text}</span>;
    case 'marked':
      return (
        <span
          className="inline-flex items-center gap-1 font-mono text-xs font-semibold tabular-nums"
          style={{ color: 'var(--marked-ink)' }}
        >
          <TriangleAlert className="size-3 shrink-0" aria-hidden />
          {text}
        </span>
      );
    default:
      return assertNever(figure.weight, 'phase weight');
  }
}

/**
 * Times as named numbers. A proportional bar cannot show them: an engine step
 * of a few milliseconds beside a task code of seconds is less than a pixel.
 * Each figure has its weight: the task code is bold, a planned wait is plain,
 * an engine step in its normal range is quiet, and a long engine step is
 * marked. Each figure explains itself on hover.
 */
export function TimeFigures({
  figures,
  layout,
  label,
}: {
  figures: readonly TimeFigure[];
  layout: FiguresLayout;
  /** The accessible name of the list. */
  label: string;
}) {
  switch (layout) {
    case 'stacked':
      return (
        <ul aria-label={label} className="flex flex-wrap gap-x-4 gap-y-1.5">
          {figures.map(figure => (
            <li key={figure.id} data-figure={figure.id} data-weight={figure.weight.kind} className="min-w-16">
              <Hint text={figureHint(figure)} className="flex flex-col gap-px">
                <span className="whitespace-nowrap text-10 leading-tight text-muted-foreground">
                  {figure.label}
                </span>
                <FigureTime figure={figure} />
              </Hint>
            </li>
          ))}
        </ul>
      );
    case 'inline':
      return (
        <ul aria-label={label} className="flex flex-wrap items-baseline gap-x-4 gap-y-0.5">
          {figures.map(figure => (
            <li key={figure.id} data-figure={figure.id} data-weight={figure.weight.kind}>
              <Hint text={figureHint(figure)} className="inline-flex items-baseline gap-1.5 whitespace-nowrap">
                <span className="text-xs text-muted-foreground">{figure.label}</span>
                <FigureTime figure={figure} />
              </Hint>
            </li>
          ))}
        </ul>
      );
    default:
      return assertNever(layout, 'figures layout');
  }
}
