import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';

import { phaseFill, phaseLabel, phaseMeaning } from './phase';
import type { PhaseShare } from './timeline-geometry';

/** A mark of a view that is not a phase. */
export type LegendMark = 'paused' | 'end' | 'critical_row' | 'critical_edge' | 'tolerant_join';

function MarkSwatch({ mark }: { mark: LegendMark }) {
  switch (mark) {
    case 'paused':
      return (
        <span
          className="inline-block h-2.5 w-3 rounded-sm border border-border"
          style={{ background: 'var(--pause-band)' }}
          aria-hidden
        />
      );
    case 'end':
      return (
        <span
          className="inline-block size-2 rotate-45 border border-foreground"
          style={{ background: 'var(--card)' }}
          aria-hidden
        />
      );
    case 'critical_row':
    case 'critical_edge':
      return (
        <span
          className="inline-block h-0.5 w-3 rounded-full"
          style={{ background: 'var(--critical-path)' }}
          aria-hidden
        />
      );
    case 'tolerant_join':
      return <span className="inline-block h-2.5 w-3 rounded-sm border border-dashed border-foreground" aria-hidden />;
    default:
      return assertNever(mark, 'legend mark');
  }
}

function markText(mark: LegendMark): { label: string; meaning: string } {
  switch (mark) {
    case 'paused':
      return { label: 'paused', meaning: 'The run was paused: a band over every row.' };
    case 'end':
      return {
        label: 'end',
        meaning: 'The node reached its terminal status: a marker in its status color, not a duration.',
      };
    case 'critical_row':
      return {
        label: 'critical path',
        meaning:
          'The line under a row marks a node of the critical path: the chain of nodes whose end times decided the end of the run.',
      };
    case 'critical_edge':
      return {
        label: 'critical path',
        meaning:
          'The bold edges join the nodes of the critical path: the chain of nodes whose end times decided the end of the run.',
      };
    case 'tolerant_join':
      return {
        label: 'tolerant join',
        meaning: 'A dashed node runs even when a dependency failed.',
      };
    default:
      return assertNever(mark, 'legend mark');
  }
}

/**
 * The one legend of the run views: each phase of the run with its total
 * time, then the marks of the view. It wraps instead of clipping. Each item
 * states its meaning in its tooltip and to a screen reader.
 */
export function TimingLegend({
  shares,
  marks,
}: {
  shares: readonly PhaseShare[];
  marks: readonly LegendMark[];
}) {
  return (
    <ul
      aria-label="Legend"
      className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground"
    >
      {shares.map(share => (
        <li
          key={share.key}
          data-legend-phase={share.key}
          className="inline-flex items-center gap-1.5"
          title={phaseMeaning(share.key)}
        >
          <span
            className="inline-block h-2.5 w-3 rounded-sm"
            style={{ background: phaseFill(share.key) }}
            aria-hidden
          />
          <span>{phaseLabel(share.key)}</span>
          <span className="font-mono text-foreground tabular-nums">{formatMilliseconds(share.ms)}</span>
          <span className="sr-only">: {phaseMeaning(share.key)}</span>
        </li>
      ))}
      {marks.map(mark => {
        const text = markText(mark);
        return (
          <li
            key={mark}
            data-legend-mark={mark}
            className="inline-flex items-center gap-1.5"
            title={text.meaning}
          >
            <MarkSwatch mark={mark} />
            <span>{text.label}</span>
            <span className="sr-only">: {text.meaning}</span>
          </li>
        );
      })}
    </ul>
  );
}
