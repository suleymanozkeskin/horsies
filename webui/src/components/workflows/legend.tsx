import { Hint } from '@/components/ui/hint';
import { assertNever } from '@/lib/exhaustive';

/** A mark of the graph: the bold edges of the longest chain, and the dashed
 * border of a tolerant join. */
export type LegendMark = 'chain_edge' | 'tolerant_join';

function MarkSwatch({ mark }: { mark: LegendMark }) {
  switch (mark) {
    case 'chain_edge':
      return (
        <span
          className="inline-block h-0.5 w-3 rounded-full"
          style={{ background: 'var(--longest-chain)' }}
          aria-hidden
        />
      );
    case 'tolerant_join':
      return <span className="inline-block h-2.5 w-3 rounded-sm border border-dashed border-foreground" aria-hidden />;
    default:
      return assertNever(mark, 'legend mark');
  }
}

export function markText(mark: LegendMark): { label: string; meaning: string } {
  switch (mark) {
    case 'chain_edge':
      return {
        label: 'longest chain',
        meaning:
          'The bold edges join the nodes of the longest chain: the chain of nodes whose end times decided the end of the run.',
      };
    case 'tolerant_join':
      return {
        label: 'tolerant join',
        meaning: 'A node with a dashed border runs even when a dependency failed.',
      };
    default:
      return assertNever(mark, 'legend mark');
  }
}

/**
 * The key to the marks of the graph. It wraps instead of clipping. Each item
 * gives its meaning on hover and to a screen reader.
 */
export function TimingLegend({ marks }: { marks: readonly LegendMark[] }) {
  if (marks.length === 0) {
    return null;
  }
  return (
    <ul
      aria-label="Legend"
      className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground"
    >
      {marks.map(mark => {
        const text = markText(mark);
        return (
          <li key={mark} data-legend-mark={mark}>
            <Hint text={text.meaning} className="inline-flex items-center gap-1.5">
              <MarkSwatch mark={mark} />
              <span>{text.label}</span>
            </Hint>
          </li>
        );
      })}
    </ul>
  );
}
