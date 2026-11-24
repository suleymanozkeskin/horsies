import { Hint } from '@/components/ui/hint';
import { assertNever } from '@/lib/exhaustive';

import { phaseFill, phaseLabel, phaseMeaning, phaseTrack, type PhaseKey } from './phase';

/** A mark of a view that is not a phase. */
export type LegendMark = 'paused' | 'end' | 'critical_row' | 'critical_edge' | 'tolerant_join';

function PhaseSwatch({ phase }: { phase: PhaseKey }) {
  const track = phaseTrack(phase);
  switch (track) {
    case 'thin':
      return (
        <span
          className="inline-block h-1 w-3 rounded-full"
          style={{ background: phaseFill(phase) }}
          aria-hidden
        />
      );
    case 'full':
      return (
        <span
          className="inline-block h-2.5 w-3 rounded-sm"
          style={{ background: phaseFill(phase) }}
          aria-hidden
        />
      );
    default:
      return assertNever(track, 'phase track');
  }
}

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

export function markText(mark: LegendMark): { label: string; meaning: string } {
  switch (mark) {
    case 'paused':
      return {
        label: 'run paused',
        meaning: 'The run was paused: a band over every row, from the pause to the resume, to the end of the run, or to now.',
      };
    case 'end':
      return {
        label: 'end of a node',
        meaning: 'The time at which a node got its terminal status, in the color of that status. It is a point in time, not a duration.',
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
        meaning: 'A node with a dashed border runs even when a dependency failed.',
      };
    default:
      return assertNever(mark, 'legend mark');
  }
}

/**
 * The key to the drawing of a run view: the fill of each phase that the view
 * draws, then the marks of the view. It wraps instead of clipping. Each item
 * gives its meaning on hover and to a screen reader.
 */
export function TimingLegend({
  phases,
  marks,
}: {
  phases: readonly PhaseKey[];
  marks: readonly LegendMark[];
}) {
  if (phases.length === 0 && marks.length === 0) {
    return null;
  }
  return (
    <ul
      aria-label="Legend"
      className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground"
    >
      {phases.map(phase => (
        <li key={phase} data-legend-phase={phase}>
          <Hint text={phaseMeaning(phase)} className="inline-flex items-center gap-1.5">
            <PhaseSwatch phase={phase} />
            <span>{phaseLabel(phase)}</span>
          </Hint>
        </li>
      ))}
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
