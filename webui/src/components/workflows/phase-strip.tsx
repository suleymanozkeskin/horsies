import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import type { NodeTiming } from '@/types/timing';

import { phaseFill, phaseLabel } from './phase';
import { phaseComposition, type PhaseShare } from './timeline-geometry';
import { nodeCoverageText } from './timing-text';

/** The time of each phase in words: `enqueued 1.0s, running 4.2s`. */
export function compositionText(shares: readonly PhaseShare[]): string {
  return shares.length === 0
    ? 'no measured time'
    : shares.map(share => `${phaseLabel(share.key)} ${formatMilliseconds(share.ms)}`).join(', ');
}

/** The accessible name of a phase strip: its coverage and its phase times. */
export function phaseStripText(timing: NodeTiming, drawAtMs: number): string {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return `${nodeCoverageText(timing)}; ${compositionText(phaseComposition(timing.segments, drawAtMs))}`;
    case 'unavailable':
    case 'inconsistent':
      return nodeCoverageText(timing);
    default:
      return assertNever(timing, 'node timing');
  }
}

/**
 * Thin normalized phase composition of one node: the time of each phase,
 * summed over retries and tasks, as a share of the node total, in lifecycle
 * order. A node with unavailable timing shows a hatch, an inconsistent node
 * an error hatch. The strip is an image with its numbers in its name.
 */
export function PhaseStrip({
  timing,
  drawAtMs,
}: {
  timing: NodeTiming;
  drawAtMs: number;
}) {
  const label = phaseStripText(timing, drawAtMs);
  switch (timing.kind) {
    case 'exact':
    case 'partial': {
      const shares = phaseComposition(timing.segments, drawAtMs);
      return (
        <div
          role="img"
          aria-label={label}
          title={label}
          data-coverage={timing.kind}
          className="flex h-1.5 w-full overflow-hidden rounded-full bg-muted"
        >
          {shares.map(share => (
            <span
              key={share.key}
              data-phase={share.key}
              className="h-full"
              style={{ width: `${share.fraction * 100}%`, background: phaseFill(share.key) }}
            />
          ))}
        </div>
      );
    }
    case 'unavailable':
      return (
        <div
          role="img"
          aria-label={label}
          title={label}
          data-coverage={timing.kind}
          className="h-1.5 w-full rounded-full"
          style={{ background: 'var(--timing-unavailable-hatch)' }}
        />
      );
    case 'inconsistent':
      return (
        <div
          role="img"
          aria-label={label}
          title={label}
          data-coverage={timing.kind}
          className="h-1.5 w-full rounded-full"
          style={{ background: 'var(--timing-inconsistent-hatch)' }}
        />
      );
    default:
      return assertNever(timing, 'node timing');
  }
}
