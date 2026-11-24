import { assertNever } from '@/lib/exhaustive';

import { phaseFill, phaseTrack, type PhaseKey } from './phase';

/** One part of a phase bar: a phase, its time, and its tooltip. */
export interface PhaseBarPart {
  key: PhaseKey;
  ms: number;
  title: string;
}

/** The smallest width of a part, so a phase of a few milliseconds still
 * shows; its time and its tooltip do not change. */
const MIN_PART_WIDTH_PX = 3;

/** Bar height: `regular` for a summary, `slim` inside a list. */
export type PhaseBarSize = 'regular' | 'slim';

const heightClass = (size: PhaseBarSize): string => {
  switch (size) {
    case 'regular':
      return 'h-3';
    case 'slim':
      return 'h-2';
    default:
      return assertNever(size, 'phase bar size');
  }
};

function PartFill({ part }: { part: PhaseBarPart }) {
  const track = phaseTrack(part.key);
  switch (track) {
    case 'thin':
      return (
        <span
          className="absolute inset-x-0 top-1/2 h-1/3 -translate-y-1/2 rounded-full"
          style={{ background: phaseFill(part.key) }}
        />
      );
    case 'full':
      return <span className="absolute inset-0" style={{ background: phaseFill(part.key) }} />;
    default:
      return assertNever(track, 'phase track');
  }
}

/**
 * A proportional bar of phase parts in the given order. Each part is as wide
 * as its share of the time, and at least a few pixels; a one-pixel gap marks
 * each boundary. Dependency wait is a thin track. Each part has a tooltip;
 * the bar is an image whose name is `label`.
 */
export function PhaseBar({
  parts,
  label,
  size,
}: {
  parts: readonly PhaseBarPart[];
  label: string;
  size: PhaseBarSize;
}) {
  return (
    <div
      role="img"
      aria-label={label}
      className={`flex w-full gap-px overflow-hidden rounded-sm ${heightClass(size)}`}
    >
      {parts.map((part, index) => (
        <span
          key={index}
          data-phase={part.key}
          title={part.title}
          className="relative h-full"
          style={{
            flexGrow: Math.max(part.ms, 0),
            flexShrink: 1,
            flexBasis: 0,
            minWidth: MIN_PART_WIDTH_PX,
          }}
        >
          <PartFill part={part} />
        </span>
      ))}
    </div>
  );
}
