import { assertNever } from '@/lib/exhaustive';
import { statusColorVar } from '@/lib/status-utils';

/** The sign of an outcome: a check for a completed task or node, a cross
 * for a failed one, an open circle for a skipped node, and a dash in the
 * status color for every other status. */
export type OutcomeSign = 'completed' | 'failed' | 'skipped' | 'other';

export function outcomeSign(status: string): OutcomeSign {
  switch (status) {
    case 'COMPLETED':
    case 'SUCCESS':
      return 'completed';
    case 'FAILED':
    case 'WORKER_FAILURE':
      return 'failed';
    case 'SKIPPED':
      return 'skipped';
    default:
      return 'other';
  }
}

const STROKE = {
  fill: 'none',
  stroke: 'var(--background)',
  strokeWidth: 1.5,
  strokeLinecap: 'round',
  strokeLinejoin: 'round',
} as const;

function SignShape({ sign, color }: { sign: OutcomeSign; color: string }) {
  switch (sign) {
    case 'completed':
      return (
        <>
          <circle cx="6" cy="6" r="6" fill="var(--success)" />
          <path d="M3.3 6.3l1.8 1.8 3.6-3.9" {...STROKE} />
        </>
      );
    case 'failed':
      return (
        <>
          <circle cx="6" cy="6" r="6" fill="var(--error)" />
          <path d="M3.9 3.9l4.2 4.2M8.1 3.9l-4.2 4.2" {...STROKE} />
        </>
      );
    case 'skipped':
      return (
        <>
          <circle cx="6" cy="6" r="5.25" fill="var(--background)" stroke="var(--muted-foreground)" strokeWidth={1.5} />
          <path d="M3.7 6h4.6" {...STROKE} stroke="var(--muted-foreground)" />
        </>
      );
    case 'other':
      return (
        <>
          <circle cx="6" cy="6" r="6" fill={color} />
          <path d="M3.7 6h4.6" {...STROKE} />
        </>
      );
    default:
      return assertNever(sign, 'outcome sign');
  }
}

/** The sign of `status` as a circle of `sizePx`. The text next to it names
 * the status, so the sign is hidden from a screen reader. */
export function OutcomeCap({ status, sizePx }: { status: string; sizePx: number }) {
  return (
    <svg
      data-outcome={outcomeSign(status)}
      className="block shrink-0"
      width={sizePx}
      height={sizePx}
      viewBox="0 0 12 12"
      aria-hidden
    >
      <SignShape sign={outcomeSign(status)} color={statusColorVar(status)} />
    </svg>
  );
}
