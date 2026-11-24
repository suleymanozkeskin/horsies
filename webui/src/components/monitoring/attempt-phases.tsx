import { assertNever } from '@/lib/exhaustive';
import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import { instantMs } from '@/lib/time-text';
import type {
  AttemptClaims,
  AttemptFinalizing,
  AttemptRecordedBy,
} from '@/types/timing';
import type { TaskAttempt } from '@/types/workflows';

/** One recorded time of an attempt. */
export interface AttemptBoundary {
  label: string;
  atMs: number;
}

/** The phase times of an attempt in record order, with its claim count and
 * its writer, or `not_captured` for a row from before the lifecycle record. */
export type AttemptPhaseTimes =
  | { kind: 'not_captured' }
  | {
      kind: 'captured';
      boundaries: AttemptBoundary[];
      claims: string;
      recordedBy: string;
    };

function claimsText(claims: AttemptClaims): string {
  switch (claims.kind) {
    case 'counted':
      return claims.count === 1 ? '1 claim' : `${claims.count} claims`;
    case 'not_counted':
      return 'claims not counted';
    default:
      return assertNever(claims, 'attempt claims');
  }
}

function recordedByText(recordedBy: AttemptRecordedBy): string {
  switch (recordedBy.kind) {
    case 'worker':
      switch (recordedBy.placement.kind) {
        case 'placed':
          return 'recorded by the worker';
        case 'not_observed':
          return 'recorded by the worker; placement not observed';
        default:
          return assertNever(recordedBy.placement, 'attempt placement');
      }
    case 'recovery':
      return 'recorded by stale recovery; the worker was lost after the last sign of life';
    case 'cancel':
      return 'recorded by the cancel; an operator cancelled the task while it ran';
    default:
      return assertNever(recordedBy, 'attempt writer');
  }
}

function firstClaimTimes(claims: AttemptClaims): AttemptBoundary[] {
  switch (claims.kind) {
    case 'counted':
      // With one claim, the first claim is the claim of the attempt.
      return claims.count > 1
        ? [{ label: 'first claim', atMs: instantMs(claims.first_claimed_at) }]
        : [];
    case 'not_counted':
      return [];
    default:
      return assertNever(claims, 'attempt claims');
  }
}

function placementTimes(recordedBy: AttemptRecordedBy): AttemptBoundary[] {
  switch (recordedBy.kind) {
    case 'worker':
      switch (recordedBy.placement.kind) {
        case 'placed':
          return [{ label: 'placed', atMs: instantMs(recordedBy.placement.placed_at) }];
        case 'not_observed':
          return [];
        default:
          return assertNever(recordedBy.placement, 'attempt placement');
      }
    case 'recovery':
    case 'cancel':
      return [];
    default:
      return assertNever(recordedBy, 'attempt writer');
  }
}

function finalizingTimes(finalizing: AttemptFinalizing): AttemptBoundary[] {
  switch (finalizing.kind) {
    case 'marked':
      return [{ label: 'finalizing', atMs: instantMs(finalizing.finalizing_at) }];
    case 'not_marked':
      return [];
    default:
      return assertNever(finalizing, 'attempt finalizing');
  }
}

function lastSeenTimes(recordedBy: AttemptRecordedBy): AttemptBoundary[] {
  switch (recordedBy.kind) {
    case 'recovery':
      return [{ label: 'last seen', atMs: instantMs(recordedBy.last_seen_at) }];
    case 'worker':
    case 'cancel':
      return [];
    default:
      return assertNever(recordedBy, 'attempt writer');
  }
}

const optionalTime = (label: string, text: string | null): AttemptBoundary[] =>
  text === null ? [] : [{ label, atMs: instantMs(text) }];

/**
 * The recorded times of an attempt, in the order of its lifecycle: due,
 * first claim (only with more than one claim), claim, placement on a child,
 * start, finalizing marker, last sign of life (recovery only), recorded. A
 * time the record does not hold is left out. The times keep their record
 * order even when a clock step puts two of them in the wrong order.
 */
export function attemptPhaseTimes(attempt: TaskAttempt): AttemptPhaseTimes {
  const phases = attempt.phases;
  switch (phases.kind) {
    case 'not_captured':
      return { kind: 'not_captured' };
    case 'captured':
      return {
        kind: 'captured',
        boundaries: [
          { label: 'due', atMs: instantMs(phases.enqueued_at) },
          ...firstClaimTimes(phases.claims),
          { label: 'claimed', atMs: instantMs(phases.claimed_at) },
          ...placementTimes(phases.recorded_by),
          ...optionalTime('started', attempt.started_at),
          ...finalizingTimes(phases.finalizing),
          ...lastSeenTimes(phases.recorded_by),
          ...optionalTime('recorded', attempt.finished_at),
        ],
        claims: claimsText(phases.claims),
        recordedBy: recordedByText(phases.recorded_by),
      };
    default:
      return assertNever(phases, 'attempt phases');
  }
}

/** The time from the previous boundary, with its sign. */
const stepText = (ms: number): string =>
  ms < 0 ? formatMilliseconds(ms) : `+${formatMilliseconds(ms)}`;

/** The phase times of one attempt: each time with the time since the one
 * before it. */
export function AttemptPhases({ attempt }: { attempt: TaskAttempt }) {
  const times = attemptPhaseTimes(attempt);
  switch (times.kind) {
    case 'not_captured':
      return (
        <p className="mt-2 text-10 text-muted-foreground">
          Phase times not captured: the attempt was recorded before the lifecycle record.
        </p>
      );
    case 'captured':
      return (
        <div className="mt-2" role="group" aria-label="Phase times">
          <dl className="grid grid-cols-[auto_1fr_auto] gap-x-3 gap-y-0.5 text-10">
            {times.boundaries.map((boundary, index) => {
              const previous = times.boundaries[index - 1];
              return (
                <div key={boundary.label} className="contents">
                  <dt className="text-muted-foreground">{boundary.label}</dt>
                  <dd className="font-mono">{formatInstant(boundary.atMs)}</dd>
                  <dd className="text-right font-mono text-muted-foreground">
                    {previous === undefined ? '' : stepText(boundary.atMs - previous.atMs)}
                  </dd>
                </div>
              );
            })}
          </dl>
          <p className="mt-1 text-10 text-muted-foreground">
            {times.claims} · {times.recordedBy}
          </p>
        </div>
      );
    default:
      return assertNever(times, 'attempt phase times');
  }
}
