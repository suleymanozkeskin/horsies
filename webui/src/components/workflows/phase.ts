// The phases of the lifecycle record as the timing views draw them: their
// order, labels, meanings, fills and tracks. A fill is a token of tokens.css
// (a color, a stripe for a wait, or a hatch for a gap of the record), so both
// themes draw from one palette.

import { assertNever } from '@/lib/exhaustive';
import type { SegmentClaim, SegmentPhase, UnknownReason } from '@/types/timing';

/** A phase of the API, without the data that a `claimed` or an `unknown`
 * phase holds. */
export type PhaseKey = SegmentPhase['kind'];

/** A phase as the timing views draw it: a phase of the API, or one of two
 * parts that have a name of their own. `earlier_claims`: the claims before
 * the claim that ran the attempt (a `claimed` phase). `worker_lost`: the time
 * after the last sign of life of a lost worker (an `unknown` phase). */
export type TimingKind = PhaseKey | 'earlier_claims' | 'worker_lost';

/** Every kind, in the order of the keys, the bars and the lanes: the phases
 * of a node and of an attempt in time order, then the gaps of the record,
 * then the retry wait between two attempts. */
export const TIMING_KIND_ORDER = [
  'pending',
  'ready',
  'enqueued',
  'earlier_claims',
  'claimed',
  'running',
  'finalizing',
  'advancing',
  'worker_lost',
  'unknown',
  'retry_wait',
] as const satisfies readonly TimingKind[];

// Compile-time check: TIMING_KIND_ORDER lists every kind.
type UnlistedKind = Exclude<TimingKind, (typeof TIMING_KIND_ORDER)[number]>;
const everyKindListed: [UnlistedKind] extends [never] ? true : never = true;
void everyKindListed;

function claimKind(claim: SegmentClaim): TimingKind {
  switch (claim.kind) {
    case 'earlier':
      return 'earlier_claims';
    case 'recorded':
    case 'not_recorded':
      return 'claimed';
    default:
      return assertNever(claim, 'segment claim');
  }
}

function unknownKind(reason: UnknownReason): TimingKind {
  switch (reason) {
    case 'WORKER_LOST':
      return 'worker_lost';
    case 'ATTEMPT_NOT_CAPTURED':
    case 'CLAIMS_NOT_COUNTED':
    case 'HISTORY_PRUNED':
      return 'unknown';
    default:
      return assertNever(reason, 'unknown reason');
  }
}

/** The kind of the phase of a segment. */
export function segmentKind(phase: SegmentPhase): TimingKind {
  switch (phase.kind) {
    case 'claimed':
      return claimKind(phase.claim);
    case 'unknown':
      return unknownKind(phase.reason);
    case 'pending':
    case 'ready':
    case 'enqueued':
    case 'retry_wait':
    case 'running':
    case 'finalizing':
    case 'advancing':
      return phase.kind;
    default:
      return assertNever(phase, 'segment phase');
  }
}

/** How a timeline row draws the kind: dependency wait is a thin line under
 * the full-height phases, so the phases that explain a delay stand out. */
export type PhaseTrack = 'thin' | 'full';

export function phaseTrack(kind: TimingKind): PhaseTrack {
  switch (kind) {
    case 'pending':
      return 'thin';
    case 'ready':
    case 'enqueued':
    case 'earlier_claims':
    case 'claimed':
    case 'running':
    case 'finalizing':
    case 'advancing':
    case 'worker_lost':
    case 'unknown':
    case 'retry_wait':
      return 'full';
    default:
      return assertNever(kind, 'timing kind');
  }
}

/** Lowercase label: the name of the time, not of the record state. */
export function phaseLabel(kind: TimingKind): string {
  switch (kind) {
    case 'pending':
      return 'dependency wait';
    case 'ready':
      return 'ready';
    case 'enqueued':
      return 'queue wait';
    case 'earlier_claims':
      return 'earlier claims';
    case 'claimed':
      return 'slot wait';
    case 'running':
      return 'running';
    case 'finalizing':
      return 'finalizing';
    case 'advancing':
      return 'advancing';
    case 'worker_lost':
      return 'worker lost';
    case 'unknown':
      return 'unknown';
    case 'retry_wait':
      return 'retry wait';
    default:
      return assertNever(kind, 'timing kind');
  }
}

/** What the time of the kind is, from its start to its end, as a short
 * phrase for the key and the tooltips. */
export function phaseMeaning(kind: TimingKind): string {
  switch (kind) {
    case 'pending':
      return 'the node waits for its dependencies';
    case 'ready':
      return 'dependencies met → task of the node';
    case 'enqueued':
      return 'due time → claim by a worker';
    case 'earlier_claims':
      return 'first claim → the claim that ran the attempt';
    case 'claimed':
      return 'claim → start of the task code';
    case 'running':
      return 'the task code runs';
    case 'finalizing':
      return 'end of the task code → record of the attempt';
    case 'advancing':
      return 'end of the task → end of the node';
    case 'worker_lost':
      return 'last sign of life → record by recovery';
    case 'unknown':
      return 'the record does not show this phase';
    case 'retry_wait':
      return 'record of a failed attempt → due time of the next';
    default:
      return assertNever(kind, 'timing kind');
  }
}

/** CSS `background` value of the kind: a token. The two parts with a name of
 * their own keep the fill of their phase. */
export function phaseFill(kind: TimingKind): string {
  switch (kind) {
    case 'pending':
      return 'var(--phase-pending-fill)';
    case 'ready':
      return 'var(--phase-ready-fill)';
    case 'enqueued':
      return 'var(--phase-enqueued-fill)';
    case 'earlier_claims':
    case 'claimed':
      return 'var(--phase-claimed-fill)';
    case 'running':
      return 'var(--phase-running-fill)';
    case 'finalizing':
      return 'var(--phase-finalizing-fill)';
    case 'advancing':
      return 'var(--phase-advancing-fill)';
    case 'worker_lost':
    case 'unknown':
      return 'var(--phase-unknown-hatch)';
    case 'retry_wait':
      return 'var(--phase-retry-wait-fill)';
    default:
      return assertNever(kind, 'timing kind');
  }
}
