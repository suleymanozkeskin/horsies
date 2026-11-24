// The nine phases of the lifecycle record: their order, labels, meanings,
// fills and drawing. A fill is a token of tokens.css (a color, a stripe for a
// wait, or a hatch for `unknown`), so both themes draw from one palette.

import { assertNever } from '@/lib/exhaustive';
import type { SegmentPhase } from '@/types/timing';

/** A phase, without the data that a `claimed` or an `unknown` phase holds. */
export type PhaseKey = SegmentPhase['kind'];

/** Every phase, in lifecycle order: the order of the legend and of a phase
 * composition. */
export const PHASE_ORDER = [
  'pending',
  'ready',
  'enqueued',
  'retry_wait',
  'claimed',
  'running',
  'finalizing',
  'advancing',
  'unknown',
] as const satisfies readonly PhaseKey[];

// Compile-time check: PHASE_ORDER lists every phase of the API.
type UnlistedPhase = Exclude<PhaseKey, (typeof PHASE_ORDER)[number]>;
const everyPhaseListed: [UnlistedPhase] extends [never] ? true : never = true;
void everyPhaseListed;

/** How a timeline row draws the phase: dependency wait is a thin track under
 * the full-height phases, so the phases that explain a delay stand out. */
export type PhaseTrack = 'thin' | 'full';

export function phaseTrack(key: PhaseKey): PhaseTrack {
  switch (key) {
    case 'pending':
      return 'thin';
    case 'ready':
    case 'enqueued':
    case 'retry_wait':
    case 'claimed':
    case 'running':
    case 'finalizing':
    case 'advancing':
    case 'unknown':
      return 'full';
    default:
      return assertNever(key, 'phase');
  }
}

/** Lowercase label: the name of the time, not of the record state. */
export function phaseLabel(key: PhaseKey): string {
  switch (key) {
    case 'pending':
      return 'dependency wait';
    case 'ready':
      return 'ready';
    case 'enqueued':
      return 'queue wait';
    case 'retry_wait':
      return 'retry wait';
    case 'claimed':
      return 'slot wait';
    case 'running':
      return 'running';
    case 'finalizing':
      return 'finalizing';
    case 'advancing':
      return 'advancing';
    case 'unknown':
      return 'unknown';
    default:
      return assertNever(key, 'phase');
  }
}

/** What the phase means, for the legend and the tooltips. */
export function phaseMeaning(key: PhaseKey): string {
  switch (key) {
    case 'pending':
      return 'The node waits for its dependencies.';
    case 'ready':
      return 'The dependencies are met; nothing is dispatched yet.';
    case 'enqueued':
      return 'The attempt is due and waits in the queue for a claim. A sub-workflow node waits for its child start.';
    case 'retry_wait':
      return 'The retry delay of the attempt has not passed.';
    case 'claimed':
      return 'A worker holds a claim and the task code did not start: the wait for a free slot (a child), then the start. Claims that lost their lease are slot wait too.';
    case 'running':
      return 'The task code runs. A sub-workflow node: the child run runs.';
    case 'finalizing':
      return 'The task body ended; the attempt is not recorded yet.';
    case 'advancing':
      return 'The task is in history, or the child run ended; the workflow has not applied the node yet.';
    case 'unknown':
      return 'A known interval whose phase the record cannot show.';
    default:
      return assertNever(key, 'phase');
  }
}

/** CSS `background` value of the phase: a token. */
export function phaseFill(key: PhaseKey): string {
  switch (key) {
    case 'pending':
      return 'var(--phase-pending-fill)';
    case 'ready':
      return 'var(--phase-ready-fill)';
    case 'enqueued':
      return 'var(--phase-enqueued-fill)';
    case 'retry_wait':
      return 'var(--phase-retry-wait-fill)';
    case 'claimed':
      return 'var(--phase-claimed-fill)';
    case 'running':
      return 'var(--phase-running-fill)';
    case 'finalizing':
      return 'var(--phase-finalizing-fill)';
    case 'advancing':
      return 'var(--phase-advancing-fill)';
    case 'unknown':
      return 'var(--phase-unknown-hatch)';
    default:
      return assertNever(key, 'phase');
  }
}

/** CSS `color` of a text drawn on the fill of the phase: a token. */
export function phaseInk(key: PhaseKey): string {
  switch (key) {
    case 'running':
      return 'var(--phase-running-ink)';
    case 'pending':
    case 'ready':
    case 'enqueued':
    case 'retry_wait':
    case 'claimed':
    case 'finalizing':
    case 'advancing':
      return 'var(--phase-ink)';
    case 'unknown':
      return 'var(--foreground)';
    default:
      return assertNever(key, 'phase');
  }
}
