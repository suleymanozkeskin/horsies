// The nine phases of the lifecycle record: their order, labels, meanings and
// fills. A fill is a token of tokens.css (a color, or a hatch for `unknown`),
// so both themes draw from one palette.

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

/** Lowercase label, in the style of the status labels. */
export function phaseLabel(key: PhaseKey): string {
  switch (key) {
    case 'pending':
      return 'pending';
    case 'ready':
      return 'ready';
    case 'enqueued':
      return 'enqueued';
    case 'retry_wait':
      return 'retry wait';
    case 'claimed':
      return 'claimed';
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

/** What the phase means, for the legend. */
export function phaseMeaning(key: PhaseKey): string {
  switch (key) {
    case 'pending':
      return 'The node waits for its dependencies.';
    case 'ready':
      return 'The dependencies are met; nothing is dispatched.';
    case 'enqueued':
      return 'The attempt is due and waits for a claim. A sub-workflow node waits for its child start.';
    case 'retry_wait':
      return 'The retry delay of the attempt has not passed.';
    case 'claimed':
      return 'A worker holds a claim; the task code did not start.';
    case 'running':
      return 'The task code runs. A sub-workflow node: the child run runs.';
    case 'finalizing':
      return 'The task body ended; the attempt is not recorded yet.';
    case 'advancing':
      return 'The task is in history, or the child run ended; the workflow has not applied the node.';
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
      return 'var(--phase-pending)';
    case 'ready':
      return 'var(--phase-ready)';
    case 'enqueued':
      return 'var(--phase-enqueued)';
    case 'retry_wait':
      return 'var(--phase-retry-wait)';
    case 'claimed':
      return 'var(--phase-claimed)';
    case 'running':
      return 'var(--phase-running)';
    case 'finalizing':
      return 'var(--phase-finalizing)';
    case 'advancing':
      return 'var(--phase-advancing)';
    case 'unknown':
      return 'var(--phase-unknown-hatch)';
    default:
      return assertNever(key, 'phase');
  }
}
