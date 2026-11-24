// The nine phases of the lifecycle record: their order, labels, meanings,
// kinds, weights, fills and drawing. A fill is a token of tokens.css (a color,
// a stripe for a wait, or a hatch for `unknown`), so both themes draw from one
// palette.

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
      return 'The node waits for its dependencies. This time is the shape of the workflow, not a fault. A node also waits here while its run is paused.';
    case 'ready':
      return 'The dependencies are met, and the node has no task. This occurs after a pause or a recovery pass.';
    case 'enqueued':
      return 'From the due time of the attempt to its claim by a worker. For a sub-workflow node: the wait for its child start.';
    case 'retry_wait':
      return 'The delay of the retry policy: from the record of a failed attempt to the due time of the next attempt. Expected, not a fault.';
    case 'claimed':
      return 'From the claim to the start of the task code: the wait for a free child, then the start. Claims that lost their lease are slot wait too.';
    case 'running':
      return 'The task code runs. A sub-workflow node: the child run runs.';
    case 'finalizing':
      return 'From the end of the task code to the record of the attempt.';
    case 'advancing':
      return 'From the end of the task, or of the child run, to the end of the node in the workflow.';
    case 'unknown':
      return 'A known interval whose phase the record cannot show.';
    default:
      return assertNever(key, 'phase');
  }
}

/** An engine step, or a gap of the record, of this time or more is marked.
 * A normal engine step is a few database round trips: milliseconds with a
 * local database, and 3 round trips of 33 ms are 100 ms. The causes of a
 * long step (a queue at its concurrency limit, a child that starts, a lost
 * lease, the 5 s poll) give this time or more. */
export const LONG_ENGINE_STEP_MS = 250;

/**
 * How loud the time of a phase is. The weight follows what controls the
 * time, because the kinds differ in scale: an engine step is a few database
 * round trips (milliseconds), and the task code and the planned waits are
 * seconds or more.
 * - `main`: the task code (`running`). The author of the task controls it.
 * - `plain`: a planned wait: the dependency wait (the workflow definition)
 *   and the retry wait (the retry policy). Expected, not a fault.
 * - `quiet`: an engine step in its normal range (`ready`, `enqueued`,
 *   `claimed`, `finalizing`, `advancing`: the queue, the workers and the
 *   workflow steps), or a short gap of the record (`unknown`).
 * - `marked`: an engine step or a gap of the long step limit or more. It
 *   holds what the long time tells the reader.
 */
export type PhaseWeight =
  | { kind: 'main' }
  | { kind: 'plain' }
  | { kind: 'quiet' }
  | { kind: 'marked'; tells: string };

/** What a long time of the phase tells: its usual cause. */
function longStepTells(
  key: 'ready' | 'enqueued' | 'claimed' | 'finalizing' | 'advancing' | 'unknown'
): string {
  switch (key) {
    case 'enqueued':
      return 'No worker claimed the task in this time. The usual cause is capacity: the queue is at its concurrency limit, or no worker has a free slot.';
    case 'claimed':
      return 'A worker held the claim, and the task code did not start. The usual cause: no child was free, or a child was starting.';
    case 'finalizing':
      return 'The task code ended, and the record of the attempt came this much later. The outcome write waited for the database.';
    case 'advancing':
      return 'The task ended, and the workflow applied its end this much later. The worker did not apply it at once, so a recovery pass did.';
    case 'ready':
      return 'The node had no task in this time. A pause took its task, or a recovery pass repaired the node.';
    case 'unknown':
      return 'The record does not show which phase this time was.';
    default:
      return assertNever(key, 'long step');
  }
}

/** The weight of a phase time. `longestMs` is the time of the longest single
 * interval of the phase: a sum of short intervals is not a long step. */
export function phaseWeight(key: PhaseKey, longestMs: number): PhaseWeight {
  switch (key) {
    case 'running':
      return { kind: 'main' };
    case 'pending':
    case 'retry_wait':
      return { kind: 'plain' };
    case 'ready':
    case 'enqueued':
    case 'claimed':
    case 'finalizing':
    case 'advancing':
    case 'unknown':
      return longestMs >= LONG_ENGINE_STEP_MS
        ? { kind: 'marked', tells: longStepTells(key) }
        : { kind: 'quiet' };
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
