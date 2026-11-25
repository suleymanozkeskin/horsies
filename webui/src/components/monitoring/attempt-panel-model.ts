// The model of the attempt panel: the attempts of one task as intervals in
// time order (each phase of each attempt, and the retry wait between two
// attempts), the key entries, the errors grouped by code and message, and
// what a hovered mark lights. Pure: no React, no DOM.

import { TIMING_KIND_ORDER, type TimingKind } from '@/components/workflows/phase';
import type { KeyLight, KeyPhase } from '@/components/workflows/timing-key';
import { earlierClaimsText, placementText, unknownReasonText } from '@/components/workflows/timing-text';
import { clockText, clockTextFrom } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import type { LeafTask } from '@/types/workflows';

import {
  attemptPhaseKind,
  type AttemptPhase,
  type AttemptStory,
  type LiveAttempt,
  type RecordedAttempt,
} from './attempt-story';

/** Whose time an interval is: a phase of an attempt, or the retry wait from
 * the record of one attempt to the due time of the next. */
export type IntervalOwner =
  | { kind: 'attempt'; attempt: number }
  | { kind: 'retry_wait'; fromAttempt: number; toAttempt: number };

/** One interval of the strip and of the bars. Two times in the wrong order
 * (a step of the database clock) give a negative length; the times keep
 * their record order. `detail`: what the interval holds beyond its kind. */
export interface PanelInterval {
  id: string;
  kind: TimingKind;
  owner: IntervalOwner;
  startMs: number;
  endMs: number;
  detail: string | null;
}

export const intervalMs = (interval: PanelInterval): number => interval.endMs - interval.startMs;

/** The attempt that an interval belongs to: its own attempt, or for a retry
 * wait the attempt before it. */
export function ownerAttempt(owner: IntervalOwner): number {
  switch (owner.kind) {
    case 'attempt':
      return owner.attempt;
    case 'retry_wait':
      return owner.fromAttempt;
    default:
      return assertNever(owner, 'interval owner');
  }
}

/** `1`, or `1→2` for the retry wait from attempt 1 to attempt 2. */
export function ownerText(owner: IntervalOwner): string {
  switch (owner.kind) {
    case 'attempt':
      return String(owner.attempt);
    case 'retry_wait':
      return `${owner.fromAttempt}→${owner.toAttempt}`;
    default:
      return assertNever(owner, 'interval owner');
  }
}

/** `attempt 2`, or `attempt 1 → 2` for a retry wait. */
export function ownerContext(owner: IntervalOwner): string {
  switch (owner.kind) {
    case 'attempt':
      return `attempt ${owner.attempt}`;
    case 'retry_wait':
      return `attempt ${owner.fromAttempt} → ${owner.toAttempt}`;
    default:
      return assertNever(owner, 'interval owner');
  }
}

/** What a phase of an attempt holds beyond its kind, in words. */
function phaseDetailText(phase: AttemptPhase): string | null {
  const detail = phase.detail;
  switch (detail.kind) {
    case 'earlier_claims':
      return earlierClaimsText(detail.count);
    case 'placed':
      return placementText(phase.startMs, detail.placedAtMs);
    case 'claims_not_counted':
      return unknownReasonText('CLAIMS_NOT_COUNTED');
    case 'not_captured':
      return unknownReasonText('ATTEMPT_NOT_CAPTURED');
    case 'none':
    case 'placement_not_observed':
    case 'worker_lost':
      return null;
    default:
      return assertNever(detail, 'attempt phase detail');
  }
}

function attemptIntervals(attempt: RecordedAttempt): PanelInterval[] {
  return attempt.phases.map((phase, index) => ({
    id: `phase:${attempt.number}:${index}`,
    kind: attemptPhaseKind(phase),
    owner: { kind: 'attempt', attempt: attempt.number },
    startMs: phase.startMs,
    endMs: phase.endMs,
    detail: phaseDetailText(phase),
  }));
}

/** The task as the panel draws it: its recorded attempts in attempt order,
 * its intervals in time order, and its attempt in progress. */
export interface PanelModel {
  attempts: RecordedAttempt[];
  intervals: PanelInterval[];
  live: { kind: 'live'; attempt: LiveAttempt } | { kind: 'not_live' };
}

export function panelModel(story: AttemptStory): PanelModel {
  const attempts: RecordedAttempt[] = [];
  const intervals: PanelInterval[] = [];
  let live: PanelModel['live'] = { kind: 'not_live' };
  for (const item of story.items) {
    switch (item.kind) {
      case 'attempt':
        attempts.push(item.attempt);
        intervals.push(...attemptIntervals(item.attempt));
        break;
      case 'retry_wait':
        intervals.push({
          id: `wait:${item.fromAttempt}:${item.toAttempt}`,
          kind: 'retry_wait',
          owner: { kind: 'retry_wait', fromAttempt: item.fromAttempt, toAttempt: item.toAttempt },
          startMs: item.startMs,
          endMs: item.endMs,
          detail: null,
        });
        break;
      case 'live':
        live = { kind: 'live', attempt: item.attempt };
        break;
      default:
        assertNever(item, 'story item');
    }
  }
  return { attempts, intervals, live };
}

/** The smallest span of a strip: one microsecond, so a task of one instant
 * still divides. */
const MIN_STRIP_SPAN_MS = 0.001;

/** The time of the strip: from the first due time to the last record. */
export type PanelSpan = { kind: 'span'; fromMs: number; toMs: number; spanMs: number } | { kind: 'no_times' };

export function panelSpan(intervals: readonly PanelInterval[]): PanelSpan {
  if (intervals.length === 0) {
    return { kind: 'no_times' };
  }
  const fromMs = Math.min(...intervals.map(interval => interval.startMs));
  const toMs = Math.max(...intervals.map(interval => interval.endMs));
  return { kind: 'span', fromMs, toMs, spanMs: Math.max(toMs - fromMs, MIN_STRIP_SPAN_MS) };
}

/** The first time of the task, from which a time of another day carries its
 * day offset; or none, for a task with no recorded time. */
export type PanelBase = { kind: 'at'; ms: number } | { kind: 'no_times' };

export function panelBase(span: PanelSpan): PanelBase {
  switch (span.kind) {
    case 'span':
      return { kind: 'at', ms: span.fromMs };
    case 'no_times':
      return { kind: 'no_times' };
    default:
      return assertNever(span, 'panel span');
  }
}

/** The time of day of `epochMs`, with its day offset from the base. */
export function clockFrom(base: PanelBase, epochMs: number): string {
  switch (base.kind) {
    case 'at':
      return clockTextFrom(base.ms, epochMs);
    case 'no_times':
      return clockText(epochMs);
    default:
      return assertNever(base, 'panel base');
  }
}

/** The kinds of the intervals in the order of the keys. */
export function panelKinds(intervals: readonly PanelInterval[]): TimingKind[] {
  return TIMING_KIND_ORDER.filter(kind => intervals.some(interval => interval.kind === kind));
}

/** The key entries of the panel: each kind of its intervals, with its total
 * over the attempts when the task has more than one attempt. An interval
 * whose times are in the wrong order counts as no time. */
export function panelKeyPhases(intervals: readonly PanelInterval[], attemptCount: number): KeyPhase[] {
  return panelKinds(intervals).map(kind => {
    const own = intervals.filter(interval => interval.kind === kind);
    return {
      phase: kind,
      total:
        attemptCount > 1
          ? {
              kind: 'total',
              ms: own.reduce((sum, interval) => sum + Math.max(intervalMs(interval), 0), 0),
              context: `total of ${own.length}`,
            }
          : { kind: 'no_total' },
    };
  });
}

/** The outcomes of the attempts for the key, each once, in attempt order. */
export function panelOutcomes(attempts: readonly RecordedAttempt[]): string[] {
  return [...new Set(attempts.map(attempt => attempt.outcome))];
}

/** The longest time of the intervals, for the log axis; no time is 0. */
export const longestIntervalMs = (intervals: readonly PanelInterval[]): number =>
  Math.max(0, ...intervals.map(intervalMs));

/** The error code of a group of errors. */
export type ErrorCodeOf = { kind: 'code'; code: string } | { kind: 'no_code' };

/** The message of a row of errors. */
export type ErrorMessage = { kind: 'text'; text: string } | { kind: 'no_message' };

/** One message of a group, once, with the attempts that have it. */
export interface ErrorRow {
  message: ErrorMessage;
  attempts: number[];
}

export interface ErrorGroup {
  code: ErrorCodeOf;
  rows: ErrorRow[];
}

const sameCode = (a: ErrorCodeOf, b: ErrorCodeOf): boolean =>
  a.kind === b.kind && (a.kind === 'no_code' || (b.kind === 'code' && a.code === b.code));

const sameMessage = (a: ErrorMessage, b: ErrorMessage): boolean =>
  a.kind === b.kind && (a.kind === 'no_message' || (b.kind === 'text' && a.text === b.text));

/** The errors of the attempts, grouped by code, then by message: each
 * message appears once, with the attempts that have it. An attempt with no
 * code and no message has no error. */
export function errorGroups(attempts: readonly RecordedAttempt[]): ErrorGroup[] {
  const groups: ErrorGroup[] = [];
  for (const attempt of attempts) {
    if (attempt.errorCode === null && attempt.message === null) {
      continue;
    }
    const code: ErrorCodeOf = attempt.errorCode === null ? { kind: 'no_code' } : { kind: 'code', code: attempt.errorCode };
    const message: ErrorMessage =
      attempt.message === null ? { kind: 'no_message' } : { kind: 'text', text: attempt.message };
    let group = groups.find(candidate => sameCode(candidate.code, code));
    if (group === undefined) {
      group = { code, rows: [] };
      groups.push(group);
    }
    const row = group.rows.find(candidate => sameMessage(candidate.message, message));
    if (row === undefined) {
      group.rows.push({ message, attempts: [attempt.number] });
    } else {
      row.attempts.push(attempt.number);
    }
  }
  return groups;
}

/** Attempt numbers as ranges: `1–4`, `1, 3`, `2`. The numbers are in
 * ascending order. */
export function rangeText(numbers: readonly number[]): string {
  const parts: string[] = [];
  let start: number | undefined;
  let last: number | undefined;
  for (const number of numbers) {
    if (last !== undefined && number === last + 1) {
      last = number;
      continue;
    }
    if (start !== undefined && last !== undefined) {
      parts.push(start === last ? String(start) : `${start}–${last}`);
    }
    start = number;
    last = number;
  }
  if (start !== undefined && last !== undefined) {
    parts.push(start === last ? String(start) : `${start}–${last}`);
  }
  return parts.join(', ');
}

/** The failure of the task that no attempt has as its message (an expiry,
 * or a failure outside an attempt), or null when the attempts show it. */
export function taskOnlyFailure(leaf: LeafTask, attempts: readonly RecordedAttempt[]): string | null {
  const reason = leaf.failed_reason;
  return reason === null || attempts.some(attempt => attempt.message === reason) ? null : reason;
}

/** How the bars are grouped: one group for each phase with a row for each
 * attempt, or one group for each attempt with a row for each phase. */
export type BarGrouping = 'phase' | 'attempt';

export const DEFAULT_BAR_GROUPING: BarGrouping = 'phase';

/** The grouping that the browser stored, read at the storage boundary: a
 * value that is not a grouping is the default. */
export function barGroupingOf(stored: unknown): BarGrouping {
  switch (stored) {
    case 'phase':
    case 'attempt':
      return stored;
    default:
      return DEFAULT_BAR_GROUPING;
  }
}

/** What the mark under the cursor lights in the panel; every other mark
 * dims. `none`: a mark that lights nothing, so every mark stays lit. */
export type PanelHighlight =
  | { kind: 'none' }
  | { kind: 'interval'; id: string; phase: TimingKind; attempt: number }
  | { kind: 'attempt'; attempt: number }
  | { kind: 'phase'; phase: TimingKind };

/** Whether an interval mark (a strip segment, a bar row) is lit. */
export function intervalLit(highlight: PanelHighlight, interval: PanelInterval): boolean {
  switch (highlight.kind) {
    case 'none':
      return true;
    case 'interval':
      return highlight.id === interval.id;
    case 'attempt':
      return highlight.attempt === ownerAttempt(interval.owner);
    case 'phase':
      return highlight.phase === interval.kind;
    default:
      return assertNever(highlight, 'panel highlight');
  }
}

/** Whether the mark of an attempt (its number, its bracket) is lit. */
export function attemptLit(highlight: PanelHighlight, attempt: number): boolean {
  switch (highlight.kind) {
    case 'none':
    case 'phase':
      return true;
    case 'interval':
    case 'attempt':
      return highlight.attempt === attempt;
    default:
      return assertNever(highlight, 'panel highlight');
  }
}

/** The key entry that the highlight lights: the kind of the interval or of
 * the key entry under the cursor, or every entry. */
export function keyLightOf(highlight: PanelHighlight): KeyLight {
  switch (highlight.kind) {
    case 'none':
    case 'attempt':
      return { kind: 'all' };
    case 'interval':
    case 'phase':
      return { kind: 'phase', phase: highlight.phase };
    default:
      return assertNever(highlight, 'panel highlight');
  }
}
