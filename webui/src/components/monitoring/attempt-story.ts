// The attempts of one task as one story: the phases of each recorded
// attempt, the retry wait between two attempts, and the attempt that is
// live at the read. Pure: no React, no DOM. The phases follow the
// composition of the core (core/src/monitoring/timing.rs, `recorded_attempt`),
// so an attempt here and its segments on the run timeline have the same
// boundaries.

import { phaseWeight, type PhaseKey } from '@/components/workflows/phase';
import { phaseTotals, type PhaseInterval, type PhaseTotal } from '@/components/workflows/timeline-geometry';
import { assertNever } from '@/lib/exhaustive';
import { instantMs } from '@/lib/time-text';
import type { AttemptClaims, AttemptPhases, AttemptRecordedBy } from '@/types/timing';
import type { LeafTask, TaskAttempt } from '@/types/workflows';

/** What a phase of an attempt holds beyond its key. */
export type AttemptPhaseDetail =
  | { kind: 'none' }
  /** Claims before the claim that ran the attempt (lost leases, released
   * claims). */
  | { kind: 'earlier_claims'; count: number }
  /** The claim that ran the attempt; a child took the task at `placedAtMs`. */
  | { kind: 'placed'; placedAtMs: number }
  /** The claim that ran the attempt; no placement was observed. */
  | { kind: 'placement_not_observed' }
  /** Queue wait in which earlier claims can hide: the claims were not counted. */
  | { kind: 'claims_not_counted' }
  /** From the last sign of life to the record: the worker was lost. */
  | { kind: 'worker_lost' };

/** One phase of an attempt, from `startMs` to `endMs`. Two times in the wrong
 * order (a step of the database clock) give a negative length; the times keep
 * their record order. */
export interface AttemptPhase {
  key: PhaseKey;
  detail: AttemptPhaseDetail;
  startMs: number;
  endMs: number;
}

/** When a child of the worker took the task of a long slot wait: the time
 * after the claim, or not recorded. It tells which part of the wait was long:
 * the wait for a child, or the start on the child. */
export type ChildTook = { kind: 'after'; ms: number } | { kind: 'not_recorded' };

/** A fact of an attempt that the normal case does not have. A long step
 * holds what its time tells (see `phaseWeight`). */
export type AttemptNote =
  | { kind: 'long_step'; key: 'enqueued' | 'finalizing'; ms: number; tells: string }
  | { kind: 'long_slot_wait'; ms: number; tells: string; childTook: ChildTook }
  | { kind: 'claimed_more_than_once'; count: number }
  | { kind: 'claims_not_counted' }
  | { kind: 'worker_lost'; lastSeenMs: number }
  | { kind: 'cancelled' }
  | { kind: 'not_captured' };

/** The interval of an attempt: its first recorded time to its last. */
export type AttemptSpan =
  | { kind: 'span'; startMs: number; endMs: number }
  | { kind: 'no_times' };

/** One attempt that has its attempt row. */
export interface RecordedAttempt {
  number: number;
  outcome: string;
  errorCode: string | null;
  message: string | null;
  host: string | null;
  /** In lifecycle order; empty for an attempt with no phase record. */
  phases: AttemptPhase[];
  span: AttemptSpan;
  notes: AttemptNote[];
}

/** The attempt of a live task, which has no attempt row yet. A `pending`
 * attempt waits for its retry before `dueMs` and in the queue after it. */
export type LiveAttempt =
  | { kind: 'pending'; number: number; dueMs: number }
  | { kind: 'claimed'; number: number }
  | { kind: 'running'; number: number; startedMs: number }
  /** The task row holds no time that the live status needs. */
  | { kind: 'no_times'; number: number; status: string };

export type StoryItem =
  | { kind: 'attempt'; attempt: RecordedAttempt }
  /** The retry delay between the record of one attempt and the due time of
   * the next. */
  | { kind: 'retry_wait'; startMs: number; endMs: number }
  | { kind: 'live'; attempt: LiveAttempt };

/** How many attempts the task may make, from its retry limit. */
export type AllowedAttempts = { kind: 'limit'; count: number } | { kind: 'no_task_row' };

export interface AttemptStory {
  items: StoryItem[];
  /** Recorded attempts plus a live one. */
  attemptCount: number;
  allowed: AllowedAttempts;
}

const optionalMs = (text: string | null): number | null =>
  text === null ? null : instantMs(text);

/** The start of a phase; it ends where the next one starts. */
interface PhaseStart {
  key: PhaseKey;
  detail: AttemptPhaseDetail;
  atMs: number;
}

function claimStarts(
  claims: AttemptClaims,
  claimedAtMs: number,
  claimed: AttemptPhaseDetail,
  dueMs: number
): PhaseStart[] {
  const claim: PhaseStart = { key: 'claimed', detail: claimed, atMs: claimedAtMs };
  switch (claims.kind) {
    case 'not_counted':
      return [
        { key: 'unknown', detail: { kind: 'claims_not_counted' }, atMs: dueMs },
        claim,
      ];
    case 'counted': {
      const queue: PhaseStart = { key: 'enqueued', detail: { kind: 'none' }, atMs: dueMs };
      return claims.count > 1
        ? [
            queue,
            {
              key: 'claimed',
              detail: { kind: 'earlier_claims', count: claims.count - 1 },
              atMs: instantMs(claims.first_claimed_at),
            },
            claim,
          ]
        : [queue, claim];
    }
    default:
      return assertNever(claims, 'attempt claims');
  }
}

function claimDetail(recordedBy: AttemptRecordedBy): AttemptPhaseDetail {
  switch (recordedBy.kind) {
    case 'worker':
      switch (recordedBy.placement.kind) {
        case 'placed':
          return { kind: 'placed', placedAtMs: instantMs(recordedBy.placement.placed_at) };
        case 'not_observed':
          return { kind: 'placement_not_observed' };
        default:
          return assertNever(recordedBy.placement, 'attempt placement');
      }
    case 'recovery':
    case 'cancel':
      return { kind: 'placement_not_observed' };
    default:
      return assertNever(recordedBy, 'attempt writer');
  }
}

/** The note of the writer of the attempt row. A worker is the normal case.
 * A placement that the worker did not observe has no note of its own: the
 * placement matters only for a long slot wait, and the note of that wait
 * says that the record does not show it. */
function writerNotes(recordedBy: AttemptRecordedBy): AttemptNote[] {
  switch (recordedBy.kind) {
    case 'worker':
      return [];
    case 'recovery':
      return [{ kind: 'worker_lost', lastSeenMs: instantMs(recordedBy.last_seen_at) }];
    case 'cancel':
      return [{ kind: 'cancelled' }];
    default:
      return assertNever(recordedBy, 'attempt writer');
  }
}

function claimNotes(claims: AttemptClaims): AttemptNote[] {
  switch (claims.kind) {
    case 'counted':
      return claims.count > 1 ? [{ kind: 'claimed_more_than_once', count: claims.count }] : [];
    case 'not_counted':
      return [{ kind: 'claims_not_counted' }];
    default:
      return assertNever(claims, 'attempt claims');
  }
}

/** The phase starts of a captured attempt, in record order: queue wait,
 * earlier claims, the claim, running, finalizing, and the time after the last
 * sign of life of a lost worker. */
function capturedStarts(
  attempt: TaskAttempt,
  phases: Extract<AttemptPhases, { kind: 'captured' }>
): PhaseStart[] {
  const startedMs = optionalMs(attempt.started_at);
  const running: PhaseStart[] =
    startedMs === null ? [] : [{ key: 'running', detail: { kind: 'none' }, atMs: startedMs }];
  const finalizing: PhaseStart[] =
    phases.finalizing.kind === 'marked'
      ? [
          {
            key: 'finalizing',
            detail: { kind: 'none' },
            atMs: instantMs(phases.finalizing.finalizing_at),
          },
        ]
      : [];
  const lost: PhaseStart[] =
    phases.recorded_by.kind === 'recovery'
      ? [
          {
            key: 'unknown',
            detail: { kind: 'worker_lost' },
            atMs: instantMs(phases.recorded_by.last_seen_at),
          },
        ]
      : [];
  return [
    ...claimStarts(
      phases.claims,
      instantMs(phases.claimed_at),
      claimDetail(phases.recorded_by),
      instantMs(phases.enqueued_at)
    ),
    ...running,
    ...finalizing,
    ...lost,
  ];
}

/** Each start runs to the next start; the last runs to `endMs`, or is left
 * out when the attempt has no record time. */
function phasesFromStarts(starts: readonly PhaseStart[], endMs: number | null): AttemptPhase[] {
  return starts.flatMap((start, index) => {
    const next = starts[index + 1];
    const phaseEndMs = next === undefined ? endMs : next.atMs;
    return phaseEndMs === null
      ? []
      : [{ key: start.key, detail: start.detail, startMs: start.atMs, endMs: phaseEndMs }];
  });
}

/** The note of a long queue wait, slot wait or finalizing time. A long time
 * of earlier claims or of a record gap has its own note (more than one claim,
 * claims not counted, worker lost), so it gets none here. */
function longStepNotes(phase: AttemptPhase): AttemptNote[] {
  const ms = phase.endMs - phase.startMs;
  const weight = phaseWeight(phase.key, ms);
  switch (weight.kind) {
    case 'main':
    case 'plain':
    case 'quiet':
      return [];
    case 'marked':
      break;
    default:
      return assertNever(weight, 'phase weight');
  }
  const detail = phase.detail;
  switch (phase.key) {
    case 'enqueued':
    case 'finalizing':
      return [{ kind: 'long_step', key: phase.key, ms, tells: weight.tells }];
    case 'claimed':
      switch (detail.kind) {
        case 'placed':
          return [
            {
              kind: 'long_slot_wait',
              ms,
              tells: weight.tells,
              childTook: { kind: 'after', ms: detail.placedAtMs - phase.startMs },
            },
          ];
        case 'placement_not_observed':
          return [
            { kind: 'long_slot_wait', ms, tells: weight.tells, childTook: { kind: 'not_recorded' } },
          ];
        case 'none':
        case 'earlier_claims':
        case 'claims_not_counted':
        case 'worker_lost':
          return [];
        default:
          return assertNever(detail, 'attempt phase detail');
      }
    case 'pending':
    case 'ready':
    case 'retry_wait':
    case 'running':
    case 'advancing':
    case 'unknown':
      return [];
    default:
      return assertNever(phase.key, 'phase');
  }
}

function spanOf(phases: readonly AttemptPhase[], attempt: TaskAttempt): AttemptSpan {
  const first = phases[0];
  const last = phases[phases.length - 1];
  if (first !== undefined && last !== undefined) {
    return { kind: 'span', startMs: first.startMs, endMs: last.endMs };
  }
  const startedMs = optionalMs(attempt.started_at);
  const finishedMs = optionalMs(attempt.finished_at);
  return startedMs === null || finishedMs === null
    ? { kind: 'no_times' }
    : { kind: 'span', startMs: startedMs, endMs: finishedMs };
}

/** One attempt row as phases, span and notes. */
export function recordedAttempt(attempt: TaskAttempt): RecordedAttempt {
  const base = {
    number: attempt.attempt,
    outcome: attempt.outcome,
    errorCode: attempt.error_code,
    message: attempt.error_message ?? attempt.failed_reason,
    host: attempt.worker_hostname,
  };
  const record = attempt.phases;
  switch (record.kind) {
    case 'not_captured':
      return {
        ...base,
        phases: [],
        span: spanOf([], attempt),
        notes: [{ kind: 'not_captured' }],
      };
    case 'captured': {
      const phases = phasesFromStarts(capturedStarts(attempt, record), optionalMs(attempt.finished_at));
      return {
        ...base,
        phases,
        span: spanOf(phases, attempt),
        notes: [
          ...phases.flatMap(longStepNotes),
          ...claimNotes(record.claims),
          ...writerNotes(record.recorded_by),
        ],
      };
    }
    default:
      return assertNever(record, 'attempt phases');
  }
}

/** The task statuses with an attempt in progress. Other statuses are terminal
 * (or unknown to this client) and have no live attempt. */
type LiveStatus = 'PENDING' | 'CLAIMED' | 'RUNNING';

type StatusRead = { kind: 'live'; status: LiveStatus } | { kind: 'not_live' };

function readStatus(status: string): StatusRead {
  switch (status) {
    case 'PENDING':
    case 'CLAIMED':
    case 'RUNNING':
      return { kind: 'live', status };
    default:
      return { kind: 'not_live' };
  }
}

/** The attempt in progress of a task, or `not_live` for a task with none. */
export type LiveRead = { kind: 'live'; attempt: LiveAttempt } | { kind: 'not_live' };

function liveOf(status: LiveStatus, leaf: LeafTask): LiveAttempt {
  const number = leaf.retry_count + 1;
  const dueMs = optionalMs(leaf.enqueued_at);
  const startedMs = optionalMs(leaf.started_at);
  switch (status) {
    case 'PENDING':
      return dueMs === null
        ? { kind: 'no_times', number, status }
        : { kind: 'pending', number, dueMs };
    case 'CLAIMED':
      return { kind: 'claimed', number };
    case 'RUNNING':
      return startedMs === null
        ? { kind: 'no_times', number, status }
        : { kind: 'running', number, startedMs };
    default:
      return assertNever(status, 'live task status');
  }
}

/** The live attempt of a task: its number is the retry count plus one. */
export function liveAttempt(leaf: LeafTask): LiveRead {
  const read = readStatus(leaf.status);
  switch (read.kind) {
    case 'not_live':
      return { kind: 'not_live' };
    case 'live':
      return { kind: 'live', attempt: liveOf(read.status, leaf) };
    default:
      return assertNever(read, 'task status read');
  }
}

/** The retry wait from the record of `previous` to the due time `nextDueMs`
 * of the next attempt, or nothing when either time is missing. */
function retryWaitBetween(previous: TaskAttempt | undefined, nextDueMs: number | null): StoryItem[] {
  const recordedMs = previous === undefined ? null : optionalMs(previous.finished_at);
  return recordedMs === null || nextDueMs === null
    ? []
    : [{ kind: 'retry_wait', startMs: recordedMs, endMs: nextDueMs }];
}

const dueOf = (attempt: TaskAttempt): number | null => {
  switch (attempt.phases.kind) {
    case 'captured':
      return instantMs(attempt.phases.enqueued_at);
    case 'not_captured':
      return null;
    default:
      return assertNever(attempt.phases, 'attempt phases');
  }
};

/** The due time of a live attempt, where the retry wait before it ends. The
 * retry wait of a pending attempt can still be open: its live entry shows the
 * due time instead. */
const liveDueOf = (live: LiveAttempt, leaf: LeafTask): number | null => {
  switch (live.kind) {
    case 'pending':
      return null;
    case 'claimed':
    case 'running':
    case 'no_times':
      return optionalMs(leaf.enqueued_at);
    default:
      return assertNever(live, 'live attempt');
  }
};

function liveItems(ordered: readonly TaskAttempt[], leaf: LeafTask | null): StoryItem[] {
  if (leaf === null) {
    return [];
  }
  const read = liveAttempt(leaf);
  switch (read.kind) {
    case 'not_live':
      return [];
    case 'live':
      // A refetch can bring the attempt row before the task row moves on.
      return ordered.some(attempt => attempt.attempt === read.attempt.number)
        ? []
        : [
            ...retryWaitBetween(ordered[ordered.length - 1], liveDueOf(read.attempt, leaf)),
            { kind: 'live', attempt: read.attempt },
          ];
    default:
      return assertNever(read, 'live read');
  }
}

/**
 * The attempts of a task in order, with the retry wait between two of them,
 * and the live attempt of a task that is not terminal. `leaf` is null when
 * the node has no task row.
 */
export function attemptStory(attempts: readonly TaskAttempt[], leaf: LeafTask | null): AttemptStory {
  const ordered = [...attempts].sort((a, b) => a.attempt - b.attempt);
  const recorded: StoryItem[] = ordered.flatMap((attempt, index) => [
    ...(index === 0 ? [] : retryWaitBetween(ordered[index - 1], dueOf(attempt))),
    { kind: 'attempt' as const, attempt: recordedAttempt(attempt) },
  ]);
  const live = liveItems(ordered, leaf);
  return {
    items: [...recorded, ...live],
    attemptCount: ordered.length + live.filter(item => item.kind === 'live').length,
    allowed:
      leaf === null ? { kind: 'no_task_row' } : { kind: 'limit', count: leaf.max_retries + 1 },
  };
}

/** The time from the first recorded time of the story to its last, with the
 * time of each phase over the attempts and the retry waits, in lifecycle
 * order. */
export type StoryTotals =
  | { kind: 'measured'; spanMs: number; totals: PhaseTotal[] }
  | { kind: 'no_times' };

/** The phase intervals of the story: each phase of each recorded attempt,
 * and each retry wait. */
function storyIntervals(items: readonly StoryItem[]): PhaseInterval[] {
  return items.flatMap((item): PhaseInterval[] => {
    switch (item.kind) {
      case 'attempt':
        return item.attempt.phases.map(phase => ({
          key: phase.key,
          startMs: phase.startMs,
          endMs: phase.endMs,
          open: false,
        }));
      case 'retry_wait':
        return [{ key: 'retry_wait', startMs: item.startMs, endMs: item.endMs, open: false }];
      case 'live':
        return [];
      default:
        return assertNever(item, 'story item');
    }
  });
}

export function storyTotals(items: readonly StoryItem[]): StoryTotals {
  const intervals = storyIntervals(items);
  if (intervals.length === 0) {
    return { kind: 'no_times' };
  }
  let firstMs = Number.POSITIVE_INFINITY;
  let lastMs = Number.NEGATIVE_INFINITY;
  for (const interval of intervals) {
    firstMs = Math.min(firstMs, interval.startMs);
    lastMs = Math.max(lastMs, interval.endMs);
  }
  return { kind: 'measured', spanMs: Math.max(lastMs - firstMs, 0), totals: phaseTotals(intervals) };
}
