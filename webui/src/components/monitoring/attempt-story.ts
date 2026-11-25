// The attempts of one task as one story: the phases of each recorded
// attempt, the retry wait between two attempts, and the attempt that is
// live at the read. Pure: no React, no DOM. The phases follow the
// composition of the core (core/src/monitoring/timing.rs, `recorded_attempt`),
// so an attempt here and its segments on the run timeline have the same
// boundaries.

import type { PhaseKey, TimingKind } from '@/components/workflows/phase';
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
  | { kind: 'worker_lost' }
  /** From the start to the record of an attempt from before the lifecycle
   * record: its phases were not captured. */
  | { kind: 'not_captured' };

/** One phase of an attempt, from `startMs` to `endMs`. Two times in the wrong
 * order (a step of the database clock) give a negative length; the times keep
 * their record order. */
export interface AttemptPhase {
  key: PhaseKey;
  detail: AttemptPhaseDetail;
  startMs: number;
  endMs: number;
}

/** Who wrote the attempt row: the worker (the normal case), recovery after
 * the worker was lost, or an operator cancel while the attempt ran (the
 * attempt ends at the cancel). `not_recorded`: the attempt is from before
 * the lifecycle record. */
export type AttemptWriter =
  | { kind: 'worker' }
  | { kind: 'recovery'; lastSeenMs: number }
  | { kind: 'cancel' }
  | { kind: 'not_recorded' };

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
  /** In record order. An attempt with no phase record has one unknown
 * phase from its start to its record, or none when it has no such times. */
  phases: AttemptPhase[];
  span: AttemptSpan;
  writer: AttemptWriter;
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
  /** The retry delay between the record of attempt `fromAttempt` and the
   * due time of attempt `toAttempt`. */
  | { kind: 'retry_wait'; startMs: number; endMs: number; fromAttempt: number; toAttempt: number }
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

/** The writer of a captured attempt row. */
function writerOf(recordedBy: AttemptRecordedBy): AttemptWriter {
  switch (recordedBy.kind) {
    case 'worker':
      return { kind: 'worker' };
    case 'recovery':
      return { kind: 'recovery', lastSeenMs: instantMs(recordedBy.last_seen_at) };
    case 'cancel':
      return { kind: 'cancel' };
    default:
      return assertNever(recordedBy, 'attempt writer');
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

/** The one phase of an attempt from before the lifecycle record: unknown,
 * from its start to its record, or none when either time is missing. */
function notCapturedPhases(attempt: TaskAttempt): AttemptPhase[] {
  const span = spanOf([], attempt);
  switch (span.kind) {
    case 'span':
      return [{ key: 'unknown', detail: { kind: 'not_captured' }, startMs: span.startMs, endMs: span.endMs }];
    case 'no_times':
      return [];
    default:
      return assertNever(span, 'attempt span');
  }
}

/** One attempt row as phases, span and writer. */
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
    case 'not_captured': {
      const phases = notCapturedPhases(attempt);
      return { ...base, phases, span: spanOf(phases, attempt), writer: { kind: 'not_recorded' } };
    }
    case 'captured': {
      const phases = phasesFromStarts(capturedStarts(attempt, record), optionalMs(attempt.finished_at));
      return { ...base, phases, span: spanOf(phases, attempt), writer: writerOf(record.recorded_by) };
    }
    default:
      return assertNever(record, 'attempt phases');
  }
}

/** The kind of a phase of an attempt: the claims before the claim that ran
 * the attempt are `earlier_claims`, and the time after the last sign of life
 * of a lost worker is `worker_lost`. */
export function attemptPhaseKind(phase: AttemptPhase): TimingKind {
  switch (phase.detail.kind) {
    case 'earlier_claims':
      return 'earlier_claims';
    case 'worker_lost':
      return 'worker_lost';
    case 'none':
    case 'placed':
    case 'placement_not_observed':
    case 'claims_not_counted':
    case 'not_captured':
      return phase.key;
    default:
      return assertNever(phase.detail, 'attempt phase detail');
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
 * of attempt `next`, or nothing when either time is missing. */
function retryWaitBetween(previous: TaskAttempt | undefined, next: number, nextDueMs: number | null): StoryItem[] {
  if (previous === undefined) {
    return [];
  }
  const recordedMs = optionalMs(previous.finished_at);
  return recordedMs === null || nextDueMs === null
    ? []
    : [{ kind: 'retry_wait', startMs: recordedMs, endMs: nextDueMs, fromAttempt: previous.attempt, toAttempt: next }];
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
            ...retryWaitBetween(ordered[ordered.length - 1], read.attempt.number, liveDueOf(read.attempt, leaf)),
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
    ...(index === 0 ? [] : retryWaitBetween(ordered[index - 1], attempt.attempt, dueOf(attempt))),
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
