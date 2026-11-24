import { describe, expect, it } from 'vitest';

import {
  attemptStory,
  liveAttempt,
  recordedAttempt,
  storyTotals,
  type AttemptPhase,
  type RecordedAttempt,
} from '@/components/monitoring/attempt-story';
import { LONG_ENGINE_STEP_MS } from '@/components/workflows/phase';
import {
  at,
  CANCELLED,
  exampleAttempt,
  leafTask,
  NORMAL_TIMES,
  normalAttempt,
  NOT_CAPTURED,
  ONE_CLAIM_NOT_OBSERVED,
  PLACED_TWO_CLAIMS,
  RECOVERED,
  timedAttempt,
} from '@/test/attempt-fixtures';
import { instantMs } from '@/lib/time-text';

const ms = (seconds: number): number => instantMs(at(seconds));

/** Each phase as `key detail start-end` in seconds after 12:00:00. */
const phaseRows = (phases: readonly AttemptPhase[]): string[] =>
  phases.map(
    phase =>
      `${phase.key} ${phase.detail.kind} ${(phase.startMs - ms(0)) / 1_000}-${(phase.endMs - ms(0)) / 1_000}`
  );

/** Each note as its kind, with the phase of a long step. */
const noteKinds = (attempt: RecordedAttempt): string[] =>
  attempt.notes.map(note => (note.kind === 'long_step' ? `long_step ${note.key}` : note.kind));

describe('recordedAttempt', () => {
  it('splits a placed attempt with an earlier claim into its phases, in record order', () => {
    const attempt = recordedAttempt(exampleAttempt(PLACED_TWO_CLAIMS));

    expect(phaseRows(attempt.phases)).toEqual([
      'enqueued none 0-1',
      'claimed earlier_claims 1-2',
      'claimed placed 2-4',
      'running none 4-8',
      'finalizing none 8-9',
    ]);
    expect(attempt.span).toEqual({ kind: 'span', startMs: ms(0), endMs: ms(9) });
    expect(attempt.phases[1]?.detail).toEqual({ kind: 'earlier_claims', count: 1 });
    expect(attempt.phases[2]?.detail).toEqual({ kind: 'placed', placedAtMs: ms(3) });
    // The example has phases of seconds: each engine step is a long step. The
    // time of the earlier claim has the note of the claim count, not a second one.
    expect(noteKinds(attempt)).toEqual([
      'long_step enqueued',
      'long_slot_wait',
      'long_step finalizing',
      'claimed_more_than_once',
    ]);
  });

  it('marks the time after the last sign of life of a lost worker as unknown', () => {
    const attempt = recordedAttempt(exampleAttempt(RECOVERED));

    expect(phaseRows(attempt.phases)).toEqual([
      'unknown claims_not_counted 0-2',
      'claimed placement_not_observed 2-4',
      'running none 4-7',
      'unknown worker_lost 7-9',
    ]);
    // The two gaps of the record have their own notes: claims not counted,
    // worker lost. The slot wait of 2 s is a long step with no placement.
    expect(attempt.notes).toEqual([
      expect.objectContaining({ kind: 'long_slot_wait', ms: 2_000, childTook: { kind: 'not_recorded' } }),
      { kind: 'claims_not_counted' },
      { kind: 'worker_lost', lastSeenMs: ms(7) },
    ]);
  });

  it('runs to the record when no finalizing marker was written, and has no note for a missing placement alone', () => {
    const attempt = recordedAttempt(exampleAttempt(ONE_CLAIM_NOT_OBSERVED));

    expect(phaseRows(attempt.phases)).toEqual([
      'enqueued none 0-2',
      'claimed placement_not_observed 2-4',
      'running none 4-9',
    ]);
    // The long slot wait says that the record does not show the placement.
    expect(attempt.notes).toEqual([
      expect.objectContaining({ kind: 'long_step', key: 'enqueued' }),
      expect.objectContaining({ kind: 'long_slot_wait', childTook: { kind: 'not_recorded' } }),
    ]);
  });

  it('notes an attempt that an operator cancel recorded', () => {
    const attempt = recordedAttempt(exampleAttempt(CANCELLED));

    expect(phaseRows(attempt.phases)).toEqual([
      'enqueued none 0-2',
      'claimed placement_not_observed 2-4',
      'running none 4-7',
      'finalizing none 7-9',
    ]);
    expect(noteKinds(attempt)).toEqual([
      'long_step enqueued',
      'long_slot_wait',
      'long_step finalizing',
      'cancelled',
    ]);
  });

  it('has no phases for an attempt from before the lifecycle record, and keeps its row times', () => {
    const attempt = recordedAttempt(exampleAttempt(NOT_CAPTURED));

    expect(attempt.phases).toEqual([]);
    expect(attempt.span).toEqual({ kind: 'span', startMs: ms(4), endMs: ms(9) });
    expect(attempt.notes).toEqual([{ kind: 'not_captured' }]);
  });

  it('has no note for a normal attempt: one claim, the worker, engine steps of milliseconds', () => {
    const attempt = recordedAttempt(normalAttempt(1, 0, 'COMPLETED'));

    expect(attempt.notes).toEqual([]);
    expect(phaseRows(attempt.phases)).toEqual([
      'enqueued none 0-0.003',
      'claimed placed 0.003-0.005',
      'running none 0.005-3.005',
      'finalizing none 3.005-3.006',
    ]);
  });

  it('notes a long queue wait with what it tells, and keeps the steps below the limit quiet', () => {
    const queued = recordedAttempt(
      timedAttempt(1, 0, { ...NORMAL_TIMES, queueMs: 7_200 }, 'COMPLETED')
    );
    const justBelow = recordedAttempt(
      timedAttempt(1, 0, { ...NORMAL_TIMES, queueMs: LONG_ENGINE_STEP_MS - 1 }, 'COMPLETED')
    );

    expect(queued.notes).toEqual([
      expect.objectContaining({ kind: 'long_step', key: 'enqueued', ms: 7_200 }),
    ]);
    expect(queued.notes[0]?.kind === 'long_step' ? queued.notes[0].tells : '').toContain(
      'concurrency limit'
    );
    expect(justBelow.notes).toEqual([]);
  });

  it('notes a long slot wait with the time after which a child took the task', () => {
    const waited = recordedAttempt(
      timedAttempt(1, 0, { ...NORMAL_TIMES, slotMs: 800, placedMs: 790 }, 'COMPLETED')
    );

    expect(waited.notes).toEqual([
      expect.objectContaining({ kind: 'long_slot_wait', ms: 800, childTook: { kind: 'after', ms: 790 } }),
    ]);
  });

  it('notes a long finalizing time', () => {
    const slowRecord = recordedAttempt(
      timedAttempt(1, 0, { ...NORMAL_TIMES, finalizingMs: 400 }, 'COMPLETED')
    );

    expect(noteKinds(slowRecord)).toEqual(['long_step finalizing']);
  });

  it('carries the answer of the attempt: outcome, error code, message and host', () => {
    const attempt = recordedAttempt(exampleAttempt(PLACED_TWO_CLAIMS, 2));

    expect(attempt).toMatchObject({
      number: 2,
      outcome: 'FAILED',
      errorCode: 'PSP_UNAVAILABLE',
      message: 'payment provider unreachable',
      host: 'worker-a',
    });
  });
});

describe('attemptStory', () => {
  it('puts the retry wait from each record to the next due time between the attempts, in attempt order', () => {
    const story = attemptStory(
      [normalAttempt(3, 30, 'COMPLETED'), normalAttempt(1, 0, 'FAILED'), normalAttempt(2, 10, 'FAILED')],
      leafTask('COMPLETED', 2, 4, { enqueuedAt: at(30), startedAt: at(30.005) })
    );

    expect(
      story.items.map(item =>
        item.kind === 'attempt'
          ? `attempt ${item.attempt.number}`
          : item.kind === 'retry_wait'
            ? `retry wait ${(item.endMs - item.startMs) / 1_000}s`
            : 'live'
      )
    ).toEqual(['attempt 1', 'retry wait 6.994s', 'attempt 2', 'retry wait 16.994s', 'attempt 3']);
    expect(story.attemptCount).toBe(3);
    expect(story.allowed).toEqual({ kind: 'limit', count: 5 });
  });

  it('adds the attempt in progress of a running task after the retry wait before it', () => {
    const story = attemptStory(
      [normalAttempt(1, 0, 'FAILED')],
      leafTask('RUNNING', 1, 2, { enqueuedAt: at(10), startedAt: at(12) })
    );

    expect(story.items.slice(1)).toEqual([
      { kind: 'retry_wait', startMs: ms(3.006), endMs: ms(10) },
      { kind: 'live', attempt: { kind: 'running', number: 2, startedMs: ms(12) } },
    ]);
    expect(story.attemptCount).toBe(2);
  });

  it('shows a pending retry as a live attempt with its due time and no closed retry wait', () => {
    const story = attemptStory(
      [normalAttempt(1, 0, 'FAILED')],
      leafTask('PENDING', 1, 2, { enqueuedAt: at(20), startedAt: null })
    );

    expect(story.items.slice(1)).toEqual([
      { kind: 'live', attempt: { kind: 'pending', number: 2, dueMs: ms(20) } },
    ]);
  });

  it('has no live attempt for a terminal task, or when the attempt row already came', () => {
    expect(
      attemptStory([normalAttempt(1, 0, 'COMPLETED')], leafTask('COMPLETED', 0, 0, { enqueuedAt: at(0), startedAt: at(2) }))
        .items.filter(item => item.kind === 'live')
    ).toEqual([]);
    expect(
      attemptStory([normalAttempt(1, 0, 'FAILED')], leafTask('RUNNING', 0, 2, { enqueuedAt: at(0), startedAt: at(2) }))
        .items.filter(item => item.kind === 'live')
    ).toEqual([]);
  });

  it('has no retry limit without a task row, and no item without attempts', () => {
    expect(attemptStory([], null)).toEqual({
      items: [],
      attemptCount: 0,
      allowed: { kind: 'no_task_row' },
    });
  });
});

describe('liveAttempt', () => {
  it('reads the attempt in progress from the task status', () => {
    const times = { enqueuedAt: at(10), startedAt: at(12) };

    expect(liveAttempt(leafTask('CLAIMED', 0, 0, times))).toEqual({
      kind: 'live',
      attempt: { kind: 'claimed', number: 1 },
    });
    expect(liveAttempt(leafTask('RUNNING', 0, 0, { enqueuedAt: at(10), startedAt: null }))).toEqual({
      kind: 'live',
      attempt: { kind: 'no_times', number: 1, status: 'RUNNING' },
    });
    expect(liveAttempt(leafTask('EXPIRED', 0, 0, times))).toEqual({ kind: 'not_live' });
  });
});

describe('storyTotals', () => {
  it('sums each phase over the attempts and the retry waits, and spans the whole story', () => {
    const story = attemptStory(
      [normalAttempt(1, 0, 'FAILED'), normalAttempt(2, 10, 'COMPLETED')],
      null
    );
    const totals = storyTotals(story.items);

    expect(totals.kind).toBe('measured');
    if (totals.kind !== 'measured') {
      return;
    }
    // From the due time of attempt 1 to the record of attempt 2 at 13.006 s.
    expect(totals.spanMs).toBe(13_006);
    expect(totals.totals).toEqual([
      { key: 'enqueued', ms: 6, longestMs: 3, open: false },
      { key: 'retry_wait', ms: 6_994, longestMs: 6_994, open: false },
      { key: 'claimed', ms: 4, longestMs: 2, open: false },
      { key: 'running', ms: 6_000, longestMs: 3_000, open: false },
      { key: 'finalizing', ms: 2, longestMs: 1, open: false },
    ]);
    // The phases of the story add up to its span.
    expect(totals.totals.reduce((sum, total) => sum + total.ms, 0)).toBe(13_006);
  });

  it('has no times for a story with no recorded time', () => {
    expect(storyTotals([])).toEqual({ kind: 'no_times' });
  });
});
