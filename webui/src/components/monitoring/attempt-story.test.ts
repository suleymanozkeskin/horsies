import { describe, expect, it } from 'vitest';

import {
  attemptStory,
  liveAttempt,
  recordedAttempt,
  storyTotals,
  type AttemptPhase,
} from '@/components/monitoring/attempt-story';
import {
  at,
  CANCELLED,
  exampleAttempt,
  leafTask,
  normalAttempt,
  NOT_CAPTURED,
  ONE_CLAIM_NOT_OBSERVED,
  PLACED_TWO_CLAIMS,
  RECOVERED,
} from '@/test/attempt-fixtures';
import { instantMs } from '@/lib/time-text';

const ms = (seconds: number): number => instantMs(at(seconds));

/** Each phase as `key detail start-end` in seconds after 12:00:00. */
const phaseRows = (phases: readonly AttemptPhase[]): string[] =>
  phases.map(
    phase =>
      `${phase.key} ${phase.detail.kind} ${(phase.startMs - ms(0)) / 1_000}-${(phase.endMs - ms(0)) / 1_000}`
  );

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
    expect(attempt.runTime).toEqual({ kind: 'measured', ms: 4_000 });
    expect(attempt.span).toEqual({ kind: 'span', startMs: ms(0), endMs: ms(9) });
    expect(attempt.notes).toEqual([{ kind: 'claimed_more_than_once', count: 2 }]);
    expect(attempt.phases[1]?.detail).toEqual({ kind: 'earlier_claims', count: 1 });
    expect(attempt.phases[2]?.detail).toEqual({ kind: 'placed', placedAtMs: ms(3) });
  });

  it('marks the time after the last sign of life of a lost worker as unknown', () => {
    const attempt = recordedAttempt(exampleAttempt(RECOVERED));

    expect(phaseRows(attempt.phases)).toEqual([
      'unknown claims_not_counted 0-2',
      'claimed placement_not_observed 2-4',
      'running none 4-7',
      'unknown worker_lost 7-9',
    ]);
    expect(attempt.runTime).toEqual({ kind: 'measured', ms: 3_000 });
    expect(attempt.notes).toEqual([
      { kind: 'claims_not_counted' },
      { kind: 'worker_lost', lastSeenMs: ms(7) },
    ]);
  });

  it('runs to the record when no finalizing marker was written, and notes a missing placement', () => {
    const attempt = recordedAttempt(exampleAttempt(ONE_CLAIM_NOT_OBSERVED));

    expect(phaseRows(attempt.phases)).toEqual([
      'enqueued none 0-2',
      'claimed placement_not_observed 2-4',
      'running none 4-9',
    ]);
    expect(attempt.notes).toEqual([{ kind: 'placement_not_observed' }]);
  });

  it('notes an attempt that an operator cancel recorded', () => {
    const attempt = recordedAttempt(exampleAttempt(CANCELLED));

    expect(phaseRows(attempt.phases)).toEqual([
      'enqueued none 0-2',
      'claimed placement_not_observed 2-4',
      'running none 4-7',
      'finalizing none 7-9',
    ]);
    expect(attempt.notes).toEqual([{ kind: 'cancelled' }]);
  });

  it('has no phases for an attempt from before the lifecycle record, and keeps its row times', () => {
    const attempt = recordedAttempt(exampleAttempt(NOT_CAPTURED));

    expect(attempt.phases).toEqual([]);
    expect(attempt.runTime).toEqual({ kind: 'not_started' });
    expect(attempt.span).toEqual({ kind: 'span', startMs: ms(4), endMs: ms(9) });
    expect(attempt.notes).toEqual([{ kind: 'not_captured' }]);
  });

  it('has no normal-case notes for one placed claim recorded by the worker', () => {
    expect(recordedAttempt(normalAttempt(1, 0, 'COMPLETED')).notes).toEqual([]);
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
      leafTask('COMPLETED', 2, 4, { enqueuedAt: at(30), startedAt: at(32) })
    );

    expect(
      story.items.map(item =>
        item.kind === 'attempt'
          ? `attempt ${item.attempt.number}`
          : item.kind === 'retry_wait'
            ? `retry wait ${(item.endMs - item.startMs) / 1_000}s`
            : 'live'
      )
    ).toEqual(['attempt 1', 'retry wait 4s', 'attempt 2', 'retry wait 14s', 'attempt 3']);
    expect(story.attemptCount).toBe(3);
    expect(story.allowed).toEqual({ kind: 'limit', count: 5 });
  });

  it('adds the attempt in progress of a running task after the retry wait before it', () => {
    const story = attemptStory(
      [normalAttempt(1, 0, 'FAILED')],
      leafTask('RUNNING', 1, 2, { enqueuedAt: at(10), startedAt: at(12) })
    );

    expect(story.items.slice(1)).toEqual([
      { kind: 'retry_wait', startMs: ms(6), endMs: ms(10) },
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
    expect(totals.spanMs).toBe(16_000);
    expect(totals.byPhase.get('running')).toBe(6_000);
    expect(totals.byPhase.get('retry_wait')).toBe(4_000);
  });

  it('has no times for a story with no recorded time', () => {
    expect(storyTotals([])).toEqual({ kind: 'no_times' });
  });
});
