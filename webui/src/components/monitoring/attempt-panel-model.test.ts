import { describe, expect, it } from 'vitest';

import {
  attemptLit,
  barGroupingOf,
  errorGroups,
  intervalLit,
  intervalMs,
  keyLightOf,
  ownerText,
  panelKeyPhases,
  panelModel,
  panelOutcomes,
  panelSpan,
  rangeText,
  taskOnlyFailure,
  type PanelHighlight,
} from '@/components/monitoring/attempt-panel-model';
import { attemptStory, recordedAttempt } from '@/components/monitoring/attempt-story';
import { formatMilliseconds } from '@/lib/format-duration';
import { instantMs } from '@/lib/time-text';
import {
  at,
  exampleAttempt,
  leafTask,
  normalAttempt,
  NOT_CAPTURED,
  PLACED_TWO_CLAIMS,
  RECOVERED,
} from '@/test/attempt-fixtures';
import type { TaskAttempt } from '@/types/workflows';

const ms = (seconds: number): number => instantMs(at(seconds));

/** Three attempts: two failures, then a success, with retry waits. */
const RETRIED = [normalAttempt(1, 0, 'FAILED'), normalAttempt(2, 10, 'FAILED'), normalAttempt(3, 30, 'COMPLETED')];

const modelOf = (attempts: readonly TaskAttempt[]) => panelModel(attemptStory(attempts, null));

describe('panelModel', () => {
  it('orders the strip in time: each attempt, then the retry wait to the next', () => {
    const model = modelOf(RETRIED);

    expect(model.intervals.map(interval => `${ownerText(interval.owner)} ${interval.kind}`)).toEqual([
      '1 enqueued',
      '1 claimed',
      '1 running',
      '1 finalizing',
      '1→2 retry_wait',
      '2 enqueued',
      '2 claimed',
      '2 running',
      '2 finalizing',
      '2→3 retry_wait',
      '3 enqueued',
      '3 claimed',
      '3 running',
      '3 finalizing',
    ]);
    for (let index = 1; index < model.intervals.length; index += 1) {
      expect(model.intervals[index]?.startMs).toBeGreaterThanOrEqual(model.intervals[index - 1]?.startMs ?? 0);
    }
    expect(model.attempts.map(attempt => attempt.number)).toEqual([1, 2, 3]);
    expect(model.live).toEqual({ kind: 'not_live' });
  });

  it('names earlier claims and a lost worker as kinds, with what they hold', () => {
    const model = modelOf([exampleAttempt(PLACED_TWO_CLAIMS, 1), exampleAttempt(RECOVERED, 2)]);
    const rows = model.intervals.map(interval => [interval.kind, interval.detail]);

    expect(rows).toContainEqual(['earlier_claims', '1 earlier claim']);
    expect(rows).toContainEqual(['claimed', 'a child took the task after 1.00s']);
    expect(rows).toContainEqual(['unknown', 'claims not counted']);
    expect(rows).toContainEqual(['worker_lost', null]);
  });

  it('draws an attempt from before the lifecycle record as one unknown interval', () => {
    const model = modelOf([exampleAttempt(NOT_CAPTURED)]);

    expect(model.intervals.map(interval => [interval.kind, interval.detail, intervalMs(interval)])).toEqual([
      ['unknown', 'attempt row with no phase record', 5_000],
    ]);
  });

  it('keeps the attempt in progress apart from the intervals', () => {
    const model = panelModel(
      attemptStory([normalAttempt(1, 0, 'FAILED')], leafTask('RUNNING', 1, 2, { enqueuedAt: at(10), startedAt: at(12) }))
    );

    expect(model.live).toEqual({ kind: 'live', attempt: { kind: 'running', number: 2, startedMs: ms(12) } });
    expect(model.intervals.at(-1)?.kind).toBe('retry_wait');
  });
});

describe('panelSpan and panelKeyPhases', () => {
  it('spans from the first due time to the last record; the totals of the key add up to it', () => {
    const model = modelOf(RETRIED);
    const span = panelSpan(model.intervals);
    const phases = panelKeyPhases(model.intervals, model.attempts.length);

    expect(span).toEqual({ kind: 'span', fromMs: ms(0), toMs: ms(33.006), spanMs: 33_006 });
    expect(phases.map(entry => entry.phase)).toEqual(['enqueued', 'claimed', 'running', 'finalizing', 'retry_wait']);
    expect(phases[4]?.total).toEqual({ kind: 'total', ms: 23_988, context: 'total of 2' });
    const sum = phases.reduce((total, entry) => total + (entry.total.kind === 'total' ? entry.total.ms : 0), 0);
    expect(sum).toBeCloseTo(33_006, 6);
  });

  it('gives the key of one attempt no totals: its bars show each time', () => {
    const model = modelOf([normalAttempt(1, 0, 'COMPLETED')]);

    expect(panelKeyPhases(model.intervals, 1).every(entry => entry.total.kind === 'no_total')).toBe(true);
  });

  it('has no times for a task with no recorded time', () => {
    expect(panelSpan([])).toEqual({ kind: 'no_times' });
  });

  it('lists each outcome once, in attempt order, for the key', () => {
    expect(panelOutcomes(modelOf(RETRIED).attempts)).toEqual(['FAILED', 'COMPLETED']);
  });
});

describe('errorGroups', () => {
  const failed = (number: number, code: string | null, message: string | null): TaskAttempt => ({
    ...normalAttempt(number, number * 10, 'FAILED'),
    error_code: code,
    error_message: message,
  });

  it('shows identical errors once, with the attempts that had them', () => {
    const groups = errorGroups(
      [1, 2, 3, 4].map(number => recordedAttempt(failed(number, 'SUPPLIER_TIMEOUT', 'did not answer')))
    );

    expect(groups).toEqual([
      {
        code: { kind: 'code', code: 'SUPPLIER_TIMEOUT' },
        rows: [{ message: { kind: 'text', text: 'did not answer' }, attempts: [1, 2, 3, 4] }],
      },
    ]);
  });

  it('keeps two messages of one code apart, and a code with no message', () => {
    const groups = errorGroups([
      recordedAttempt(failed(1, 'PSP_UNAVAILABLE', 'unreachable on attempt 1')),
      recordedAttempt(failed(2, 'PSP_UNAVAILABLE', 'unreachable on attempt 2')),
      recordedAttempt(failed(3, 'WORKER_CRASHED', null)),
      recordedAttempt(failed(4, 'PSP_UNAVAILABLE', 'unreachable on attempt 1')),
    ]);

    expect(groups.map(group => [group.code, group.rows.map(row => [row.message, row.attempts])])).toEqual([
      [
        { kind: 'code', code: 'PSP_UNAVAILABLE' },
        [
          [{ kind: 'text', text: 'unreachable on attempt 1' }, [1, 4]],
          [{ kind: 'text', text: 'unreachable on attempt 2' }, [2]],
        ],
      ],
      [{ kind: 'code', code: 'WORKER_CRASHED' }, [[{ kind: 'no_message' }, [3]]]],
    ]);
  });

  it('has no error for an attempt with no code and no message', () => {
    expect(errorGroups([recordedAttempt(normalAttempt(1, 0, 'COMPLETED'))])).toEqual([]);
  });
});

describe('rangeText', () => {
  it('writes attempt numbers as ranges', () => {
    expect(rangeText([1, 2, 3, 4])).toBe('1–4');
    expect(rangeText([1, 3])).toBe('1, 3');
    expect(rangeText([2])).toBe('2');
    expect(rangeText([1, 2, 4, 5, 6])).toBe('1–2, 4–6');
  });
});

describe('taskOnlyFailure', () => {
  const attempts = [recordedAttempt({ ...normalAttempt(1, 0, 'FAILED'), error_message: 'boom' })];

  it('keeps the failure of the task that no attempt shows', () => {
    expect(taskOnlyFailure({ ...leafTask('EXPIRED', 0, 0, { enqueuedAt: null, startedAt: null }), failed_reason: 'expired' }, attempts)).toBe(
      'expired'
    );
  });

  it('has none when an attempt shows it, or when the task has none', () => {
    const leaf = leafTask('FAILED', 0, 0, { enqueuedAt: null, startedAt: null });

    expect(taskOnlyFailure({ ...leaf, failed_reason: 'boom' }, attempts)).toBeNull();
    expect(taskOnlyFailure(leaf, attempts)).toBeNull();
  });
});

describe('barGroupingOf', () => {
  it('reads a stored grouping, and takes the default for any other value', () => {
    expect(barGroupingOf('phase')).toBe('phase');
    expect(barGroupingOf('attempt')).toBe('attempt');
    expect(barGroupingOf('by attempt')).toBe('phase');
    expect(barGroupingOf(null)).toBe('phase');
    expect(barGroupingOf(42)).toBe('phase');
  });
});

describe('the highlight of the panel', () => {
  const model = modelOf(RETRIED);
  const [queue1] = model.intervals;
  const wait12 = model.intervals.find(interval => interval.kind === 'retry_wait');
  if (queue1 === undefined || wait12 === undefined) {
    throw new Error('the retried task has a queue wait and a retry wait');
  }
  const none: PanelHighlight = { kind: 'none' };
  const onInterval: PanelHighlight = { kind: 'interval', id: queue1.id, phase: 'enqueued', attempt: 1 };

  it('lights every mark when the cursor is on no mark', () => {
    expect(intervalLit(none, wait12)).toBe(true);
    expect(attemptLit(none, 2)).toBe(true);
    expect(keyLightOf(none)).toEqual({ kind: 'all' });
  });

  it('lights an interval, its attempt and its key entry from one of its marks', () => {
    expect(intervalLit(onInterval, queue1)).toBe(true);
    expect(intervalLit(onInterval, wait12)).toBe(false);
    expect(attemptLit(onInterval, 1)).toBe(true);
    expect(attemptLit(onInterval, 2)).toBe(false);
    expect(keyLightOf(onInterval)).toEqual({ kind: 'phase', phase: 'enqueued' });
  });

  it('lights the marks of an attempt from its number, the retry wait after it included', () => {
    const onAttempt: PanelHighlight = { kind: 'attempt', attempt: 1 };

    expect(intervalLit(onAttempt, queue1)).toBe(true);
    expect(intervalLit(onAttempt, wait12)).toBe(true);
    expect(attemptLit(onAttempt, 2)).toBe(false);
    expect(keyLightOf(onAttempt)).toEqual({ kind: 'all' });
  });

  it('lights the marks of a kind from its key entry, and every attempt number', () => {
    const onPhase: PanelHighlight = { kind: 'phase', phase: 'retry_wait' };

    expect(intervalLit(onPhase, wait12)).toBe(true);
    expect(intervalLit(onPhase, queue1)).toBe(false);
    expect(attemptLit(onPhase, 3)).toBe(true);
  });

  it('writes the retry wait from attempt 1 to attempt 2 as 1→2', () => {
    expect(ownerText(wait12.owner)).toBe('1→2');
    expect(formatMilliseconds(intervalMs(wait12))).toBe('6.99s');
  });
});
