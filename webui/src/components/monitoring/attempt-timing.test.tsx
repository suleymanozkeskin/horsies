import { render, screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { recordedAttempt } from '@/components/monitoring/attempt-story';
import {
  allowedText,
  attemptNoteView,
  attemptPhaseText,
  AttemptTiming,
  liveText,
} from '@/components/monitoring/attempt-timing';
import { clockText } from '@/lib/clock-text';
import { instantMs } from '@/lib/time-text';
import {
  at,
  CANCELLED,
  exampleAttempt,
  leafTask,
  normalAttempt,
  NOT_CAPTURED,
  PLACED_TWO_CLAIMS,
  RECOVERED,
} from '@/test/attempt-fixtures';

const ms = (seconds: number): number => instantMs(at(seconds));
const BASE = { kind: 'at', ms: ms(0) } as const;

const attemptCard = (number: number): HTMLElement =>
  screen.getByRole('article', { name: `Attempt ${number}` });

describe('AttemptTiming with one attempt', () => {
  it('answers first: outcome, error code and run time, then the date line once', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    const card = attemptCard(1);
    expect(card.textContent).toContain('failed');
    expect(card.textContent).toContain('PSP_UNAVAILABLE');
    expect(card.textContent).toContain('ran 3.0s');
    expect(screen.getAllByText(/times in/)).toHaveLength(1);
    // Times of day only: the date is in the date line, not on each row.
    expect(card.textContent).not.toMatch(/2026-11-22/);
    expect(card.textContent).toContain(clockText(ms(2)));
  });

  it('lists each phase with its duration first, its name and its time span', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    const rows = within(attemptCard(1)).getAllByRole('row');
    expect(rows.map(row => row.getAttribute('data-phase'))).toEqual([
      'enqueued',
      'claimed',
      'running',
      'finalizing',
    ]);
    const running = rows[2];
    expect(running?.firstElementChild?.textContent).toBe('3.0s');
    expect(running?.textContent).toContain(`${clockText(ms(2))} → ${clockText(ms(5))}`);
  });

  it('draws a phase bar of the attempt in the timeline fills', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    const bar = within(attemptCard(1)).getByRole('img');
    expect([...bar.querySelectorAll('[data-phase]')].map(part => part.getAttribute('data-phase'))).toEqual([
      'enqueued',
      'claimed',
      'running',
      'finalizing',
    ]);
  });

  it('shows no note in the normal case, and keeps the error message', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    expect(screen.queryAllByRole('note')).toHaveLength(0);
    expect(attemptCard(1).textContent).toContain('unreachable on attempt 1');
    expect(attemptCard(1).textContent).not.toContain('1 claim');
  });

  it('shows the unusual facts as notes', () => {
    render(
      <AttemptTiming
        attempts={[exampleAttempt(PLACED_TWO_CLAIMS, 1), exampleAttempt(RECOVERED, 2), exampleAttempt(CANCELLED, 3)]}
        leaf={null}
        isLoading={false}
      />
    );

    expect(within(attemptCard(1)).getByRole('note').textContent).toContain('Claimed 2 times');
    const lost = within(attemptCard(2)).getAllByRole('note').map(note => note.textContent);
    expect(lost.some(text => text?.startsWith('Worker lost'))).toBe(true);
    expect(lost.some(text => text?.startsWith('Claims not counted'))).toBe(true);
    expect(within(attemptCard(3)).getByRole('note').textContent).toContain('Cancelled');
  });

  it('says why an attempt from before the lifecycle record has no phases', () => {
    render(<AttemptTiming attempts={[exampleAttempt(NOT_CAPTURED)]} leaf={null} isLoading={false} />);

    expect(within(attemptCard(1)).queryByRole('table')).toBeNull();
    expect(within(attemptCard(1)).getByRole('note').textContent).toContain('Phase times not captured');
  });
});

describe('AttemptTiming with retries', () => {
  const attempts = [normalAttempt(1, 0, 'FAILED'), normalAttempt(2, 10, 'FAILED'), normalAttempt(3, 30, 'COMPLETED')];

  it('reads as one story: a summary bar, then each attempt with the retry wait between them', () => {
    render(
      <AttemptTiming
        attempts={attempts}
        leaf={leafTask('COMPLETED', 2, 4, { enqueuedAt: at(30), startedAt: at(32) })}
        isLoading={false}
      />
    );

    const section = screen.getByRole('region', { name: 'Attempts' });
    expect(section.textContent).toContain('3 of 5 allowed');
    expect(section.textContent).toContain('36.0s in all · running 9.0s · retry wait 18.0s');
    expect(screen.getAllByRole('article').map(card => card.getAttribute('aria-label'))).toEqual([
      'Attempt 1',
      'Attempt 2',
      'Attempt 3',
    ]);
    const gaps = section.querySelectorAll('[data-story="retry_wait"]');
    expect([...gaps].map(gap => gap.textContent?.split('retry wait')[0])).toEqual(['4.0s', '14.0s']);
  });

  it('ends with the attempt in progress of a running task', () => {
    render(
      <AttemptTiming
        attempts={[normalAttempt(1, 0, 'FAILED')]}
        leaf={leafTask('RUNNING', 1, 2, { enqueuedAt: at(10), startedAt: at(12) })}
        isLoading={false}
      />
    );

    const live = screen.getByRole('article', { name: 'Attempt 2 in progress' });
    expect(live.textContent).toContain(`Running since ${clockText(ms(12))}`);
  });
});

describe('AttemptTiming empty states', () => {
  it('says loading, then that no attempt is recorded', () => {
    const { rerender } = render(<AttemptTiming attempts={[]} leaf={null} isLoading />);
    expect(screen.getByText('Loading…')).not.toBeNull();

    rerender(<AttemptTiming attempts={[]} leaf={null} isLoading={false} />);
    expect(screen.getByText('No attempts recorded yet.')).not.toBeNull();
  });
});

describe('attempt texts', () => {
  it('writes the placement as an offset from the claim, and earlier claims as a count', () => {
    const phases = recordedAttempt(exampleAttempt(PLACED_TWO_CLAIMS)).phases;

    expect(phases.map(attemptPhaseText)).toEqual([
      'queue wait',
      'slot wait · 1 earlier claim',
      'slot wait · placed +1.0s',
      'running',
      'finalizing',
    ]);
  });

  it('gives the lost worker note its last sign of life, as a time of day', () => {
    expect(attemptNoteView({ kind: 'worker_lost', lastSeenMs: ms(7) }, BASE)).toEqual({
      tone: 'warning',
      text: `Worker lost: stale recovery recorded the attempt. The last sign of life was at ${clockText(ms(7))}.`,
    });
  });

  it('counts the allowed attempts, and says nothing for one attempt of a task with no retry', () => {
    expect(allowedText(3, { kind: 'limit', count: 5 })).toBe('3 of 5 allowed');
    expect(allowedText(1, { kind: 'limit', count: 1 })).toBeNull();
    expect(allowedText(2, { kind: 'no_task_row' })).toBeNull();
  });

  it('says whether a pending attempt still waits for its retry or waits in the queue', () => {
    const pending = { kind: 'pending', number: 2, dueMs: ms(20) } as const;

    expect(liveText(pending, ms(17), BASE)).toBe(
      `Waits for its retry: due at ${clockText(ms(20))}, in 3.0s.`
    );
    expect(liveText(pending, ms(21), BASE)).toBe(
      `Waits in the queue since ${clockText(ms(20))}: 1.0s so far.`
    );
  });
});
