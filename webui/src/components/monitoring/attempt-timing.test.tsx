import { render, screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { recordedAttempt } from '@/components/monitoring/attempt-story';
import {
  allowedText,
  attemptFigures,
  attemptNoteView,
  attemptPhaseLabel,
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
  NORMAL_TIMES,
  normalAttempt,
  NOT_CAPTURED,
  PLACED_TWO_CLAIMS,
  RECOVERED,
  timedAttempt,
} from '@/test/attempt-fixtures';

const ms = (seconds: number): number => instantMs(at(seconds));
const BASE = { kind: 'at', ms: ms(0) } as const;

const attemptCard = (number: number): HTMLElement =>
  screen.getByRole('article', { name: `Attempt ${number}` });

/** The figures of a card as `label time weight`. */
const figureRows = (card: HTMLElement): string[] =>
  [...card.querySelectorAll<HTMLElement>('[data-figure]')].map(
    figure =>
      `${figure.querySelector('span span')?.textContent} ${figure.querySelector('span span:last-child')?.textContent} ${figure.getAttribute('data-weight')}`
  );

describe('AttemptTiming with one attempt', () => {
  it('answers first: outcome, error code and the time of the attempt, then the date line once', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    const card = attemptCard(1);
    expect(card.textContent).toContain('failed');
    expect(card.textContent).toContain('PSP_UNAVAILABLE');
    expect(card.querySelector('[data-attempt-time]')?.textContent).toBe('3.0s');
    expect(screen.getAllByText(/times in/)).toHaveLength(1);
    // Times of day only: the date is in the date line, not on each attempt.
    expect(card.textContent).not.toMatch(/2026-11-22/);
  });

  it('gives each phase as a named number, in record order', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    expect(figureRows(attemptCard(1))).toEqual([
      'queue wait 3ms quiet',
      'slot wait 2ms quiet',
      'running 3.0s main',
      'finalizing 1ms quiet',
    ]);
  });

  it('draws no proportional bar: an engine step of milliseconds has no width beside seconds', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    expect(within(attemptCard(1)).queryByRole('img')).toBeNull();
    expect(attemptCard(1).querySelector('[data-phase]')).toBeNull();
  });

  it('shows no note in the normal case, and keeps the error message', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'FAILED')]} leaf={null} isLoading={false} />);

    expect(screen.queryAllByRole('note')).toHaveLength(0);
    expect(attemptCard(1).textContent).toContain('unreachable on attempt 1');
    expect(attemptCard(1).textContent).not.toContain('placed');
  });

  it('shows when and where the attempt ran, with the whole host name', () => {
    const host = 'worker-17.eu-central-1.compute.internal';
    render(
      <AttemptTiming
        attempts={[{ ...normalAttempt(1, 0, 'COMPLETED'), worker_hostname: host }]}
        leaf={null}
        isLoading={false}
      />
    );

    const where = attemptCard(1).querySelector('[data-attempt-where]');
    expect(where?.textContent).toContain(`${clockText(ms(0))} → ${clockText(ms(3.006))}`);
    expect(where?.textContent).toContain(`on ${host}`);
    // The host wraps; no class cuts it.
    expect(where?.innerHTML).not.toContain('truncate');
  });
});

describe('AttemptTiming with a long engine step', () => {
  it('marks a long queue wait and says what it tells', () => {
    render(
      <AttemptTiming
        attempts={[timedAttempt(1, 0, { ...NORMAL_TIMES, queueMs: 7_200 }, 'COMPLETED')]}
        leaf={null}
        isLoading={false}
      />
    );

    expect(figureRows(attemptCard(1))).toEqual([
      'queue wait 7.2s marked',
      'slot wait 2ms quiet',
      'running 3.0s main',
      'finalizing 1ms quiet',
    ]);
    const note = within(attemptCard(1)).getByRole('note');
    expect(note.textContent).toContain('Queue wait 7.2s.');
    expect(note.textContent).toContain('the queue is at its concurrency limit');
    expect(attemptCard(1).querySelector('[data-attempt-time]')?.textContent).toBe('10.2s');
  });

  it('marks a long slot wait and splits it at the time a child took the task', () => {
    render(
      <AttemptTiming
        attempts={[timedAttempt(1, 0, { ...NORMAL_TIMES, slotMs: 800, placedMs: 790 }, 'COMPLETED')]}
        leaf={null}
        isLoading={false}
      />
    );

    expect(figureRows(attemptCard(1))[1]).toBe('slot wait 800ms marked');
    const note = within(attemptCard(1)).getByRole('note');
    expect(note.textContent).toContain('Slot wait 800ms.');
    expect(note.textContent).toContain('A child took the task after 790ms; the start on the child took 10ms.');
  });
});

describe('AttemptTiming with unusual records', () => {
  it('shows the unusual facts as notes', () => {
    render(
      <AttemptTiming
        attempts={[exampleAttempt(PLACED_TWO_CLAIMS, 1), exampleAttempt(RECOVERED, 2), exampleAttempt(CANCELLED, 3)]}
        leaf={null}
        isLoading={false}
      />
    );

    const notesOf = (number: number): (string | null)[] =>
      within(attemptCard(number))
        .getAllByRole('note')
        .map(note => note.textContent);

    expect(notesOf(1).some(text => text?.startsWith('Claimed 2 times'))).toBe(true);
    expect(notesOf(2).some(text => text?.startsWith('Worker lost'))).toBe(true);
    expect(notesOf(2).some(text => text?.startsWith('Claims not counted'))).toBe(true);
    expect(notesOf(3).some(text => text?.startsWith('Cancelled'))).toBe(true);
  });

  it('names the time of earlier claims and of a record gap by what they are', () => {
    render(
      <AttemptTiming
        attempts={[exampleAttempt(PLACED_TWO_CLAIMS, 1), exampleAttempt(RECOVERED, 2)]}
        leaf={null}
        isLoading={false}
      />
    );

    expect(figureRows(attemptCard(1))).toEqual([
      'queue wait 1.0s marked',
      'earlier claims 1.0s marked',
      'slot wait 2.0s marked',
      'running 4.0s main',
      'finalizing 1.0s marked',
    ]);
    expect(figureRows(attemptCard(2))).toEqual([
      'unknown 2.0s marked',
      'slot wait 2.0s marked',
      'running 3.0s main',
      'worker lost 2.0s marked',
    ]);
  });

  it('says why an attempt from before the lifecycle record has no phases', () => {
    render(<AttemptTiming attempts={[exampleAttempt(NOT_CAPTURED)]} leaf={null} isLoading={false} />);

    expect(attemptCard(1).querySelector('[data-figure]')).toBeNull();
    expect(within(attemptCard(1)).getByRole('note').textContent).toContain('Phase times not captured');
    // Its row times still give its time: started at 4 s, recorded at 9 s.
    expect(attemptCard(1).querySelector('[data-attempt-time]')?.textContent).toBe('5.0s');
  });
});

describe('AttemptTiming with retries', () => {
  const attempts = [normalAttempt(1, 0, 'FAILED'), normalAttempt(2, 10, 'FAILED'), normalAttempt(3, 30, 'COMPLETED')];

  it('reads as one story: the totals, then each attempt with the retry wait between them', () => {
    render(
      <AttemptTiming
        attempts={attempts}
        leaf={leafTask('COMPLETED', 2, 4, { enqueuedAt: at(30), startedAt: at(30.005) })}
        isLoading={false}
      />
    );

    const section = screen.getByRole('region', { name: 'Attempts' });
    expect(section.textContent).toContain('3 of 5 allowed');
    expect(screen.getAllByRole('article').map(card => card.getAttribute('aria-label'))).toEqual([
      'Attempt 1',
      'Attempt 2',
      'Attempt 3',
    ]);
    const gaps = section.querySelectorAll('[data-story="retry_wait"]');
    expect([...gaps].map(gap => gap.textContent?.replace(/\d\d:.*/, ''))).toEqual([
      'retry wait7.0s',
      'retry wait17.0s',
    ]);
  });

  it('totals the phases over all attempts: the retry wait is a plain time, not a marked one', () => {
    render(<AttemptTiming attempts={attempts} leaf={null} isLoading={false} />);

    const totals = screen.getByRole('region', { name: 'Attempts' }).querySelector<HTMLElement>(
      '[data-story="totals"]'
    );
    if (totals === null) {
      throw new Error('the story of three attempts has its totals');
    }
    expect(totals.textContent).toContain('3 attempts');
    // From the first due time to the last record: 33.006 s.
    expect(totals.textContent).toContain('33.0s');
    expect(figureRows(totals)).toEqual([
      'queue wait 9ms quiet',
      'retry wait 24.0s plain',
      'slot wait 6ms quiet',
      'running 9.0s main',
      'finalizing 3ms quiet',
    ]);
  });

  it('has no totals for one attempt: its card says the same', () => {
    render(<AttemptTiming attempts={[normalAttempt(1, 0, 'COMPLETED')]} leaf={null} isLoading={false} />);

    expect(document.querySelector('[data-story="totals"]')).toBeNull();
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
    expect(screen.getByText('No attempt is recorded.')).not.toBeNull();
  });
});

describe('attempt texts', () => {
  it('names the time before the claim that ran the attempt "earlier claims"', () => {
    const phases = recordedAttempt(exampleAttempt(PLACED_TWO_CLAIMS)).phases;

    expect(phases.map(attemptPhaseLabel)).toEqual([
      'queue wait',
      'earlier claims',
      'slot wait',
      'running',
      'finalizing',
    ]);
  });

  it('gives each figure a hint that says what the time is', () => {
    const figures = attemptFigures(recordedAttempt(exampleAttempt(RECOVERED)));

    expect(figures.map(figure => figure.label)).toEqual(['unknown', 'slot wait', 'running', 'worker lost']);
    expect(figures[0]?.meaning).toContain('claims of this attempt were not counted');
    expect(figures[3]?.meaning).toContain('last sign of life');
  });

  it('gives the lost worker note its last sign of life, as a time of day', () => {
    const view = attemptNoteView({ kind: 'worker_lost', lastSeenMs: ms(7) }, BASE);

    expect(view.tone).toBe('warning');
    expect(view.text).toContain(`The last sign of life was at ${clockText(ms(7))}`);
  });

  it('writes a long step as its name, its time and what it tells', () => {
    expect(
      attemptNoteView({ kind: 'long_step', key: 'finalizing', ms: 400, tells: 'The outcome write waited.' }, BASE)
    ).toEqual({ tone: 'warning', text: 'Finalizing 400ms. The outcome write waited.' });
    expect(
      attemptNoteView(
        { kind: 'long_slot_wait', ms: 2_000, tells: 'No child was free.', childTook: { kind: 'not_recorded' } },
        BASE
      ).text
    ).toBe('Slot wait 2.0s. No child was free. The record does not show when a child took the task.');
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
