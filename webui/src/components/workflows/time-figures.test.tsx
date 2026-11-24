import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { LONG_ENGINE_STEP_MS } from '@/components/workflows/phase';
import {
  figureHint,
  phaseFigure,
  TimeFigures,
  type TimeFigure,
} from '@/components/workflows/time-figures';
import type { PhaseTotal } from '@/components/workflows/timeline-geometry';

const total = (key: PhaseTotal['key'], ms: number, longestMs = ms, open = false): PhaseTotal => ({
  key,
  ms,
  longestMs,
  open,
});

/** The totals of a normal attempt, and one with a long queue wait. */
const NORMAL: TimeFigure[] = [
  total('enqueued', 3),
  total('claimed', 2),
  total('running', 3_000),
  total('finalizing', 1),
].map(phaseFigure);

const item = (id: string): HTMLElement => {
  const found = document.querySelector<HTMLElement>(`[data-figure="${id}"]`);
  if (found === null) {
    throw new Error(`no figure ${id}`);
  }
  return found;
};

describe('phaseFigure', () => {
  it('labels a phase total by its phase and weighs it by its kind', () => {
    expect(phaseFigure(total('running', 3_000))).toMatchObject({
      id: 'running',
      label: 'running',
      ms: 3_000,
      weight: { kind: 'main' },
    });
    expect(phaseFigure(total('retry_wait', 17_400)).weight).toEqual({ kind: 'plain' });
    expect(phaseFigure(total('enqueued', 3)).weight).toEqual({ kind: 'quiet' });
  });

  it('marks a total by its longest interval, not by its sum', () => {
    // Three queue waits of 100 ms each are 300 ms, and no long step.
    expect(phaseFigure(total('enqueued', 300, 100)).weight).toEqual({ kind: 'quiet' });
    expect(phaseFigure(total('enqueued', 7_203, 7_200)).weight.kind).toBe('marked');
    expect(phaseFigure(total('claimed', LONG_ENGINE_STEP_MS)).weight.kind).toBe('marked');
  });
});

describe('figureHint', () => {
  it('says what the time is, and for a marked time what it tells', () => {
    const quiet = phaseFigure(total('enqueued', 3));
    const marked = phaseFigure(total('enqueued', 7_200));

    expect(figureHint(quiet)).toBe(quiet.meaning);
    expect(figureHint(marked)).toContain(marked.meaning);
    expect(figureHint(marked)).toContain('concurrency limit');
  });
});

describe('TimeFigures', () => {
  it('shows each time as a named number, with no bar', () => {
    render(<TimeFigures figures={NORMAL} layout="stacked" label="Phases of attempt 1" />);

    const list = screen.getByRole('list', { name: 'Phases of attempt 1' });
    expect([...list.querySelectorAll('li')].map(row => row.textContent)).toEqual([
      'queue wait3ms',
      'slot wait2ms',
      'running3.0s',
      'finalizing1ms',
    ]);
    expect(list.querySelector('[role="img"]')).toBeNull();
  });

  it('keeps the normal engine steps quiet and makes the task code the main time', () => {
    render(<TimeFigures figures={NORMAL} layout="stacked" label="Phases" />);

    expect(NORMAL.map(figure => item(figure.id).getAttribute('data-weight'))).toEqual([
      'quiet',
      'quiet',
      'main',
      'quiet',
    ]);
    expect(item('running').innerHTML).toContain('font-semibold');
    expect(item('enqueued').innerHTML).toContain('text-muted-foreground');
  });

  it('marks a long engine step with an icon and its own ink, not with color alone', () => {
    render(
      <TimeFigures
        figures={[phaseFigure(total('enqueued', 7_200)), phaseFigure(total('running', 3_700))]}
        layout="stacked"
        label="Phases"
      />
    );

    const marked = item('enqueued');
    expect(marked.getAttribute('data-weight')).toBe('marked');
    expect(marked.querySelector('svg')).not.toBeNull();
    expect(marked.innerHTML).toContain('var(--marked-ink)');
    expect(item('running').querySelector('svg')).toBeNull();
  });

  it('shows a time with no end yet as a time so far', () => {
    render(
      <TimeFigures figures={[phaseFigure(total('running', 4_200, 4_200, true))]} layout="inline" label="Phases" />
    );

    expect(item('running').textContent).toBe('running4.2s so far');
  });

  it('gives each figure its meaning as its description, for the hover and for a screen reader', () => {
    render(<TimeFigures figures={NORMAL} layout="inline" label="Phases" />);

    const description = item('claimed').querySelector('[aria-description]')?.getAttribute('aria-description');
    expect(description).toContain('From the claim to the start of the task code');
  });

  it('cuts no label: the figures wrap', () => {
    render(
      <TimeFigures figures={[phaseFigure(total('pending', 18_900))]} layout="stacked" label="Phases" />
    );

    expect(item('pending').innerHTML).not.toContain('truncate');
    expect(item('pending').textContent).toBe('dependency wait18.9s');
  });
});
