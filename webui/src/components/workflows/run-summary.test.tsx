import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { criticalPathText, RunTimingSummaryView } from '@/components/workflows/run-summary';
import { criticalPath } from '@/components/workflows/run-timing';
import { nodeStatusCounts } from '@/components/workflows/status';
import { readClockOf } from '@/hooks/use-draw-time';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  CASCADE_EDGES,
  CASCADE_NODES,
  exactTiming,
  nth,
  OPEN_RUN_WITH_PAUSES,
  UNAVAILABLE_RUN,
  workflowNode,
  workflowRun,
} from '@/test/workflow-fixtures';
import type { RunTiming } from '@/types/timing';
import type { WorkflowRunDetail } from '@/types/workflows';

/** A closed composed run from 12:00:00 to 12:00:50, read at 12:01:40. */
const CLOSED_RUN: RunTiming = nth(TIMING_EXAMPLES.run_timings, 2);
const READ_MS = instantMs('2026-11-22T12:01:40Z');

function cascadeRun(timing: RunTiming): WorkflowRunDetail {
  return { ...workflowRun('FAILED', CASCADE_NODES, timing), edges: CASCADE_EDGES };
}

/** A run of two nodes with real times: 3 s of task code each, engine steps
 * of milliseconds, from 12:00:00 to 12:00:06.012. */
function normalRun(): WorkflowRunDetail {
  const first = exactTiming(
    [
      ['enqueued', 0, 0.003],
      ['claimed', 0.003, 0.005],
      ['running', 0.005, 3.005],
      ['finalizing', 3.005, 3.006],
    ],
    'COMPLETED',
    3.006
  );
  const second = exactTiming(
    [
      ['pending', 0, 3.006],
      ['enqueued', 3.006, 3.009],
      ['claimed', 3.009, 3.011],
      ['running', 3.011, 6.011],
      ['finalizing', 6.011, 6.012],
    ],
    'COMPLETED',
    6.012
  );
  if (CLOSED_RUN.kind !== 'composed') {
    throw new Error('the example run is composed');
  }
  return {
    ...workflowRun(
      'COMPLETED',
      [workflowNode(0, 'first', 'COMPLETED', first), workflowNode(1, 'second', 'COMPLETED', second)],
      { ...CLOSED_RUN, coverage: { kind: 'exact' }, range_ended_at: '2026-11-22T12:00:06.012Z' }
    ),
    edges: [{ from_index: 0, to_index: 1 }],
  };
}

function renderSummary(
  detail: WorkflowRunDetail,
  onToggle = vi.fn(),
  active: ReadonlySet<string> = new Set()
): ReturnType<typeof render> {
  return render(
    <RunTimingSummaryView
      detail={detail}
      readClock={readClockOf(detail.timing, READ_MS)}
      statusFilter={active}
      onToggleStatus={onToggle}
    />
  );
}

const summary = (): HTMLElement => screen.getByRole('region', { name: 'Run summary' });

const figureRows = (): string[] =>
  [...summary().querySelectorAll<HTMLElement>('[data-figure]')].map(
    figure => `${figure.textContent} ${figure.getAttribute('data-weight')}`
  );

describe('RunTimingSummaryView', () => {
  it('shows the duration of the run in plain words', () => {
    renderSummary(normalRun());

    expect(summary().textContent).toContain('Duration6.0s');
    expect(summary().querySelector('[data-run-duration]')?.textContent).toBe('6.0s');
    // No heading in capitals, and no word that needs a glossary.
    expect(summary().innerHTML).not.toContain('uppercase');
    expect(summary().textContent).not.toMatch(/wall|where the time went/i);
  });

  it('splits the duration into the phases of the critical path, which add up to it', () => {
    renderSummary(normalRun());

    expect(figureRows()).toEqual([
      'queue wait6ms quiet',
      'slot wait4ms quiet',
      'running6.0s main',
      'finalizing2ms quiet',
    ]);
    const path = criticalPath(normalRun().nodes, normalRun().edges, READ_MS);
    expect(path.kind === 'path' ? path.lengthMs : 0).toBe(6_012);
  });

  it('draws no bar and holds no legend: each view has the key to its own drawing', () => {
    renderSummary(cascadeRun(CLOSED_RUN));

    expect(screen.queryByRole('img')).toBeNull();
    expect(screen.queryByRole('list', { name: 'Legend' })).toBeNull();
  });

  it('marks a long engine step of the critical path', () => {
    renderSummary(cascadeRun(CLOSED_RUN));

    // The cascade has two queue waits of 1 s on its path.
    expect(figureRows()).toEqual(['queue wait2.0s marked', 'running5.5s main', 'advancing500ms marked']);
  });

  it('explains the critical path and names its chain on its label', () => {
    renderSummary(cascadeRun(CLOSED_RUN));

    const label = screen.getByText('critical path');
    const text = label.closest('[aria-description]')?.getAttribute('aria-description') ?? '';
    expect(text).toContain('the chain of nodes whose end times decided the end of the run: a → b → e.');
    expect(text).toContain('queue wait 2.0s, running 5.5s, advancing 500ms');
    expect(text).toBe(
      criticalPathText(cascadeRun(CLOSED_RUN), criticalPath(CASCADE_NODES, CASCADE_EDGES, READ_MS))
    );
  });

  it('shows the pause time of a paused run as a plain time, and the duration so far', () => {
    // An open run follows the clock of the browser: the read arrives now.
    vi.useFakeTimers();
    vi.setSystemTime(READ_MS);
    try {
      renderSummary(cascadeRun(OPEN_RUN_WITH_PAUSES));

      expect(summary().querySelector('[data-run-duration]')?.textContent).toBe('1m 40s so far');
      expect(figureRows()).toContain('paused1m 37s so far plain');
    } finally {
      vi.useRealTimers();
    }
  });

  it('names the long engine steps of the nodes off the critical path', () => {
    // Off the path of first → last: `slow` waits 7 s in the queue, `held`
    // waits 3 s in the queue and 800 ms for a slot.
    const timing = exactTiming;
    const nodes = [
      workflowNode(0, 'first', 'COMPLETED', timing([['running', 0, 2]], 'COMPLETED', 2)),
      workflowNode(1, 'slow', 'COMPLETED', timing([['pending', 0, 2], ['enqueued', 2, 9], ['running', 9, 10]], 'COMPLETED', 10)),
      workflowNode(2, 'held', 'COMPLETED', timing([['pending', 0, 2], ['enqueued', 2, 5], ['claimed', 5, 5.8], ['running', 5.8, 6]], 'COMPLETED', 6)),
      workflowNode(3, 'last', 'COMPLETED', timing([['pending', 0, 2], ['running', 2, 20]], 'COMPLETED', 20)),
    ];
    renderSummary({
      ...workflowRun('COMPLETED', nodes, CLOSED_RUN),
      edges: [1, 2, 3].map(to => ({ from_index: 0, to_index: to })),
    });

    const other = summary().querySelector('[data-off-path]');
    expect(other?.textContent).toBe('other nodesqueue waitup to 7.0sin 2 nodesslot wait800msin held');
    const hint = screen.getByText('up to 7.0s').closest('[aria-description]')?.getAttribute('aria-description');
    expect(hint).toContain('not on the critical path: slow 7.0s, held 3.0s.');
    expect(hint).toContain('did not decide the end of the run');
    // The split of the duration holds the path alone: no long step on it.
    expect(figureRows()).toEqual(['running20.0s main']);
  });

  it('has no line for the other nodes when none of them has a long step', () => {
    renderSummary(normalRun());

    expect(summary().querySelector('[data-off-path]')).toBeNull();
  });

  it('shows the pause time alone for a paused run in which no node has timing', () => {
    vi.useFakeTimers();
    vi.setSystemTime(READ_MS);
    try {
      renderSummary(workflowRun('PAUSED', [], OPEN_RUN_WITH_PAUSES));

      expect(figureRows()).toEqual(['paused1m 37s so far plain']);
      expect(screen.queryByText('critical path')).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it('shows no pause time for a run with no pause', () => {
    renderSummary(normalRun());

    expect(summary().querySelector('[data-figure="paused"]')).toBeNull();
  });

  it('counts the nodes by status; a count toggles the status filter', () => {
    const onToggle = vi.fn();
    renderSummary(cascadeRun(CLOSED_RUN), onToggle, new Set(['FAILED']));

    const failed = screen.getByRole('button', { name: /1\s*failed/ });
    expect(failed.getAttribute('aria-pressed')).toBe('true');
    expect(screen.getByRole('button', { name: /4\s*completed/ }).getAttribute('aria-pressed')).toBe('false');
    fireEvent.click(failed);
    expect(onToggle).toHaveBeenCalledWith('FAILED');
  });

  it('says why a run has no timing, and keeps the status counts', () => {
    renderSummary(cascadeRun(UNAVAILABLE_RUN));

    expect(summary().textContent).toContain('Timing unavailable. The run has more than 100,000 events.');
    expect(screen.getByRole('button', { name: /1\s*skipped/ })).not.toBeNull();
    expect(summary().querySelector('[data-figure]')).toBeNull();
  });
});

describe('nodeStatusCounts', () => {
  it('counts each status in display order, other statuses last in name order', () => {
    expect(nodeStatusCounts(['SKIPPED', 'COMPLETED', 'WAITING', 'FAILED', 'COMPLETED', 'ARCHIVED'])).toEqual([
      { status: 'COMPLETED', count: 2 },
      { status: 'FAILED', count: 1 },
      { status: 'SKIPPED', count: 1 },
      { status: 'ARCHIVED', count: 1 },
      { status: 'WAITING', count: 1 },
    ]);
  });
});
