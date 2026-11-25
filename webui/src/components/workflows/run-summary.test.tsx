import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { RunTimingSummaryView } from '@/components/workflows/run-summary';
import { longestChain } from '@/components/workflows/run-timing';
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
  active: ReadonlySet<string> = new Set(),
  onIsolate = vi.fn()
): ReturnType<typeof render> {
  return render(
    <RunTimingSummaryView
      detail={detail}
      readClock={readClockOf(detail.timing, READ_MS)}
      statusFilter={active}
      onToggleStatus={onToggle}
      isolation={{ kind: 'all' }}
      onIsolate={onIsolate}
    />
  );
}

const summary = (): HTMLElement => screen.getByRole('region', { name: 'Run summary' });

/** The kinds of the key with their totals. */
const keyRows = (): string[] =>
  [...summary().querySelectorAll<HTMLElement>('[data-key-phase]')].map(item => item.textContent ?? '');

describe('RunTimingSummaryView', () => {
  it('shows the duration of the run in plain words, with its date and its start → end', () => {
    renderSummary(normalRun());

    expect(summary().querySelector('[data-run-duration]')?.textContent).toBe('6.01s');
    expect(summary().textContent).toContain('duration6.01s');
    expect(summary().querySelector('[data-run-range]')?.textContent).toMatch(/^\d{4}-\d{2}-\d{2} · \d{2}:\d{2}:\d{2} → \d{2}:\d{2}:\d{2}$/);
    // No heading in capitals, and no word that needs a glossary.
    expect(summary().innerHTML).not.toContain('uppercase');
    expect(summary().textContent).not.toMatch(/wall|where the time went|to scale|critical/i);
  });

  it('splits the duration into the kinds of the longest chain, with their totals, which add up to it', () => {
    renderSummary(normalRun());

    expect(keyRows()).toEqual(['queue wait6.00ms', 'slot wait4.00ms', 'running6.00s', 'finalizing2.00ms']);
    const chain = longestChain(normalRun().nodes, normalRun().edges, READ_MS);
    expect(chain.kind === 'chain' ? chain.lengthMs : 0).toBe(6_012);
  });

  it('names the run, its chain and its phases in the tooltip of its duration', () => {
    renderSummary(cascadeRun(CLOSED_RUN));

    fireEvent.pointerMove(summary().querySelector('[data-run-duration]') as Element, { clientX: 5, clientY: 5 });

    const tooltip = screen.getByRole('tooltip');
    expect(tooltip.textContent).toContain('run');
    expect(tooltip.textContent).toContain('the phases along the longest chain: a → b → e');
    expect(tooltip.textContent).toContain('queue wait2.00s');
  });

  it('lights the marks of a kind while its key entry is under the pointer', () => {
    const onIsolate = vi.fn();
    renderSummary(cascadeRun(CLOSED_RUN), vi.fn(), new Set(), onIsolate);
    const running = summary().querySelector('[data-key-phase="running"]') as Element;

    fireEvent.pointerMove(running, { clientX: 5, clientY: 5 });
    expect(onIsolate).toHaveBeenLastCalledWith({ kind: 'phase', phase: 'running' });

    fireEvent.pointerLeave(running);
    expect(onIsolate).toHaveBeenLastCalledWith({ kind: 'all' });
  });

  it('marks no long step: the kinds of the chain, and no line for the other nodes', () => {
    // Off the chain of first → last: `slow` waits 7 s in the queue.
    const timing = exactTiming;
    const nodes = [
      workflowNode(0, 'first', 'COMPLETED', timing([['running', 0, 2]], 'COMPLETED', 2)),
      workflowNode(1, 'slow', 'COMPLETED', timing([['pending', 0, 2], ['enqueued', 2, 9], ['running', 9, 10]], 'COMPLETED', 10)),
      workflowNode(2, 'last', 'COMPLETED', timing([['pending', 0, 2], ['running', 2, 20]], 'COMPLETED', 20)),
    ];
    renderSummary({ ...workflowRun('COMPLETED', nodes, CLOSED_RUN), edges: [1, 2].map(to => ({ from_index: 0, to_index: to })) });

    expect(keyRows()).toEqual(['running20.0s']);
    expect(summary().querySelector('[data-off-path]')).toBeNull();
    expect(summary().textContent).not.toMatch(/other nodes|long step/);
  });

  it('shows the pause time of a paused run, and the duration so far', () => {
    // An open run follows the clock of the browser: the read arrives now.
    vi.useFakeTimers();
    vi.setSystemTime(READ_MS);
    try {
      renderSummary(cascadeRun(OPEN_RUN_WITH_PAUSES));

      expect(summary().querySelector('[data-run-duration]')?.textContent).toBe('1m 40.0s so far');
      expect(summary().querySelector('[data-run-paused]')?.textContent).toBe('paused1m 37.0s so far');
    } finally {
      vi.useRealTimers();
    }
  });

  it('shows the pause time alone for a paused run in which no node has timing', () => {
    vi.useFakeTimers();
    vi.setSystemTime(READ_MS);
    try {
      renderSummary(workflowRun('PAUSED', [], OPEN_RUN_WITH_PAUSES));

      expect(keyRows()).toEqual([]);
      expect(summary().querySelector('[data-run-paused]')).not.toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it('shows no pause time for a run with no pause', () => {
    renderSummary(normalRun());

    expect(summary().querySelector('[data-run-paused]')).toBeNull();
  });

  it('counts the nodes by status with the sign of each; a count toggles the status filter', () => {
    const onToggle = vi.fn();
    renderSummary(cascadeRun(CLOSED_RUN), onToggle, new Set(['FAILED']));

    expect(summary().textContent).toContain('6 nodes');
    const failed = screen.getByRole('button', { name: /1\s*failed/ });
    expect(failed.getAttribute('aria-pressed')).toBe('true');
    expect(failed.querySelector('[data-outcome]')?.getAttribute('data-outcome')).toBe('failed');
    expect(screen.getByRole('button', { name: /4\s*completed/ }).querySelector('[data-outcome]')?.getAttribute('data-outcome')).toBe(
      'completed'
    );
    expect(screen.getByRole('button', { name: /1\s*skipped/ }).querySelector('[data-outcome]')?.getAttribute('data-outcome')).toBe(
      'skipped'
    );
    fireEvent.click(failed);
    expect(onToggle).toHaveBeenCalledWith('FAILED');
  });

  it('says why a run has no timing, and keeps the status counts', () => {
    renderSummary(cascadeRun(UNAVAILABLE_RUN));

    expect(summary().textContent).toContain('Timing unavailable. The run has more than 100,000 events.');
    expect(screen.getByRole('button', { name: /1\s*skipped/ })).not.toBeNull();
    expect(summary().querySelector('[data-key-phase]')).toBeNull();
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
