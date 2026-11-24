import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { RunTimingSummaryView, type RunView } from '@/components/workflows/run-summary';
import { nodeStatusCounts } from '@/components/workflows/status';
import { readClockOf } from '@/hooks/use-draw-time';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  CASCADE_EDGES,
  CASCADE_NODES,
  nth,
  OPEN_RUN_WITH_PAUSES,
  UNAVAILABLE_RUN,
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

function renderSummary(
  detail: WorkflowRunDetail,
  view: RunView,
  onToggle = vi.fn(),
  active: ReadonlySet<string> = new Set()
): ReturnType<typeof render> {
  return render(
    <RunTimingSummaryView
      detail={detail}
      readClock={readClockOf(detail.timing, READ_MS)}
      view={view}
      statusFilter={active}
      onToggleStatus={onToggle}
    />
  );
}

const summary = (): HTMLElement => screen.getByRole('region', { name: 'Run summary' });

describe('RunTimingSummaryView', () => {
  it('shows the wall time of the run and the length of its critical path', () => {
    renderSummary(cascadeRun(CLOSED_RUN), 'timeline');

    expect(summary().textContent).toContain('wall50.0s');
    expect(summary().textContent).toContain('critical path8.0s');
    expect(summary().textContent).toContain('3 nodes');
  });

  it('explains the critical path and names its chain', () => {
    renderSummary(cascadeRun(CLOSED_RUN), 'timeline');

    const trigger = screen.getByRole('button', { name: /^Critical path:/ });
    expect(trigger.getAttribute('aria-label')).toBe(
      'Critical path: a → b → e. 8.0s without dependency wait: queue wait 2.0s, running 5.5s, advancing 500ms.'
    );
  });

  it('draws one bar of where the node time went, with the numbers in the legend', () => {
    renderSummary(cascadeRun(CLOSED_RUN), 'timeline');

    const bar = screen.getByRole('img', { name: /^Where the time went, summed over 6 nodes/ });
    expect([...bar.querySelectorAll('[data-phase]')].map(part => part.getAttribute('data-phase'))).toEqual([
      'pending',
      'enqueued',
      'running',
      'advancing',
    ]);
    const legend = screen.getByRole('list', { name: 'Legend' });
    expect(legend.querySelector('[data-legend-phase="pending"]')?.textContent).toContain('17.0s');
  });

  it('gives the timeline legend its marks, with the pause band only for a paused run', () => {
    renderSummary(cascadeRun(CLOSED_RUN), 'timeline');
    const marks = (): (string | null)[] =>
      [...document.querySelectorAll('[data-legend-mark]')].map(mark => mark.getAttribute('data-legend-mark'));
    expect(marks()).toEqual(['end', 'critical_row']);

    renderSummary(cascadeRun(OPEN_RUN_WITH_PAUSES), 'timeline');
    expect(marks()).toContain('paused');
  });

  it('gives the graph legend the critical edges, and the tolerant join of a run that has one', () => {
    renderSummary(cascadeRun(CLOSED_RUN), 'graph');

    expect(
      [...document.querySelectorAll('[data-legend-mark]')].map(mark => mark.getAttribute('data-legend-mark'))
    ).toEqual(['critical_edge', 'tolerant_join']);
  });

  it('counts the nodes by status; a count toggles the status filter', () => {
    const onToggle = vi.fn();
    renderSummary(cascadeRun(CLOSED_RUN), 'graph', onToggle, new Set(['FAILED']));

    const failed = screen.getByRole('button', { name: /1\s*failed/ });
    expect(failed.getAttribute('aria-pressed')).toBe('true');
    expect(screen.getByRole('button', { name: /4\s*completed/ }).getAttribute('aria-pressed')).toBe('false');
    fireEvent.click(failed);
    expect(onToggle).toHaveBeenCalledWith('FAILED');
  });

  it('says why a run has no timing, and keeps the status counts', () => {
    renderSummary(cascadeRun(UNAVAILABLE_RUN), 'timeline');

    expect(summary().textContent).toContain('Timing unavailable. The run has more than 100,000 events.');
    expect(screen.getByRole('button', { name: /1\s*skipped/ })).not.toBeNull();
    expect(screen.queryByRole('img')).toBeNull();
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
