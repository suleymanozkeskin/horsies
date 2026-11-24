import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { segmentLabel, TimelineView } from '@/components/workflows/workflow-timeline';
import { readClockOf } from '@/hooks/use-draw-time';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  CASCADE_EDGES,
  CASCADE_NODES,
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  nth,
  OPEN_RUN_WITH_PAUSES,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
  UNAVAILABLE_RUN,
  workflowNode,
  workflowRun,
} from '@/test/workflow-fixtures';
import type { RunTiming } from '@/types/timing';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

/** The browser receives the read at the database time of the read. */
const RECEIVED_AT_MS = instantMs('2026-11-22T12:01:40Z');

/** Four nodes, out of task-index order: a retried exact node, a partial node
 * with an open segment, a node with unavailable timing, an inconsistent node. */
const NODES: WorkflowNode[] = [
  workflowNode(3, 'broken', 'RUNNING', CLOCK_ORDER_NODE),
  workflowNode(1, 'load', 'RUNNING', PARTIAL_OPEN_NODE),
  workflowNode(0, 'extract', 'COMPLETED', EXACT_RETRIED_NODE),
  workflowNode(2, 'legacy', 'COMPLETED', UNAVAILABLE_NODE),
];

function renderTimeline(detail: WorkflowRunDetail): ReturnType<typeof render> {
  return render(
    <TimelineView
      detail={detail}
      readClock={readClockOf(detail.timing, RECEIVED_AT_MS)}
      selectedIndex={null}
      onSelectNode={vi.fn()}
      statusFilter={new Set()}
    />
  );
}

const openRun = (): WorkflowRunDetail => workflowRun('RUNNING', NODES, OPEN_RUN_WITH_PAUSES);

function rowOf(nodeId: string): HTMLElement {
  const row = screen
    .getAllByRole('button')
    .find(button => button.getAttribute('aria-label')?.startsWith(`${nodeId},`));
  if (row === undefined) {
    throw new Error(`no timeline row for ${nodeId}`);
  }
  return row;
}

function segmentOf(row: HTMLElement, index: number): HTMLElement {
  const segment = row.querySelector<HTMLElement>(`[data-segment-index="${index}"]`);
  if (segment === null) {
    throw new Error(`no segment ${index} in the row`);
  }
  return segment;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(RECEIVED_AT_MS);
  vi.stubGlobal('fetch', vi.fn());
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('TimelineView rows', () => {
  it('draws one row per node, in task-index order', () => {
    renderTimeline(openRun());

    const names = screen
      .getAllByRole('button')
      .map(row => row.getAttribute('aria-label')?.split(',')[0]);

    expect(names).toEqual(['extract', 'load', 'legacy', 'broken']);
  });

  it('stacks every phase segment of a retried node, attempt by attempt', () => {
    renderTimeline(openRun());

    const phases = [...rowOf('extract').querySelectorAll('[data-phase]')].map(segment =>
      segment.getAttribute('data-phase')
    );

    // Attempt 1: queue, earlier claims, the recorded claim, running,
    // finalizing; attempt 2: retry wait and a claim; then the node advances.
    expect(phases).toEqual([
      'pending',
      'ready',
      'enqueued',
      'claimed',
      'claimed',
      'running',
      'finalizing',
      'retry_wait',
      'claimed',
      'advancing',
    ]);
  });

  it('places each segment at its absolute time on the run axis', () => {
    renderTimeline(openRun());

    // The run axis is 100 s; the enqueued segment runs from 2 s to 3 s.
    const enqueued = segmentOf(rowOf('extract'), 2);

    expect(enqueued.style.left).toBe('2%');
    expect(enqueued.style.width).toBe('1%');
  });

  it('marks the terminal status as an endpoint, and a node that is not terminal has none', () => {
    renderTimeline(openRun());

    const endpoint = rowOf('extract').querySelector<HTMLElement>('[data-endpoint]');

    expect(endpoint?.getAttribute('data-endpoint')).toBe('COMPLETED');
    expect(endpoint?.style.left).toBe('12%');
    expect(rowOf('load').querySelector('[data-endpoint]')).toBeNull();
  });

  it('hatches a row whose timing is unavailable or inconsistent, with its label', () => {
    renderTimeline(openRun());

    const legacy = rowOf('legacy');
    const broken = rowOf('broken');

    expect(legacy.textContent).toContain('Timing unavailable');
    expect(legacy.querySelector('[data-phase]')).toBeNull();
    expect(broken.textContent).toContain('Timing inconsistent');
    expect(broken.querySelector('[data-phase]')).toBeNull();
    expect(broken.querySelector('[title^="Clock order"]')).not.toBeNull();
  });

  it('shows the wall time of each node in the right column', () => {
    renderTimeline(openRun());

    // From the first segment (0 s) to the endpoint (12 s), and from 0 s to
    // the read for the open node.
    expect(rowOf('extract').textContent).toContain('12.0s');
    expect(rowOf('load').textContent).toContain('1m 40s');
    expect(rowOf('legacy').textContent).toContain('—');
  });

  it('names each row with its timing summary', () => {
    renderTimeline(openRun());

    const name = rowOf('extract').getAttribute('aria-label') ?? '';

    expect(name).toContain('extract, completed, exact timing; dependency wait 1.0s');
    expect(name).toContain('slot wait 4.0s');
    expect(name).toContain('wall 12.0s');
    expect(name).toContain('completed at');
    expect(rowOf('broken').getAttribute('aria-label')).toContain('timing inconsistent');
  });
});

describe('TimelineView pauses and open segments', () => {
  it('draws each pause as a band over the rows; an open pause runs to the read', () => {
    const { container } = renderTimeline(openRun());

    const bands = [...container.querySelectorAll<HTMLElement>('[data-pause-band]')];

    expect(bands).toHaveLength(3);
    expect(nth(bands, 0).style.left).toBe('1%');
    expect(nth(bands, 0).style.width).toBe('1%');
    expect(nth(bands, 2).style.left).toBe('5%');
    expect(nth(bands, 2).style.width).toBe('95%');
  });

  it('grows an open segment on the one-second clock, with no refetch', () => {
    renderTimeline(openRun());
    const open = (): HTMLElement => segmentOf(rowOf('load'), 1);

    expect(open().getAttribute('data-end')).toBe('open');
    expect(open().style.width).toBe('95%');
    expect(rowOf('load').textContent).toContain('1m 40s');

    act(() => {
      vi.advanceTimersByTime(2_000);
    });

    expect(rowOf('load').textContent).toContain('1m 42s');
    expect(Number.parseFloat(open().style.width)).toBeGreaterThan(95);
    expect(fetch).not.toHaveBeenCalled();
  });

  it('runs no clock for a run with no open segment', () => {
    const closed = workflowRun(
      'COMPLETED',
      [workflowNode(0, 'extract', 'COMPLETED', EXACT_RETRIED_NODE)],
      nth(TIMING_EXAMPLES.run_timings, 2)
    );
    renderTimeline(closed);

    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('TimelineView run coverage', () => {
  it('hatches every row of a run whose timing is unavailable and says why', () => {
    renderTimeline(workflowRun('COMPLETED', NODES, UNAVAILABLE_RUN));

    expect(screen.getByText(/The run has more than 100,000 events\./)).not.toBeNull();
    for (const row of screen.getAllByRole('button')) {
      expect(row.textContent).toContain('Timing unavailable');
      expect(row.querySelector('[data-phase]')).toBeNull();
      expect(row.getAttribute('aria-label')).toContain('timing unavailable');
    }
  });

  it('names the reasons of a partial run and the conflicts of an inconsistent run', () => {
    const partial: RunTiming = nth(TIMING_EXAMPLES.run_timings, 2);
    const inconsistent: RunTiming = nth(TIMING_EXAMPLES.run_timings, 3);

    renderTimeline(workflowRun('COMPLETED', NODES, partial));
    expect(screen.getByText('Partial timing: claims not counted.')).not.toBeNull();
    cleanup();

    renderTimeline(workflowRun('COMPLETED', NODES, inconsistent));
    expect(
      screen.getByText(
        'Inconsistent timing: node #2; pause record: Unexpected event WORKFLOW_RESUMED (#4).'
      )
    ).not.toBeNull();
  });

  it('shows no coverage banner for an exact run', () => {
    renderTimeline(openRun());

    expect(screen.queryByText(/Partial timing|Inconsistent timing|Timing unavailable\./)).toBeNull();
  });
});

describe('TimelineView tooltip', () => {
  it('shows the phase, source, times and duration of the segment under the pointer', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(segmentOf(rowOf('extract'), 3));
    const tooltip = screen.getByRole('tooltip');

    expect(tooltip.textContent).toContain('slot wait · 1 earlier claim');
    expect(tooltip.textContent).toContain('task 1 · attempt 1');
    expect(tooltip.textContent).toContain('1.0s');
    expect(rowOf('extract').getAttribute('aria-describedby')).toBe(tooltip.id);

    fireEvent.pointerLeave(segmentOf(rowOf('extract'), 3));
    expect(screen.queryByRole('tooltip')).toBeNull();
  });

  it('steps through the segments of a row with the arrow keys and moves between rows', () => {
    renderTimeline(openRun());
    const extract = rowOf('extract');
    extract.focus();

    fireEvent.keyDown(extract, { key: 'ArrowRight' });
    expect(screen.getByRole('tooltip').textContent).toContain('dependency wait');
    fireEvent.keyDown(extract, { key: 'ArrowRight' });
    expect(screen.getByRole('tooltip').textContent).toContain('ready');
    fireEvent.keyDown(extract, { key: 'Escape' });
    expect(screen.queryByRole('tooltip')).toBeNull();

    fireEvent.keyDown(extract, { key: 'ArrowDown' });
    expect(document.activeElement).toBe(rowOf('load'));
  });

  it('shows an open segment as open, with its duration so far', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(segmentOf(rowOf('load'), 1));
    const tooltip = screen.getByRole('tooltip');

    expect(tooltip.textContent).toContain('unknown · worker lost');
    expect(tooltip.textContent).toContain('open');
    expect(tooltip.textContent).toContain('1m 35s so far');
  });
});

describe('TimelineView emphasis', () => {
  it('draws dependency wait as a thin track and the other phases at full height', () => {
    renderTimeline(openRun());

    const pending = segmentOf(rowOf('extract'), 0);
    const enqueued = segmentOf(rowOf('extract'), 2);

    expect(pending.getAttribute('data-phase')).toBe('pending');
    expect(pending.firstElementChild?.className).toContain('h-1');
    expect(enqueued.firstElementChild?.className).toContain('inset-0');
  });

  it('marks the nodes of the critical path: a line under the active span, and the name of the row', () => {
    const run = {
      ...workflowRun('FAILED', CASCADE_NODES, nth(TIMING_EXAMPLES.run_timings, 2)),
      edges: CASCADE_EDGES,
    };
    renderTimeline(run);

    const onPath = ['a', 'b', 'e'].map(rowOf);
    const offPath = ['c', 'd'].map(rowOf);

    for (const row of onPath) {
      expect(row.getAttribute('aria-label')).toContain('on the critical path');
      expect(row.querySelector('[data-critical-line]')).not.toBeNull();
    }
    for (const row of offPath) {
      expect(row.getAttribute('aria-label')).not.toContain('on the critical path');
      expect(row.querySelector('[data-critical-line]')).toBeNull();
    }
    // e waits 5 s for its dependencies: its line starts at 5 s of the 50 s axis.
    expect(rowOf('e').querySelector<HTMLElement>('[data-critical-line]')?.style.left).toBe('10%');
  });
});

describe('segmentLabel', () => {
  const wide = { kind: 'measured', px: 1_000 } as const;

  it('labels a full-height segment with its duration when the text fits', () => {
    expect(segmentLabel('running', 4_500, 10, wide)).toEqual({ kind: 'label', text: '4.5s' });
  });

  it('leaves a narrow segment, dependency wait and an unmeasured track without a label', () => {
    expect(segmentLabel('running', 4_500, 1, wide)).toEqual({ kind: 'no_label' });
    expect(segmentLabel('pending', 60_000, 90, wide)).toEqual({ kind: 'no_label' });
    expect(segmentLabel('running', 4_500, 50, { kind: 'unmeasured' })).toEqual({ kind: 'no_label' });
  });
});
