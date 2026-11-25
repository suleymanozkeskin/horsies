import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { KeyLight } from '@/components/workflows/timing-key';
import { segmentDrawing } from '@/components/workflows/timeline-row';
import { TimelineView } from '@/components/workflows/workflow-timeline';
import { readClockOf } from '@/hooks/use-draw-time';
import { clockText } from '@/lib/clock-text';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  at,
  CASCADE_EDGES,
  CASCADE_NODES,
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  exactTiming,
  nth,
  OPEN_RUN_WITH_PAUSES,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
  UNAVAILABLE_RUN,
  workflowNode,
  workflowRun,
} from '@/test/workflow-fixtures';
import type { RunTiming } from '@/types/timing';
import type { WorkflowNode, WorkflowRunDetail, WorkflowTaskDetail } from '@/types/workflows';

/** The browser receives the read at the database time of the read. */
const RECEIVED_AT_MS = instantMs('2026-11-22T12:01:40Z');
/** A closed composed run from 12:00:00 to 12:00:50. */
const CLOSED_RUN: RunTiming = nth(TIMING_EXAMPLES.run_timings, 2);

/** Four nodes, out of task-index order: a retried exact node, a partial node
 * with an open segment, a node with unavailable timing, an inconsistent node. */
const NODES: WorkflowNode[] = [
  workflowNode(3, 'broken', 'RUNNING', CLOCK_ORDER_NODE),
  workflowNode(1, 'load', 'RUNNING', PARTIAL_OPEN_NODE),
  workflowNode(0, 'extract', 'COMPLETED', EXACT_RETRIED_NODE),
  workflowNode(2, 'legacy', 'COMPLETED', UNAVAILABLE_NODE),
];

function renderTimeline(
  detail: WorkflowRunDetail,
  isolation: KeyLight = { kind: 'all' },
  onIsolate = vi.fn()
): ReturnType<typeof render> {
  // The tooltip of a failed node reads the error code of its task.
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <TimelineView
        detail={detail}
        readClock={readClockOf(detail.timing, RECEIVED_AT_MS)}
        selectedIndex={null}
        onSelectNode={vi.fn()}
        statusFilter={new Set()}
        isolation={isolation}
        onIsolate={onIsolate}
      />
    </QueryClientProvider>
  );
}

/** The linear cell of a row: the pointer on it shows the mark under it. */
function trackOf(row: HTMLElement): HTMLElement {
  const track = row.querySelector<HTMLElement>('[data-row-track]');
  if (track === null) {
    throw new Error('the row has no linear cell');
  }
  return track;
}

/** The part of the tooltip that names the mark under the cursor. */
function focusText(kind: string): string {
  const focus = screen.getByRole('tooltip').querySelector(`[data-focus="${kind}"]`);
  if (focus === null) {
    throw new Error(`the tooltip has no ${kind} focus`);
  }
  return focus.textContent ?? '';
}

/** A run with one node of real times: 3 s of task code, engine steps of
 * milliseconds, on an axis of 50 s. */
const NORMAL_NODE = exactTiming(
  [
    ['pending', 0, 5],
    ['enqueued', 5, 5.003],
    ['claimed', 5.003, 5.005],
    ['running', 5.005, 8.005],
    ['finalizing', 8.005, 8.006],
    ['advancing', 8.006, 8.007],
  ],
  'COMPLETED',
  8.007
);

const openRun = (): WorkflowRunDetail => workflowRun('RUNNING', NODES, OPEN_RUN_WITH_PAUSES);
const cascadeRun = (): WorkflowRunDetail => ({ ...workflowRun('FAILED', CASCADE_NODES, CLOSED_RUN), edges: CASCADE_EDGES });

function rowOf(nodeId: string): HTMLElement {
  const row = screen
    .getAllByRole('button')
    .find(button => button.getAttribute('aria-label')?.startsWith(`${nodeId},`));
  if (row === undefined) {
    throw new Error(`no timeline row for ${nodeId}`);
  }
  return row;
}

const runRow = (): HTMLElement => screen.getByRole('group', { name: /^run,/ });

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
  it('draws one row per node, in task-index order, after the run row', () => {
    renderTimeline(openRun());

    const names = screen.getAllByRole('button').map(row => row.getAttribute('aria-label')?.split(',')[0]);

    expect(names).toEqual(['extract', 'load', 'legacy', 'broken']);
    expect(runRow().getAttribute('aria-label')).toMatch(/^run, running, duration 1m 40\.0s/);
  });

  it('stacks every segment of a retried node in its linear cell, attempt by attempt', () => {
    renderTimeline(openRun());

    const phases = [...rowOf('extract').querySelectorAll('[data-phase]')].map(segment => segment.getAttribute('data-phase'));

    // Attempt 1: queue, earlier claims, the recorded claim, running,
    // finalizing; attempt 2: retry wait and a claim; then the node advances.
    expect(phases).toEqual([
      'pending',
      'ready',
      'enqueued',
      'earlier_claims',
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

  it('draws dependency wait as a thin line and the other phases at full height', () => {
    renderTimeline(openRun());

    expect(segmentOf(rowOf('extract'), 0).firstElementChild?.className).toContain('h-0.5');
    expect(segmentOf(rowOf('extract'), 2).firstElementChild?.className).toContain('h-2.5');
  });

  it('draws one thin bar of the log cell for each kind of the node, in the lane of its kind', () => {
    renderTimeline(openRun());

    const lanes = [...rowOf('extract').querySelectorAll<HTMLElement>('[data-lane]')];

    // Dependency wait is a line of the linear cell, not a bar.
    expect(lanes.map(lane => lane.getAttribute('data-lane'))).toEqual([
      'ready',
      'enqueued',
      'earlier_claims',
      'claimed',
      'running',
      'finalizing',
      'advancing',
      'retry_wait',
    ]);
    // The lanes keep their place in every row: the lost worker of `load` is
    // under the advancing of `extract`.
    expect(rowOf('load').querySelector<HTMLElement>('[data-lane="worker_lost"]')?.style.top).toBe('42px');
  });

  it('sets the height of the rows from the number of lanes', () => {
    renderTimeline(cascadeRun());

    // Three lanes (queue wait, running, advancing): 3 × 6 + 12 is less than 34.
    expect(rowOf('a').closest('li')?.style.height).toBe('34px');
    expect(runRow().style.height).toBe('40px');
  });

  it('marks the terminal status with the diamond, and a node that is not terminal has none', () => {
    renderTimeline(openRun());

    const endpoint = rowOf('extract').querySelector<HTMLElement>('[data-endpoint]');

    expect(endpoint?.getAttribute('data-endpoint')).toBe('COMPLETED');
    expect(endpoint?.className).toContain('rotate-45');
    expect(endpoint?.style.left).toBe('12%');
    expect(rowOf('load').querySelector('[data-endpoint]')).toBeNull();
  });

  it('hatches a row whose timing is unavailable or inconsistent, with its label', () => {
    renderTimeline(openRun());

    expect(rowOf('legacy').textContent).toContain('Timing unavailable');
    expect(rowOf('legacy').querySelector('[data-phase]')).toBeNull();
    expect(rowOf('broken').textContent).toContain('Timing inconsistent');
    expect(rowOf('broken').querySelector('[data-phase]')).toBeNull();
  });

  it('says why a hatched row has no timing in the tooltip of the row', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(trackOf(rowOf('broken')));
    expect(screen.getByRole('tooltip').textContent).toContain('timing inconsistent: Clock order: claim');

    fireEvent.pointerMove(trackOf(rowOf('legacy')));
    expect(screen.getByRole('tooltip').textContent).toContain('timing unavailable: The run began before the lifecycle record.');
  });

  it('shows the time of each node without its dependency wait in the duration column', () => {
    renderTimeline(openRun());

    // The retried node: 11 s of segments less 1 s of dependency wait. The
    // open node: from 0 s to the read.
    expect(rowOf('extract').textContent).toContain('10.0s');
    expect(rowOf('extract').textContent).not.toContain('11.0s');
    expect(rowOf('load').textContent).toContain('1m 40.0s');
    expect(rowOf('legacy').textContent).toContain('—');
  });

  it('names each row with its timing summary', () => {
    renderTimeline(openRun());

    const name = rowOf('extract').getAttribute('aria-label') ?? '';

    expect(name).toContain('extract, completed, exact timing; dependency wait 1.00s');
    expect(name).toContain('slot wait 3.00s');
    expect(name).toContain('duration 10.0s');
    expect(name).toContain('completed at');
    expect(rowOf('broken').getAttribute('aria-label')).toContain('timing inconsistent');
  });
});

describe('TimelineView header', () => {
  it('labels the linear axis with round times, and the log axis by decade', () => {
    const { container } = renderTimeline(cascadeRun());

    expect([...container.querySelectorAll('[data-axis-tick]')].map(tick => tick.textContent)).toEqual([
      '0',
      '10s',
      '20s',
      '30s',
      '40s',
      '50s',
    ]);
    expect([...container.querySelectorAll('[data-log-tick]')].map(tick => tick.textContent)).toEqual([
      '0.1ms',
      '1ms',
      '10ms',
      '100ms',
      '1s',
    ]);
    expect(screen.getByText('phases · log scale')).not.toBeNull();
    expect(screen.getByText('duration')).not.toBeNull();
  });
});

describe('TimelineView run row', () => {
  it('draws the phases along the longest chain, with the time of each kind in its log cell', () => {
    renderTimeline(cascadeRun());

    const segments = [...runRow().querySelectorAll('[data-run-segment]')].map(
      segment => `${segment.getAttribute('data-task-index')} ${segment.getAttribute('data-phase')}`
    );
    expect(segments).toEqual(['0 enqueued', '0 running', '0 advancing', '1 running', '4 enqueued', '4 running']);
    expect([...runRow().querySelectorAll('[data-lane]')].map(lane => lane.getAttribute('data-lane'))).toEqual([
      'enqueued',
      'running',
      'advancing',
    ]);
    expect(runRow().textContent).toContain('50.0s');
  });

  it('names the run and its chain in the tooltip of the run row', () => {
    renderTimeline(cascadeRun());

    fireEvent.pointerMove(runRow());

    expect(screen.getByRole('tooltip').textContent).toContain('the phases along the longest chain: a → b → e');
  });

  it('lights the row of the node of a segment of the run row', () => {
    renderTimeline(cascadeRun());

    fireEvent.pointerMove(runRow().querySelector('[data-run-segment][data-task-index="1"]') as Element);

    expect(screen.getByRole('tooltip').textContent).toContain('b · task 1 · attempt 1');
    expect(rowOf('b').className).toContain('bg-surface-hover');
    expect(rowOf('c').className).not.toContain(' bg-surface-hover');
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
    expect(rowOf('load').textContent).toContain('1m 40.0s');
    expect(rowOf('load').textContent).not.toContain('so far');

    act(() => {
      vi.advanceTimersByTime(2_000);
    });

    expect(rowOf('load').textContent).toContain('1m 42.0s');
    expect(Number.parseFloat(open().style.width)).toBeGreaterThan(95);
    expect(fetch).not.toHaveBeenCalled();
  });

  it('runs no clock for a run with no open segment', () => {
    renderTimeline(workflowRun('COMPLETED', [workflowNode(0, 'extract', 'COMPLETED', EXACT_RETRIED_NODE)], CLOSED_RUN));

    expect(vi.getTimerCount()).toBe(0);
  });

  it('says that the run was paused at the time under the pointer', () => {
    renderTimeline(openRun());
    const track = trackOf(rowOf('extract'));
    // A cell of 1,000 px from x = 200: the pointer at 50 % is at 50 s, in
    // the open pause that began at 5 s.
    track.getBoundingClientRect = () =>
      ({ left: 200, top: 0, width: 1_000, height: 16, right: 1_200, bottom: 16, x: 200, y: 0 }) as DOMRect;

    fireEvent.pointerMove(track, { clientX: 700, clientY: 8 });
    expect(focusText('pause')).toContain('run paused');
    expect(focusText('pause')).toContain(`${clockText(instantMs(at(5)))} → open`);

    // At 0.5 % (0.5 s) no pause holds the pointer.
    fireEvent.pointerMove(track, { clientX: 205, clientY: 8 });
    expect(screen.getByRole('tooltip').querySelector('[data-focus="pause"]')).toBeNull();
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
    expect(screen.queryByRole('group', { name: /^run,/ })).toBeNull();
  });

  it('names the reasons of a partial run and the conflicts of an inconsistent run', () => {
    const inconsistent: RunTiming = nth(TIMING_EXAMPLES.run_timings, 3);

    renderTimeline(workflowRun('COMPLETED', NODES, CLOSED_RUN));
    expect(screen.getByText('Partial timing: claims not counted.')).not.toBeNull();
    cleanup();

    renderTimeline(workflowRun('COMPLETED', NODES, inconsistent));
    expect(screen.getByText('Inconsistent timing: node #2; pause record: Unexpected event WORKFLOW_RESUMED (#4).')).not.toBeNull();
  });

  it('shows no coverage banner for an exact run', () => {
    renderTimeline(openRun());

    expect(screen.queryByText(/Partial timing|Inconsistent timing|Timing unavailable\./)).toBeNull();
  });
});

describe('TimelineView tooltips', () => {
  it('names the segment under the pointer: its kind, its node and source, its time and its start → end', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(segmentOf(rowOf('extract'), 4));
    const tooltip = screen.getByRole('tooltip');

    expect(focusText('segment')).toContain('slot wait');
    expect(focusText('segment')).toContain('extract · task 1 · attempt 1');
    expect(focusText('segment')).toContain('2.00s');
    expect(focusText('segment')).toContain(`${clockText(instantMs(at(4)))} → ${clockText(instantMs(at(6)))}`);
    expect(rowOf('extract').getAttribute('aria-describedby')).toBe(tooltip.id);

    fireEvent.pointerLeave(rowOf('extract'));
    expect(screen.queryByRole('tooltip')).toBeNull();
  });

  it('names a bar of the log cell: its kind, its node and its total', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(rowOf('extract').querySelector('[data-lane="claimed"]') as Element);

    expect(focusText('lane')).toContain('slot wait');
    expect(focusText('lane')).toContain('extract');
    expect(focusText('lane')).toContain('3.00s');
  });

  it('gives each phase of the node as a number on its name, also the phases too short to draw', () => {
    renderTimeline(workflowRun('COMPLETED', [workflowNode(0, 'pick', 'COMPLETED', NORMAL_NODE)], CLOSED_RUN));

    fireEvent.pointerMove(rowOf('pick').firstElementChild as Element);
    const tooltip = screen.getByRole('tooltip');

    expect(tooltip.textContent).toContain('dependency wait5.00s');
    expect(tooltip.textContent).toContain('queue wait3.00ms');
    expect(tooltip.textContent).toContain('finalizing1.00ms');
    expect(tooltip.textContent).toContain('3.01s');
    // The pointer is on the name, not on a mark: no focus line.
    expect(tooltip.querySelector('[data-focus]')).toBeNull();
  });

  it('names the end marker: end of the node, its status and the time with milliseconds', () => {
    renderTimeline(openRun());
    const marker = rowOf('extract').querySelector<HTMLElement>('[data-endpoint]');
    if (marker === null) {
      throw new Error('the completed row has its end marker');
    }

    fireEvent.pointerMove(marker);

    expect(focusText('endpoint')).toContain('end of extract');
    expect(focusText('endpoint')).toContain('completed');
    expect(focusText('endpoint')).toContain(clockText(instantMs(at(12))));
    expect(marker.getAttribute('title')).toBeNull();
  });

  it('adds the error code of the task to the end of a failed node', async () => {
    vi.useRealTimers();
    const failed: WorkflowTaskDetail = {
      task_index: 1,
      node_id: 'b',
      task_name: 'tasks.b',
      node_status: 'FAILED',
      is_subworkflow: false,
      node_error: null,
      leaf: {
        task_id: 't',
        status: 'FAILED',
        error_code: 'PSP_UNAVAILABLE',
        failed_reason: null,
        retry_count: 0,
        max_retries: 0,
        enqueued_at: null,
        started_at: null,
        completed_at: null,
        failed_at: null,
        queue_s: null,
        exec_s: null,
        worker_hostname: null,
        good_until: null,
        trace_id: null,
      },
      attempts: [],
    };
    const fetchStub = vi.fn((_input: RequestInfo | URL) =>
      Promise.resolve(
        new Response(JSON.stringify(failed), { status: 200, headers: { 'Content-Type': 'application/json' } })
      )
    );
    vi.stubGlobal('fetch', fetchStub);
    renderTimeline(cascadeRun());
    const marker = rowOf('b').querySelector<HTMLElement>('[data-endpoint="FAILED"]');
    if (marker === null) {
      throw new Error('the failed row has its end marker');
    }

    fireEvent.pointerMove(marker);

    expect(await screen.findByText('error code PSP_UNAVAILABLE')).not.toBeNull();
    expect(String(nth(fetchStub.mock.calls, 0)[0])).toContain('/workflows/run-1/tasks/1');
  });

  it('reads no error code for a node that did not fail', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(trackOf(rowOf('extract')));

    expect(fetch).not.toHaveBeenCalled();
  });

  it('steps through the segments of a row with the arrow keys and moves between rows', () => {
    renderTimeline(openRun());
    const extract = rowOf('extract');
    extract.focus();

    fireEvent.keyDown(extract, { key: 'ArrowRight' });
    expect(focusText('segment')).toContain('dependency wait');
    fireEvent.keyDown(extract, { key: 'ArrowRight' });
    expect(focusText('segment')).toContain('ready');
    fireEvent.keyDown(extract, { key: 'ArrowLeft' });
    expect(focusText('segment')).toContain('dependency wait');
    fireEvent.keyDown(extract, { key: 'Escape' });
    expect(screen.queryByRole('tooltip')).toBeNull();

    fireEvent.keyDown(extract, { key: 'ArrowDown' });
    expect(document.activeElement).toBe(rowOf('load'));
  });

  it('shows an open segment as open, with its duration so far', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(segmentOf(rowOf('load'), 1));

    expect(focusText('segment')).toContain('worker lost');
    expect(focusText('segment')).toContain('open');
    expect(focusText('segment')).toContain('1m 35.0s so far');
  });
});

describe('TimelineView longest chain', () => {
  it('names the nodes of the longest chain, and links each to the next at its end', () => {
    const { container } = renderTimeline(cascadeRun());

    for (const id of ['a', 'b', 'e']) {
      expect(rowOf(id).getAttribute('aria-label')).toContain('on the longest chain');
    }
    for (const id of ['c', 'd']) {
      expect(rowOf(id).getAttribute('aria-label')).not.toContain('on the longest chain');
    }
    const links = [...container.querySelectorAll<HTMLElement>('[data-chain-link]')];
    expect(links.map(link => link.getAttribute('data-chain-link'))).toEqual(['0-1', '1-4']);
    // a ends at 3.5 s of the 50 s axis.
    expect(Number.parseFloat(nth(links, 0).style.left)).toBeCloseTo(7);
    expect(document.body.textContent).not.toMatch(/critical/i);
  });

  it('says what a link is, and lights the rows of its two nodes', () => {
    const { container } = renderTimeline(cascadeRun());

    fireEvent.pointerMove(container.querySelector('[data-chain-link="1-4"]') as Element);

    expect(focusText('chain_link')).toContain('longest chain');
    expect(focusText('chain_link')).toContain('b → e');
    expect(focusText('chain_link')).toContain('e starts when b ends');
    expect(rowOf('b').className).toContain('bg-surface-hover');
    expect(rowOf('e').className).toContain('bg-surface-hover');
  });
});

describe('TimelineView key', () => {
  it('names the kinds off the chain, dependency wait, the end marker, the chain and the two patterns', () => {
    renderTimeline(cascadeRun());

    const key = screen.getByRole('list', { name: 'Key' });
    // The kinds of the chain are in the run summary, with their totals.
    expect([...key.querySelectorAll('li[tabindex]')].map(item => item.textContent)).toEqual([
      'dependency wait',
      'end of a node',
      'longest chain',
      'wait',
      'work',
    ]);
  });

  it('says what the end marker is, in the words of the app', () => {
    renderTimeline(cascadeRun());

    fireEvent.pointerMove(document.querySelector('[data-key-mark="end"]') as Element);

    expect(screen.getByRole('tooltip').textContent).toContain('end of a node');
    expect(screen.getByRole('tooltip').textContent).toContain('It is a point in time, not a duration.');
  });

  it('adds the pause band to the key of a paused run', () => {
    renderTimeline(openRun());

    expect(document.querySelector('[data-key-mark="paused"]')).not.toBeNull();
  });

  it('has no key for a run with no timing', () => {
    renderTimeline(workflowRun('COMPLETED', NODES, UNAVAILABLE_RUN));

    expect(screen.queryByRole('list', { name: 'Key' })).toBeNull();
  });

  it('lights the marks of one kind from its key entry: the view dims the other kinds', () => {
    const onIsolate = vi.fn();
    const { unmount } = renderTimeline(cascadeRun(), { kind: 'all' }, onIsolate);

    fireEvent.pointerMove(document.querySelector('[data-key-phase="pending"]') as Element);
    expect(onIsolate).toHaveBeenLastCalledWith({ kind: 'phase', phase: 'pending' });
    unmount();

    renderTimeline(cascadeRun(), { kind: 'phase', phase: 'running' });
    const lane = (kind: string): string | undefined =>
      rowOf('a').querySelector<HTMLElement>(`[data-lane="${kind}"]`)?.style.opacity;
    expect(lane('running')).toBe('1');
    expect(lane('enqueued')).toBe('0.22');
  });
});

describe('TimelineView with no long-step mark', () => {
  it('marks no long step on a segment, a bar or a tooltip', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(segmentOf(rowOf('extract'), 2));

    expect(focusText('segment')).toContain('queue wait');
    expect(screen.getByRole('tooltip').textContent).not.toMatch(/concurrency limit|long step|usual cause/);
    expect(document.body.innerHTML).not.toContain('marked');
  });
});

describe('segmentDrawing', () => {
  const wide = { kind: 'measured', px: 1_000 } as const;

  it('draws a segment of one pixel or more', () => {
    // 3 s of 50 s on 1,000 px: 60 px.
    expect(segmentDrawing(6, wide)).toBe('drawn');
    expect(segmentDrawing(0.1, wide)).toBe('drawn');
  });

  it('does not draw a segment below one pixel, whatever its kind and its time: the log bars show it', () => {
    // 3 ms of 50 s on 1,000 px: 0.06 px; 800 ms of 10 min: 1.3 px is drawn.
    expect(segmentDrawing(0.006, wide)).toBe('not_drawn');
    expect(segmentDrawing(0.13, wide)).toBe('drawn');
  });

  it('draws every segment before the cell has a width', () => {
    expect(segmentDrawing(0.006, { kind: 'unmeasured' })).toBe('drawn');
  });
});
